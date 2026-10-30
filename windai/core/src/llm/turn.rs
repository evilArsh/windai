use super::domain::{LlmDomain, LlmSpec};
use super::tools::{SpawnAgentAction, ToolCatalog};
use crate::agent::tree::Phase;
use crate::agent::{SPAWN_AGENT_NAME, SpawnRequest};
use crate::error::{CoreError, Result};
use crate::llm::block::BlockAbsorb;
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use wind_ai::chat::{Chunk, stream_body};
use wind_ai::{ChatAdapter, ChatModel, Target};
use wind_event::{DispatchCx, Origin, Scope, Source};
use wind_rule::RuleSet;
use wind_storage::error::Result as StorageResult;
use wind_storage::{Storage, Topic};
use wind_wire::{
    AgentMode, AgentPhase, AgentRole, Block, Content, FunctionCallOutput, Message, Role, Usage,
};

struct ChildState {
    child_id: i64,
    mode: Option<AgentMode>,
    phase: Phase<LlmDomain>,
    reply: String,
}

pub struct Prepared {
    pub(crate) adapter: Box<dyn ChatAdapter>,
    pub(crate) model: ChatModel,
    pub(crate) model_id: i64,
    pub(crate) provider: String,
    pub(crate) target: Target,
    pub(crate) rules: RuleSet,
    pub(crate) prompt: String,
    pub(crate) catalog: Arc<ToolCatalog>,
}

#[derive(Debug, Clone, Copy)]
pub struct ComposedInput {
    pub turn_id: i64,
    pub message_id: i64,
}

/// 一轮操作的期限
const ROUND_BUDGET: Duration = Duration::from_secs(120);

pub fn round_cx(topic_id: i64) -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id },
        Source::core("llm"),
        ROUND_BUDGET,
    )
}

/// TODO: 查询 instance_id 所在的 topic
pub async fn topic_of(storage: &Storage, instance_id: i64) -> Result<Topic> {
    storage
        .get_topic_by_instance(instance_id)
        .await?
        .ok_or_else(|| CoreError::Denied(format!("instance {instance_id} is not in the tree")))
}

/// 将 pending 数据合并成一条 user 消息
pub async fn compose_pending(
    storage: &Storage,
    topic_id: i64,
    instance_id: i64,
    specs: &[LlmSpec],
) -> Result<Option<ComposedInput>> {
    let pids: Vec<i64> = specs.iter().filter_map(|spec| spec.pending_id).collect();
    if pids.is_empty() {
        return Ok(None);
    }

    let tx = storage.begin().await?;
    let storage = tx.storage();
    let rows: Vec<_> = storage
        .get_pending_inputs_by_ids(&pids)
        .await?
        .into_iter()
        .filter(|row| row.instance_id == instance_id)
        .collect();
    if rows.is_empty() {
        return Ok(None);
    }
    let ids: Vec<i64> = rows.iter().map(|row| row.id).collect();
    let _partial = Message {
        role: Role::User,
        content: rows.into_iter().flat_map(|row| row.payload).collect(),
        ..Message::default()
    };
    let turn_id = storage.create_turn(instance_id, topic_id, None).await?;
    let message_id = storage.create_message(turn_id, Role::User, None).await?;
    // TODO：
    // 1. 需要一个 absorb 将 partial 分解为多个无id的Block数组，排序方式为BlockKind枚举字段从上到下的固定顺序（决定数据库对每条数据id的分配）
    // 2. 批量插入这些块，并返回完整的Block实体（带id）
    // 3. 创建每个块的日志，payload为block id
    // 4. 删除pending数据
    // 当事务失败，所有数据都没有丢失
    storage.delete_pending_by_ids(&ids).await?;
    tx.commit().await?;
    // TODO:
    // 5. 发送BlockOpen事件
    // 6. 发送BlockGrow事件
    // 7. 发送BlockCommit事件 （这些是用于回放，保证整个流程和sse流程一致）
    // 8. 发送日志事件，注意日志已经有id了（Work需要对已落库的日志支持？）
    // 注意： absorb 累加器是否要全局？这种情况下是没有blockid的
    // sse流会先开块再进入absorb累加器，是有block id的

    Ok(Some(ComposedInput {
        turn_id,
        message_id,
    }))
}

/// agent 被唤醒时，将 agent tool_call 的执行结果回补
pub async fn report_spawns(
    storage: &Storage,
    topic_id: i64,
    instance_id: i64,
) -> Result<Option<i64>> {
    let spawn_turn = storage
        .with_tx(async move |storage| {
            let unpaired: Vec<_> = storage
                .tool_calls_without_results(instance_id)
                .await?
                .into_iter()
                .filter(|call| {
                    call.data
                        .as_ref()
                        .and_then(|data| data.get("name"))
                        .and_then(Value::as_str)
                        == Some(SPAWN_AGENT_NAME)
                })
                .collect();
            if unpaired.is_empty() {
                return Ok(None);
            }

            let call_ids: Vec<&str> = unpaired.iter().map(|call| call.call_id.as_str()).collect();
            let mut states = child_states(&storage, topic_id, &call_ids).await?;
            // (turn, message) -> 该消息下待回补的结果
            let mut batches: HashMap<(i64, i64), Vec<FunctionCallOutput>> = HashMap::new();
            for call in unpaired {
                let state = states.remove(&call.call_id);
                let output = spawn_output(call.call_id, state);
                batches
                    .entry((call.turn_id, call.message_id))
                    .or_default()
                    .push(output);
            }
            write_tool_results(&storage, &batches).await?;
            Ok(batches.keys().map(|(turn_id, _)| *turn_id).max())
        })
        .await?;
    Ok(spawn_turn)
}

pub async fn prepare(_stroage: &Storage, _topic_id: i64, _instance_id: i64) -> Result<Prepared> {
    todo!()
}

/// 一次模型调用
pub async fn exchange(
    prepared: &Prepared,
    body: Value,
    mut emit_delta: impl FnMut(Block),
) -> Result<BlockAbsorb> {
    let mut absorber = BlockAbsorb::new();
    let chunks = stream_body(
        prepared.adapter.as_ref(),
        &prepared.model,
        &prepared.target,
        body,
    )
    .await?;

    let mut assembled = Message {
        role: Role::Assistant,
        ..Message::default()
    };
    let mut usage = usage_of(0, 0);
    let mut chunks = pin!(chunks);
    while let Some(chunk) = chunks.next().await {
        match chunk {
            Ok(Chunk::Done) => todo!(),
            Ok(Chunk::Delta(delta)) => {
                usage = add_usage(usage, &delta);
                for block in absorber.absorb(delta) {
                    emit_delta(block);
                }
            }
            Err(error) => {
                absorber.absorb(Message {
                    role: Role::Assistant,
                    content: vec![Content::new_text(error.to_string())],
                    reasoning_content: None,
                    created_at: 0,
                    input_tokens: 0,
                    output_tokens: 0,
                    tool_calls: None,
                });
                break;
            }
        }
    }
    assembled.input_tokens = usage.input_tokens;
    assembled.output_tokens = usage.output_tokens;
    // Ok(intercept_response(runner, cx, prepared.model_id, assembled, usage).await)
    Ok(absorber)
}

pub async fn create_spawn_requests(
    storage: &Storage,
    cx: &DispatchCx,
    topic_id: i64,
    instance_id: i64,
    actions: Vec<SpawnAgentAction>,
) -> Result<Vec<SpawnRequest<LlmDomain>>> {
    let phase = AgentPhase::Running;
    let written = storage
        .with_tx(|storage| async move {
            let mut built = Vec::with_capacity(actions.len());
            for action in actions {
                let child_id = storage
                    .create_child_agent_instance(
                        topic_id,
                        Some(instance_id),
                        Some(action.data.mode),
                    )
                    .await?;
                // storage
                //     .spawn_call_insert(instance_id, &action.call_id, child_id)
                //     .await?;
                // let (turn_id, blocks) =
                //     write_task(runner, storage, topic_id, child_id, &action.data.task).await?;
                // let request = SpawnRequest {
                //     id: child_id,
                //     spec: LlmSpec {
                //         instance_id: child_id,
                //         message_id: None,
                //         pending_id: None,
                //         turn_id: Some(turn_id),
                //     },
                //     detached: action.data.mode.as_str() == AgentMode::Background.as_ref(),
                // };
                // built.push((request, blocks));
            }
            Ok(built)
        })
        .await?;
    // let mut spawn = Vec::with_capacity(written.len());
    // let mut blocks = Vec::new();
    // for (request, task_blocks) in written {
    //     spawn.push(request);
    //     blocks.extend(task_blocks);
    // }
    // runner.writer.announce_atomic(cx, &blocks).await;
    // Ok(spawn)
    todo!()
}

// TODO: 待补，按调用标识一次查出子实例的状态与收尾回复
async fn child_states(
    _storage: &Storage,
    _topic_id: i64,
    _call_ids: &[&str],
) -> StorageResult<HashMap<String, ChildState>> {
    todo!()
}

// TODO: 待补，每批落一条 tool 消息 （批量写入）
async fn write_tool_results(
    _storage: &Storage,
    _batches: &HashMap<(i64, i64), Vec<FunctionCallOutput>>,
) -> StorageResult<()> {
    todo!()
}

fn spawn_output(call_id: String, state: Option<ChildState>) -> FunctionCallOutput {
    let content = match state {
        None => json!({ "error": format!("spawn call {call_id} has no child") }),
        Some(state) if state.mode == Some(AgentMode::Background) => {
            json!({ "instance_id": state.child_id, "phase": "running" })
        }
        Some(state) => match &state.phase {
            Phase::Done(_) => Value::String(state.reply.clone()),
            Phase::Suspended(wait) => {
                json!({ "instance_id": state.child_id, "phase": "suspended", "phase_payload": wait })
            }
            Phase::Failed(_) | Phase::Cancelled(_) | Phase::Aborted => {
                json!({ "error": "child task did not complete" })
            }
            Phase::Running | Phase::WaitingChildren | Phase::Cancelling(_) => {
                json!({ "instance_id": state.child_id, "phase": "running" })
            }
        },
    };
    FunctionCallOutput {
        id: call_id,
        content,
    }
}

fn usage_of(input_tokens: i32, output_tokens: i32) -> Usage {
    Usage {
        input_tokens,
        output_tokens,
    }
}

fn add_usage(usage: Usage, delta: &Message) -> Usage {
    usage_of(
        usage.input_tokens.max(delta.input_tokens),
        usage.output_tokens.max(delta.output_tokens),
    )
}
