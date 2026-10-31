use super::ModService;
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
    AgentMode, AgentPhase, AgentRole, Block, Content, FunctionCallOutput, Message, Role,
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

    let mut absorber = BlockAbsorb::new();
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
    let partial = Message {
        role: Role::User,
        content: rows.into_iter().flat_map(|row| row.payload).collect(),
        ..Message::default()
    };
    absorber.absorb(partial);
    let blocks = absorber.settle();
    let turn_id = storage.create_turn(topic_id, instance_id, None).await?;
    let message_id = storage.create_message(turn_id, Role::User, None).await?;
    let _block_ids = storage.create_blocks(message_id, &blocks).await?;
    storage.delete_pending_by_ids(&ids).await?;
    tx.commit().await?;
    // 1. 发送CORE_BLOCK_DELTA （delta block 无id）
    // 2. block_ids 发送CORE_BLOCK_COMMITED （这些是用于回放，保证整个流程和sse流程一致）
    // 3. （writer 日志记录机制）

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

pub async fn prepare(
    mod_service: &dyn ModService,
    _stroage: &Storage,
    topic_id: i64,
    _instance_id: i64,
) -> Result<Prepared> {
    let mod_prompts = mod_service.prompt_fragments(topic_id).await?;
    // TODO: 查出 prepare 数据
    todo!()
}

/// 一次模型调用
pub async fn exchange(
    prepared: &Prepared,
    body: Value,
    mut emit_delta: impl FnMut(&Block),
) -> Result<BlockAbsorb> {
    let mut absorber = BlockAbsorb::new();
    let chunks = stream_body(
        prepared.adapter.as_ref(),
        &prepared.model,
        &prepared.target,
        body,
    )
    .await?;

    let mut chunks = pin!(chunks);
    while let Some(chunk) = chunks.next().await {
        match chunk {
            Ok(Chunk::Done) => break,
            Ok(Chunk::Delta(delta)) => {
                for block in absorber.absorb(delta) {
                    emit_delta(block);
                }
            }
            Err(error) => {
                absorber.absorb(Message {
                    role: Role::Assistant,
                    content: vec![Content::new_text(error.to_string())],
                    ..Message::default()
                });
                break;
            }
        }
    }
    Ok(absorber)
}

pub async fn create_spawn_requests(
    storage: &Storage,
    _cx: &DispatchCx,
    topic: &Topic,
    instance_id: i64,
    actions: Vec<SpawnAgentAction>,
) -> Result<Vec<SpawnRequest<LlmDomain>>> {
    let written = storage
        .with_tx(move |storage| async move {
            let mut built: Vec<(SpawnRequest<LlmDomain>, i64, Vec<Block>, Vec<i64>)> =
                Vec::with_capacity(actions.len());
            for action in actions {
                let mode = action.data.mode;
                let task = action.data.task;
                let child_instance_id = storage
                    .create_agent_instance(
                        topic.id,
                        Some(instance_id),
                        AgentRole::Child,
                        Some(mode),
                        AgentPhase::Running,
                        None,
                    )
                    .await?;
                storage
                    .spawn_call_map_insert(instance_id, &action.call_id, child_instance_id)
                    .await?;

                let mut absorber = BlockAbsorb::new();
                absorber.absorb(Message {
                    role: Role::User,
                    content: vec![Content::new_text(task)],
                    ..Message::default()
                });
                let blocks = absorber.settle();
                let turn_id = storage
                    .create_turn(topic.id, child_instance_id, None)
                    .await?;
                let message_id = storage
                    .create_message(turn_id, Role::User, topic.model_id)
                    .await?;
                let block_ids = storage.create_blocks(message_id, &blocks).await?;
                let request = SpawnRequest {
                    id: child_instance_id,
                    spec: LlmSpec {
                        instance_id: child_instance_id,
                        message_id: Some(message_id),
                        pending_id: None,
                        turn_id: Some(turn_id),
                    },
                    detached: mode == AgentMode::Background,
                };
                built.push((request, message_id, blocks, block_ids));
            }
            Ok(built)
        })
        .await?;

    let mut spawn = Vec::with_capacity(written.len());
    for (request, _message_id, _blocks, _block_ids) in written {
        // 1. 每块一条 CORE_BLOCK_DELTA：块行已提交，内容一次给全，revision 不再增长
        // 2. 再发 CORE_BLOCK_COMMITTED，携带 block_ids，供回放拼回与 SSE 一致的状态
        // 3. 块的 finished 置位与 block.committed 日志行由 writer 在同一事务写
        spawn.push(request);
    }
    todo!("send one delta per block, then commit the blocks")
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
