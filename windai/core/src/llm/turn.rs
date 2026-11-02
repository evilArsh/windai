use super::ModService;
use super::domain::{LlmDomain, LlmSpec};
use super::events::{
    BlockCommitted, BlockDelta, CORE_BLOCK_COMMITTED, CORE_BLOCK_DELTA, CORE_MESSAGE_SEAL,
    MessageSeal,
};
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
use wind_event::{Committed, DispatchCx, Dispatcher, Envelope, Origin, Scope, Source};
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

pub fn round_cx(topic_id: i64, instance_id: i64) -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic {
            topic_id,
            instance_id,
        },
        Source::core("llm"),
        ROUND_BUDGET,
    )
}

/// 为已落库的块写提交日志
pub(crate) async fn log_blocks(
    tx: &Storage,
    cx: &DispatchCx,
    message_id: i64,
    blocks: &[Block],
    block_ids: &[i64],
) -> StorageResult<Vec<Committed>> {
    let mut events = Vec::with_capacity(blocks.len());
    for (block, block_id) in blocks.iter().zip(block_ids.iter().copied()) {
        let envelope = Envelope::of(
            &CORE_BLOCK_COMMITTED,
            cx,
            json!(BlockCommitted {
                message_id,
                seq: block.seq(),
                block_id,
            }),
        );
        let log_id = tx.create_event_log(&envelope).await?;
        events.push(Committed { log_id, envelope });
    }
    Ok(events)
}

/// 为一次模型往返写结束标记
pub(crate) async fn log_message_seal(
    tx: &Storage,
    cx: &DispatchCx,
    message_id: i64,
    finish_reason: &str,
    usage: Usage,
) -> StorageResult<Committed> {
    let envelope = Envelope::of(
        &CORE_MESSAGE_SEAL,
        cx,
        json!(MessageSeal {
            message_id,
            finish_reason: finish_reason.to_string(),
            usage,
        }),
    );
    let log_id = tx.create_event_log(&envelope).await?;
    Ok(Committed { log_id, envelope })
}

/// 把完整块作为实时增量发给订阅者
pub(crate) fn emit_block_deltas(
    bus: &Dispatcher,
    cx: &DispatchCx,
    message_id: i64,
    blocks: &[Block],
) {
    for block in blocks {
        bus.notify(
            &CORE_BLOCK_DELTA,
            cx,
            BlockDelta {
                message_id,
                delta: block.clone(),
            },
        );
    }
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
    bus: &Dispatcher,
    cx: &DispatchCx,
    topic_id: i64,
    instance_id: i64,
    specs: &[LlmSpec],
) -> Result<Option<ComposedInput>> {
    let pids: Vec<i64> = specs.iter().filter_map(|spec| spec.pending_id).collect();
    if pids.is_empty() {
        return Ok(None);
    }

    let mut absorber = BlockAbsorb::new();
    let storage_tx = storage.begin().await?;
    let tx = storage_tx.storage();
    let rows: Vec<_> = tx
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
    let turn_id = tx.create_turn(topic_id, instance_id, None).await?;
    let message_id = tx.create_message(turn_id, Role::User, None).await?;
    let block_ids = tx.create_blocks(message_id, &blocks).await?;
    let committed = log_blocks(&tx, cx, message_id, &blocks, &block_ids).await?;
    tx.delete_pending_by_ids(&ids).await?;
    storage_tx.commit().await?;

    emit_block_deltas(bus, cx, message_id, &blocks);
    for event in committed {
        bus.notify_committed(event);
    }

    Ok(Some(ComposedInput {
        turn_id,
        message_id,
    }))
}

/// agent 被唤醒时，将 agent tool_call 的执行结果回补。
///
/// 子 agent 实例的结果属于父 agent tool_call 结果
pub async fn report_spawns(
    storage: &Storage,
    topic_id: i64,
    instance_id: i64,
) -> Result<Option<i64>> {
    let spawn_turn = storage
        .with_tx(async move |tx| {
            let unpaired: Vec<_> = tx
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
            let mut states = child_states(&tx, topic_id, &call_ids).await?;
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
            write_tool_results(&tx, &batches).await?;
            Ok(batches.keys().map(|(turn_id, _)| *turn_id).max())
        })
        .await?;
    Ok(spawn_turn)
}

pub async fn prepare(
    _stroage: &Storage,
    mod_service: &dyn ModService,
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
            Ok(Chunk::Done) => {
                absorber.set_finish_reason("stop");
                break;
            }
            Ok(Chunk::Delta(delta)) => {
                for block in absorber.absorb(delta) {
                    emit_delta(block);
                }
            }
            Err(error) => {
                absorber.set_finish_reason(&error);
                absorber.absorb(Message {
                    role: Role::Assistant,
                    content: vec![Content::new_text(error.to_string())],
                    ..Message::default()
                });
                break;
            }
        }
    }
    if absorber.finish_reason().is_empty() {
        absorber.set_finish_reason("stop");
    }
    Ok(absorber)
}

/// 创建 spawn 子 agent 请求
pub async fn create_spawn_requests(
    storage: &Storage,
    bus: &Dispatcher,
    _cx: &DispatchCx,
    topic: &Topic,
    instance_id: i64,
    actions: Vec<SpawnAgentAction>,
) -> Result<Vec<SpawnRequest<LlmDomain>>> {
    let written = storage
        .with_tx(move |tx| async move {
            let mut built: Vec<(SpawnRequest<LlmDomain>, i64, Vec<Block>, Vec<Committed>)> =
                Vec::with_capacity(actions.len());
            for action in actions {
                let mode = action.data.mode;
                let task = action.data.task;
                let child_instance_id = tx
                    .create_agent_instance(
                        topic.id,
                        Some(instance_id),
                        AgentRole::Child,
                        Some(mode),
                        AgentPhase::Running,
                        None,
                    )
                    .await?;
                tx.create_spawn_call_map(instance_id, &action.call_id, child_instance_id)
                    .await?;

                let mut absorber = BlockAbsorb::new();
                absorber.absorb(Message {
                    role: Role::User,
                    content: vec![Content::new_text(task)],
                    ..Message::default()
                });
                let blocks = absorber.settle();
                let turn_id = tx.create_turn(topic.id, child_instance_id, None).await?;
                let message_id = tx
                    .create_message(turn_id, Role::User, topic.model_id)
                    .await?;
                let block_ids = tx.create_blocks(message_id, &blocks).await?;
                let child_cx = round_cx(topic.id, child_instance_id);
                let committed = log_blocks(&tx, &child_cx, message_id, &blocks, &block_ids).await?;
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
                built.push((request, message_id, blocks, committed));
            }
            Ok(built)
        })
        .await?;

    let mut spawn = Vec::with_capacity(written.len());
    for (request, message_id, blocks, committed) in written {
        let child_cx = round_cx(topic.id, request.id);
        emit_block_deltas(bus, &child_cx, message_id, &blocks);
        for event in committed {
            bus.notify_committed(event);
        }
        spawn.push(request);
    }
    Ok(spawn)
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
