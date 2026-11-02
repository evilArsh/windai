use super::domain::{LlmDomain, LlmFailure, LlmOutcome, LlmSpec, Suspended};
use super::events::{
    BlockCommitted, BlockDelta, CORE_BLOCK_COMMITTED, CORE_BLOCK_DELTA, CORE_MESSAGE_SEAL,
    CORE_PROMPT_COLLECT, MessageSeal, PromptCollect,
};
use super::rule::compile_rules;
use super::tools::{SpawnAgentAction, ToolCatalog};
use super::{McpService, ModService};
use crate::agent::SPAWN_AGENT_NAME;
use crate::agent::SpawnRequest;
use crate::agent::tree::Phase;
use crate::agent::tree::phase::CancelOrigin;
use crate::error::{CoreError, Result};
use crate::llm::block::BlockAbsorb;
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use wind_ai::chat::{Chunk, stream_body};
use wind_ai::{ChatAdapter, ChatModel, Target, adapter_of};
use wind_event::{Committed, DispatchCx, Dispatcher, Envelope, Origin, Scope, Source};
use wind_rule::RuleSet;
use wind_storage::error::Result as StorageResult;
use wind_storage::vendors::config::ModelConfig;
use wind_storage::{ChildStateRow, Storage, Topic};
use wind_wire::{
    AgentMode, AgentPhase, AgentRole, Block, Content, FunctionCallOutput, Message, Role, Usage,
};

struct ChildState {
    child_id: i64,
    mode: Option<AgentMode>,
    phase: std::result::Result<Phase<LlmDomain>, String>,
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

pub struct WrittenToolResults {
    pub message_id: i64,
    pub blocks: Vec<Block>,
    /// 已提交的块事件
    pub committed: Vec<Committed>,
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

/// 查询 instance_id 所在的 topic
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
    bus: &Dispatcher,
    topic_id: i64,
    instance_id: i64,
) -> Result<Option<i64>> {
    let cx = round_cx(topic_id, instance_id);
    let log_cx = cx.clone();
    let (written, spawn_turn) = storage
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
                return Ok((Vec::new(), None));
            }

            let call_ids: Vec<&str> = unpaired.iter().map(|call| call.call_id.as_str()).collect();
            let mut states = child_states(&tx, instance_id, &call_ids).await?;
            // (turn, message) -> 该消息下待回补的结果
            let mut batches: BTreeMap<(i64, i64), Vec<FunctionCallOutput>> = BTreeMap::new();
            for call in unpaired {
                let state = states.remove(&call.call_id);
                let output = spawn_output(call.call_id, state);
                batches
                    .entry((call.turn_id, call.message_id))
                    .or_default()
                    .push(output);
            }
            let spawn_turn = batches.keys().map(|(turn_id, _)| *turn_id).max();
            let written = write_tool_results(&tx, &log_cx, &batches).await?;
            Ok((written, spawn_turn))
        })
        .await?;

    for written in written {
        emit_block_deltas(bus, &cx, written.message_id, &written.blocks);
        for event in written.committed {
            bus.notify_committed(event);
        }
    }
    Ok(spawn_turn)
}

pub async fn prepare(
    storage: &Storage,
    bus: &Dispatcher,
    cx: &DispatchCx,
    mod_service: &dyn ModService,
    mcp_service: &dyn McpService,
    topic: &Topic,
    instance_id: i64,
) -> Result<Prepared> {
    let model_id = topic
        .model_id
        .ok_or_else(|| CoreError::Denied(format!("topic {} names no model", topic.id)))?;
    let config = storage
        .get_model_config(model_id)
        .await?
        .ok_or_else(|| CoreError::Denied(format!("model {model_id} is unknown")))?;
    let ModelConfig {
        model,
        provider,
        api_key,
    } = config;
    let provider = provider
        .ok_or_else(|| CoreError::Denied(format!("provider {} is unknown", model.provider_id)))?;
    let rules = compile_rules(&storage.list_rules(provider.id, model.adapter).await?)?;
    let fragments = mod_service.prompt_fragments(topic.id).await?;
    let prompt = bus
        .decide(
            &CORE_PROMPT_COLLECT,
            cx,
            PromptCollect {
                topic_id: topic.id,
                instance_id,
                fragments,
            },
        )
        .await
        .fragments
        .join("\n\n");
    let from_mods = mod_service.agent_tools(topic.id).await?;
    let from_mcp = mcp_service.declarations(cx, topic.id).await?;
    let catalog = Arc::new(ToolCatalog::assemble(from_mods, from_mcp));
    Ok(Prepared {
        adapter: adapter_of(model.adapter),
        model: ChatModel {
            name: model.name,
            adapter: model.adapter,
            endpoint: model.endpoint,
            config: model.config,
        },
        model_id,
        provider: provider.name,
        target: Target {
            base_url: provider.base_url,
            api_key,
        },
        rules,
        prompt,
        catalog,
    })
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

async fn child_states(
    storage: &Storage,
    parent_id: i64,
    call_ids: &[&str],
) -> StorageResult<HashMap<String, ChildState>> {
    let rows = storage.list_child_states(parent_id, call_ids).await?;
    let mut states: HashMap<String, ChildState> = HashMap::new();
    for row in rows {
        let ChildStateRow {
            call_id,
            child_id,
            mode,
            phase,
            phase_payload,
            reply_part,
        } = row;
        let state = states.entry(call_id).or_insert_with(|| ChildState {
            child_id,
            mode: mode.as_deref().and_then(|text| text.parse().ok()),
            phase: decode_phase(&phase, phase_payload.as_deref()),
            reply: String::new(),
        });
        if let Some(part) = reply_part {
            state.reply.push_str(&part);
        }
    }
    Ok(states)
}

pub(crate) async fn write_tool_results(
    tx: &Storage,
    cx: &DispatchCx,
    batches: &BTreeMap<(i64, i64), Vec<FunctionCallOutput>>,
) -> StorageResult<Vec<WrittenToolResults>> {
    let mut written = Vec::with_capacity(batches.len());
    for ((turn_id, _message_id), results) in batches {
        if results.is_empty() {
            continue;
        }
        let message_id = tx.create_message(*turn_id, Role::Tool, None).await?;
        let mut absorber = BlockAbsorb::new();
        absorber.absorb(Message {
            role: Role::Tool,
            content: results
                .iter()
                .map(|result| Content::new_function_call(result.id.clone(), result.content.clone()))
                .collect(),
            ..Message::default()
        });
        let blocks = absorber.settle();
        let block_ids = tx.create_blocks(message_id, &blocks).await?;
        let committed = log_blocks(tx, cx, message_id, &blocks, &block_ids).await?;
        written.push(WrittenToolResults {
            message_id,
            blocks,
            committed,
        });
    }
    Ok(written)
}

fn decode_phase(
    name: &str,
    payload: Option<&str>,
) -> std::result::Result<Phase<LlmDomain>, String> {
    let phase: AgentPhase = name
        .parse()
        .map_err(|_| format!("unknown phase name: {name}"))?;
    let payload =
        match payload {
            Some(text) => Some(serde_json::from_str::<Value>(text).map_err(|error| {
                format!("payload for phase {name} could not be decoded: {error}")
            })?),
            None => None,
        };
    if !phase.takes_payload() {
        if payload.is_some() {
            return Err(format!("phase {name} does not take a payload"));
        }
        return match phase {
            AgentPhase::Running => Ok(Phase::Running),
            AgentPhase::WaitingChildren => Ok(Phase::WaitingChildren),
            AgentPhase::Aborted => Ok(Phase::Aborted),
            _ => Err(format!("phase {name} does not take a payload")),
        };
    }
    let payload = payload.ok_or_else(|| format!("phase {name} requires a payload"))?;
    let decoded = match phase {
        AgentPhase::Suspended => serde_json::from_value::<Suspended>(payload).map(Phase::Suspended),
        AgentPhase::Cancelling => {
            serde_json::from_value::<CancelOrigin>(payload).map(Phase::Cancelling)
        }
        AgentPhase::Done => serde_json::from_value::<LlmOutcome>(payload).map(Phase::Done),
        AgentPhase::Failed => serde_json::from_value::<LlmFailure>(payload).map(Phase::Failed),
        AgentPhase::Cancelled => {
            serde_json::from_value::<CancelOrigin>(payload).map(Phase::Cancelled)
        }
        AgentPhase::Running | AgentPhase::WaitingChildren | AgentPhase::Aborted => {
            return Err(format!("phase {name} does not take a payload"));
        }
    };
    decoded.map_err(|error| format!("payload for phase {name} could not be decoded: {error}"))
}

fn spawn_output(call_id: String, state: Option<ChildState>) -> FunctionCallOutput {
    let content = match state {
        None => json!({ "error": format!("spawn call {call_id} has no child") }),
        Some(state) if state.mode == Some(AgentMode::Background) => {
            json!({ "instance_id": state.child_id, "phase": "running" })
        }
        Some(state) => match &state.phase {
            Ok(phase) => match phase {
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
            Err(detail) => {
                json!({ "error": format!("child {} has an unreadable phase: {detail}", state.child_id) })
            }
        },
    };
    FunctionCallOutput {
        id: call_id,
        content,
    }
}
