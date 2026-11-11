use super::domain::{LlmDomain, LlmSpec};
use super::events::{CORE_PROMPT_COLLECT, PromptCollect};
use super::rule::compile_rules;
use super::tools::{SpawnAgentAction, ToolCatalog};
use super::{McpService, ModService};
use crate::agent::SPAWN_AGENT_NAME;
use crate::agent::SpawnRequest;
use crate::agent::tree::Phase;
use crate::error::{CoreError, Result};
use crate::llm::block::BlockAbsorb;
use futures::{Stream, StreamExt, stream};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use wind_ai::chat::{Chunk, ProviderError, once_body, stream_body};
use wind_ai::{ChatAdapter, ChatModel, Target, adapter_of};
use wind_event::{DispatchCx, Dispatcher, Origin, Scope, Source};
use wind_rule::RuleSet;
use wind_storage::error::Result as StorageResult;
use wind_storage::vendors::config::ModelConfig;
use wind_storage::{ChildStateRow, Storage, Topic};
use wind_wire::{AgentMode, AgentRole, Block, Content, FunctionCallOutput, Message, Role};

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

pub struct WrittenBlocks {
    pub message_id: i64,
    pub blocks: Vec<Block>,
    pub block_ids: Vec<i64>,
}

#[derive(Debug, Clone, Copy)]
pub struct ComposedInput {
    pub turn_id: i64,
    #[allow(dead_code)]
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
    topic_id: i64,
    instance_id: i64,
    specs: &[LlmSpec],
) -> Result<(Option<WrittenBlocks>, Option<ComposedInput>)> {
    let pids: Vec<i64> = specs.iter().filter_map(|spec| spec.pending_id).collect();
    if pids.is_empty() {
        return Ok((None, None));
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
        return Ok((None, None));
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
    tx.delete_pending_by_ids(&ids).await?;
    storage_tx.commit().await?;

    Ok((
        Some(WrittenBlocks {
            message_id,
            block_ids,
            blocks,
        }),
        Some(ComposedInput {
            turn_id,
            message_id,
        }),
    ))
}

/// agent 被唤醒时，将 agent tool_call 的执行结果回补。
///
/// 子 agent 实例的结果属于父 agent tool_call 结果
pub async fn report_spawns(
    storage: &Storage,
    instance_id: i64,
) -> Result<(Vec<WrittenBlocks>, Option<i64>)> {
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
            let written = write_tool_results(&tx, &batches).await?;
            Ok((written, spawn_turn))
        })
        .await?;
    Ok((written, spawn_turn))
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
    let fragments = mod_service
        .prompt_fragments(topic.id)
        .await
        .map_err(CoreError::Internal)?;
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
    let from_mcp = mcp_service
        .list_tools(topic.id)
        .await
        .map_err(CoreError::Internal)?;
    let catalog = Arc::new(ToolCatalog::assemble(from_mcp)?);
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
    let streaming = prepared
        .model
        .config
        .as_ref()
        .and_then(|config| config.get("stream"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut chunks: Pin<
        Box<dyn Stream<Item = std::result::Result<Chunk, ProviderError>> + Send + '_>,
    > = if streaming {
        Box::pin(
            stream_body(
                prepared.adapter.as_ref(),
                &prepared.model,
                &prepared.target,
                body,
            )
            .await?,
        )
    } else {
        let message = once_body(
            prepared.adapter.as_ref(),
            &prepared.model,
            &prepared.target,
            body,
        )
        .await?;
        Box::pin(stream::iter([Ok(Chunk::Delta(message)), Ok(Chunk::Done)]))
    };

    while let Some(chunk) = chunks.next().await {
        match chunk {
            Ok(Chunk::Done) => {
                absorber.set_finish_reason("stop");
                break;
            }
            Ok(Chunk::Delta(delta)) => {
                for block in absorber.absorb(delta) {
                    emit_delta(&block);
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
    topic: &Topic,
    instance_id: i64,
    actions: Vec<SpawnAgentAction>,
) -> Result<Vec<(SpawnRequest<LlmDomain>, WrittenBlocks)>> {
    let writtens = storage
        .with_tx(move |tx| async move {
            let mut built: Vec<(SpawnRequest<LlmDomain>, WrittenBlocks)> =
                Vec::with_capacity(actions.len());
            for action in actions {
                let mode = action.data.mode;
                let task = action.data.task;
                let (phase, phase_payload) = Phase::<LlmDomain>::Running.columns()?;
                let child_instance_id = tx
                    .create_agent_instance(
                        topic.id,
                        Some(instance_id),
                        AgentRole::Child,
                        Some(mode),
                        &phase,
                        phase_payload,
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
                built.push((
                    request,
                    WrittenBlocks {
                        message_id,
                        block_ids,
                        blocks,
                    },
                ));
            }
            Ok(built)
        })
        .await?;
    Ok(writtens)
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
            phase: Phase::<LlmDomain>::from_columns(&phase, phase_payload.as_deref())
                .map_err(|error| error.to_string()),
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
    batches: &BTreeMap<(i64, i64), Vec<FunctionCallOutput>>,
) -> StorageResult<Vec<WrittenBlocks>> {
    let mut written: Vec<WrittenBlocks> = Vec::with_capacity(batches.len());
    for ((turn_id, _), results) in batches {
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
        written.push(WrittenBlocks {
            message_id,
            block_ids,
            blocks,
        });
    }
    Ok(written)
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
                Phase::Running | Phase::WaitingChildren => {
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
