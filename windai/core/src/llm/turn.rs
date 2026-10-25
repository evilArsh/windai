//! 一轮的驱动
//!
//! ```text
//! 组装上下文
//! 发出请求
//! 把分片收成块并定稿
//! 执行这批调用
//! ```

use super::events::{CORE_LLM_REQUEST, CORE_LLM_RESPONSE, LlmRequest, LlmRequestBody, LlmResponse};
use super::execute::{ExecutedPlan, execute_plan};
use super::policy::{CallPlan, decide_calls};
use super::ports::FunctionCallOutput;
use super::prompt::PromptBuilder;
use super::rules::{apply_rules, compile_rules, context_of, intercept};
use super::tools::ToolCatalog;
use crate::agent::behavior::SpawnRequest;
use crate::agent::domain::{AgentDomain, AgentSpec};
use crate::agent::helper::spawn_response;
use crate::agent::phase_codec;
use crate::agent::runtime::CachedCatalog;
pub use crate::agent::runtime::{AgentRunner, DEFAULT_MAX_ROUNDS};
use crate::agent::tool::{SPAWN_AGENT_NAME, SpawnAgentAction};
use crate::agent::tree::phase::Phase;
use crate::conversation::events::Usage;
use crate::conversation::writer::block_create;
use crate::error::{CoreError, Result};
use crate::models::{
    AgentInstance, AgentInstanceCreate, AgentMode, AgentPhase, AgentRole, AllowPolicy, BlockRow,
    MessageCreate, Model, PendingKind, TurnCreate,
};
use crate::storage::Storage;
use crate::usecase::error::UseCaseError;
use futures::StreamExt;
use std::collections::{BTreeMap, HashMap};
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use wind_ai::adapter::ChatAdapter;
use wind_ai::adapter_of;
use wind_ai::chat::{Chunk, stream_body};
use wind_ai::model::{AdapterType, ChatModel, Target};
use wind_event::context::DispatchCx;
use wind_event::envelope::{Origin, Scope, Source};
use wind_rule::RuleSet;
use wind_stream::block::{Block, BlockKind};
use wind_stream::id::{BlockId, MessageId};
use wind_wire::message::{Content, Message, Role};
use wind_wire::tool::FunctionCall;

/// 一轮操作的期限
const ROUND_BUDGET: Duration = Duration::from_secs(120);

/// 工具清单缓存的有效窗口
const CATALOG_WINDOW: Duration = Duration::from_secs(1);

pub(crate) fn round_cx(topic_id: i64) -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id },
        Source::core("llm"),
        ROUND_BUDGET,
    )
}

pub(crate) async fn topic_of(storage: &Storage, instance_id: i64) -> Result<i64> {
    storage
        .agent_get(instance_id)
        .await?
        .map(|instance| instance.topic_id)
        .ok_or_else(|| CoreError::Denied(format!("instance {instance_id} is not in the tree")))
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Locus {
    pub(crate) message_id: i64,
    pub(crate) turn_id: i64,
}

pub(crate) async fn assemble(runner: &AgentRunner, instance_id: i64) -> Result<Vec<Message>> {
    runner.context(instance_id).await
}

fn composes(kind: PendingKind) -> bool {
    matches!(kind, PendingKind::User | PendingKind::BackgroundResult)
}

#[derive(Debug, Clone, Copy)]
pub struct ComposedInput {
    pub turn_id: i64,
    pub message_id: i64,
}

pub async fn compose_pending(
    runner: &AgentRunner,
    cx: &DispatchCx,
    topic_id: i64,
    instance_id: i64,
    specs: &[AgentSpec],
) -> Result<Option<ComposedInput>> {
    let wanted: Vec<i64> = specs.iter().filter_map(|spec| spec.pending_id).collect();
    if wanted.is_empty() {
        return Ok(None);
    }
    let rows: Vec<_> = runner
        .storage
        .pending_list_by_ids(&wanted)
        .await?
        .into_iter()
        .filter(|row| row.instance_id == instance_id && composes(row.kind))
        .collect();
    if rows.is_empty() {
        return Ok(None);
    }
    let ids: Vec<i64> = rows.iter().map(|row| row.id).collect();
    let content: Vec<Content> = rows
        .iter()
        .flat_map(|row| row.payload.iter().cloned())
        .collect();
    let partial = Message {
        role: Role::User,
        content,
        ..Message::default()
    };

    // 先建立 turn 与 user message，拿到 message_id
    let (turn_id, message_id) = {
        let tx = runner.storage.begin().await?;
        let storage = tx.storage();
        let turn_id = open_turn(&storage, instance_id, topic_id, None).await?;
        let message_id = new_message(&storage, turn_id, Role::User, None).await?;
        tx.commit().await?;
        (turn_id, message_id)
    };

    // 事务外决定块内容，只改内存
    let touched = runner
        .writer
        .absorb(cx, topic_id, MessageId(message_id), &partial)
        .await;
    let mut written = Vec::with_capacity(touched.len());
    for id in touched {
        if let Some(block) = runner.writer.settle(id) {
            written.push(block);
        }
    }

    // 认领 pending 并写入内容，两者同进同退
    let tx = runner.storage.begin().await?;
    let storage = tx.storage();
    let consumed = storage.pending_delete(&ids).await?;
    if consumed != ids.len() as u64 {
        tx.rollback().await?;
        return Ok(None);
    }
    for block in &written {
        runner.writer.persist(&storage, block).await?;
    }
    storage.turn_set_from(turn_id, message_id).await?;
    tx.commit().await?;

    for block in &written {
        runner.writer.commit_memory(block);
    }
    for block in &written {
        runner.writer.announce(cx, block).await;
    }

    Ok(Some(ComposedInput {
        turn_id,
        message_id,
    }))
}

/// 父 agent 被唤醒时，将未投递的子 agent tool_call 结果回补
pub(crate) async fn report_spawns(runner: &AgentRunner, instance_id: i64) -> Result<Option<i64>> {
    let unpaired: Vec<(i64, i64, String)> = runner
        .storage
        .tool_calls_without_results(instance_id)
        .await?
        .into_iter()
        .filter(|call| stored_call_name(call.data.as_ref()) == Some(SPAWN_AGENT_NAME))
        .map(|call| (call.message_id, call.turn_id, call.call_id))
        .collect();
    if unpaired.is_empty() {
        return Ok(None);
    }
    let message_turns: HashMap<i64, i64> = unpaired
        .iter()
        .map(|(message_id, turn_id, _)| (*message_id, *turn_id))
        .collect();

    let topic_id = topic_of(&runner.storage, instance_id).await?;
    let mappings = runner
        .storage
        .spawn_call_list_by_parent(instance_id)
        .await?;
    let mapping: HashMap<String, i64> = mappings
        .into_iter()
        .map(|row| (row.call_id, row.child_id))
        .collect();

    let instances = runner.storage.agent_list_by_topic(topic_id).await?;
    let children: HashMap<i64, AgentInstance> = instances
        .into_iter()
        .map(|instance| (instance.id, instance))
        .collect();
    let mut phases: HashMap<i64, std::result::Result<Phase<AgentDomain>, String>> = HashMap::new();
    let mut reply_message_ids: Vec<i64> = Vec::new();
    for child_id in unpaired
        .iter()
        .filter_map(|(_, _, call_id)| mapping.get(call_id).copied())
    {
        if phases.contains_key(&child_id) {
            continue;
        }
        let Some(instance) = children.get(&child_id) else {
            continue;
        };
        match stored_phase(instance) {
            Ok(phase) => {
                if let Phase::Done(outcome) = &phase {
                    reply_message_ids.push(outcome.message_id);
                }
                phases.insert(child_id, Ok(phase));
            }
            Err(error) => {
                phases.insert(child_id, Err(error.to_string()));
            }
        }
    }
    reply_message_ids.sort_unstable();
    reply_message_ids.dedup();
    let replies = replies_of(runner, &reply_message_ids).await?;

    let mut groups: BTreeMap<i64, Vec<FunctionCallOutput>> = BTreeMap::new();
    for (message_id, _turn_id, call_id) in unpaired {
        let output = match mapping.get(&call_id).copied() {
            None => error_result(
                &call_id,
                format!("spawn call {call_id} has no child mapping"),
            ),
            Some(child_id) => match children.get(&child_id) {
                None => error_result(
                    &call_id,
                    format!("spawn call {call_id} names a missing child {child_id}"),
                ),
                Some(instance) if instance.mode == Some(AgentMode::Background) => {
                    spawn_response(&call_id, child_id, &Phase::Running, "")
                }
                Some(_) => match phases.get(&child_id) {
                    Some(Ok(phase)) => {
                        let reply = match phase {
                            Phase::Done(outcome) => replies
                                .get(&outcome.message_id)
                                .map(String::as_str)
                                .unwrap_or_default(),
                            _ => "",
                        };
                        spawn_response(&call_id, child_id, phase, reply)
                    }
                    Some(Err(detail)) => error_result(
                        &call_id,
                        format!("child {child_id} has an unreadable phase: {detail}"),
                    ),
                    None => {
                        error_result(&call_id, format!("child {child_id} has no readable phase"))
                    }
                },
            },
        };
        groups.entry(message_id).or_default().push(output);
    }

    let cx = round_cx(topic_id);
    let mut spawn_turn = None;
    for (message_id, results) in groups {
        let Some(&turn_id) = message_turns.get(&message_id) else {
            continue;
        };
        let locus = Locus {
            message_id,
            turn_id,
        };
        fold(runner, &cx, topic_id, &locus, &results).await?;
        spawn_turn = Some(turn_id);
    }
    Ok(spawn_turn)
}

fn stored_call_name(data: Option<&serde_json::Value>) -> Option<&str> {
    data?.get("name")?.as_str()
}

fn stored_phase(
    instance: &AgentInstance,
) -> std::result::Result<Phase<AgentDomain>, phase_codec::PhaseCodecError> {
    let payload = match instance.phase_payload.as_deref() {
        Some(raw) => Some(
            serde_json::from_str::<serde_json::Value>(raw).map_err(|error| {
                phase_codec::PhaseCodecError::BadPayload {
                    name: instance.phase.to_string(),
                    detail: error.to_string(),
                }
            })?,
        ),
        None => None,
    };
    phase_codec::decode(&phase_codec::StoredPhase {
        name: instance.phase.to_string(),
        payload,
    })
}

async fn replies_of(runner: &AgentRunner, message_ids: &[i64]) -> Result<HashMap<i64, String>> {
    if message_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let blocks = runner.storage.block_list_by_messages(message_ids).await?;
    let mut replies: HashMap<i64, String> = HashMap::new();
    for block in blocks {
        if block.kind == BlockKind::Text {
            replies
                .entry(block.message_id)
                .or_default()
                .push_str(&block.content);
        }
    }
    Ok(replies)
}

fn error_result(call_id: &str, detail: String) -> FunctionCallOutput {
    FunctionCallOutput {
        id: call_id.to_string(),
        content: serde_json::json!({ "error": detail }),
    }
}

pub(crate) async fn open_turn(
    storage: &Storage,
    instance_id: i64,
    topic_id: i64,
    from_id: Option<i64>,
) -> Result<i64> {
    storage
        .turn_create(TurnCreate {
            instance_id,
            topic_id,
            from_id,
            is_boundary: false,
            created_at: None,
        })
        .await
}

pub(crate) async fn open_round(storage: &Storage, turn_id: i64, model_id: i64) -> Result<Locus> {
    let message_id = new_message(storage, turn_id, Role::Assistant, Some(model_id)).await?;
    Ok(Locus {
        message_id,
        turn_id,
    })
}

async fn new_message(
    storage: &Storage,
    turn_id: i64,
    role: Role,
    model_id: Option<i64>,
) -> Result<i64> {
    storage
        .message_create(MessageCreate {
            turn_id,
            role,
            model_id,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: None,
        })
        .await
}

pub(crate) struct Prepared {
    pub(crate) adapter: Box<dyn ChatAdapter>,
    pub(crate) model: ChatModel,
    pub(crate) model_id: i64,
    pub(crate) provider: String,
    pub(crate) target: Target,
    pub(crate) rules: RuleSet,
    pub(crate) prompt: String,
    pub(crate) catalog: Arc<ToolCatalog>,
}

pub(crate) async fn prepare(
    runner: &AgentRunner,
    cx: &DispatchCx,
    topic_id: i64,
    instance_id: i64,
) -> Result<Prepared> {
    let fragments = runner.mods.prompt_fragments(topic_id).await?;
    let prompt = PromptBuilder::new(runner.bus.clone())
        .collect(cx, topic_id, instance_id, fragments)
        .await;
    let topic = runner
        .storage
        .topic_get(topic_id)
        .await?
        .ok_or_else(|| CoreError::Denied(format!("topic {topic_id} is unknown")))?;
    let model_id = topic
        .model_id
        .ok_or_else(|| CoreError::Denied(format!("topic {topic_id} names no model")))?;
    let model = runner
        .usecases
        .get_model(model_id)
        .await
        .map_err(denied)?
        .ok_or_else(|| CoreError::Denied(format!("model {model_id} is unknown")))?;
    let provider = runner
        .usecases
        .get_provider(model.provider_id)
        .await
        .map_err(denied)?
        .ok_or_else(|| CoreError::Denied(format!("provider {} is unknown", model.provider_id)))?;
    let key = runner
        .usecases
        .list_credentials_by_provider(provider.id)
        .await
        .map_err(denied)?
        .into_iter()
        .find(|credential| credential.active)
        .map(|credential| credential.key);
    let kind = model.adapter;
    let catalog = catalog_of(runner, cx, topic_id).await?;
    let rules = compile_rules(&runner.storage.rule_list_active(provider.id, kind).await?)?;
    Ok(Prepared {
        adapter: adapter_of(kind),
        model: chat_model(&model, kind),
        model_id,
        provider: provider.name.clone(),
        target: Target {
            base_url: provider.base_url.clone(),
            api_key: key,
        },
        rules,
        prompt,
        catalog,
    })
}

pub(crate) async fn catalog_of(
    runner: &AgentRunner,
    cx: &DispatchCx,
    topic_id: i64,
) -> Result<Arc<ToolCatalog>> {
    if let Some(hit) = cached_catalog(runner, topic_id) {
        return Ok(hit);
    }
    let from_mods = runner.mods.agent_tools(topic_id).await?;
    let from_mcp = match runner.mcp.as_ref() {
        Some(mcp) => mcp.declarations(cx, topic_id).await?,
        None => Vec::new(),
    };
    let catalog = Arc::new(ToolCatalog::assemble(from_mods, from_mcp));
    let mut cache = runner
        .catalog
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *cache = Some(CachedCatalog {
        topic_id,
        at: Instant::now(),
        catalog: Arc::clone(&catalog),
    });
    drop(cache);
    Ok(catalog)
}

fn cached_catalog(runner: &AgentRunner, topic_id: i64) -> Option<Arc<ToolCatalog>> {
    let cache = runner
        .catalog
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let cached = cache.as_ref()?;
    fresh(cached, topic_id, Instant::now()).then(|| Arc::clone(&cached.catalog))
}

fn fresh(cached: &CachedCatalog, topic_id: i64, now: Instant) -> bool {
    cached.topic_id == topic_id && now.duration_since(cached.at) < CATALOG_WINDOW
}

fn chat_model(model: &Model, adapter: AdapterType) -> ChatModel {
    ChatModel {
        name: model.name.clone(),
        adapter,
        endpoint: model.endpoint.clone(),
        config: model.config.clone(),
    }
}

fn denied(error: UseCaseError) -> CoreError {
    CoreError::Denied(error.to_string())
}

pub(crate) async fn exchange(
    runner: &AgentRunner,
    cx: &DispatchCx,
    topic_id: i64,
    locus: &Locus,
    prepared: &Prepared,
    contexts: &[Message],
) -> Result<Message> {
    let body = unified_body(runner, cx, prepared.model_id, body_of(prepared, contexts)).await;
    let provider = provider_body(runner, cx, prepared, &body).await?;
    let chunks = match stream_body(
        prepared.adapter.as_ref(),
        &prepared.model,
        &prepared.target,
        provider,
    )
    .await
    {
        Ok(chunks) => chunks,
        Err(error) => {
            let error = CoreError::Internal(format!("the request failed: {error}"));
            return Err(seal_failure(
                runner,
                cx,
                topic_id,
                locus.message_id,
                &[],
                usage_of(0, 0),
                error,
            )
            .await);
        }
    };
    let mut assembled = Message {
        role: Role::Assistant,
        ..Message::default()
    };
    let mut usage = usage_of(0, 0);
    let mut touched: Vec<BlockId> = Vec::new();
    let mut chunks = pin!(chunks);
    while let Some(item) = chunks.next().await {
        match item {
            Ok(Chunk::Done) => break,
            Ok(Chunk::Delta(delta)) => {
                merge(&mut assembled, &delta);
                usage = add_usage(usage, &delta);
                touched.extend(
                    runner
                        .writer
                        .absorb(cx, topic_id, MessageId(locus.message_id), &delta)
                        .await,
                );
            }
            Err(error) => {
                let error = CoreError::Internal(format!("the stream broke: {error}"));
                return Err(seal_failure(
                    runner,
                    cx,
                    topic_id,
                    locus.message_id,
                    &touched,
                    usage,
                    error,
                )
                .await);
            }
        }
    }
    if let Err(error) = commit_all(runner, cx, topic_id, &touched).await {
        return Err(seal_failure(
            runner,
            cx,
            topic_id,
            locus.message_id,
            &touched,
            usage,
            error,
        )
        .await);
    }
    runner
        .writer
        .seal(
            cx,
            MessageId(locus.message_id),
            finish_reason(&assembled),
            usage,
        )
        .await?;
    Ok(intercept_response(runner, cx, prepared.model_id, assembled, usage).await)
}

async fn unified_body(
    runner: &AgentRunner,
    cx: &DispatchCx,
    model_id: i64,
    body: LlmRequestBody,
) -> LlmRequestBody {
    let payload = LlmRequest {
        model_id,
        request: body,
    };
    runner
        .bus
        .decide(&CORE_LLM_REQUEST, cx, payload)
        .await
        .request
}

fn body_of(prepared: &Prepared, contexts: &[Message]) -> LlmRequestBody {
    let mut messages = Vec::with_capacity(contexts.len() + 1);
    messages.push(Message {
        role: Role::System,
        content: vec![Content::new_text(prepared.prompt.clone())],
        ..Message::default()
    });
    messages.extend(contexts.iter().cloned());
    LlmRequestBody {
        contexts: messages,
        tools: Some(prepared.catalog.declarations()),
    }
}

async fn provider_body(
    runner: &AgentRunner,
    cx: &DispatchCx,
    prepared: &Prepared,
    body: &LlmRequestBody,
) -> Result<serde_json::Value> {
    let mut provider = prepared
        .adapter
        .build_request(&prepared.model, &body.contexts, body.tools.as_deref())
        .map_err(|error| CoreError::Denied(error.to_string()))?;
    apply_rules(
        &prepared.rules,
        &mut provider,
        &context_of(&prepared.provider, &prepared.model.name),
    )?;
    Ok(intercept(&runner.bus, cx, prepared.model.adapter, provider).await)
}

async fn intercept_response(
    runner: &AgentRunner,
    cx: &DispatchCx,
    model_id: i64,
    message: Message,
    usage: Usage,
) -> Message {
    let payload = LlmResponse {
        model_id,
        message,
        usage,
    };
    runner
        .bus
        .decide(&CORE_LLM_RESPONSE, cx, payload)
        .await
        .message
}

pub(crate) async fn adopt(
    runner: &AgentRunner,
    cx: &DispatchCx,
    topic_id: i64,
    calls: Vec<FunctionCall>,
    instance_id: i64,
) -> Result<CallPlan> {
    let topic = runner
        .storage
        .topic_get(topic_id)
        .await?
        .ok_or_else(|| CoreError::Denied(format!("topic {topic_id} is unknown")))?;
    let policy: AllowPolicy = topic.tool_approval_policy;
    Ok(decide_calls(&runner.bus, cx, &policy, calls, instance_id).await)
}

pub(crate) async fn calls_on_message(
    runner: &AgentRunner,
    message_id: i64,
) -> Result<Vec<FunctionCall>> {
    let rows = runner.storage.block_list_by_message(message_id).await?;
    rows.iter()
        .filter(|row| row.kind == BlockKind::ToolCall)
        .map(stored_call)
        .collect()
}

fn stored_call(row: &BlockRow) -> Result<FunctionCall> {
    let id = row
        .call_id
        .clone()
        .ok_or_else(|| CoreError::MalformedColumn {
            column: "block.call_id".into(),
            detail: "a tool call block has no pairing key".into(),
        })?;
    let data = row.data.as_ref();
    let name = data
        .and_then(|data| data.get("name"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| CoreError::MalformedColumn {
            column: "block.data".into(),
            detail: "a tool call block has no tool name".into(),
        })?;
    let arguments = match data.and_then(|data| data.get("arguments")) {
        Some(arguments) => {
            serde_json::to_string(arguments).map_err(|error| CoreError::MalformedColumn {
                column: "block.data".into(),
                detail: error.to_string(),
            })?
        }
        None => row.content.clone(),
    };
    Ok(FunctionCall {
        id,
        name: name.to_string(),
        arguments,
    })
}

pub(crate) async fn execute(
    runner: &AgentRunner,
    cx: &DispatchCx,
    plan: CallPlan,
    catalog: &ToolCatalog,
    _instance_id: i64,
) -> Result<ExecutedPlan> {
    execute_plan(
        plan,
        catalog,
        cx,
        runner.mcp.as_deref(),
        runner.requests.as_ref(),
        runner.mods.as_ref(),
    )
    .await
}
pub(crate) async fn fold(
    runner: &AgentRunner,
    cx: &DispatchCx,
    topic_id: i64,
    locus: &Locus,
    results: &[FunctionCallOutput],
) -> Result<Message> {
    let partial = Message {
        role: Role::Tool,
        content: results
            .iter()
            .map(|result| Content::new_function_call(result.id.clone(), result.content.clone()))
            .collect(),
        ..Message::default()
    };
    let message_id = new_message(&runner.storage, locus.turn_id, Role::Tool, None).await?;
    let touched = runner
        .writer
        .absorb(cx, topic_id, MessageId(message_id), &partial)
        .await;
    commit_all(runner, cx, topic_id, &touched).await?;
    Ok(partial)
}

pub(crate) async fn spawn_requests(
    runner: &AgentRunner,
    cx: &DispatchCx,
    topic_id: i64,
    instance_id: i64,
    actions: Vec<SpawnAgentAction>,
) -> Result<Vec<SpawnRequest<AgentDomain>>> {
    let phase = AgentPhase::Running;
    let written = runner
        .storage
        .with_tx(|storage| async move {
            let mut built = Vec::with_capacity(actions.len());
            for action in actions {
                built.push(
                    build_child(runner, &storage, topic_id, instance_id, phase, &action).await?,
                );
            }
            Ok(built)
        })
        .await?;
    let mut spawn = Vec::with_capacity(written.len());
    let mut blocks = Vec::new();
    for (request, task_blocks) in written {
        spawn.push(request);
        blocks.extend(task_blocks);
    }
    runner.writer.announce_atomic(cx, &blocks).await;
    Ok(spawn)
}

async fn build_child(
    runner: &AgentRunner,
    storage: &Storage,
    topic_id: i64,
    instance_id: i64,
    phase: AgentPhase,
    action: &SpawnAgentAction,
) -> Result<(SpawnRequest<AgentDomain>, Vec<Block>)> {
    let child = storage
        .agent_create(AgentInstanceCreate {
            parent_id: Some(instance_id),
            topic_id,
            role: AgentRole::Child,
            mode: Some(action.data.mode.parse()?),
            phase,
            phase_payload: None,
        })
        .await?;
    storage
        .spawn_call_insert(instance_id, &action.call_id, child)
        .await?;
    let (turn_id, blocks) = write_task(runner, storage, topic_id, child, &action.data.task).await?;
    let request = SpawnRequest {
        id: child,
        spec: AgentSpec {
            instance_id: child,
            message_id: None,
            pending_id: None,
            turn_id: Some(turn_id),
        },
        detached: action.data.mode.as_str() == AgentMode::Background.as_ref(),
    };
    Ok((request, blocks))
}

async fn write_task(
    runner: &AgentRunner,
    storage: &Storage,
    topic_id: i64,
    instance_id: i64,
    task: &str,
) -> Result<(i64, Vec<Block>)> {
    let turn_id = open_turn(storage, instance_id, topic_id, None).await?;
    let message_id = storage
        .message_create(MessageCreate {
            turn_id,
            role: Role::User,
            model_id: None,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: None,
        })
        .await?;
    let block = Block {
        id: runner.writer.allocate_block_id(),
        of_message: MessageId(message_id),
        role: Role::User,
        kind: BlockKind::Text,
        call_id: None,
        revision: 1,
        content: task.to_string(),
        data: None,
        finished: true,
    };
    storage.block_append(block_create(&block)?).await?;
    storage.turn_set_from(turn_id, message_id).await?;
    Ok((turn_id, vec![block]))
}

async fn commit_all(
    runner: &AgentRunner,
    cx: &DispatchCx,
    topic_id: i64,
    touched: &[BlockId],
) -> Result<()> {
    let mut ids = touched.to_vec();
    ids.sort_unstable();
    ids.dedup();
    for id in ids {
        if runner.writer.commit(cx, topic_id, id).await?.is_none() {
            log::warn!("[llm] block {} stayed out of the database", id.0);
        }
    }
    Ok(())
}

async fn seal_failure(
    runner: &AgentRunner,
    cx: &DispatchCx,
    topic_id: i64,
    message_id: i64,
    touched: &[BlockId],
    usage: Usage,
    error: CoreError,
) -> CoreError {
    if let Err(failure) = commit_all(runner, cx, topic_id, touched).await {
        log::error!("[llm] committing the blocks of a broken round failed: {failure}");
    }
    let reason = format!("failed: {error}");
    if let Err(failure) = runner
        .writer
        .seal(cx, MessageId(message_id), reason, usage)
        .await
    {
        log::error!("[llm] sealing a broken round failed: {failure}");
    }
    error
}

fn finish_reason(message: &Message) -> String {
    let has_calls = message
        .tool_calls
        .as_ref()
        .is_some_and(|calls| !calls.is_empty());
    match has_calls {
        true => "tool_calls".to_string(),
        false => "stop".to_string(),
    }
}

fn usage_of(input_tokens: i64, output_tokens: i64) -> Usage {
    Usage {
        input_tokens,
        output_tokens,
    }
}

fn add_usage(usage: Usage, delta: &Message) -> Usage {
    usage_of(
        usage.input_tokens.max(i64::from(delta.input_tokens)),
        usage.output_tokens.max(i64::from(delta.output_tokens)),
    )
}

fn merge(assembled: &mut Message, delta: &Message) {
    for item in &delta.content {
        match item {
            Content::Text { data } => append_text(assembled, data),
            other => assembled.content.push(other.clone()),
        }
    }
    if let Some(reasoning) = delta.reasoning_content.as_deref() {
        let sink = assembled.reasoning_content.get_or_insert_with(String::new);
        sink.push_str(reasoning);
    }
    for call in delta.tool_calls.iter().flatten() {
        push_call(assembled, call);
    }
}

fn append_text(assembled: &mut Message, text: &str) {
    match assembled.content.last_mut() {
        Some(Content::Text { data }) => data.push_str(text),
        _ => assembled.content.push(Content::new_text(text.to_string())),
    }
}

fn push_call(assembled: &mut Message, call: &FunctionCall) {
    let calls = assembled.tool_calls.get_or_insert_with(Vec::new);
    if call.id.is_empty() {
        // 没有标识的分片一定属于最近一次带标识的调用
        if let Some(last) = calls.last_mut() {
            if last.name.is_empty() {
                last.name = call.name.clone();
            }
            last.arguments.push_str(&call.arguments);
        }
        return;
    }
    let known = calls.iter_mut().find(|known| known.id == call.id);
    match known {
        Some(known) => {
            if known.name.is_empty() {
                known.name = call.name.clone();
            }
            known.arguments.push_str(&call.arguments);
        }
        None => calls.push(call.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_text_delta_is_appended_to_the_last_text_item() {
        let mut assembled = Message::default();
        merge(&mut assembled, &text("hello"));
        merge(&mut assembled, &text(", world"));
        assert_eq!(
            assembled.content.len(),
            1,
            "text of the same message is merged into one item"
        );
        let Content::Text { data } = &assembled.content[0] else {
            panic!("merge the delta into a text item");
        };
        assert_eq!(data, "hello, world");
    }

    #[test]
    fn the_arguments_of_a_call_are_appended_to_its_own_entry() {
        let mut assembled = Message::default();
        merge(
            &mut assembled,
            &calls(vec![call("c-1", "fs__read", r#"{"path""#)]),
        );
        merge(&mut assembled, &calls(vec![call("c-2", "fs__write", "{}")]));
        merge(&mut assembled, &calls(vec![call("c-1", "", r#":"/tmp"}"#)]));
        let calls = assembled.tool_calls.expect("read the assembled tool calls");
        assert_eq!(
            calls.len(),
            2,
            "match by id rather than taking the last one"
        );
        assert_eq!(
            calls[0].name, "fs__read",
            "an empty name fragment does not overwrite the existing name"
        );
        assert_eq!(calls[0].arguments, r#"{"path":"/tmp"}"#);
    }

    #[test]
    fn an_argument_delta_without_an_id_extends_the_last_call() {
        let mut assembled = Message::default();
        merge(&mut assembled, &calls(vec![call("c-1", "time__get", "")]));
        merge(&mut assembled, &calls(vec![call("", "", r#"{"tz":"#)]));
        merge(&mut assembled, &calls(vec![call("", "", r#""UTC"}"#)]));

        let calls = assembled.tool_calls.expect("read the assembled tool calls");
        assert_eq!(calls.len(), 1, "an id-less fragment is not a new call");
        assert_eq!(calls[0].id, "c-1");
        assert_eq!(calls[0].name, "time__get");
        assert_eq!(calls[0].arguments, r#"{"tz":"UTC"}"#);
    }

    #[test]
    fn id_less_deltas_follow_the_most_recent_call_with_an_id() {
        let mut assembled = Message::default();
        merge(&mut assembled, &calls(vec![call("c-1", "a", r#"{"x":"#)]));
        merge(&mut assembled, &calls(vec![call("", "", r#"1}"#)]));
        merge(&mut assembled, &calls(vec![call("c-2", "b", r#"{"y":"#)]));
        merge(&mut assembled, &calls(vec![call("", "", r#"2}"#)]));

        let calls = assembled.tool_calls.expect("read the assembled tool calls");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].arguments, r#"{"x":1}"#);
        assert_eq!(calls[1].arguments, r#"{"y":2}"#);
    }

    #[test]
    fn a_round_that_adopts_calls_says_so_in_its_finish_reason() {
        let mut message = Message::default();
        assert_eq!(finish_reason(&message), "stop");
        message.tool_calls = Some(vec![call("c-1", "fs__read", "{}")]);
        assert_eq!(finish_reason(&message), "tool_calls");
    }

    #[test]
    fn a_cached_catalog_goes_stale_by_topic_and_by_age() {
        let at = Instant::now();
        let cached = CachedCatalog {
            topic_id: 7,
            at,
            catalog: Arc::new(ToolCatalog::assemble(Vec::new(), Vec::new())),
        };
        assert!(
            fresh(&cached, 7, at + Duration::from_millis(10)),
            "same topic, within the window: use it directly"
        );
        assert!(
            !fresh(&cached, 8, at + Duration::from_millis(10)),
            "the catalog is per topic, so a topic change forces a rebuild"
        );
        assert!(
            !fresh(&cached, 7, at + CATALOG_WINDOW),
            "the window is its upper bound; it expires when reached"
        );
    }

    #[test]
    fn the_usage_of_a_repeated_report_is_counted_once() {
        let mut usage = usage_of(0, 0);
        usage = add_usage(usage, &delta(120, 8));
        usage = add_usage(usage, &delta(120, 8));
        assert_eq!(
            usage.input_tokens, 120,
            "the same usage reported twice is not doubled"
        );
        assert_eq!(usage.output_tokens, 8);
    }

    /// 一段文本增量
    fn text(data: &str) -> Message {
        Message {
            content: vec![Content::new_text(data.to_string())],
            ..Message::default()
        }
    }

    /// 一次带工具调用的增量
    fn calls(calls: Vec<FunctionCall>) -> Message {
        Message {
            tool_calls: Some(calls),
            ..Message::default()
        }
    }

    /// 一次调用
    fn call(id: &str, name: &str, arguments: &str) -> FunctionCall {
        FunctionCall {
            id: id.to_string(),
            name: name.to_string(),
            arguments: arguments.to_string(),
        }
    }

    /// 一片带用量的增量
    fn delta(input_tokens: i32, output_tokens: i32) -> Message {
        Message {
            input_tokens,
            output_tokens,
            ..Message::default()
        }
    }
}
