use self::domain::{LlmDomain, LlmFailure, LlmOutcome, LlmSpec, Suspended, WaitAnswer};
use self::events::{
    CORE_LLM_REQUEST, CORE_LLM_TOOLCALL, CORE_RULE_APPLY, InterruptedReason, LlmRequest, RuleApply,
    ToolCalls,
};
use self::rule::{apply_rules, context_of};
use self::stream::{MessageStream, notify_persisted_block};
use self::tools::{DeclaredTool, ExecutedPlan, SpawnAgentAction, denied_result};
use crate::agent::{Ctx, Input, Outcome, Runner, SpawnRequest};
use crate::error::CoreError;
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use wind_event::{DispatchCx, Dispatcher, Scope};
use wind_storage::error::StorageError;
use wind_storage::{Storage, Topic};
use wind_wire::{Content, FunctionCall, FunctionCallOutput, Message, Role};

pub mod block;
pub mod domain;
pub mod events;
pub mod stream;
pub mod tools;

pub(crate) mod contexts;
pub(crate) mod rule;
pub(crate) mod turn;

/// 一次请求最多跑几轮工具调用
pub const DEFAULT_MAX_ROUNDS: u32 = 40;

#[async_trait::async_trait]
pub trait ModService: Send + Sync {
    async fn prompt_fragments(&self, topic_id: i64) -> Result<Vec<String>, CoreError>;
    async fn agent_tools(&self, topic_id: i64) -> Result<Vec<DeclaredTool>, CoreError>;
    async fn version_of(&self, mod_name: &str) -> Result<i64, CoreError>;
    /// 调用 mod 中的 scripts 脚本。调用入口为 `request` handler
    async fn call(
        &self,
        scope: &Scope,
        mod_name: &str,
        version: i64,
        request: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, CoreError>;
}

#[async_trait::async_trait]
pub trait McpService: Send + Sync {
    async fn declarations(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
    ) -> Result<Vec<DeclaredTool>, CoreError>;

    async fn call(
        &self,
        cx: &DispatchCx,
        call_id: &str,
        server: &str,
        tool: &str,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, CoreError>;
}

pub struct LlmRunner {
    pub(crate) bus: Arc<Dispatcher>,
    pub(crate) storage: Storage,
    pub(crate) max_rounds: u32,
    pub(crate) mod_service: Arc<dyn ModService>,
    pub(crate) mcp_service: Arc<dyn McpService>,
}

impl LlmRunner {
    pub fn new(
        storage: Storage,
        bus: Arc<Dispatcher>,
        mod_service: Arc<dyn ModService>,
        mcp_service: Arc<dyn McpService>,
    ) -> Self {
        Self {
            bus,
            storage,
            max_rounds: DEFAULT_MAX_ROUNDS,
            mod_service,
            mcp_service,
        }
    }

    /// 一轮 LLM 调用
    async fn serve(
        &self,
        instance_id: i64,
        specs: Vec<LlmSpec>,
        message_id: Option<i64>,
        turn_id: Option<i64>,
        reentry: bool,
        cancel: CancellationToken,
    ) -> Result<Outcome<LlmDomain>, LlmFailure> {
        let topic = turn::topic_of(&self.storage, instance_id)
            .await
            .map_err(|error| failure(error, message_id))?;

        let cx = turn::round_cx(topic.id, instance_id);
        let (written, composed) =
            turn::compose_pending(&self.storage, topic.id, instance_id, &specs)
                .await
                .map_err(|error| failure(error, message_id))?;

        if let Some(written) = written {
            self.emit_written(&cx, [written]);
        }

        let (writtens, spawn_turn) = turn::report_spawns(&self.storage, instance_id)
            .await
            .map_err(|error| failure(error, message_id))?;
        self.emit_written(&cx, writtens);

        let contexts = contexts::assemble(&self.storage, instance_id)
            .await
            .map_err(|error| failure(error, message_id))?;

        let turn_id = match (turn_id, message_id, composed) {
            (Some(turn_id), ..) => turn_id,
            (None, Some(message_id), _) => self
                .storage
                .get_turn_of(message_id)
                .await
                .map_err(|error| failure(error.into(), Some(message_id)))?
                .map(|turn| turn.id)
                .ok_or_else(|| LlmFailure {
                    message_id: Some(message_id),
                    error: format!("message {message_id} belongs to no turn"),
                })?,
            (None, None, Some(input)) => input.turn_id,
            (None, None, None) => {
                if reentry && let Some(turn_id) = spawn_turn {
                    turn_id
                } else {
                    self.storage
                        .create_turn(topic.id, instance_id, None)
                        .await
                        .map_err(|error| failure(error.into(), None))?
                }
            }
        };

        self.rounds(topic, instance_id, turn_id, contexts, None, cancel)
            .await
    }

    async fn rounds(
        &self,
        topic: Topic,
        instance_id: i64,
        turn_id: i64,
        mut contexts: Vec<Message>,
        prepared: Option<turn::Prepared>,
        cancel: CancellationToken,
    ) -> Result<Outcome<LlmDomain>, LlmFailure> {
        let mut rounds = 0;
        let prepared = match prepared {
            Some(prepared) => prepared,
            None => {
                let prepare_cx = turn::round_cx(topic.id, instance_id);
                turn::prepare(
                    &self.storage,
                    &self.bus,
                    &prepare_cx,
                    &*self.mod_service,
                    &*self.mcp_service,
                    &topic,
                    instance_id,
                )
                .await
                .map_err(|error| failure(error, None))?
            }
        };
        // 组装 mod 提示词；只在每次请求前组装一次
        contexts.insert(
            0,
            Message {
                role: Role::System,
                content: vec![Content::new_text(prepared.prompt.clone())],
                ..Message::default()
            },
        );
        loop {
            let cx = turn::round_cx(topic.id, instance_id);
            rounds += 1;
            if rounds > self.max_rounds {
                return Err(LlmFailure {
                    message_id: None,
                    error: format!("tool calls exceed max rounds: {}", self.max_rounds),
                });
            }

            let message_id = self
                .storage
                .create_message(turn_id, Role::Assistant, Some(prepared.model_id))
                .await
                .map_err(|error| failure(error.into(), None))?;
            let mut stream = MessageStream::new(
                message_id,
                Arc::clone(&self.bus),
                cancel.clone(),
                cx.clone(),
            );

            // body patch
            let body = self
                .bus
                .decide_without_payload(
                    &CORE_LLM_REQUEST,
                    &cx,
                    LlmRequest {
                        contexts: contexts.clone(),
                        tools: Some(prepared.catalog.declarations()),
                    },
                )
                .await;
            let mut body = prepared
                .adapter
                .build_request(&prepared.model, &body.contexts, body.tools.as_deref())
                .map_err(|error| failure(error.into(), Some(message_id)))?;

            // rule patch
            apply_rules(
                &prepared.rules,
                &mut body,
                &context_of(&prepared.provider, &prepared.model.name),
            )
            .map_err(|error| failure(error, Some(message_id)))?;
            let payload = RuleApply {
                adapter: prepared.model.adapter,
                body,
            };
            body = self
                .bus
                .decide_without_payload(&CORE_RULE_APPLY, &cx, payload)
                .await
                .body;

            let mut absorber =
                match turn::exchange(&prepared, body, |block| stream.delta(block)).await {
                    Ok(absorber) => absorber,
                    Err(error) => {
                        stream.interrupted(InterruptedReason::Failed);
                        return Err(failure(error, Some(message_id)));
                    }
                };
            let usage = absorber.usage();
            let finish_reason = absorber.finish_reason().to_string();
            contexts.push(absorber.compose());
            let blocks = absorber.settle();
            let calls: Vec<FunctionCall> =
                blocks.iter().flat_map(|block| block.tool_call()).collect();
            stream
                .complete(&self.storage, blocks, finish_reason, usage)
                .await
                .map_err(|error| failure(error.into(), Some(message_id)))?;

            if calls.is_empty() {
                return Ok(Outcome::Completed(LlmOutcome { message_id }));
            }

            let original_calls = calls.clone();
            let call_payload = ToolCalls {
                instance_id,
                message_id,
                calls,
                suspend: None,
            };
            // ! 一批 tool_calls 只允许全通过或者全部挂起等审批
            let decided_calls = self.bus.decide(&CORE_LLM_TOOLCALL, &cx, call_payload).await;
            if decided_calls.suspend == Some(true) {
                let external_wait_id = self
                    .storage
                    .create_external_wait(instance_id, message_id, json!(original_calls))
                    .await
                    .map_err(|error| failure(error.into(), Some(message_id)))?;
                return Ok(Outcome::Suspend(Suspended { external_wait_id }));
            }
            let calls = filter_calls(&original_calls, &decided_calls.calls);
            let executed = tools::execute_plan(
                calls,
                &prepared.catalog,
                &cx,
                &*self.mcp_service,
                &*self.mod_service,
            )
            .await
            .map_err(|error| failure(error, Some(message_id)))?;

            let ExecutedPlan { spawns, results } = executed;
            let has_results = !results.is_empty();
            let mut spawn = None;
            if has_results {
                let tool_message = Message {
                    role: Role::Tool,
                    content: results
                        .iter()
                        .map(|result| {
                            Content::new_function_call(result.id.clone(), result.content.clone())
                        })
                        .collect(),
                    ..Message::default()
                };
                self.commit_tool_results(&cx, turn_id, message_id, results)
                    .await?;
                contexts.push(tool_message);
            }

            if !spawns.is_empty() {
                spawn = Some(
                    self.spawn_requests(&cx, &topic, instance_id, spawns, Some(message_id))
                        .await?,
                );
            }

            if let Some(spawn) = spawn {
                return Ok(Outcome::Spawn { spawn });
            }
            if !has_results {
                return Ok(Outcome::Completed(LlmOutcome { message_id }));
            }
        }
    }

    /// 恢复一轮会话
    async fn resume(
        &self,
        instance_id: i64,
        answer: WaitAnswer,
        cancel: CancellationToken,
    ) -> Result<Outcome<LlmDomain>, LlmFailure> {
        let (turn, wait, answer) = self
            .storage
            .with_tx(async |tx| {
                let wait_id = answer.external_wait_id;
                let wait = tx.get_external_wait(wait_id).await?.ok_or_else(|| {
                    StorageError::Internal(format!("external wait {} is unknown", wait_id))
                })?;
                let turn = tx.get_turn_of(wait.message_id).await?.ok_or_else(|| {
                    StorageError::Internal(format!(
                        "message {} belongs to no turn",
                        wait.message_id
                    ))
                })?;
                if turn.instance_id != instance_id {
                    return Err(StorageError::Internal(format!(
                        "turn {} does not belong to instance {instance_id}",
                        turn.id
                    )));
                }
                if wait.instance_id != instance_id {
                    return Err(StorageError::Internal(format!(
                        "external wait {} does not belong to instance {instance_id}",
                        wait.id
                    )));
                }
                if wait.answered_at.is_some() && wait.decision.is_some() {
                    return Err(StorageError::Internal(format!(
                        "external wait {} answered",
                        wait.id
                    )));
                }
                if tx
                    .answer_external_wait(wait_id, serde_json::to_value(&answer.decisions)?)
                    .await?
                {
                    Ok((turn, wait, answer))
                } else {
                    Err(StorageError::Internal(format!(
                        "external wait {} answer failed, the answer existed",
                        wait.id
                    )))
                }
            })
            .await
            .map_err(|error| failure(error.into(), None))?;

        let calls: Vec<FunctionCall> =
            serde_json::from_value(wait.payload).map_err(|error| LlmFailure {
                message_id: Some(wait.message_id),
                error: format!(
                    "external wait {} has an unreadable payload: {error}",
                    wait.id
                ),
            })?;
        let topic = turn::topic_of(&self.storage, instance_id)
            .await
            .map_err(|error| failure(error, Some(wait.message_id)))?;
        let cx = turn::round_cx(topic.id, instance_id);
        let prepared = turn::prepare(
            &self.storage,
            &self.bus,
            &cx,
            &*self.mod_service,
            &*self.mcp_service,
            &topic,
            instance_id,
        )
        .await
        .map_err(|error| failure(error, Some(wait.message_id)))?;

        let mut decisions: HashMap<String, bool> = answer
            .decisions
            .into_iter()
            .map(|decision| (decision.call_id, decision.allow))
            .collect();
        // 未表态的默认允许
        let allowed: Vec<FunctionCall> = calls
            .iter()
            .filter(|call| decisions.get(&call.call_id).copied().unwrap_or(true))
            .cloned()
            .collect();
        let executed = tools::execute_plan(
            allowed,
            &prepared.catalog,
            &cx,
            &*self.mcp_service,
            &*self.mod_service,
        )
        .await
        .map_err(|error| failure(error, Some(wait.message_id)))?;
        let (ordered, spawns) = merge_resume_results(calls, &mut decisions, executed);
        if !ordered.is_empty() {
            self.commit_tool_results(&cx, turn.id, wait.message_id, ordered)
                .await?;
        }
        if !spawns.is_empty() {
            let spawn = self
                .spawn_requests(&cx, &topic, instance_id, spawns, Some(wait.message_id))
                .await?;
            return Ok(Outcome::Spawn { spawn });
        }
        let contexts = contexts::assemble(&self.storage, instance_id)
            .await
            .map_err(|error| failure(error, Some(wait.message_id)))?;
        self.rounds(
            topic,
            instance_id,
            turn.id,
            contexts,
            Some(prepared),
            cancel,
        )
        .await
    }

    async fn run_inner(
        &self,
        instance_id: i64,
        cancel: CancellationToken,
        input: Input<LlmDomain>,
    ) -> Outcome<LlmDomain> {
        let (message_id, turn_id, reentry, specs) = match input {
            Input::Fresh(spec) => (spec.message_id, spec.turn_id, false, vec![spec]),
            Input::Children => (
                None,
                None,
                true,
                vec![LlmSpec {
                    instance_id,
                    message_id: None,
                    pending_id: None,
                    turn_id: None,
                }],
            ),
            Input::External(answer) => {
                return match self.resume(instance_id, answer, cancel).await {
                    Ok(outcome) => outcome,
                    Err(failure) => Outcome::Failed(failure),
                };
            }
            Input::Idle(specs) => (None, None, false, specs),
        };
        match self
            .serve(instance_id, specs, message_id, turn_id, reentry, cancel)
            .await
        {
            Ok(outcome) => outcome,
            Err(failure) => Outcome::Failed(failure),
        }
    }

    async fn commit_tool_results(
        &self,
        cx: &DispatchCx,
        turn_id: i64,
        message_id: i64,
        results: Vec<FunctionCallOutput>,
    ) -> Result<(), LlmFailure> {
        let mut batches = BTreeMap::new();
        batches.insert((turn_id, message_id), results);
        let writtens = self
            .storage
            .with_tx(async |tx| turn::write_tool_results(&tx, &batches).await)
            .await
            .map_err(|error| failure(error.into(), Some(message_id)))?;
        self.emit_written(cx, writtens);
        Ok(())
    }

    async fn spawn_requests(
        &self,
        cx: &DispatchCx,
        topic: &Topic,
        instance_id: i64,
        spawns: Vec<SpawnAgentAction>,
        message_id: Option<i64>,
    ) -> Result<Vec<SpawnRequest<LlmDomain>>, LlmFailure> {
        let requests = turn::create_spawn_requests(&self.storage, topic, instance_id, spawns)
            .await
            .map_err(|error| failure(error, message_id))?;
        let (spawns, writtens): (Vec<_>, Vec<_>) = requests.into_iter().unzip();
        self.emit_written(cx, writtens);
        Ok(spawns)
    }

    fn emit_written(
        &self,
        cx: &DispatchCx,
        writtens: impl IntoIterator<Item = turn::WrittenBlocks>,
    ) {
        for written in writtens {
            for (block, block_id) in written.blocks.into_iter().zip(written.block_ids) {
                notify_persisted_block(&self.bus, cx, written.message_id, block_id, block);
            }
        }
    }
}

/// agent运行时 llm 侧的实现
#[async_trait::async_trait]
impl Runner<LlmDomain> for LlmRunner {
    async fn run(&self, ctx: Ctx, input: Input<LlmDomain>) -> Outcome<LlmDomain> {
        tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => Outcome::Failed(cancel_failure(None)),
            outcome = self.run_inner(ctx.node, ctx.cancel.clone(), input) => outcome,
        }
    }
}

fn failure(error: CoreError, message_id: Option<i64>) -> LlmFailure {
    LlmFailure {
        message_id,
        error: error.to_string(),
    }
}

fn cancel_failure(message_id: Option<i64>) -> LlmFailure {
    LlmFailure {
        message_id,
        error: "run cancelled".to_string(),
    }
}

fn filter_calls(original: &[FunctionCall], decided: &[FunctionCall]) -> Vec<FunctionCall> {
    original
        .iter()
        .enumerate()
        .map(|(index, call)| match decided.get(index) {
            Some(decided) if decided.call_id == call.call_id && decided.name == call.name => {
                decided.clone()
            }
            _ => call.clone(),
        })
        .collect()
}

fn merge_resume_results(
    calls: Vec<FunctionCall>,
    decisions: &mut HashMap<String, bool>,
    executed: ExecutedPlan,
) -> (Vec<FunctionCallOutput>, Vec<SpawnAgentAction>) {
    let ExecutedPlan { spawns, results } = executed;
    let mut results = results;
    let spawn_ids: HashSet<&str> = spawns.iter().map(|spawn| spawn.call_id.as_str()).collect();
    let mut ordered = Vec::with_capacity(calls.len());
    for call in calls {
        match decisions.remove(&call.call_id) {
            Some(false) => {
                ordered.push(denied_result(
                    call.call_id,
                    "the user did not allow this call",
                ));
            }
            Some(true) | None => {
                let result = results
                    .iter()
                    .position(|result| result.id == call.call_id)
                    .map(|index| results.swap_remove(index));
                if let Some(result) = result {
                    ordered.push(result);
                } else if !spawn_ids.contains(call.call_id.as_str()) {
                    ordered.push(FunctionCallOutput {
                        id: call.call_id,
                        content: json!({ "error": "the call produced no result" }),
                    });
                }
            }
        }
    }
    (ordered, spawns)
}
