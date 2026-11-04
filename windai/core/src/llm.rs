use self::domain::{LlmDomain, LlmFailure, LlmOutcome, LlmSpec, Suspended, WaitAnswer};
use self::events::{
    BlockCommitted, BlockDelta, CORE_BLOCK_COMMITTED, CORE_BLOCK_DELTA, CORE_LLM_REQUEST,
    CORE_LLM_TOOLCALL, CORE_MESSAGE_SEAL, CORE_RULE_APPLY, LlmRequest, MessageSeal, RuleApply,
    ToolCalls,
};
use self::rule::{apply_rules, context_of};
use self::tools::{DeclaredTool, ExecutedPlan, SpawnAgentAction, denied_result};
use crate::agent::{Ctx, Input, Outcome, Runner};
use crate::error::CoreError;
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use wind_event::{DispatchCx, Dispatcher, Scope};
use wind_storage::error::StorageError;
use wind_storage::{Storage, Topic};
use wind_wire::{Block, Content, FunctionCall, FunctionCallOutput, Message, Role, Usage};

pub mod block;
pub mod domain;
pub mod events;
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
            // TODO: 提出公共处理函数
            for (block, block_id) in written
                .blocks
                .into_iter()
                .zip(written.block_ids.into_iter())
            {
                self.emit_block_committed(
                    &cx,
                    written.message_id,
                    block_id,
                    block.seq,
                    Some(block),
                );
            }
        }

        let (writtens, spawn_turn) = turn::report_spawns(&self.storage, instance_id)
            .await
            .map_err(|error| failure(error, message_id))?;
        for written in writtens {
            // TODO: 提出公共处理函数
            for (block, block_id) in written
                .blocks
                .into_iter()
                .zip(written.block_ids.into_iter())
            {
                self.emit_block_committed(
                    &cx,
                    written.message_id,
                    block_id,
                    block.seq,
                    Some(block),
                );
            }
        }

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

        self.rounds(topic, instance_id, turn_id, contexts, None)
            .await
    }

    async fn rounds(
        &self,
        topic: Topic,
        instance_id: i64,
        turn_id: i64,
        mut contexts: Vec<Message>,
        prepared: Option<turn::Prepared>,
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

            let mut absorber = turn::exchange(&prepared, body, |block| {
                self.emit_block_delta(&cx, message_id, block);
            })
            .await
            .map_err(|error| failure(error, None))?;
            let usage = absorber.usage();
            let finish_reason = absorber.finish_reason().to_string();
            contexts.push(absorber.compose());
            let blocks = absorber.settle();
            let calls: Vec<FunctionCall> =
                blocks.iter().flat_map(|block| block.tool_call()).collect();

            let event_cx = cx.clone();
            let block_ids = self
                .storage
                .with_tx(async |tx| {
                    let block_ids = tx.create_blocks(message_id, &blocks).await?;
                    tx.seal_message(
                        message_id,
                        &finish_reason,
                        usage.input_tokens,
                        usage.output_tokens,
                    )
                    .await?;
                    Ok(block_ids)
                })
                .await
                .map_err(|error| failure(error.into(), Some(message_id)))?;

            for (block, block_id) in blocks.iter().zip(block_ids.iter()) {
                self.emit_block_committed(&event_cx, message_id, *block_id, block.seq, None);
            }
            self.emit_block_seal(&event_cx, message_id, finish_reason, usage);
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
                let mut batches = BTreeMap::new();
                batches.insert((turn_id, message_id), results);
                let writtens = self
                    .storage
                    .with_tx(async |tx| turn::write_tool_results(&tx, &batches).await)
                    .await
                    .map_err(|error| failure(error.into(), Some(message_id)))?;
                for written in writtens {
                    // TODO: 提出公共处理函数
                    for (block, block_id) in written
                        .blocks
                        .into_iter()
                        .zip(written.block_ids.into_iter())
                    {
                        self.emit_block_committed(
                            &cx,
                            written.message_id,
                            block_id,
                            block.seq,
                            Some(block),
                        );
                    }
                }
                contexts.push(tool_message);
            }

            if !spawns.is_empty() {
                let requests =
                    turn::create_spawn_requests(&self.storage, &topic, instance_id, spawns)
                        .await
                        .map_err(|error| failure(error, Some(message_id)))?;
                let (spawns, writtens): (Vec<_>, Vec<_>) = requests.into_iter().unzip();
                spawn = Some(spawns);
                for written in writtens {
                    // TODO: 提出公共处理函数
                    for (block, block_id) in written
                        .blocks
                        .into_iter()
                        .zip(written.block_ids.into_iter())
                    {
                        self.emit_block_committed(
                            &cx,
                            written.message_id,
                            block_id,
                            block.seq,
                            Some(block),
                        );
                    }
                }
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
    ) -> Result<Outcome<LlmDomain>, LlmFailure> {
        let (turn, wait, answer) = self
            .storage
            .with_tx(async |tx| {
                let wait_id = answer.external_wait_id;
                let wait = tx.get_external_wait(wait_id).await?.ok_or_else(|| {
                    StorageError::Internal(format!("external wait {} is unknown", wait_id))
                })?;
                let turn = self
                    .storage
                    .get_turn_of(wait.message_id)
                    .await?
                    .ok_or_else(|| {
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
            let mut batches = BTreeMap::new();
            batches.insert((turn.id, wait.message_id), ordered);
            // FIXME: 优化：下面代码重复
            let writtens = self
                .storage
                .with_tx(async |tx| turn::write_tool_results(&tx, &batches).await)
                .await
                .map_err(|error| failure(error.into(), Some(wait.message_id)))?;
            for written in writtens {
                // TODO: 提出公共处理函数
                for (block, block_id) in written
                    .blocks
                    .into_iter()
                    .zip(written.block_ids.into_iter())
                {
                    self.emit_block_committed(
                        &cx,
                        written.message_id,
                        block_id,
                        block.seq,
                        Some(block),
                    );
                }
            }
        }
        if !spawns.is_empty() {
            let requests = turn::create_spawn_requests(&self.storage, &topic, instance_id, spawns)
                .await
                .map_err(|error| failure(error, Some(wait.message_id)))?;

            let (spawn, writtens): (Vec<_>, Vec<_>) = requests.into_iter().unzip();
            for written in writtens {
                // TODO: 提出公共处理函数
                for (block, block_id) in written
                    .blocks
                    .into_iter()
                    .zip(written.block_ids.into_iter())
                {
                    self.emit_block_committed(
                        &cx,
                        written.message_id,
                        block_id,
                        block.seq,
                        Some(block),
                    );
                }
            }
            return Ok(Outcome::Spawn { spawn });
        }
        let contexts = contexts::assemble(&self.storage, instance_id)
            .await
            .map_err(|error| failure(error, Some(wait.message_id)))?;
        self.rounds(topic, instance_id, turn.id, contexts, Some(prepared))
            .await
    }

    async fn run_inner(&self, instance_id: i64, input: Input<LlmDomain>) -> Outcome<LlmDomain> {
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
                return match self.resume(instance_id, answer).await {
                    Ok(outcome) => outcome,
                    Err(failure) => Outcome::Failed(failure),
                };
            }
            Input::Idle(specs) => (None, None, false, specs),
        };
        match self
            .serve(instance_id, specs, message_id, turn_id, reentry)
            .await
        {
            Ok(outcome) => outcome,
            Err(failure) => Outcome::Failed(failure),
        }
    }

    /// 把完整块作为实时增量发给订阅者
    fn emit_block_delta(&self, cx: &DispatchCx, message_id: i64, block: &Block) {
        self.bus.notify(
            &CORE_BLOCK_DELTA,
            cx,
            BlockDelta {
                message_id,
                delta: block.clone(),
            },
        );
    }

    fn emit_block_committed(
        &self,
        cx: &DispatchCx,
        message_id: i64,
        block_id: i64,
        block_seq: i64,
        block: Option<Block>,
    ) {
        self.bus.notify(
            &CORE_BLOCK_COMMITTED,
            &cx,
            BlockCommitted {
                message_id,
                seq: block_seq,
                block_id,
                block,
            },
        );
    }

    fn emit_block_seal(
        &self,
        cx: &DispatchCx,
        message_id: i64,
        finish_reason: String,
        usage: Usage,
    ) {
        self.bus.notify(
            &CORE_MESSAGE_SEAL,
            &cx,
            MessageSeal {
                message_id,
                finish_reason,
                usage,
            },
        );
    }
}

/// agent运行时 llm 侧的实现
#[async_trait::async_trait]
impl Runner<LlmDomain> for LlmRunner {
    async fn run(&self, ctx: Ctx, input: Input<LlmDomain>) -> Outcome<LlmDomain> {
        tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => Outcome::Failed(cancel_failure(None)),
            outcome = self.run_inner(ctx.node, input) => outcome,
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
