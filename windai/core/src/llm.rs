use self::domain::{LlmDomain, LlmFailure, LlmOutcome, LlmSpec, Suspended, WaitAnswer};
use self::events::{
    BlockDelta, CORE_BLOCK_DELTA, CORE_LLM_REQUEST, CORE_LLM_TOOLCALL, CORE_RULE_APPLY, LlmRequest,
    RuleApply, ToolCalls,
};
use self::rule::{apply_rules, context_of};
use self::tools::DeclaredTool;
use crate::agent::{Ctx, Input, Outcome, Runner};
use crate::error::CoreError;
use serde_json::json;
use std::sync::Arc;
use wind_event::{DispatchCx, Dispatcher, Scope};
use wind_storage::{Storage, Topic};
use wind_wire::{Content, FunctionCall, Message, Role};

pub mod block;
pub mod domain;
pub mod events;
pub mod tools;

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
        let composed =
            turn::compose_pending(&self.storage, &self.bus, &cx, topic.id, instance_id, &specs)
                .await
                .map_err(|error| failure(error, message_id))?;

        let spawn_turn = turn::report_spawns(&self.storage, topic.id, instance_id)
            .await
            .map_err(|error| failure(error, message_id))?;

        // TODO: 获取上下文
        let contexts = vec![];

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

        self.rounds(topic, instance_id, turn_id, contexts).await
    }

    async fn rounds(
        &self,
        topic: Topic,
        instance_id: i64,
        turn_id: i64,
        mut contexts: Vec<Message>,
    ) -> Result<Outcome<LlmDomain>, LlmFailure> {
        let mut rounds = 0;
        let prepared = turn::prepare(&self.storage, &*self.mod_service, topic.id, instance_id)
            .await
            .map_err(|error| failure(error, None))?;
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
                .decide(
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
            body = self.bus.decide(&CORE_RULE_APPLY, &cx, payload).await.body;

            let mut absorber = turn::exchange(&prepared, body, |block| {
                self.bus.notify(
                    &CORE_BLOCK_DELTA,
                    &cx,
                    BlockDelta {
                        message_id,
                        delta: block.clone(),
                    },
                );
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
            let committed = self
                .storage
                .with_tx(async move |tx| {
                    let block_ids = tx.create_blocks(message_id, &blocks).await?;
                    tx.seal_message(
                        message_id,
                        &finish_reason,
                        usage.input_tokens,
                        usage.output_tokens,
                    )
                    .await?;
                    let mut events =
                        turn::log_blocks(&tx, &event_cx, message_id, &blocks, &block_ids).await?;
                    events.push(
                        turn::log_message_seal(&tx, &event_cx, message_id, &finish_reason, usage)
                            .await?,
                    );
                    Ok(events)
                })
                .await
                .map_err(|error| failure(error.into(), Some(message_id)))?;
            for event in committed {
                self.bus.notify_committed(event);
            }
            if calls.is_empty() {
                return Ok(Outcome::Completed(LlmOutcome { message_id }));
            }

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
                    .create_external_wait(instance_id, message_id, json!(decided_calls.calls))
                    .await
                    .map_err(|error| failure(error.into(), Some(message_id)))?;
                return Ok(Outcome::Suspend(Suspended { external_wait_id }));
            }
            let executed = tools::execute_plan(
                decided_calls.calls,
                &prepared.catalog,
                &cx,
                &*self.mcp_service,
                &*self.mod_service,
            )
            .await
            .map_err(|error| failure(error, Some(message_id)))?;

            let has_results = !executed.results.is_empty();
            let mut spawn = None;
            if has_results {
                let tool_res_id = self
                    .storage
                    .create_message(turn_id, Role::Tool, None)
                    .await
                    .map_err(|error| failure(error.into(), None))?;
                absorber.absorb(Message {
                    role: Role::Tool,
                    content: executed
                        .results
                        .iter()
                        .map(|result| {
                            Content::new_function_call(result.id.clone(), result.content.clone())
                        })
                        .collect(),
                    ..Message::default()
                });
                let tool_message = absorber.compose();
                let blocks = absorber.settle();
                let event_cx = cx.clone();
                let (blocks, committed) = self
                    .storage
                    .with_tx(async move |tx| {
                        let block_ids = tx.create_blocks(tool_res_id, &blocks).await?;
                        let events =
                            turn::log_blocks(&tx, &event_cx, tool_res_id, &blocks, &block_ids)
                                .await?;
                        Ok((blocks, events))
                    })
                    .await
                    .map_err(|error| failure(error.into(), Some(tool_res_id)))?;
                turn::emit_block_deltas(&self.bus, &cx, tool_res_id, &blocks);
                for event in committed {
                    self.bus.notify_committed(event);
                }
                contexts.push(tool_message);
            }

            if !executed.spawns.is_empty() {
                spawn = Some(
                    turn::create_spawn_requests(
                        &self.storage,
                        &self.bus,
                        &cx,
                        &topic,
                        instance_id,
                        executed.spawns,
                    )
                    .await
                    .map_err(|error| failure(error, Some(message_id)))?,
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
        _instance_id: i64,
        _answer: WaitAnswer,
    ) -> Result<Outcome<LlmDomain>, LlmFailure> {
        todo!()
    }
}

/// agent运行时 llm 侧的实现
#[async_trait::async_trait]
impl Runner<LlmDomain> for LlmRunner {
    async fn run(&self, ctx: Ctx, input: Input<LlmDomain>) -> Outcome<LlmDomain> {
        let instance_id = ctx.node;
        let (message_id, turn_id, reentry, specs) = match input {
            Input::Fresh(spec) => (spec.message_id, spec.turn_id, false, vec![spec]),
            Input::Children(_) => (
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
}

fn failure(error: CoreError, message_id: Option<i64>) -> LlmFailure {
    LlmFailure {
        message_id,
        error: error.to_string(),
    }
}
