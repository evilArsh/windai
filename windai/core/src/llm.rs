use self::domain::{LlmDomain, LlmFailure, LlmOutcome, LlmSpec, Suspended, WaitAnswer};
use self::events::{
    CORE_LLM_REQUEST, CORE_LLM_TOOLCALL, CORE_RULE_APPLY, LlmRequest, RuleApply, ToolCalls,
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
    ) -> Result<Outcome<LlmDomain>, LlmFailure> {
        let topic = turn::topic_of(&self.storage, instance_id)
            .await
            .map_err(|error| failure(error, message_id))?;

        let composed = turn::compose_pending(&self.storage, topic.id, instance_id, &specs)
            .await
            .map_err(|error| failure(error, message_id))?;

        let spawn_turn = turn::report_spawns(&self.storage, topic.id, instance_id)
            .await
            .map_err(|error| failure(error, message_id))?;

        // TODO: 获取上下文
        let contexts = vec![];

        // 这次唤醒续在哪个 turn
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
            (None, None, None) => match spawn_turn {
                Some(turn_id) => turn_id,
                None => self
                    .storage
                    .create_turn(topic.id, instance_id, None)
                    .await
                    .map_err(|error| failure(error.into(), None))?,
            },
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
        let prepared = turn::prepare(&*self.mod_service, &self.storage, topic.id, instance_id)
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
            let cx = turn::round_cx(topic.id);
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

            let mut absorber = turn::exchange(&prepared, body, |_delta| {
                // TODO: Writer 旁路通知，旁路日志不能落库，WriteHandle如何做？
                // self.bus.notify(
                //     &CORE_BLOCK_DELTA,
                //     &cx,
                //     BlockDelta {
                //         message_id,
                //         seq,
                //         delta: block,
                //     },
                // );
            })
            .await
            .map_err(|error| failure(error, None))?;
            contexts.push(absorber.compose());
            let blocks = absorber.settle();
            let _block_ids = self
                .storage
                .create_blocks(message_id, &blocks)
                .await
                .map_err(|error| failure(error.into(), Some(message_id)))?;
            // TODO: Writer 发送 CORE_BLOCK_COMMITTED，同事务置 finished 并落 event_log
            // TODO: Writer 发送 CORE_MESSAGE_SEAL（finish_reason 与 usage）

            let calls: Vec<FunctionCall> = blocks.into_iter().flat_map(|b| b.tool_call()).collect();
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
            // 0. mod 后端半侧改写了 suspend == true
            let call_res = self.bus.decide(&CORE_LLM_TOOLCALL, &cx, call_payload).await;
            if call_res.suspend == Some(true) {
                // 1. mod 后端半侧js脚本：向本mod ui侧发送自定义pending事件，带上tool_calls payload （需要有挂起记录，UI 刷新/重连后要重画弹框）
                // 2. mod 前端半侧UI受到pending事件，展示给用户
                // 3. 前端宿主添加新hook,ctx.agent.resolve(wait_id, decisions) 调用后端
                /*
                    │
                    ▼ core：条件写 waits.answer + 校验 wait_id == 当前 suspended
                    Event::Resume { expect: Suspended{wait_id}, input: decisions }
                    │
                    ▼ resume：按 decisions 执行/拒绝，回到 rounds
                    之后 core/agent.status 变为 running，UI 据此收起弹框
                */
                // 注意：sse数据快照也要包含mod 的 stateful
                // 旧代码里面有 external_waits 实现了

                // 如果存在 mod_approval
                // 1. mod js 发送自定事件给前端UI侧（通过系统bus推送），前端弹窗让用户一次性审核
                // 2. 现在将instance payload message 入库 external_waits，将id 放到Suspended，这一轮llm结束
                // 注意：如果用户在后端还没有保存这条wait前就点击,则会报错，保持幂等
                let external_wait_id = self
                    .storage
                    .create_external_wait(instance_id, message_id, json!(call_res.calls))
                    .await
                    .map_err(|error| failure(error.into(), Some(message_id)))?;

                return Ok(Outcome::Suspend(Suspended { external_wait_id }));
            }
            // 如果没有mod拦截 core/llm.toolcall 则自动执行：
            // 1. 先执行普通的 tool_call，将结果入库
            // 2. block 开块，commit，日志通知
            // 再执行spawn 子agent, 返回spawn 状态
            let executed = tools::execute_plan(
                call_res.calls,
                &prepared.catalog,
                &cx,
                &*self.mcp_service,
                &*self.mod_service,
            )
            .await
            .map_err(|error| failure(error, Some(message_id)))?;

            // TODO: 是否能和 report_spawns 形成闭环？

            let has_results = !executed.results.is_empty();
            let mut spawn = None;
            if has_results {
                let tool_res_id = self
                    .storage
                    .create_message(turn_id, Role::Tool, None)
                    .await
                    .map_err(|error| failure(error.into(), None))?;
                for _block in absorber.absorb(Message {
                    role: Role::Tool,
                    content: executed
                        .results
                        .iter()
                        .map(|result| {
                            Content::new_function_call(result.id.clone(), result.content.clone())
                        })
                        .collect(),
                    ..Message::default()
                }) {
                    // TODO: Writer 发送 CORE_BLOCK_DELTA（整块内容，一条即完整）
                }
                let tool_message = absorber.compose();
                let blocks = absorber.settle();
                let _block_ids = self
                    .storage
                    .create_blocks(tool_res_id, &blocks)
                    .await
                    .map_err(|error| failure(error.into(), Some(tool_res_id)))?;
                // TODO: Writer 发送 CORE_BLOCK_COMMITTED，同事务置 finished 并落 event_log
                contexts.push(tool_message);
            }

            if !executed.spawns.is_empty() {
                spawn = Some(
                    turn::create_spawn_requests(
                        &self.storage,
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

// TODO: agent运行时 llm 侧的实现
#[async_trait::async_trait]
impl Runner<LlmDomain> for LlmRunner {
    async fn run(&self, _ctx: Ctx, _input: Input<LlmDomain>) -> Outcome<LlmDomain> {
        // reentry
        todo!()
        // match self.serve(instance_id, specs, message_id, turn_id).await {
        //     Ok(outcome) => outcome,
        //     Err(failure) => Outcome::Failed(failure),
        // }
    }
}

fn failure(error: CoreError, message_id: Option<i64>) -> LlmFailure {
    LlmFailure {
        message_id,
        error: error.to_string(),
    }
}
