use self::domain::{LlmDomain, LlmFailure, LlmOutcome, LlmSpec, Suspended, WaitAnswer};
use self::events::{
    CORE_LLM_REQUEST, CORE_LLM_TOOLCALL, CORE_RULE_APPLY, LlmRequest, RuleApply, ToolCalls,
};
use self::rule::{apply_rules, context_of};
use self::tools::DeclaredTool;
use crate::agent::{Ctx, Input, Outcome, Runner};
use crate::error::CoreError;
use std::sync::Arc;
use wind_event::{DispatchCx, Dispatcher, Scope};
use wind_storage::{Storage, Topic};
use wind_wire::{Content, FunctionCall, Message, Role, tool};

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
                    .create_turn(instance_id, topic.id, None)
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
        let prepared = turn::prepare(&self.storage, topic.id, instance_id)
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
            let blocks = absorber.settle();
            let _block_ids = self
                .storage
                .create_blocks(message_id, &blocks)
                .await
                .map_err(|error| failure(error.into(), Some(message_id)))?;
            // TODO: Writer 发送CORE_BLOCK_COMMITED，和记录日志
            // 或者提交给writer后执行 create_blocks？

            let calls: Vec<FunctionCall> = blocks.into_iter().flat_map(|b| b.tool_call()).collect();
            if calls.is_empty() {
                return Ok(Outcome::Completed(LlmOutcome { message_id }));
            }

            let call_payload = ToolCalls {
                instance_id,
                calls,
                suspend: None,
            };
            // ! 一批 tool_calls 只允许全通过或者全部挂起等审批
            let call_res = self.bus.decide(&CORE_LLM_TOOLCALL, &cx, call_payload).await;
            if call_res.suspend == Some(true) {
                // TODO: 机制：待审批入库，前端审批完放入上下文
                // TODO:(设想) "询问" 原语
                //     Question {
                //      id: string                 // 唯一id
                //      kind: string               // 定位位置归属 "core/approval" | "m_<mod>/<name>"
                //      subject: Subject           // 谁被阻塞：{ kind: "instance", id } | { kind: "none" }
                //      ask: Value                 // 具体问题的 payload
                //      answer: Value              // 回答
                //      state: Open | Answered{answer, by, at} | Closed{reason, at}
                //    }
                return Ok(Outcome::Suspend(Suspended {
                    instance_id,
                    message_id,
                }));
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
            .map_err(|error| failure(error.into(), Some(message_id)))?;

            let tool_res_id = self
                .storage
                .create_message(turn_id, Role::Tool, None)
                .await
                .map_err(|error| failure(error.into(), None))?;
            absorber.clean();

            // TODO: 是否能和 report_spawns 形成闭环？

            if !executed.results.is_empty() {
                for result in executed.results {
                    for block in absorber.absorb(Message {
                        role: Role::Tool,
                        content: vec![Content::new_function_call(result.id, result.content)],
                        ..Message::default()
                    }) {
                        // CORE_BLOCK_DELTA 旁路通知
                    }
                }
                let blocks = absorber.settle();
                let _block_ids = self
                    .storage
                    .create_blocks(message_id, &blocks)
                    .await
                    .map_err(|error| failure(error.into(), Some(message_id)))?;
                // TODO: Writer 发送CORE_BLOCK_COMMITED，和记录日志
                contexts.push(absorber.compose());
            }

            if !executed.spawns.is_empty() {}
        }
        todo!()
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
