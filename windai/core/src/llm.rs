use self::block::BlockAbsorb;
use self::domain::{LlmDomain, LlmFailure, LlmSpec, WaitAnswer};
use self::events::{
    BlockDelta, CORE_BLOCK_DELTA, CORE_LLM_REQUEST, CORE_RULE_APPLY, LlmRequest, RuleApply,
};
use self::ports::{McpService, ModService};
use self::rule::{apply_rules, context_of};
use crate::agent::{Ctx, Input, Outcome, Runner};
use crate::error::CoreError;
use std::sync::Arc;
use wind_event::{DispatchCx, Dispatcher};
use wind_storage::{Storage, Topic};
use wind_wire::{Content, Message, Role};

pub mod block;
pub mod domain;
pub mod events;
pub mod ports;
pub mod tools;

pub(crate) mod rule;
pub(crate) mod turn;

/// 一次请求最多跑几轮工具调用
pub const DEFAULT_MAX_ROUNDS: u32 = 40;

pub struct LlmRunner {
    pub(crate) bus: Arc<Dispatcher>,
    pub(crate) storage: Storage,
    pub(crate) max_rounds: u32,
    pub(crate) mod_service: Arc<dyn ModService>,
    pub(crate) mcp_service: Arc<dyn McpService>,
}

fn failure(error: CoreError, message_id: Option<i64>) -> LlmFailure {
    LlmFailure {
        message_id,
        error: error.to_string(),
    }
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
                    .create_new_turn(instance_id, topic.id, None)
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
        let mut absorber = BlockAbsorb::new();
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
                .create_new_message(turn_id, Role::Assistant, Some(prepared.model_id))
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

            let blocks =
                turn::exchange(&prepared, &mut absorber, message_id, body, |seq, block| {
                    // TODO: Writer 旁路通知
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
            // TODO:
            // 1. 批量有序插入 blocks 并且返回id
            // 2. 发送CORE_BLOCK_COMMITED
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
