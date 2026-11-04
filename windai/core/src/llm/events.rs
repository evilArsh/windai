use serde::{Deserialize, Serialize};
use wind_event::def::{Directory, EventDef, Shape};
use wind_event::schema;
use wind_wire::message::Message;
use wind_wire::tool::{FunctionCall, Tools};
use wind_wire::{AdapterType, Block, Usage};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PromptCollect {
    /// 话题标识
    pub topic_id: i64,
    /// 实例标识
    pub instance_id: i64,
    /// 待拼装的全部片段，顺序即最终顺序
    pub fragments: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RuleApply {
    /// 该请求用的是哪一种适配器
    pub adapter: AdapterType,
    /// 提供商结构侧的请求体
    pub body: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct LlmRequest {
    pub contexts: Vec<Message>,
    pub tools: Option<Vec<Tools>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct LlmResponse {
    pub model_id: i64,
    pub message: Message,
    pub usage: Usage,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ToolCalls {
    pub instance_id: i64,
    pub message_id: i64,
    pub calls: Vec<FunctionCall>,
    /// 本批调用是否挂起。
    pub suspend: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct BlockDelta {
    pub message_id: i64,
    #[serde(flatten)]
    pub delta: Block,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct BlockCommitted {
    pub message_id: i64,
    /// block 序列
    pub seq: i64,
    pub block_id: i64,
    /// 非实时消息提交时，系统不推送 partial 数据，该字段存放完整的 Block 值。
    ///
    /// 系统推送实时 partial 消息时，消费者自行拼装成完整数据，
    /// block 推送完毕后，该字段不设置值
    pub block: Option<Block>,
}

/// 一次模型 API 往返的结束标记
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MessageSeal {
    pub message_id: i64,
    /// 模型给出的停止原因
    pub finish_reason: String,
    /// 本次往返的用量
    pub usage: Usage,
}

/// 一条用户输入已被受理并进入待消费队列
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct InputAccepted {
    /// 所属话题
    pub topic_id: i64,
    /// 目标实例
    pub instance_id: i64,
    /// agent_pending 行标识
    pub pending_id: i64,
}

pub const CORE_PROMPT_COLLECT: EventDef<PromptCollect> = EventDef::new(
    "core/prompt.collect",
    Shape::Intercept,
    true,
    schema::<PromptCollect>,
);

pub const CORE_RULE_APPLY: EventDef<RuleApply> = EventDef::new(
    "core/rule.apply",
    Shape::Intercept,
    true,
    schema::<RuleApply>,
);

pub const CORE_LLM_REQUEST: EventDef<LlmRequest> = EventDef::new(
    "core/llm.request",
    Shape::Intercept,
    true,
    schema::<LlmRequest>,
);

// pub const CORE_LLM_RESPONSE: EventDef<LlmResponse> = EventDef::new(
//     "core/llm.response",
//     Shape::Intercept,
//     true,
//     schema::<LlmResponse>,
// );

pub const CORE_LLM_TOOLCALL: EventDef<ToolCalls> = EventDef::new(
    "core/llm.toolcall",
    Shape::Intercept,
    true,
    schema::<ToolCalls>,
);

pub const CORE_MESSAGE_SEAL: EventDef<MessageSeal> = EventDef::new(
    "core/message.seal",
    Shape::Observe,
    true,
    schema::<MessageSeal>,
);

pub const CORE_BLOCK_DELTA: EventDef<BlockDelta> = EventDef::new(
    "core/block.delta",
    Shape::Observe,
    false,
    schema::<BlockDelta>,
);

// pub const CORE_BLOCK_COMMIT: EventDef<BlockCommit> = EventDef::new(
//     "core/block.commit",
//     Shape::Observe,
//     true,
//     schema::<BlockCommit>,
// );

pub const CORE_BLOCK_COMMITTED: EventDef<BlockCommitted> = EventDef::new(
    "core/block.committed",
    Shape::Observe,
    true,
    schema::<BlockCommitted>,
);

pub const CORE_INPUT_ACCEPTED: EventDef<InputAccepted> = EventDef::new(
    "core/input.accepted",
    Shape::Observe,
    true,
    schema::<InputAccepted>,
);

pub fn directory() -> Directory {
    Directory::from_metas([
        CORE_PROMPT_COLLECT.meta(),
        CORE_RULE_APPLY.meta(),
        CORE_LLM_REQUEST.meta(),
        // CORE_LLM_RESPONSE.meta(),
        CORE_LLM_TOOLCALL.meta(),
        CORE_MESSAGE_SEAL.meta(),
        CORE_BLOCK_DELTA.meta(),
        CORE_BLOCK_COMMITTED.meta(),
        CORE_INPUT_ACCEPTED.meta(),
    ])
}
