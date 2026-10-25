use serde::{Deserialize, Serialize};
use wind_event::def::{Directory, EventDef, Mount, Shape};
use wind_stream::id::MessageId;
use wind_stream::payload::{BlockCommitted, BlockDelta, BlockOpened};

/// 一次模型 API 往返的用量
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Usage {
    pub input_tokens: i64,
    pub output_tokens: i64,
}

/// 一次模型 API 往返的结束标记
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MessageSeal {
    /// 本次往返的消息标识
    #[schemars(with = "i64")]
    pub message_id: MessageId,
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

fn schema<P: schemars::JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(P)).expect("the generated schema is valid JSON")
}

fn block_opened_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["block_id", "of_message", "role", "kind"],
        "properties": {
            "block_id": { "type": "string" },
            "of_message": { "type": "integer" },
            "role": { "type": "string" },
            "kind": { "type": "string" },
            "call_id": { "type": "string" },
        }
    })
}

fn block_delta_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["block_id", "revision", "fragment"],
        "properties": {
            "block_id": { "type": "string" },
            "revision": { "type": "integer" },
            "fragment": { "type": "string" },
        }
    })
}

fn block_committed_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["block_id", "kind", "revision"],
        "properties": {
            "block_id": { "type": "string" },
            "kind": { "type": "string" },
            "call_id": { "type": "string" },
            "content": { "type": "string" },
            "data": true,
            "revision": { "type": "integer" },
        }
    })
}

pub const CORE_MESSAGE_SEAL: EventDef<MessageSeal> = EventDef::new(
    "core/message.seal",
    Shape::Observe,
    true,
    Mount::TranslationLayer,
    schema::<MessageSeal>,
);

pub const CORE_BLOCK_OPEN: EventDef<BlockOpened> = EventDef::new(
    "core/block.open",
    Shape::Observe,
    true,
    Mount::TranslationLayer,
    block_opened_schema,
);

pub const CORE_BLOCK_DELTA: EventDef<BlockDelta> = EventDef::new(
    "core/block.delta",
    Shape::Observe,
    false,
    Mount::TranslationLayer,
    block_delta_schema,
);

pub const CORE_BLOCK_COMMITTED: EventDef<BlockCommitted> = EventDef::new(
    "core/block.committed",
    Shape::Observe,
    true,
    Mount::TranslationLayer,
    block_committed_schema,
);

pub const CORE_INPUT_ACCEPTED: EventDef<InputAccepted> = EventDef::new(
    "core/input.accepted",
    Shape::Observe,
    true,
    Mount::UseCase,
    schema::<InputAccepted>,
);

pub fn directory() -> Directory {
    Directory::from_metas([
        CORE_MESSAGE_SEAL.meta(),
        CORE_BLOCK_OPEN.meta(),
        CORE_BLOCK_DELTA.meta(),
        CORE_BLOCK_COMMITTED.meta(),
        CORE_INPUT_ACCEPTED.meta(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_accepted_input_is_an_observed_use_case_event() {
        let dir = directory();
        let meta = dir
            .get("core/input.accepted")
            .expect("find the input event");
        assert_eq!(meta.shape, Shape::Observe);
        assert_eq!(meta.mount, Mount::UseCase);
        assert!(meta.log);
        let schema = (meta.schema)();
        let required = schema["required"].as_array().expect("required");
        assert!(required.contains(&serde_json::json!("topic_id")));
        assert!(required.contains(&serde_json::json!("instance_id")));
        assert!(required.contains(&serde_json::json!("pending_id")));
    }
}
