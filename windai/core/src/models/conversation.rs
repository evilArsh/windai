use serde::{Deserialize, Serialize};
pub use wind_stream::block::BlockKind;
pub use wind_wire::message::Role;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Turn {
    pub id: i64,
    pub instance_id: i64,
    pub topic_id: i64,
    pub from_id: Option<i64>,
    pub is_boundary: bool,
    pub created_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnCreate {
    pub instance_id: i64,
    pub topic_id: i64,
    pub from_id: Option<i64>,
    pub is_boundary: bool,
    pub created_at: Option<i64>,
}

impl_display!(Turn {
    id,
    instance_id,
    topic_id,
    from_id,
    is_boundary,
    created_at
});

impl_display!(TurnCreate {
    instance_id,
    topic_id,
    from_id,
    is_boundary,
    created_at
});

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub id: i64,
    pub turn_id: i64,
    pub seq: i64,
    pub role: Role,
    pub model_id: Option<i64>,
    pub finish_reason: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub created_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageCreate {
    pub turn_id: i64,
    pub role: Role,
    pub model_id: Option<i64>,
    pub finish_reason: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub created_at: Option<i64>,
}

impl_display!(Message {
    id,
    turn_id,
    seq,
    role,
    model_id,
    finish_reason,
    input_tokens,
    output_tokens,
    created_at
});

impl_display!(MessageCreate {
    turn_id,
    role,
    model_id,
    finish_reason,
    input_tokens,
    output_tokens,
    created_at
});

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockRow {
    pub id: i64,
    pub message_id: i64,
    pub seq: i64,
    pub kind: BlockKind,
    pub call_id: Option<String>,
    pub revision: i64,
    pub content: String,
    pub data: Option<serde_json::Value>,
    pub finished: bool,
}

impl_display!(BlockRow {
    id,
    message_id,
    seq,
    kind,
    call_id,
    revision,
    content,
    data,
    finished
});

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockCreate {
    pub id: i64,
    pub message_id: i64,
    pub kind: BlockKind,
    pub call_id: Option<String>,
    pub revision: i64,
    pub content: String,
    pub data: Option<serde_json::Value>,
    pub finished: bool,
}

impl_display!(BlockCreate {
    id,
    message_id,
    kind,
    call_id,
    revision,
    content,
    data,
    finished
});

/// 一条还没有对应 tool_result 的工具调用
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UnpairedToolCall {
    pub turn_id: i64,
    pub message_id: i64,
    pub call_id: String,
    pub data: Option<serde_json::Value>,
}

impl BlockRow {
    pub fn encode_data(&self) -> Result<Option<String>, serde_json::Error> {
        self.data.as_ref().map(serde_json::to_string).transpose()
    }

    pub fn decode_data(raw: Option<&str>) -> Result<Option<serde_json::Value>, serde_json::Error> {
        raw.map(serde_json::from_str).transpose()
    }
}

impl BlockCreate {
    pub fn encode_data(&self) -> Result<Option<String>, serde_json::Error> {
        self.data.as_ref().map(serde_json::to_string).transpose()
    }
}
