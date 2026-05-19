use crate::error::CoreError;
use serde::Serialize;
use wind_ai::message::Message as AiMessage;

/// 统一聊天事件，适用于流式和非流式模式。
///
/// 非流式模式：返回单个 Response 事件。
/// 流式模式：返回 Created -> Partial x N -> Finished。
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ChatEvent {
    /// Streaming start: empty-content CoreMessage with metadata.
    Created { message_id: i64 },
    /// Streaming delta: raw AI Message fragment.
    Partial {
        index: i32,
        message_id: i64,
        delta: AiMessage,
    },
    /// Streaming complete: final AI Message.
    Finished { message_id: i64 },
    /// Non-streaming: fully persisted CoreMessage returned in one shot.
    Response {
        message_id: i64,
        message: Vec<AiMessage>,
    },
    /// Error
    Error { message_id: i64, error: String },
}

impl ChatEvent {
    pub fn created(message_id: i64) -> Self {
        Self::Created { message_id }
    }

    pub fn partial(index: i32, message_id: i64, delta: AiMessage) -> Self {
        Self::Partial {
            index,
            message_id,
            delta,
        }
    }

    pub fn finished(message_id: i64) -> Self {
        Self::Finished { message_id }
    }

    pub fn response(message_id: i64, message: Vec<AiMessage>) -> Self {
        Self::Response {
            message_id,
            message,
        }
    }

    pub fn error(message_id: i64, error: CoreError) -> Self {
        Self::Error {
            message_id,
            error: error.to_string(),
        }
    }
}
