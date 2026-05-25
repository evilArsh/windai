use crate::error::CoreError;
use serde::Serialize;
use wind_ai::message::Message as AiMessage;

/// 统一聊天事件，适用于流式和非流式模式。
///
/// 非流式模式：返回单个 Finish 事件；或在多轮 tool_call 中返回 多个Partial 事件。
/// 流式模式：返回 Created -> Partial x N -> Finished。
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ChatEvent {
    /// 此次对话已创建并开始
    Created { message_id: i64 },
    /// 流式消息分块内容
    /// - 在非流式请求中，也会返回多轮工具调用结果
    Partial {
        index: i32,
        message_id: i64,
        delta: AiMessage,
    },
    /// 对话结束，可能包含错误
    Finish {
        message_id: i64,
        // 该轮对话完整信息，非流式对话中，包含所有响应结果
        message: Option<Vec<AiMessage>>,
        /// 出错信息
        error: Option<String>,
    },
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

    pub fn finish(
        message_id: i64,
        message: Option<Vec<AiMessage>>,
        error: Option<CoreError>,
    ) -> Self {
        Self::Finish {
            message_id,
            message,
            error: error.map(|e| e.to_string()),
        }
    }
}
