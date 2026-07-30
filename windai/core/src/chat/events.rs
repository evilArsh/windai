use crate::{error::CoreError, models::Message};
use serde::Serialize;
use wind_ai::{message::Message as AiMessage, tool::FunctionCall};

/// 统一对话事件，适用于流式和非流式模式。
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ChatEvent {
    /// 流式消息分块内容
    /// FIXME(修改之后重新评估): - 在非流式请求中，也会返回多轮工具调用结果
    Partial {
        index: i32,
        message_id: i64,
        delta: AiMessage,
    },
    /// 终止该轮对话，并通知上层需要审批和调用 tool_call
    AwaitToolCall {
        message: Message,
        contexts: Vec<AiMessage>,
        tools: Vec<FunctionCall>,
    },
    /// 对话结束，可能包含错误
    Finish {
        message: Message,
        contexts: Vec<AiMessage>,
        // 出错信息
        error: Option<String>,
    },
}

impl ChatEvent {
    #[inline]
    pub fn partial(index: i32, message_id: i64, delta: AiMessage) -> Self {
        Self::Partial {
            index,
            message_id,
            delta,
        }
    }

    #[inline]
    pub fn finish(message: Message, contexts: Vec<AiMessage>, error: Option<CoreError>) -> Self {
        Self::Finish {
            message,
            contexts,
            error: error.map(|e| e.to_string()),
        }
    }

    #[inline]
    pub fn await_tool_calls(
        message: Message,
        contexts: Vec<AiMessage>,
        tools: Vec<FunctionCall>,
    ) -> Self {
        Self::AwaitToolCall {
            message,
            contexts,
            tools,
        }
    }
}
