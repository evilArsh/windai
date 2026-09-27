use serde::Serialize;
use wind_ai::message::Message;

/// 统一对话事件，适用于流式和非流式模式
#[derive(Debug, Serialize, strum::AsRefStr)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ChatEvent {
    /// 分块内容
    ///
    /// 非流式请求下，返回一次 Partial 包含完整消息
    Partial { delta: Message },
    /// 对话结束
    Finish {
        /// 本轮结束后交回的完整上下文，由调用方追加本轮后续消息
        contexts: Vec<Message>,
        /// 错误信息，`None` 表示正常结束
        error: Option<String>,
    },
}
impl std::fmt::Display for ChatEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name_ref = self.as_ref();
        let (name, args) = match self {
            ChatEvent::Partial { delta } => (
                name_ref,
                format!(
                    "(message_len = {}, calls_len = {})",
                    delta.content.len(),
                    delta.tool_calls.as_ref().map(|t| t.len()).unwrap_or(0)
                ),
            ),
            ChatEvent::Finish { contexts, error } => (
                name_ref,
                format!(
                    "(contexts_len = {}, error = {})",
                    contexts.len(),
                    error.as_deref().unwrap_or("")
                ),
            ),
        };
        write!(f, "[ChatEvent {name}] {}", args)
    }
}
