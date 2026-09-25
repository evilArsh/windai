use serde::Serialize;
use wind_ai::message::Message as AiMessage;

/// 统一对话事件，适用于流式和非流式模式
#[derive(Debug, Serialize, strum::AsRefStr)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ChatEvent {
    /// 分块内容
    Partial { delta: AiMessage },
    /// 终止该轮对话，并通知上层需要审批和调用 tool_call
    ///
    /// 最后一轮的 tools 可能是未被一次性处理完的剩余的请求
    AwaitToolCall { contexts: Vec<AiMessage> },
    /// 对话结束
    Finish {
        contexts: Vec<AiMessage>,
        /// 是否以错误结束
        error: bool,
    },
}
// TODO 重构完善
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
            ChatEvent::AwaitToolCall { contexts } => {
                (name_ref, format!("(contexts_len = {})", contexts.len()))
            }
            ChatEvent::Finish { contexts, .. } => {
                (name_ref, format!("(contexts_len = {})", contexts.len()))
            }
        };
        write!(f, "[ChatEvent {name}] {}", args)
    }
}
