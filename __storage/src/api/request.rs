use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use wind_domain::chat::{ContentType, MessageContent, Role};

/// LLM 对话用户输入消息
#[derive(Debug, Serialize, Deserialize, Clone, Builder)]
pub struct ChatInput {
    /// 用户输入的数据
    #[builder(default)]
    pub content: String,
    /// 消息类型
    #[builder(default = "ContentType::Text")]
    pub content_type: ContentType,
}
impl ChatInput {
    /// 将用户输入的消息转换为统一消息类型
    pub fn to_content(self) -> MessageContent {
        MessageContent {
            content: self.content,
            content_type: self.content_type,
        }
    }
}
