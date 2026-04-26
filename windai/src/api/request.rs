use crate::domain::chat::{ContentType, MessageContent, Role};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

/// LLM 对话请求配置
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ChatConfig {
    /// 采样温度，范围 0~2。较高值使输出更随机，较低值使输出更聚焦。
    /// 通常建议只调 temperature 或 top_p 之一。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    /// 核采样阈值。模型只考虑累积概率达到 top_p 的候选 token。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    /// 最大输出 token 数。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<i32>,
    /// 是否启用流式输出
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    /// 存在性惩罚，-2.0 ~ 2.0。正值增加模型讨论新话题的可能性。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f64>,
    /// 频率惩罚，-2.0 ~ 2.0。正值降低模型逐字重复的可能性。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f64>,
    /// 是否在工具调用期间启用并行函数调用。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    /// 是否开启推理模式。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<bool>,
}

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

/// 统一对话消息上下文请求结构
#[derive(Debug, Serialize, Deserialize, Clone, Builder)]
pub struct ChatMessageContext {
    /// 角色
    pub role: Role,
    /// 解析之后的文本数据
    /// - TODO: 如果将音频数据放入上下文，放入字节数据还是 [Message::transcript]
    #[builder(default)]
    pub content: Vec<MessageContent>,
}
