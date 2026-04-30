use crate::api::request::ChatContext;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use windai_domain::{
    adaptor::AdaptorType,
    chat::{ContentType, Message, MessageContent, Role},
};

/// 提供商响应消息的统一转换格式
#[derive(Debug, Serialize, Deserialize, Clone, Builder)]
pub struct ChatMessageBase {
    /// 是否是流式消息
    #[builder(default)]
    pub stream: bool,
    /// 角色
    pub role: Role,
    /// 提供商返回的原始消息内容
    #[builder(default)]
    pub raw_content: String,
    /// 解析之后的文本数据
    #[builder(default)]
    pub content: String,
    /// 模型推理消息
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub reasoning_content: Option<String>,
    /// 语音转文字结果
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub transcript: Option<String>,
    /// 消息类型
    #[builder(default = "ContentType::Text")]
    pub content_type: ContentType,
    /// 创建时间
    #[builder(default)]
    pub created_at: i64,
    /// 用户输入的token数
    #[builder(default)]
    pub input_tokens: i32,
    /// 模型输出的token数
    #[builder(default)]
    pub output_tokens: i32,
}

impl ChatMessageBase {
    /// 流式数据更新
    /// - 拼接字段：
    /// `content`,`reasoning_content`,`transcript`
    /// - TODO: 更新 tool_call
    pub fn apply_to_message(self, message: &mut Message) {
        message.stream = self.stream;
        message.role = self.role;
        message.raw_content = self.raw_content;
        message.created_at = self.created_at;
        message.input_tokens += self.input_tokens;
        message.output_tokens += self.output_tokens;
        if let Some(first_content) = message.content.get_mut(0) {
            // assert!(first_content.content_type == self.content_type);
            first_content.content.push_str(&self.content);
        } else if message.content.is_empty() {
            message.content.push(MessageContent {
                content: self.content,
                content_type: self.content_type,
            });
        }
        if let Some(new_reasoning) = self.reasoning_content {
            match message.reasoning_content.as_mut() {
                Some(old_reasoning) => *old_reasoning += &new_reasoning,
                None => message.reasoning_content = Some(new_reasoning),
            }
        }
        if let Some(new_transcript) = self.transcript {
            match message.transcript.as_mut() {
                Some(old_transcript) => *old_transcript += &new_transcript,
                None => message.transcript = Some(new_transcript),
            }
        }
    }
}
/// 统一对话消息响应结构体
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatMessage {
    #[serde(flatten)]
    pub base: Message,
    /// 模型名字
    pub model_name: String,
    // 提供商姓名
    pub provider_name: String,
    // 提供商id
    pub provider_id: i64,
    // 适配器类型
    pub adaptor: AdaptorType,
}

impl ChatMessage {
    /// 将响应消息转换为统一消息请求上下文
    pub fn to_context(self) -> ChatContext {
        ChatContext {
            role: self.base.role,
            content: self.base.content,
        }
    }
}
