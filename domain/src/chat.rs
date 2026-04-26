use derive_builder::Builder;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy, strum::EnumString, strum::Display,
)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
    Developer,
}
/// 文本消息类型，当前文本消息细分为以下类型
///
/// - Text: 文本消息（纯文本对话）
/// - Image: 图片消息（分析图像并将其用作生成文本或音频的输入）
/// - Audio: 音频消息（音频和文本的输入与输出）
/// - File: 文件消息
#[derive(
    Debug, Serialize, Deserialize, PartialEq, Copy, Eq, Clone, strum::EnumString, strum::Display,
)]
#[serde(rename_all = "lowercase")]
pub enum ContentType {
    Text,
    Image,
    Audio,
    File,
}

/// 聊天话题
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Topic {
    /// 对话id
    pub id: i64,
    /// 父话题id
    pub parent_id: Option<i64>,
    /// 话题标签
    pub label: String,
    /// 话题图标
    pub icon: Option<String>,
    /// 创建时间
    pub created_at: i64,
    /// 最大上下文长度
    pub max_context: i32,
    /// 当前会话序号
    pub index: i64,
}

/// LLM 对话消息内容
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageContent {
    /// 消息内容
    /// - 纯文本消息
    /// - 图片消息
    /// - 音频消息
    /// - 文件消息
    pub content: String,
    /// 消息类型
    /// - 标识文本，语音，图片，文件消息
    pub content_type: ContentType,
}
/// 消息结构
/// TODO: tool_calls
#[derive(Debug, Serialize, Deserialize, Clone, Builder)]
#[builder(setter(strip_option, into))]
pub struct Message {
    /// 消息id
    #[builder(default)]
    pub id: i64,
    /// 是否是流式消息
    #[builder(default)]
    pub stream: bool,
    /// 标识该消息是来自哪条用户消息的响应
    #[builder(default)]
    pub from_id: Option<i64>,
    /// 角色
    pub role: Role,
    /// 提供商返回的原始消息内容
    #[builder(default)]
    pub raw_content: String,
    /// 模型响应消息或用户输入消息集合
    /// - 用户可能输入多种模态的消息；列如：文本，图片
    #[builder(default)]
    pub content: Vec<MessageContent>,
    /// 模型推理消息
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub reasoning_content: Option<String>,
    /// 语音转文字结果
    #[builder(default)]
    pub transcript: Option<String>,
    /// 创建时间
    #[builder(default)]
    pub created_at: i64,
    /// 模型id
    #[builder(default)]
    pub model_id: i64,
    /// 模型生成的消息所属的会话id
    #[builder(default)]
    pub topic_id: i64,
    /// 模型生成的消息在会话中的位置序号
    /// - 会话中的消息按照index排序，并且可以从中间插入消息，但是可插入次数有上限
    #[builder(default)]
    pub index: i64,
    /// 标识当前消息作为聊天上下文分割点
    #[builder(default)]
    pub is_boundary: bool,
    /// 用户输入的token数
    #[builder(default)]
    pub input_tokens: i32,
    /// 模型输出的token数
    #[builder(default)]
    pub output_tokens: i32,
}
