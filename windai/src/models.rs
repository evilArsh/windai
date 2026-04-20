use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use std::env;

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

/// 消息结构
/// TODO: tool_calls
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Message {
    /// 消息id
    pub id: i64,
    /// 是否是流式消息
    pub stream: bool,
    /// 标识该消息是来自哪条用户消息的响应
    // #[builder(default)]
    pub from_id: Option<i64>,
    /// 角色
    pub role: String,
    /// 提供商返回的原始消息内容
    pub raw_content: String,
    /// 解析之后的文本数据
    pub content: String,
    /// 模型推理消息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    /// 语音转文字结果
    pub transcript: Option<String>,
    /// 消息类型
    /// - 标识文本，语音，图片消息
    pub content_type: ContentType,
    /// 创建时间
    pub created_at: i64,
    /// 模型id
    pub model_id: i64,
    /// 模型生成的消息所属的会话id
    pub topic_id: i64,
    /// 模型生成的消息在会话中的位置序号
    /// - 会话中的消息按照index排序，并且可以从中间插入消息，但是可插入次数有上限
    pub index: i64,
    /// 标识当前消息作为聊天上下文分割点
    pub is_boundary: bool,
    /// 用户输入的token数
    pub input_tokens: i32,
    /// 模型输出的token数
    pub output_tokens: i32,
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Hash,
    Copy,
    PartialEq,
    Eq,
    Clone,
    strum::EnumString,
    strum::Display,
)]
pub enum AdaptorType {
    OpenAICompletion,
    OpenAIResponse,
}

/// 模型的模态类型
#[derive(Serialize, Deserialize, Clone, PartialEq, strum::EnumString, strum::Display)]
pub enum ModelType {
    Chat,
    Embedding,
    Reranker,
    Audio,
    Video,
}

#[derive(Serialize, Deserialize, Builder)]
pub struct Model {
    pub id: i64,
    /// 提供商提供的模型名称
    pub name: String,
    /// 自定义模型别名
    #[builder(default)]
    pub alias: Option<String>,
    /// 模型所属的提供商id
    #[builder(default)]
    pub provider_id: i64,
    /// 当前模型的适配器类型。
    /// 该类型决定了模型请求和响应结果的处理方式
    pub adaptor: AdaptorType,
    /// 模型的模态类型
    pub modalities: Vec<ModelType>,
    /// 模型是否使用
    pub active: bool,
    /// 模型图标
    pub icon: Option<String>,
    /// 模型专属端点地址
    ///
    /// 默认使用[AdaptorType]类型下的不同提供商的默认端点。
    #[builder(default)]
    pub endpoint: Option<String>,
    /// 模型使用次数统计
    pub frequency: Option<i32>,
}

/// 提供商账号信息
#[derive(Serialize, Deserialize)]
pub struct Credentials {
    pub id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<i64>,
    pub key: String,
}
impl Credentials {
    pub fn from_env() -> Self {
        let api_key = env::var("API_KEY").unwrap_or("".to_string());
        Credentials {
            id: 0,
            provider_id: None,
            key: api_key,
        }
    }
}

/// 提供商
#[derive(Serialize, Deserialize)]
pub struct Provider {
    pub id: i64,
    /// 唯一的提供商名字
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 提供商 base api 地址
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// 提供商官方文档地址
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// 提供商别名
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    pub active: bool,
}
