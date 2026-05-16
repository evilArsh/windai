use serde::{Deserialize, Serialize};
use wind_ai::message::{self};
// use derive_builder::Builder;

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
    /// 对话配置id
    pub chat_conf_id: i64,
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
#[derive(Debug, Serialize, Clone)]
// #[builder(setter(strip_option, into))]
pub struct Message {
    /// 消息id
    pub id: i64,
    /// 是否流式消息
    pub stream: bool,
    /// 标识该响应所对应的原始用户消息ID
    /// - 当多个模型同时响应同一条用户消息时，用于关联回复与原始消息
    /// - 当为None时，该消息是用户消息
    pub from_id: Option<i64>,
    /// 模型响应消息集合
    /// - 模型单次请求可能包含多轮对话记录
    pub content: Vec<message::Message>,
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
}

/// 对话消息请求配置
#[derive(Debug, Serialize, Clone)]
pub struct ChatConfig {
    /// 消息id
    pub id: i64,

    #[serde(flatten)]
    pub data: message::ReqConfig,
}
