use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use wind_ai::message::{self};
use wind_ai::model::AdaptorType;
use wind_mcp::client::TransportType;

/// 模态类型, 用于UI展示
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, strum::EnumString, strum::Display)]
pub enum ModelType {
    Chat,
    Embedding,
    Reranker,
    Audio,
    Video,
}

/// 提供商账号
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Credentials {
    pub id: i64,
    pub provider_id: i64,
    pub key: String,
    pub created_at: i64,
    pub active: bool,
}

/// 提供商
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Provider {
    pub id: i64,
    /// 唯一的提供商名字
    pub name: String,
    /// 提供商 base api 地址
    pub base_url: String,
    /// 提供商描述
    pub description: Option<String>,
    /// 提供商官方文档地址
    pub doc: Option<String>,
    /// 提供商别名
    pub alias: Option<String>,
    pub active: bool,
    pub created_at: i64,
}

/// 模型结构
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Model {
    pub id: i64,
    /// 提供商提供的模型名称
    pub name: String,
    pub provider_id: i64,
    /// 自定义模型别名
    pub alias: Option<String>,
    /// 当前模型的适配器类型。
    /// 该类型决定了模型请求和响应结果的处理方式
    pub adaptor: AdaptorType,
    /// 标注模态类型
    pub modalities: Option<Vec<ModelType>>,
    /// 模型是否启用
    pub active: bool,
    /// 模型图标
    pub icon: Option<String>,
    /// 模型专属端点地址
    ///
    /// 默认使用[AdaptorType]类型下的不同提供商的默认端点。
    pub endpoint: Option<String>,
    /// 模型使用次数统计
    pub frequency: Option<i32>,
    pub created_at: i64,
}

/// 聊天话题
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Topic {
    pub id: i64,
    pub parent_id: Option<i64>,
    /// 关联的对话配置
    pub chat_config_id: i64,
    /// 话题标签
    pub label: String,
    pub icon: Option<String>,
    /// 最大上下文长度
    pub max_context: Option<i32>,
    /// 当前会话序号
    pub index: i64,
    pub created_at: i64,
}

/// 消息结构
#[derive(Debug, Serialize, Clone)]
pub struct Message {
    pub id: i64,
    /// 标识该响应所对应的原始用户消息ID
    /// - 当多个模型同时响应同一条用户消息时，用于关联回复与原始消息
    /// - 当为None时，该消息是用户消息
    pub from_id: Option<i64>,
    pub stream: bool,
    /// 消息内容。
    /// - 在单次对话中，如果存在多轮工具调用，该字段按顺序记录所有的调用结果；
    /// 包含模型选择的工具列表，用户工具调用结果，以及模型自然语言回复
    /// - 用户消息不存在多轮MCP对话，只有一个结果
    pub content: Vec<message::Message>,
    pub model_id: i64,
    pub topic_id: i64,
    /// 模型生成的消息在会话中的位置序号
    /// - 会话中的消息按照index排序，并且可以从中间插入消息，但是可插入次数有上限
    pub index: i64,
    /// 标识当前消息作为聊天上下文分割点
    pub is_boundary: bool,
    /// 被排除的消息不会作为对话上下文
    pub is_excluded: bool,
    /// 用户输入的token数
    pub input_tokens: i32,
    /// 模型输出的token数
    pub output_tokens: i32,
    pub created_at: i64,
}
impl Message {
    pub fn append_content(&mut self, message: &message::Message) {
        self.input_tokens += message.input_tokens;
        self.output_tokens += message.output_tokens;
        self.content.push(message.clone());
    }
}
/// 对话消息请求配置
#[derive(Debug, Serialize, Clone)]
pub struct ChatConfig {
    pub id: i64,
    pub topic_id: i64,
    #[serde(flatten)]
    pub data: message::ReqConfig,
    pub created_at: i64,
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

/// JSON 规则，用于用户手动处理模型请求配置
#[derive(Debug, Serialize, Clone)]
pub struct JsonRule {
    pub id: i64,
    pub provider_id: i64,
    pub adaptor: AdaptorType,
    pub json_rule: String,
    pub active: bool,
    pub created_at: i64,
}

/// MCP 服务配置，(Stdio, Streamable-HTTP)
#[derive(Debug, Serialize, Clone)]
pub struct McpServerParam {
    pub id: i64,
    pub r#type: TransportType,
    /// 服务名称
    pub name: String,
    /// 服务地址
    pub url: Option<String>,
    /// 描述
    pub description: Option<String>,
    /// 启动命令
    pub command: Option<String>,
    /// 启动参数
    pub args: Option<Vec<String>>,
    /// 环境变量
    pub env: Option<HashMap<String, String>>,
    pub created_at: i64,
}

// ============ CRUD DTO  ============

pub struct CreateMessage {
    pub from_id: Option<i64>,
    pub stream: bool,
    pub content_json: String,
    pub model_id: i64,
    pub topic_id: i64,
    pub is_boundary: bool,
    pub input_tokens: i32,
    pub output_tokens: i32,
}

pub struct UpdateMessage {
    pub content_json: Option<String>,
    pub model_id: Option<i64>,
    pub input_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
}

impl Default for UpdateMessage {
    fn default() -> Self {
        Self {
            content_json: None,
            model_id: None,
            input_tokens: None,
            output_tokens: None,
        }
    }
}

impl From<Message> for UpdateMessage {
    fn from(value: Message) -> Self {
        Self {
            content_json: Some(
                serde_json::to_string(&value.content).unwrap_or_else(|_| "[]".to_string()),
            ),
            model_id: Some(value.model_id),
            input_tokens: Some(value.input_tokens),
            output_tokens: Some(value.output_tokens),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateProvider {
    pub name: String,
    pub description: Option<String>,
    pub base_url: String,
    pub doc: Option<String>,
    pub alias: Option<String>,
    pub active: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateProvider {
    pub name: Option<String>,
    pub description: Option<String>,
    pub base_url: Option<String>,
    pub doc: Option<String>,
    pub alias: Option<String>,
    pub active: Option<bool>,
}

impl Default for UpdateProvider {
    fn default() -> Self {
        Self {
            name: None,
            description: None,
            base_url: None,
            doc: None,
            alias: None,
            active: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateCredentials {
    pub provider_id: i64,
    pub key: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateModel {
    pub name: String,
    pub provider_id: i64,
    pub alias: Option<String>,
    pub adaptor: AdaptorType,
    pub modalities: Option<Vec<ModelType>>,
    pub active: Option<bool>,
    pub icon: Option<String>,
    pub endpoint: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateModel {
    pub name: Option<String>,
    pub alias: Option<String>,
    pub adaptor: Option<AdaptorType>,
    pub modalities: Option<Vec<ModelType>>,
    pub active: Option<bool>,
    pub icon: Option<String>,
    pub endpoint: Option<String>,
    pub frequency: Option<i32>,
}

impl Default for UpdateModel {
    fn default() -> Self {
        Self {
            name: None,
            alias: None,
            adaptor: None,
            modalities: None,
            active: None,
            icon: None,
            endpoint: None,
            frequency: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateTopic {
    pub parent_id: Option<i64>,
    pub chat_config_id: i64,
    pub label: String,
    pub icon: Option<String>,
    pub max_context: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateTopic {
    pub parent_id: Option<i64>,
    pub label: Option<String>,
    pub icon: Option<String>,
    pub max_context: Option<i32>,
}

impl Default for UpdateTopic {
    fn default() -> Self {
        Self {
            label: None,
            icon: None,
            max_context: None,
            parent_id: None,
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct CreateJsonRule {
    pub provider_id: i64,
    pub adaptor: AdaptorType,
    pub json_rule: String,
    pub active: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct UpdateJsonRule {
    pub provider_id: Option<i64>,
    pub adaptor: Option<AdaptorType>,
    pub json_rule: Option<String>,
    pub active: Option<bool>,
}

#[derive(Debug, Serialize, Clone)]
pub struct CreateMcpServer {
    pub r#type: TransportType,
    pub name: String,
    pub url: Option<String>,
    pub description: Option<String>,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub env: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct UpdateMcpServer {
    pub r#type: Option<TransportType>,
    pub name: Option<String>,
    pub url: Option<String>,
    pub description: Option<String>,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub env: Option<HashMap<String, String>>,
}
