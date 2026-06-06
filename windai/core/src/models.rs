use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sqlx::Row;
use wind_ai::message;
use wind_ai::model::AdaptorType;
use wind_mcp::client::TransportType;

use crate::db::DbRow;
use crate::storage::{self, utils};

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
impl<'s> sqlx::FromRow<'s, DbRow> for Credentials {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Credentials {
            id: row.get("id"),
            active: row.get("active"),
            provider_id: row.get("provider_id"),
            key: row.get("key"),
            created_at: row.get("created_at"),
        })
    }
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
impl<'s> sqlx::FromRow<'s, DbRow> for Provider {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Provider {
            id: row.get("id"),
            name: row.get("name"),
            alias: row.get("alias"),
            created_at: row.get("created_at"),
            base_url: row.get("base_url"),
            description: row.get("description"),
            doc: row.get("doc"),
            active: row.get("active"),
        })
    }
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

impl<'s> sqlx::FromRow<'s, DbRow> for Model {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Model {
            id: row.get("id"),
            name: row.get("name"),
            provider_id: row.get("provider_id"),
            alias: row.get("alias"),
            adaptor: utils::parse_str_to(&row.get::<String, _>("adaptor")).map_err(|e| {
                sqlx::Error::Decode(format!("Failed to deserialize adaptor type: {}", e).into())
            })?,
            modalities: utils::de_str_to(&row.get::<String, _>("modalities")).map_err(|e| {
                sqlx::Error::Decode(format!("Failed to deserialize modalities: {}", e).into())
            })?,
            active: row.get("active"),
            icon: row.get("icon"),
            endpoint: row.get("endpoint"),
            frequency: row.get("frequency"),
            created_at: row.get("created_at"),
        })
    }
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
    /// topic级别自动执行的 tool_call 名；
    /// 名字包含MCP服务名称
    pub auto_approves: Option<Vec<String>>,
    /// 引用的 MCP 服务 id
    pub mcp_server_ids: Option<Vec<i64>>,
}
impl<'s> sqlx::FromRow<'s, DbRow> for Topic {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Topic {
            id: row.get("id"),
            icon: row.get("icon"),
            created_at: row.get("created_at"),
            parent_id: row.get("parent_id"),
            chat_config_id: row.get("chat_config_id"),
            label: row.get("label"),
            max_context: row.get("max_context"),
            index: row.get("topic_index"),
            auto_approves: utils::de_str_to(&row.get::<String, _>("auto_approves")).map_err(
                |e| {
                    sqlx::Error::Decode(
                        format!("Failed to deserialize auto_approves: {}", e).into(),
                    )
                },
            )?,
            mcp_server_ids: utils::de_str_to(&row.get::<String, _>("mcp_server_ids")).map_err(
                |e| {
                    sqlx::Error::Decode(
                        format!("Failed to deserialize mcp_server_ids: {}", e).into(),
                    )
                },
            )?,
        })
    }
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
    /// （工具调用）可调用的工具名称
    pub tools_allowed: Option<Vec<String>>,
    /// （工具调用）拒绝调用的工具名称
    pub tools_denied: Option<Vec<String>>,
    pub created_at: i64,
}
impl<'s> sqlx::FromRow<'s, DbRow> for Message {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        let parsed_content = storage::utils::de_str_to(
            row.try_get::<String, _>("content")?.as_str(),
        )
        .map_err(|e| {
            sqlx::Error::Decode(format!("Failed to deserialize message content: {}", e).into())
        })?;
        let tools_allowed =
            storage::utils::de_str_to(row.try_get::<String, _>("tools_allowed")?.as_str())
                .map_err(|e| {
                    sqlx::Error::Decode(
                        format!("Failed to deserialize tools_allowed: {}", e).into(),
                    )
                })?;
        let tools_denied = storage::utils::de_str_to(
            row.try_get::<String, _>("tools_denied")?.as_str(),
        )
        .map_err(|e| {
            sqlx::Error::Decode(format!("Failed to deserialize tools_denied : {}", e).into())
        })?;

        Ok(Self {
            id: row.try_get("id")?,
            from_id: row.try_get("from_id")?,
            stream: row.try_get("stream")?,
            content: parsed_content,
            model_id: row.try_get("model_id")?,
            topic_id: row.try_get("topic_id")?,
            index: row.try_get("message_index")?,
            is_boundary: row.try_get("is_boundary")?,
            is_excluded: row.try_get("is_excluded")?,
            input_tokens: row.try_get("input_tokens")?,
            output_tokens: row.try_get("output_tokens")?,
            tools_allowed,
            tools_denied,
            created_at: row.try_get("created_at")?,
        })
    }
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
impl<'s> sqlx::FromRow<'s, DbRow> for ChatConfig {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.get("id"),
            topic_id: row.get("topic_id"),
            data: message::ReqConfig {
                temperature: row.get("temperature"),
                top_p: row.get("top_p"),
                max_tokens: row.get("max_tokens"),
                stream: row.get("stream"),
                presence_penalty: row.get("presence_penalty"),
                frequency_penalty: row.get("frequency_penalty"),
                parallel_tool_calls: row.get("parallel_tool_calls"),
                reasoning: row.get("reasoning"),
            },
            created_at: row.get("created_at"),
        })
    }
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
impl<'s> sqlx::FromRow<'s, DbRow> for JsonRule {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(JsonRule {
            id: row.get("id"),
            provider_id: row.get("provider_id"),
            adaptor: utils::parse_str_to(&row.get::<String, _>("adaptor")).map_err(|e| {
                sqlx::Error::Decode(format!("Failed to deserialize adaptor type: {}", e).into())
            })?,
            active: row.get("active"),
            created_at: row.get("created_at"),
            json_rule: row.get("json_rule"),
        })
    }
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
    /// 允许自动执行的工具名,不包含服务名前缀
    pub auto_approves: Option<Vec<String>>,
    pub created_at: i64,
}
impl<'s> sqlx::FromRow<'s, DbRow> for McpServerParam {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        let r#type: TransportType =
            storage::utils::parse_str_to(row.get::<String, _>("type").as_str()).map_err(|e| {
                sqlx::Error::Decode(format!("deserialize type failed: {}", e).into())
            })?;
        let args = storage::utils::de_str_to(row.get::<String, _>("args").as_str())
            .map_err(|e| sqlx::Error::Decode(format!("deserialize args failed: {}", e).into()))?;
        let env = storage::utils::de_str_to(row.get::<String, _>("env").as_str())
            .map_err(|e| sqlx::Error::Decode(format!("deserialize env failed: {}", e).into()))?;
        let auto_approves =
            storage::utils::de_str_to(row.get::<String, _>("auto_approves").as_str()).map_err(
                |e| sqlx::Error::Decode(format!("deserialize auto_approves failed: {}", e).into()),
            )?;
        Ok(McpServerParam {
            id: row.get("id"),
            r#type,
            name: row.get("name"),
            url: row.get("url"),
            description: row.get("description"),
            command: row.get("command"),
            args,
            env,
            auto_approves,
            created_at: row.get("created_at"),
        })
    }
}

// ============ CRUD DTO  ============

pub struct CreateMessage {
    pub from_id: Option<i64>,
    pub stream: bool,
    pub content: Vec<wind_ai::message::Message>,
    pub model_id: i64,
    pub topic_id: i64,
    pub is_boundary: bool,
    pub input_tokens: i32,
    pub output_tokens: i32,
    pub tools_allowed: Option<Vec<String>>,
    pub tools_denied: Option<Vec<String>>,
}

pub struct UpdateMessage {
    pub content: Option<Vec<wind_ai::message::Message>>,
    pub model_id: Option<i64>,
    pub input_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
    pub tools_allowed: Option<Vec<String>>,
    pub tools_denied: Option<Vec<String>>,
}

impl Default for UpdateMessage {
    fn default() -> Self {
        Self {
            content: None,
            model_id: None,
            input_tokens: None,
            output_tokens: None,
            tools_allowed: None,
            tools_denied: None,
        }
    }
}

impl From<Message> for UpdateMessage {
    fn from(value: Message) -> Self {
        Self {
            content: Some(value.content),
            model_id: Some(value.model_id),
            input_tokens: Some(value.input_tokens),
            output_tokens: Some(value.output_tokens),
            tools_allowed: value.tools_allowed,
            tools_denied: value.tools_denied,
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
    pub mcp_server_ids: Option<Vec<i64>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateTopic {
    pub parent_id: Option<i64>,
    pub label: Option<String>,
    pub icon: Option<String>,
    pub max_context: Option<i32>,
    pub auto_approves: Option<Vec<String>>,
    pub mcp_server_ids: Option<Vec<i64>>,
}

impl Default for UpdateTopic {
    fn default() -> Self {
        Self {
            label: None,
            icon: None,
            max_context: None,
            parent_id: None,
            auto_approves: None,
            mcp_server_ids: None,
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct CreateJsonRule {
    pub provider_id: i64,
    pub adaptor: AdaptorType,
    pub json_rule: String,
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
    // TODO: 暂不使用
    // pub auto_approves: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct UpdateMcpServer {
    pub r#type: Option<TransportType>,
    pub name: String,
    pub url: Option<String>,
    pub description: Option<String>,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub env: Option<HashMap<String, String>>,
    // pub auto_approves: Option<Vec<String>>,
}
