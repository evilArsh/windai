use serde::{Deserialize, Serialize};
use sqlx::Row;
use wind_ai::message;

use crate::db::DbRow;
use crate::storage;

/// 消息结构
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Message {
    pub id: i64,
    /// 标识该响应所对应的原始用户消息ID
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
    /// 标识当前消息作为聊天上下文分割点
    pub is_boundary: bool,
    /// 被排除的消息不会作为对话上下文
    ///
    /// user-assistant消息对必须同时不被排除才能作为上下文
    pub is_excluded: bool,
    /// 用户输入的token数
    pub input_tokens: i32,
    /// 模型输出的token数
    pub output_tokens: i32,
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
        Ok(Self {
            id: row.try_get("id")?,
            from_id: row.try_get("from_id")?,
            stream: row.try_get("stream")?,
            content: parsed_content,
            model_id: row.try_get("model_id")?,
            topic_id: row.try_get("topic_id")?,
            is_boundary: row.try_get("is_boundary")?,
            is_excluded: row.try_get("is_excluded")?,
            input_tokens: row.try_get("input_tokens")?,
            output_tokens: row.try_get("output_tokens")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl Message {
    pub fn append_content(&mut self, message: message::Message) {
        self.input_tokens += message.input_tokens;
        self.output_tokens += message.output_tokens;
        self.content.push(message);
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

pub struct CreateMessage {
    pub from_id: Option<i64>,
    pub stream: bool,
    pub content: Vec<message::Message>,
    pub model_id: i64,
    pub topic_id: i64,
    pub is_boundary: bool,
    pub is_exclude: bool,
    pub input_tokens: i32,
    pub output_tokens: i32,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateMessage {
    pub content: Option<Vec<message::Message>>,
    pub model_id: Option<i64>,
    pub input_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
}

impl Default for UpdateMessage {
    fn default() -> Self {
        Self {
            content: None,
            model_id: None,
            input_tokens: None,
            output_tokens: None,
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
        }
    }
}
