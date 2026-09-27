use crate::db::DbRow;
use crate::storage::utils;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use wind_ai::message::Message as AiMessage;

/// 消息结构
///
/// 只承载会话结构，正文拆分到 [`MessageContent`] 存放在 `message_contents` 表中
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone)]
pub struct Message {
    /// 唯一 id
    pub id: i64,
    /// 标识该响应所对应的原始用户消息 ID
    /// - 当为 None 时，该消息是用户消息
    pub from_id: Option<i64>,
    /// 模型 ID
    pub model_id: i64,
    /// 该消息属于指定 instance
    pub instance_id: i64,
    /// 标识当前消息作为聊天上下文分割点
    pub is_boundary: bool,
    /// 被排除的消息不会作为对话上下文
    ///
    /// user-assistant 消息对必须同时不被排除才能作为上下文
    pub is_excluded: bool,
    /// 用户输入的 token 数，取全部 [`MessageContent`] 的汇总
    pub input_tokens: i32,
    /// 模型输出的 token 数，取全部 [`MessageContent`] 的汇总
    pub output_tokens: i32,
    /// 创建时间
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for Message {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            from_id: row.try_get("from_id")?,
            model_id: row.try_get("model_id")?,
            instance_id: row.try_get("instance_id")?,
            is_boundary: row.try_get("is_boundary")?,
            is_excluded: row.try_get("is_excluded")?,
            input_tokens: row.try_get("input_tokens")?,
            output_tokens: row.try_get("output_tokens")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

/// 从数据库列解码失败时统一转成 [`sqlx::Error`]
fn decode_err(err: crate::error::CoreError) -> sqlx::Error {
    sqlx::Error::Decode(err.to_string().into())
}

/// 一条消息中的一块内容
///
/// 一整轮内容（模型响应、工具调用结果）各自成块，块顺序由自增 id 决定，
/// 避免单条消息一次性写入巨量数据
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone)]
pub struct MessageContent {
    /// 唯一 id，同时标识块在消息内的生成顺序
    pub id: i64,
    /// 标识当前内容来自该消息
    pub message_id: i64,
    /// 详细内容
    pub data: AiMessage,
}

impl<'s> sqlx::FromRow<'s, DbRow> for MessageContent {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            message_id: row.try_get("message_id")?,
            data: AiMessage {
                role: utils::parse_str_to(&row.try_get::<String, _>("role")?)
                    .map_err(decode_err)?,
                content: utils::de_str_to(&row.try_get::<String, _>("content")?)
                    .map_err(decode_err)?,
                reasoning_content: row.try_get("reasoning_content")?,
                created_at: row.try_get("created_at")?,
                input_tokens: row.try_get("input_tokens")?,
                output_tokens: row.try_get("output_tokens")?,
                tool_calls: match row.try_get::<Option<String>, _>("tool_calls")? {
                    Some(calls) => utils::de_str_to(&calls).map_err(decode_err)?,
                    None => None,
                },
            },
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

pub struct CreateMessage {
    pub from_id: Option<i64>,
    pub model_id: i64,
    pub instance_id: i64,
    pub is_boundary: bool,
    pub is_excluded: bool,
    pub input_tokens: i32,
    pub output_tokens: i32,
}

/// 创建 [`MessageContent`]，块顺序即插入顺序
pub struct CreateMessageContent {
    pub message_id: i64,
    pub data: AiMessage,
}

/// 更新消息
///
/// 正文由 [`CreateMessageContent`] 写入、token 由内容汇总刷新，两者都不在此处更新
#[derive(utoipa::ToSchema, Serialize, Deserialize, Default)]
pub struct UpdateMessage {
    /// 模型 ID
    pub model_id: Option<i64>,
}
