use serde::Serialize;
use sqlx::Row;
use wind_ai::message;

use crate::db::DbRow;

/// 对话消息请求配置
#[derive(Debug, Serialize, Clone)]
pub struct ChatConfig {
    pub id: i64,
    #[serde(flatten)]
    pub data: message::ReqConfig,
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for ChatConfig {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.get("id"),
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
