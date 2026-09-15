use crate::db::DbRow;
use serde::{Deserialize, Serialize};
use sqlx::Row;

/// Topic Agent 能力绑定
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone)]
pub struct TopicAgentMap {
    pub id: i64,
    /// topic id
    pub topic_id: i64,
    /// 被绑定的 AgentDefinition id
    pub agent_id: i64,
    /// 创建时间戳
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for TopicAgentMap {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.get("id"),
            topic_id: row.get("topic_id"),
            agent_id: row.get("agent_id"),
            created_at: row.get("created_at"),
        })
    }
}

/// 创建 TopicAgentMap 的 DTO
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone)]
pub struct CreateTopicAgentMap {
    /// topic id
    pub topic_id: i64,
    /// 绑定 AgentDefinition id
    pub agent_id: i64,
}
