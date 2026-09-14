use super::AgentRole;
use crate::db::DbRow;
use crate::storage::utils;
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
    /// agent 角色偏好
    pub role: Option<AgentRole>,
    /// 创建时间戳
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for TopicAgentMap {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.get("id"),
            topic_id: row.get("topic_id"),
            agent_id: row.get("agent_id"),
            role: match row.get::<Option<String>, _>("role") {
                Some(mode) => {
                    Some(utils::parse_str_to(&mode).map_err(|e| sqlx::Error::Decode(e.into()))?)
                }
                _ => None,
            },
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
    /// 角色
    pub role: Option<AgentRole>,
}

/// 更新 TopicAgentMap 的 DTO。
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone, Default)]
pub struct UpdateTopicAgentMap {
    pub role: Option<AgentRole>,
}
