use crate::db::DbRow;
use serde::{Deserialize, Serialize};
use sqlx::Row;

/// 对话话题
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Topic {
    pub id: i64,
    pub parent_id: Option<i64>,
    pub binding_id: Option<i64>,
    /// 话题标签
    pub label: String,
    pub icon: Option<String>,
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for Topic {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Topic {
            id: row.get("id"),
            binding_id: row.get("binding_id"),
            icon: row.get("icon"),
            created_at: row.get("created_at"),
            parent_id: row.get("parent_id"),
            label: row.get("label"),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "type", content = "tools", rename_all = "snake_case")]
pub enum ToolApprovalPolicy {
    Manual,
    AllowList(Vec<String>),
    AllowAll,
}

impl Default for ToolApprovalPolicy {
    fn default() -> Self {
        Self::AllowAll
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateTopic {
    pub parent_id: Option<i64>,
    pub binding_id: Option<i64>,
    pub label: String,
    pub icon: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateTopic {
    pub parent_id: Option<i64>,
    pub label: Option<String>,
    pub icon: Option<String>,
}

impl Default for UpdateTopic {
    fn default() -> Self {
        Self {
            label: None,
            icon: None,
            parent_id: None,
        }
    }
}
