use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::db::DbRow;
use crate::storage::utils;

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
    /// 当前会话序号
    pub index: i64,
    pub created_at: i64,
    /// 工具审批策略。
    pub tool_approval_policy: ToolApprovalPolicy,
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
            index: row.get("topic_index"),
            tool_approval_policy: utils::de_str_to(&row.get::<String, _>("tool_approval_policy"))
                .map_err(|e| {
                sqlx::Error::Decode(
                    format!("Failed to deserialize tool_approval_policy: {}", e).into(),
                )
            })?,
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
    pub chat_config_id: i64,
    pub label: String,
    pub icon: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateTopic {
    pub parent_id: Option<i64>,
    pub label: Option<String>,
    pub icon: Option<String>,
    pub tool_approval_policy: Option<ToolApprovalPolicy>,
}

impl Default for UpdateTopic {
    fn default() -> Self {
        Self {
            label: None,
            icon: None,
            parent_id: None,
            tool_approval_policy: None,
        }
    }
}
