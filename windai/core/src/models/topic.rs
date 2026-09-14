use crate::{db::DbRow, storage::utils};
use serde::{Deserialize, Serialize};
use sqlx::Row;

/// 对话话题
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone)]
pub struct Topic {
    /// 唯一id
    pub id: i64,
    /// 父话题id
    pub parent_id: Option<i64>,
    /// 话题标签
    pub label: String,
    /// 使用的模型 id
    pub model_id: Option<i64>,
    /// 工具审批策略
    pub tool_approval_policy: Option<ToolApprovalPolicy>,
    /// 话题图标
    pub icon: Option<String>,
    /// 创建时间
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for Topic {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Topic {
            id: row.get("id"),
            model_id: row.get("model_id"),
            icon: row.get("icon"),
            created_at: row.get("created_at"),
            parent_id: row.get("parent_id"),
            label: row.get("label"),
            tool_approval_policy: match row.get::<Option<String>, _>("tool_approval_policy") {
                Some(s) => Some(utils::de_str_to(&s).map_err(|e| sqlx::Error::Decode(e.into()))?),
                _ => None,
            },
        })
    }
}

/// 工具审批策略
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "type", content = "tools", rename_all = "snake_case")]
pub enum ToolApprovalPolicy {
    /// 手动审批
    Manual,
    /// 允许名单
    AllowList(Vec<String>),
    /// 允许全部
    AllowAll,
}

impl Default for ToolApprovalPolicy {
    fn default() -> Self {
        Self::AllowAll
    }
}

/// 新增话题
#[derive(utoipa::ToSchema, Serialize, Deserialize, Debug, Clone)]
pub struct CreateTopic {
    /// 父话题id
    pub parent_id: Option<i64>,
    /// 话题标签
    pub label: String,
    /// 话题图标
    pub icon: Option<String>,
    /// 使用的模型 id
    pub model_id: Option<i64>,
    /// 工具审批策略。
    pub tool_approval_policy: Option<ToolApprovalPolicy>,
}

/// 更新话题
#[derive(utoipa::ToSchema, Serialize, Deserialize, Debug, Clone)]
pub struct UpdateTopic {
    /// 父话题id
    pub parent_id: Option<i64>,
    /// 话题标签
    pub label: Option<String>,
    /// 话题图标
    pub icon: Option<String>,
    /// 新的模型 id。
    pub model_id: Option<i64>,
    /// 工具审批策略。
    pub tool_approval_policy: Option<ToolApprovalPolicy>,
}

impl Default for UpdateTopic {
    fn default() -> Self {
        Self {
            label: None,
            icon: None,
            parent_id: None,
            model_id: None,
            tool_approval_policy: None,
        }
    }
}
