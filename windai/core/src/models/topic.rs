use crate::db::DbRow;
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
    /// 话题图标
    pub icon: Option<String>,
    /// 创建时间
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for Topic {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Topic {
            id: row.get("id"),
            // binding_id: row.get("binding_id"),
            icon: row.get("icon"),
            created_at: row.get("created_at"),
            parent_id: row.get("parent_id"),
            label: row.get("label"),
        })
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
