use crate::db::DbRow;
use serde::{Deserialize, Serialize};
use sqlx::Row;

/// 可复用 prompt 模块。Agent 的 system prompt 由多个 PromptModule 按顺序组装。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PromptModule {
    /// PromptModule 主键。
    pub id: i64,
    /// 唯一短标识
    pub key: String,
    /// 用户可读名称。
    pub alias: String,
    /// 模块用途说明。
    pub description: String,
    /// Prompt 正文。
    pub content: String,
    /// 是否启用该 PromptModule。
    pub active: bool,
    /// 创建时间戳。
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for PromptModule {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.get("id"),
            key: row.get("key"),
            alias: row.get("name"),
            description: row.get("description"),
            content: row.get("content"),
            active: row.get("active"),
            created_at: row.get("created_at"),
        })
    }
}

/// AgentDefinition 对 PromptModule 的引用配置。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PromptModuleBinding {
    /// 被引用的 PromptModule id。
    pub prompt_module_id: i64,
    /// 该模块是否为 Agent 运行必需。
    pub required: bool,
    /// 当前绑定是否启用。
    pub enabled: bool,
}

/// 创建 PromptModule 的 DTO。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreatePromptModule {
    /// 稳定短标识。
    pub key: String,
    /// 用户可读名称。
    pub alias: String,
    /// 模块用途说明。
    pub description: String,
    /// Prompt 正文。
    pub content: String,
    /// 是否启用；None 时默认启用。
    pub active: Option<bool>,
}

/// 更新 PromptModule 的 DTO。
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct UpdatePromptModule {
    /// 新的稳定短标识。
    pub key: Option<String>,
    /// 新的用户可读名称。
    pub alias: Option<String>,
    /// 新的模块用途说明。
    pub description: Option<String>,
    /// 新的 Prompt 正文。
    pub content: Option<String>,
    /// 新的启用状态。
    pub active: Option<bool>,
}
