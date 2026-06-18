use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::db::DbRow;
use crate::storage::utils;

/// 可复用 prompt 模块。Agent 的 system prompt 由多个 PromptModule 按顺序组装。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PromptModule {
    /// PromptModule 主键。
    pub id: i64,
    /// 面向 UI、配置和导入导出的稳定短标识。
    pub key: String,
    /// 用户可读名称。
    pub name: String,
    /// 模块用途说明。
    pub description: String,
    /// Prompt 模块类型。
    pub module_type: PromptModuleType,
    /// Prompt 正文。
    pub content: String,
    /// 是否启用该 PromptModule。
    pub active: bool,
    /// PromptModule 扩展配置。
    pub data: PromptModuleData,
    /// 创建时间戳。
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for PromptModule {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.get("id"),
            key: row.get("key"),
            name: row.get("name"),
            description: row.get("description"),
            module_type: utils::parse_str_to(&row.get::<String, _>("module_type")).map_err(
                |e| sqlx::Error::Decode(format!("deserialize prompt module type: {}", e).into()),
            )?,
            content: row.get("content"),
            active: row.get("active"),
            data: utils::de_str_to(&row.get::<String, _>("data")).map_err(|e| {
                sqlx::Error::Decode(format!("deserialize prompt module data: {}", e).into())
            })?,
            created_at: row.get("created_at"),
        })
    }
}

/// Prompt 模块类型，用于 UI 分类和 Runtime 组装 prompt。
#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, strum::EnumString, strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum PromptModuleType {
    /// Agent 身份设定。
    Identity,
    /// Agent 行为准则。
    Behavior,
    /// Agent 能力描述。
    Capability,
    /// 工具使用规则。
    ToolUsage,
    /// 输出格式约束。
    OutputFormat,
    /// 安全策略。
    SafetyPolicy,
    /// 领域知识。
    DomainKnowledge,
    /// Runtime 协议补充说明。
    RuntimeNote,
}

/// PromptModule 的扩展数据。
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct PromptModuleData {
    /// 模板变量的 JSON Schema。
    pub variables_schema: Option<serde_json::Value>,
    /// PromptModule 标签。
    pub tags: Vec<String>,
}

/// AgentDefinition 对 PromptModule 的引用配置。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PromptModuleBinding {
    /// 被引用的 PromptModule id。
    pub prompt_module_id: i64,
    /// 该模块是否为 Agent 运行必需。
    pub required: bool,
    /// Prompt 组装顺序，数值越小越靠前。
    pub order: i32,
    /// 渲染 PromptModule 时传入的变量。
    pub variables: serde_json::Value,
    /// 当前绑定是否启用。
    pub enabled: bool,
}

/// 创建 PromptModule 的 DTO。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreatePromptModule {
    /// 稳定短标识。
    pub key: String,
    /// 用户可读名称。
    pub name: String,
    /// 模块用途说明。
    pub description: String,
    /// Prompt 模块类型。
    pub module_type: PromptModuleType,
    /// Prompt 正文。
    pub content: String,
    /// 是否启用；None 时默认启用。
    pub active: Option<bool>,
    /// PromptModule 扩展配置。
    pub data: PromptModuleData,
}

/// 更新 PromptModule 的 DTO。
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct UpdatePromptModule {
    /// 新的稳定短标识。
    pub key: Option<String>,
    /// 新的用户可读名称。
    pub name: Option<String>,
    /// 新的模块用途说明。
    pub description: Option<String>,
    /// 新的 Prompt 模块类型。
    pub module_type: Option<PromptModuleType>,
    /// 新的 Prompt 正文。
    pub content: Option<String>,
    /// 新的启用状态。
    pub active: Option<bool>,
    /// 新的扩展配置。
    pub data: Option<PromptModuleData>,
}
