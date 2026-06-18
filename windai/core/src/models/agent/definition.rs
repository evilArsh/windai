use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::db::DbRow;
use crate::storage::utils;

use super::context::ContextPolicy;
use super::policy::{OutputContract, PermissionPolicy, RuntimeLimits};
use super::prompt::PromptModuleBinding;

/// Agent 的可 CRUD 能力定义。Agent 可被多个 Topic 复用，也可以复制为某个 Topic 的专属 Agent。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentDefinition {
    /// AgentDefinition 主键。
    pub id: i64,
    /// 面向 UI、模型和导入导出的稳定短标识。
    pub key: String,
    /// 用户可读名称。
    pub name: String,
    /// Agent 能力说明，供 UI 展示和模型选择参考。
    pub description: String,
    /// Agent 作用域，决定它是全局复用还是某个 Topic 的专属副本。
    pub scope: AgentScope,
    /// 当 scope 为 topic_local 时，表示该 Agent 专属的 Topic id。
    pub owner_topic_id: Option<i64>,
    /// 如果该 Agent 由全局 Agent 复制而来，记录来源 Agent id。
    pub cloned_from_agent_id: Option<i64>,
    /// Agent 的 UI/语义角色提示。
    pub role: AgentRole,
    /// Agent 是否启用。
    pub active: bool,
    /// Agent 能力配置。
    pub data: AgentDefinitionData,
    /// 创建时间戳。
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for AgentDefinition {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.get("id"),
            key: row.get("key"),
            name: row.get("name"),
            description: row.get("description"),
            scope: utils::parse_str_to(&row.get::<String, _>("scope")).map_err(|e| {
                sqlx::Error::Decode(format!("deserialize agent scope: {}", e).into())
            })?,
            owner_topic_id: row.get("owner_topic_id"),
            cloned_from_agent_id: row.get("cloned_from_agent_id"),
            role: utils::parse_str_to(&row.get::<String, _>("role")).map_err(|e| {
                sqlx::Error::Decode(format!("deserialize agent role: {}", e).into())
            })?,
            active: row.get("active"),
            data: utils::de_str_to(&row.get::<String, _>("data")).map_err(|e| {
                sqlx::Error::Decode(format!("deserialize agent definition data: {}", e).into())
            })?,
            created_at: row.get("created_at"),
        })
    }
}

/// Agent 的作用域。Global 表示可被多个 Topic 复用，TopicLocal 表示某个 Topic 的专属副本。
#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, strum::EnumString, strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentScope {
    /// 全局 Agent，可被多个 Topic 复用。
    Global,
    /// Topic 专属 Agent，通常由修改全局 Agent 能力配置时复制产生。
    TopicLocal,
}

/// Agent 的 UI/语义角色提示。真正能否作为主 Agent 或子 Agent 由 TopicAgentBinding 决定。
#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, strum::EnumString, strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentRole {
    /// 通用助手。
    General,
    /// 专业能力 Agent。
    Specialist,
    /// 团队协作 Agent。
    Team,
    /// 工作者或执行型 Agent。
    Worker,
}

/// AgentInstance 的运行模式。第一版实现 Sync 和 Background，Team/Fork 先保留协议。
#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, strum::EnumString, strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentMode {
    /// 同步执行，父 Agent 等待结果。
    Sync,
    /// 后台执行，立即返回实例句柄。
    Background,
    /// 长期团队协作模式，第一版保留协议。
    Team,
    /// fork 上下文分支模式，第一版保留协议。
    Fork,
}

/// Agent 的能力配置。LLM 请求参数不在这里，运行参数来自 Topic 或 TopicAgentBinding 的 ChatConfig。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentDefinitionData {
    /// 该 Agent 绑定的 PromptModule 列表。
    pub prompt_modules: Vec<PromptModuleBinding>,
    /// 该 Agent 拥有的 MCP server 和工具级约束。
    pub mcp_servers: Vec<AgentMcpBinding>,
    /// 该 Agent 拥有的 Skill 绑定。
    pub skills: Vec<SkillBinding>,
    /// 该 Agent 构建上下文时允许使用的最大消息数。
    pub max_context: Option<i32>,
    /// 该 Agent 支持的运行模式。
    pub supported_modes: Vec<AgentMode>,
    /// 未显式指定模式时使用的默认运行模式。
    pub default_mode: AgentMode,
    /// 子 Agent 创建时的默认上下文策略。
    pub context_policy: ContextPolicy,
    /// Agent 调度权限边界。
    pub permission_policy: PermissionPolicy,
    /// Agent 运行资源限制。
    pub runtime_limits: RuntimeLimits,
    /// Agent 输出约束。
    pub output_contract: Option<OutputContract>,
}

/// Agent 默认拥有的 MCP server 与工具级约束。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentMcpBinding {
    /// MCP server id。
    pub mcp_server_id: i64,
    /// Agent 内部使用该 MCP server 的别名。
    pub alias: Option<String>,
    /// 允许暴露给该 Agent 的工具名列表，空列表表示不额外限制。
    pub allowed_tools: Vec<String>,
    /// 明确禁止该 Agent 使用的工具名列表。
    pub denied_tools: Vec<String>,
    /// 当前 MCP 绑定是否启用。
    pub enabled: bool,
}

/// Agent 默认拥有的 Skill 绑定。第一版可以先保存结构，执行器后续接入。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SkillBinding {
    /// Skill 的稳定短标识。
    pub skill_key: String,
    /// Agent 内部使用该 Skill 的别名。
    pub alias: Option<String>,
    /// Skill 执行或加载时使用的变量。
    pub variables: serde_json::Value,
    /// 当前 Skill 绑定是否启用。
    pub enabled: bool,
}

/// 创建 AgentDefinition 的 DTO。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateAgentDefinition {
    /// 稳定短标识。
    pub key: String,
    /// 用户可读名称。
    pub name: String,
    /// Agent 能力说明。
    pub description: String,
    /// Agent 作用域。
    pub scope: AgentScope,
    /// Topic 专属 Agent 的所属 Topic id。
    pub owner_topic_id: Option<i64>,
    /// 复制来源 Agent id。
    pub cloned_from_agent_id: Option<i64>,
    /// Agent 的 UI/语义角色提示。
    pub role: AgentRole,
    /// 是否启用；None 时默认启用。
    pub active: Option<bool>,
    /// Agent 能力配置。
    pub data: AgentDefinitionData,
}

/// 更新 AgentDefinition 的 DTO。
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct UpdateAgentDefinition {
    /// 新的稳定短标识。
    pub key: Option<String>,
    /// 新的用户可读名称。
    pub name: Option<String>,
    /// 新的 Agent 能力说明。
    pub description: Option<String>,
    /// 新的 Agent 作用域。
    pub scope: Option<AgentScope>,
    /// 新的所属 Topic id。
    pub owner_topic_id: Option<i64>,
    /// 新的复制来源 Agent id。
    pub cloned_from_agent_id: Option<i64>,
    /// 新的 UI/语义角色。
    pub role: Option<AgentRole>,
    /// 新的启用状态。
    pub active: Option<bool>,
    /// 新的 Agent 能力配置。
    pub data: Option<AgentDefinitionData>,
}
