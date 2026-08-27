use super::prompt::PromptModuleBinding;
use crate::db::DbRow;
use crate::storage::utils;
use serde::{Deserialize, Serialize};
use sqlx::Row;

/// Agent 能力定义。
/// Agent 可被多个 Topic 复用，也可以复制为某个 Topic 的专属 Agent。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentDefinition {
    /// 唯一id
    pub id: i64,
    /// Agent 唯一短标识
    pub key: String,
    /// 用户可读名称。
    pub name: String,
    /// Agent 能力说明
    pub description: String,
    /// Agent 作用域，决定它是全局复用还是某个 Topic 的专属副本。
    pub scope: AgentScope,
    /// 当 scope 为 topic_local 时，表示该 Agent 专属的 Topic id。
    pub owner_topic_id: Option<i64>,
    /// 如果该 Agent 由全局 Agent 复制而来，记录来源 Agent id。
    pub cloned_from_agent_id: Option<i64>,
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
            active: row.get("active"),
            data: utils::de_str_to(&row.get::<String, _>("data")).map_err(|e| {
                sqlx::Error::Decode(format!("deserialize agent definition data: {}", e).into())
            })?,
            created_at: row.get("created_at"),
        })
    }
}

/// Agent 作用域。
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

/// Agent 的能力配置。LLM 请求参数不在这里，运行参数来自 Topic 或 TopicAgentBinding 的 ChatConfig。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentDefinitionData {
    /// 该 Agent 绑定的 PromptModule 列表。
    pub prompt_modules: Vec<PromptModuleBinding>,
    /// 该 Agent 拥有的 MCP server 和工具级约束。
    pub mcp_servers: Vec<AgentMcpBinding>,
    /// 子 Agent 创建时的默认上下文策略。
    pub context_policy: ContextPolicy,
    /// Agent 调度权限边界。
    pub permission_policy: PermissionPolicy,
    /// Agent 运行资源限制。
    pub runtime_limits: RuntimeLimits,
}

impl Default for AgentDefinitionData {
    fn default() -> Self {
        Self {
            prompt_modules: vec![],
            mcp_servers: vec![],
            context_policy: ContextPolicy::default(),
            permission_policy: PermissionPolicy::default(),
            runtime_limits: RuntimeLimits::default(),
        }
    }
}

/// Agent 默认拥有的 MCP server 与工具级约束。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentMcpBinding {
    /// MCP server id。
    pub mcp_server_id: i64,
    /// Agent 内部使用该 MCP server 的别名。
    pub alias: Option<String>,
    /// 允许暴露给该 Agent 的工具名列表，空列表表示不额外限制。
    ///
    /// 工具名包含完整的 server_name + tool_name 组合
    pub allowed_tools: Vec<String>,
    /// 明确禁止该 Agent 使用的工具名列表。
    ///
    /// 工具名包含完整的 server_name + tool_name 组合
    pub denied_tools: Vec<String>,
    /// 当前 MCP 绑定是否启用。
    pub enabled: bool,
}

/// 创建 AgentDefinition 的 DTO。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateAgentDefinition {
    /// 用户可读名称。
    pub name: String,
    /// Agent 唯一短标识
    pub key: String,
    /// Agent 能力说明。
    pub description: String,
    /// Agent 作用域。
    pub scope: AgentScope,
    /// 当 scope 为 topic_local 时，表示该 Agent 专属的 Topic id。
    pub owner_topic_id: Option<i64>,
    /// 复制来源 Agent id。
    pub cloned_from_agent_id: Option<i64>,
    /// 是否启用；None 时默认启用。
    pub active: Option<bool>,
    /// Agent 能力配置。
    pub data: AgentDefinitionData,
}

/// 更新 AgentDefinition 的 DTO。
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct UpdateAgentDefinition {
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
    /// 新的启用状态。
    pub active: Option<bool>,
    /// 新的 Agent 能力配置。
    pub data: Option<AgentDefinitionData>,
}

/// Agent 调度权限边界。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PermissionPolicy {
    /// 当前 Agent 是否允许创建其它 Agent。
    pub can_spawn_agents: bool,
    /// 当前 Agent 是否允许创建同步子 Agent。
    pub can_spawn_sync: bool,
    /// 当前 Agent 是否允许创建后台子 Agent。
    pub can_spawn_background: bool,
    /// 当前 Agent 是否允许创建团队 Agent。
    pub can_spawn_team: bool,
    /// 当前 Agent 是否允许创建 fork Agent。
    pub can_spawn_fork: bool,
    /// 当前 Agent 创建的子 Agent 是否允许继续创建子 Agent。
    pub can_spawn_recursive: bool,
    /// Agent 调度树的最大深度。
    pub max_spawn_depth: u32,
}

impl Default for PermissionPolicy {
    fn default() -> Self {
        Self {
            can_spawn_agents: true,
            can_spawn_sync: true,
            can_spawn_background: false,
            can_spawn_team: false,
            can_spawn_fork: false,
            can_spawn_recursive: false,
            max_spawn_depth: 1,
        }
    }
}

/// 子 Agent 创建时的上下文继承策略。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContextPolicy {
    /// 当前 Agent 上下文最多包含的消息数量
    pub max_context: Option<i32>,
}

impl Default for ContextPolicy {
    fn default() -> Self {
        Self { max_context: None }
    }
}

/// Agent 运行资源限制。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RuntimeLimits {
    /// 单个 Agent 实例最大运行时间，单位秒。
    /// None 表示不限制。
    pub max_run_time: Option<u64>,
    /// 单个 Agent 实例最大 LLM 调用次数。
    pub max_llm_calls: Option<u32>,
    /// 单个 Agent 实例最大工具调用次数。
    pub max_tool_calls: Option<u32>,
    /// 单个 Agent 实例最多可创建的子 Agent 数量。
    pub max_child_agents: Option<u32>,
    /// 单个 Agent 实例最多可并行运行的子 Agent 数量。
    pub max_parallel_child_agents: Option<u32>,
    /// 单次运行允许的最大输入 token 数。
    pub max_input_tokens: Option<u64>,
    /// 单次运行允许的最大输出 token 数。
    pub max_output_tokens: Option<u64>,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            max_run_time: None,
            max_llm_calls: None,
            max_tool_calls: None,
            max_child_agents: None,
            max_parallel_child_agents: None,
            max_input_tokens: None,
            max_output_tokens: None,
        }
    }
}
