use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::db::DbRow;
use crate::storage::utils;

use super::context::ContextPolicy;
use super::definition::AgentMode;
use super::policy::RuntimeLimits;

/// 某个 Topic 对某个 Agent 的使用绑定。Topic 通过它单向依赖 Agent。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TopicAgentBinding {
    /// TopicAgentBinding 主键。
    pub id: i64,
    /// 绑定所属 Topic id。
    pub topic_id: i64,
    /// 被绑定的 AgentDefinition id。
    pub agent_id: i64,
    /// Agent 在当前 Topic 中的角色。
    pub binding_role: TopicAgentBindingRole,
    /// 当前 Topic 中暴露给 UI 或 LLM 的 Agent 别名。
    pub alias: Option<String>,
    /// 当前 Topic 中该 Agent 使用的模型 id；None 表示继承调用上下文模型。
    pub model_id: Option<i64>,
    /// 当前 Topic 中该 Agent 的专属 ChatConfig id；None 表示使用 Topic.chat_config_id。
    pub chat_config_id: Option<i64>,
    /// 当前绑定是否启用。
    pub enabled: bool,
    /// 当前 Topic 对该 Agent 的运行覆盖配置。
    pub config: TopicAgentBindingConfig,
    /// 创建时间戳。
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for TopicAgentBinding {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.get("id"),
            topic_id: row.get("topic_id"),
            agent_id: row.get("agent_id"),
            binding_role: utils::parse_str_to(&row.get::<String, _>("binding_role")).map_err(
                |e| sqlx::Error::Decode(format!("deserialize binding role: {}", e).into()),
            )?,
            alias: row.get("alias"),
            model_id: row.get("model_id"),
            chat_config_id: row.get("chat_config_id"),
            enabled: row.get("enabled"),
            config: utils::de_str_to(&row.get::<String, _>("config")).map_err(|e| {
                sqlx::Error::Decode(format!("deserialize binding config: {}", e).into())
            })?,
            created_at: row.get("created_at"),
        })
    }
}

/// Agent 在当前 Topic 中的角色。每个 Topic 只能有一个 Main。
#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, strum::EnumString, strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TopicAgentBindingRole {
    /// 当前 Topic 的主 Agent。
    Main,
    /// 当前 Topic 中可被主 Agent 调度的子 Agent。
    Child,
    /// 当前 Topic 中保留给团队协作模式的 Agent。
    Team,
}

/// Topic 使用 Agent 时的运行覆盖配置。这里不覆盖 Prompt/MCP/Skill；修改那些能力时应复制 Agent。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TopicAgentBindingConfig {
    /// 当前 Topic 中覆盖 AgentDefinition.description 的说明。
    pub description_override: Option<String>,
    /// 当前 Topic 允许该 Agent 使用的运行模式。
    pub allowed_modes: Vec<AgentMode>,
    /// 当前 Topic 中未显式指定模式时的默认运行模式。
    pub default_mode: Option<AgentMode>,
    /// 当前 Topic 中覆盖 AgentDefinition.context_policy 的上下文策略。
    pub context_policy_override: Option<ContextPolicy>,
    /// 当前 Topic 中覆盖 AgentDefinition.runtime_limits 的资源限制。
    pub runtime_limits_override: Option<RuntimeLimits>,
    /// 创建该 Agent 实例时是否需要用户审批。
    pub requires_spawn_approval: bool,
}

/// 创建 TopicAgentBinding 的 DTO。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateTopicAgentBinding {
    /// 绑定所属 Topic id。
    pub topic_id: i64,
    /// 被绑定的 AgentDefinition id。
    pub agent_id: i64,
    /// Agent 在当前 Topic 中的角色。
    pub binding_role: TopicAgentBindingRole,
    /// 当前 Topic 中暴露给 UI 或 LLM 的 Agent 别名。
    pub alias: Option<String>,
    /// 当前 Topic 中该 Agent 使用的模型 id。
    pub model_id: Option<i64>,
    /// 当前 Topic 中该 Agent 的专属 ChatConfig id。
    pub chat_config_id: Option<i64>,
    /// 当前绑定是否启用；None 时默认启用。
    pub enabled: Option<bool>,
    /// 当前 Topic 对该 Agent 的运行覆盖配置。
    pub config: TopicAgentBindingConfig,
}

/// 更新 TopicAgentBinding 的 DTO。
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct UpdateTopicAgentBinding {
    /// 新的 AgentDefinition id。
    pub agent_id: Option<i64>,
    /// 新的绑定角色。
    pub binding_role: Option<TopicAgentBindingRole>,
    /// 新的别名。
    pub alias: Option<String>,
    /// 新的模型 id。
    pub model_id: Option<i64>,
    /// 新的专属 ChatConfig id。
    pub chat_config_id: Option<i64>,
    /// 新的启用状态。
    pub enabled: Option<bool>,
    /// 新的运行覆盖配置。
    pub config: Option<TopicAgentBindingConfig>,
}
