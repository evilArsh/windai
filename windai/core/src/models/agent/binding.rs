use crate::storage::utils;
use crate::{db::DbRow, models::ToolApprovalPolicy};
use serde::{Deserialize, Serialize};
use sqlx::Row;

/// AgentInstance 生命周期状态
#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, strum::EnumString, strum::Display, Copy,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentStatus {
    /// 已创建。
    Idle,
    /// 正在运行。
    Running,
    /// 正在等待用户审批。
    WaitingApproval,
    /// 正在等待子 Agent。
    WaitingChild,
    /// 已完成。
    Finished,
    /// 已失败。
    Failed,
    /// 已取消。
    Cancelled,
}

/// Topic 对某个 Agent 的使用绑定。Topic 通过它单向依赖 Agent。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentBinding {
    pub id: i64,
    /// 该binding的父Topic id。
    pub parent_topic_id: i64,
    /// 被绑定的 AgentDefinition id。
    pub agent_id: i64,
    /// 该Agent运行模式。
    pub mode: Option<AgentMode>,
    /// Agent 在当前 Topic 中的角色。
    pub role: AgentRole,
    /// 当前实例生命周期状态。
    pub status: AgentStatus,
    /// 当前 Topic 中该 Agent 使用的模型 id；
    pub model_id: Option<i64>,
    /// 工具审批策略。
    pub tool_approval_policy: Option<ToolApprovalPolicy>,
    /// 关联的对话配置
    pub chat_config_id: Option<i64>,
    /// 当前绑定是否启用。
    pub enabled: bool,
    /// 创建时间戳。
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for AgentBinding {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.get("id"),
            parent_topic_id: row.get("parent_topic_id"),
            agent_id: row.get("agent_id"),
            role: utils::parse_str_to(&row.get::<String, _>("role")).map_err(|e| {
                sqlx::Error::Decode(format!("deserialize binding role: {}", e).into())
            })?,
            model_id: row.get("model_id"),
            chat_config_id: row.get("chat_config_id"),
            enabled: row.get("enabled"),
            created_at: row.get("created_at"),
            tool_approval_policy: match row.get::<Option<String>, _>("tool_approval_policy") {
                Some(s) => Some(utils::de_str_to(&s).map_err(|e| {
                    sqlx::Error::Decode(
                        format!("Failed to deserialize tool_approval_policy: {}", e).into(),
                    )
                })?),
                None => None,
            },
            mode: match row.get::<Option<String>, _>("mode") {
                Some(mode) => Some(utils::parse_str_to(&mode).map_err(|e| {
                    sqlx::Error::Decode(format!("deserialize agent mode: {}", e).into())
                })?),
                None => None,
            },
            status: utils::parse_str_to(&row.get::<String, _>("status")).map_err(|e| {
                sqlx::Error::Decode(format!("deserialize agent instance status: {}", e).into())
            })?,
        })
    }
}

/// 用于展示当前Agent运行模式
#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, strum::EnumString, strum::Display, Copy,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentMode {
    /// 同步执行。
    Sync,
    /// 后台执行，立即返回实例句柄。
    Background,
    // /// 长期团队协作模式。
    // Team,
    /// fork 上下文分支模式。
    Fork,
}

/// Agent 在当前 Topic 中的角色。每个 Topic 只能有一个 Main。
#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, strum::EnumString, strum::Display, Copy,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentRole {
    /// 主Agent标识
    Main,
    /// 子Agent标识
    Child,
}

/// 创建 TopicAgentBinding 的 DTO。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateAgentBinding {
    /// 该binding的父Topic id。
    pub parent_topic_id: i64,
    /// 被绑定的 AgentDefinition id。
    pub agent_id: i64,
    /// Agent 在当前 Topic 中的角色。
    pub role: AgentRole,
    /// 当前 Topic 中该 Agent 使用的模型 id。
    pub model_id: Option<i64>,
    /// 当前 Topic 中该 Agent 的专属 ChatConfig id。
    pub chat_config_id: Option<i64>,
    /// 当前绑定是否启用；None 时默认启用。
    pub enabled: Option<bool>,
}

/// 更新 TopicAgentBinding 的 DTO。
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct UpdateAgentBinding {
    /// 新的 AgentDefinition id。
    pub agent_id: Option<i64>,
    /// 新的绑定角色。
    pub role: Option<AgentRole>,
    /// 新的模型 id。
    pub model_id: Option<i64>,
    /// 新的专属 ChatConfig id。
    pub chat_config_id: Option<i64>,
    /// 新的启用状态。
    pub enabled: Option<bool>,
    /// 绑定实例生命周期状态。
    pub status: Option<AgentStatus>,
    /// 运行模式。
    pub mode: Option<AgentMode>,
    /// 工具审批策略。
    pub tool_approval_policy: Option<ToolApprovalPolicy>,
}
