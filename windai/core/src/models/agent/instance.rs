use crate::db::DbRow;
use crate::storage::utils;
use serde::{Deserialize, Serialize};
use sqlx::Row;

/// AgentInstance 生命周期状态
#[derive(
    utoipa::ToSchema,
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    strum::EnumString,
    strum::Display,
    Copy,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentStatus {
    /// 已创建
    Idle,
    /// 正在运行
    Running,
    /// 正在等待用户审批
    WaitingApproval,
    /// 正在等待子任务完成
    WaitingChild,
    /// 已完成
    Finished,
    /// 已失败
    Failed,
    /// 已取消
    Cancelled,
}

/// 一次 Agent 运行实例
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone)]
pub struct AgentInstance {
    pub id: i64,
    /// 父实例 id, 标识当前实例的派生源
    pub parent_id: Option<i64>,
    /// 实例所属 topic id
    pub topic_id: i64,
    /// 被绑定的 AgentDefinition id
    ///
    /// 实例没有绑定 Agent 能力时，回退成普通对话模式
    pub agent_id: Option<i64>,
    /// 实例运行模式
    pub mode: Option<AgentMode>,
    /// 实例在任务中的角色
    pub role: AgentRole,
    /// 实例生命周期状态
    pub status: AgentStatus,
    /// 创建时间戳
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for AgentInstance {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.get("id"),
            parent_id: row.get("parent_id"),
            topic_id: row.get("topic_id"),
            agent_id: row.get("agent_id"),
            role: utils::parse_str_to(&row.get::<String, _>("role"))
                .map_err(|e| sqlx::Error::Decode(e.into()))?,
            created_at: row.get("created_at"),
            mode: match row.get::<Option<String>, _>("mode") {
                Some(mode) => {
                    Some(utils::parse_str_to(&mode).map_err(|e| sqlx::Error::Decode(e.into()))?)
                }
                _ => None,
            },
            status: utils::parse_str_to(&row.get::<String, _>("status"))
                .map_err(|e| sqlx::Error::Decode(e.into()))?,
        })
    }
}

/// 用于展示当前 Agent 运行模式
#[derive(
    utoipa::ToSchema,
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    strum::EnumString,
    strum::Display,
    Copy,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentMode {
    /// 同步执行
    Sync,
    /// 后台执行，立即返回实例句柄
    Background,
    /// 继承上下文并同步执行
    Fork,
}

/// agent 所属角色
#[derive(
    utoipa::ToSchema,
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    strum::EnumString,
    strum::Display,
    Copy,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentRole {
    /// 主 Agent 标识
    Main,
    /// 子 Agent 标识
    Child,
}

/// 创建 AgentInstance 的 DTO
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone)]
pub struct CreateInstance {
    /// 实例所在 topic id
    pub topic_id: i64,
    /// 父实例 id
    pub parent_id: Option<i64>,
    /// 绑定 AgentDefinition id
    pub agent_id: Option<i64>,
    /// 实例运行模式
    pub mode: Option<AgentMode>,
    /// 实例生命周期状态
    pub status: Option<AgentStatus>,
    /// 实例角色
    pub role: Option<AgentRole>,
}

impl CreateInstance {
    pub fn new_main(topic_id: i64, agent_id: Option<i64>) -> Self {
        Self {
            topic_id,
            agent_id,
            parent_id: None,
            mode: Some(AgentMode::Sync),
            status: Some(AgentStatus::Idle),
            role: Some(AgentRole::Main),
        }
    }
}

/// 更新 AgentInstance 的 DTO
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone, Default)]
pub struct UpdateInstance {
    /// 实例生命周期状态
    pub status: Option<AgentStatus>,
    /// 运行模式
    pub mode: Option<AgentMode>,
    /// 绑定 AgentDefinition id
    pub agent_id: Option<i64>,
}
