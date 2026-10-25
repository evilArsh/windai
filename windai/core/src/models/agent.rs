use serde::{Deserialize, Serialize};
use wind_wire::message::Content;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::AsRefStr,
    strum::EnumString,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentRole {
    Main,
    Child,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::AsRefStr,
    strum::EnumString,
    strum::Display,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentMode {
    Sync,
    Background,
    Fork,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::AsRefStr,
    strum::EnumString,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentPhase {
    Running,
    Suspended,
    WaitingChildren,
    Cancelling,
    Done,
    Failed,
    Cancelled,
    Aborted,
}

impl AgentPhase {
    pub fn takes_payload(self) -> bool {
        matches!(
            self,
            Self::Suspended | Self::Cancelling | Self::Done | Self::Failed | Self::Cancelled
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentInstance {
    pub id: i64,
    pub parent_id: Option<i64>,
    pub topic_id: i64,
    pub role: AgentRole,
    pub mode: Option<AgentMode>,
    pub phase: AgentPhase,
    pub phase_payload: Option<String>,
    pub created_at: Option<i64>,
}

impl_display!(AgentInstance {
    id,
    parent_id,
    topic_id,
    role,
    mode,
    phase,
    phase_payload,
    created_at
});

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExternalWait {
    pub handle: String,
    pub instance_id: i64,
    pub kind: String,
    pub message_id: i64,
    pub payload: serde_json::Value,
    pub created_at: i64,
    pub answered_at: Option<i64>,
    pub decision: Option<serde_json::Value>,
}

impl_display!(ExternalWait {
    handle,
    instance_id,
    kind,
    message_id,
    payload,
    created_at,
    answered_at,
    decision
});

impl ExternalWait {
    pub fn is_answered(&self) -> bool {
        self.answered_at.is_some()
    }
}

/// spawn 调用的父子配对记录
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpawnCall {
    /// 发起 spawn 调用的实例
    pub parent_id: i64,
    /// 父实例消息内的工具调用标识
    pub call_id: String,
    /// spawn 调用派生的子实例
    pub child_id: i64,
    /// 配对写入时刻，epoch 秒
    pub created_at: i64,
    /// 结果交付时刻，epoch 秒；未交付为 NULL
    pub result_delivered_at: Option<i64>,
}

impl_display!(SpawnCall {
    parent_id,
    call_id,
    child_id,
    created_at,
    result_delivered_at
});

#[derive(Debug, Clone, PartialEq)]
pub struct AgentPending {
    /// 同时是队列顺序
    pub id: i64,
    pub instance_id: i64,
    pub kind: PendingKind,
    /// 待拼进 user 消息的内容
    pub payload: Vec<Content>,
    /// `background` 时是 child_id
    pub source_id: Option<i64>,
    pub created_at: i64,
}

impl_display!(AgentPending {
    id,
    instance_id,
    kind,
    payload,
    source_id,
    created_at
});

/// 新建 agent 实例
#[derive(Debug, Clone, PartialEq)]
pub struct AgentInstanceCreate {
    pub parent_id: Option<i64>,
    pub topic_id: i64,
    pub role: AgentRole,
    pub mode: Option<AgentMode>,
    pub phase: AgentPhase,
    pub phase_payload: Option<String>,
}

impl_display!(AgentInstanceCreate {
    parent_id,
    topic_id,
    role,
    mode,
    phase,
    phase_payload
});

/// 入队的待处理内容
#[derive(Debug, Clone, PartialEq)]
pub struct PendingCreate {
    pub instance_id: i64,
    pub kind: PendingKind,
    pub source_id: Option<i64>,
    pub payload: Vec<Content>,
}

impl_display!(PendingCreate {
    instance_id,
    kind,
    source_id,
    payload
});

/// 打开一次外部等待
#[derive(Debug, Clone, PartialEq)]
pub struct WaitOpen {
    pub handle: String,
    pub instance_id: i64,
    pub kind: String,
    pub message_id: i64,
    pub payload: serde_json::Value,
}

impl_display!(WaitOpen {
    handle,
    instance_id,
    kind,
    message_id,
    payload
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr, strum::EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum PendingKind {
    User,
    BackgroundResult,
}

impl std::fmt::Display for PendingKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}
