use serde::{Deserialize, Serialize};
use wind_event::def::{Directory, EventDef, Shape};
use wind_event::schema;

/// 请求创建一个实例
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentCreate {
    /// 所属话题
    pub topic_id: i64,
    /// 派生源
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<i64>,
    /// 运行模式
    pub mode: String,
    /// 交给该实例的任务
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,
}

/// 请求终止一个实例
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentTerminate {
    /// 实例标识
    pub instance_id: i64,
    /// 终止原因
    pub reason: String,
}

/// 请求取消一个实例
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentCancel {
    /// 实例标识
    pub instance_id: i64,
    /// 取消来源
    pub origin: serde_json::Value,
}

/// 实例已创建
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentSpawn {
    /// 实例标识
    pub instance_id: i64,
    /// 派生源
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<i64>,
    /// 不进入父节点的等待屏障
    pub detached: bool,
}

/// 整棵树静止
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentSealed {
    /// 话题主实例标识
    pub instance_id: i64,
}

/// 相位未能写入数据库
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentPersistFailed {
    pub topic_id: i64,
    /// 实例标识
    pub instance_id: i64,
    /// 未能写入的相位
    pub phase: String,
    /// 写入失败的原因
    pub error: String,
}

/// 实例的 Phase 变化，只带 Phase 判别式与载荷
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentStatus {
    /// 实例标识
    pub instance_id: i64,
    /// Phase 判别式
    pub phase: String,
    /// Phase 的载荷
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase_payload: Option<serde_json::Value>,
}

/// 树的结构快照
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentTree {
    /// 全部节点
    pub nodes: Vec<AgentNodeView>,
}

/// 快照里的一个节点
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentNodeView {
    /// 实例标识
    pub instance_id: i64,
    /// 派生源
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<i64>,
    /// 是否不进入父节点的等待屏障
    pub detached: bool,
    /// Phase 判别式
    pub phase: String,
    /// Phase 的载荷
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase_payload: Option<serde_json::Value>,
}

/// 待放行的一次调用
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PendingCall {
    /// 调用标识
    pub call_id: String,
    /// 外部服务名
    pub server_name: String,
    /// 工具名
    pub tool: String,
    /// 调用参数
    pub arguments: serde_json::Value,
}

pub const CORE_AGENT_CREATE: EventDef<AgentCreate> = EventDef::new(
    "core/agent.create",
    Shape::Intercept,
    true,
    schema::<AgentCreate>,
);

pub const CORE_AGENT_TERMINATE: EventDef<AgentTerminate> = EventDef::new(
    "core/agent.terminate",
    Shape::Intercept,
    true,
    schema::<AgentTerminate>,
);

pub const CORE_AGENT_CANCEL: EventDef<AgentCancel> = EventDef::new(
    "core/agent.cancel",
    Shape::Intercept,
    true,
    schema::<AgentCancel>,
);

pub const CORE_AGENT_SPAWN: EventDef<AgentSpawn> = EventDef::new(
    "core/agent.spawn",
    Shape::Observe,
    true,
    schema::<AgentSpawn>,
);

pub const CORE_AGENT_STATUS: EventDef<AgentStatus> = EventDef::new(
    "core/agent.status",
    Shape::Observe,
    true,
    schema::<AgentStatus>,
);

pub const CORE_AGENT_SEALED: EventDef<AgentSealed> = EventDef::new(
    "core/agent.sealed",
    Shape::Observe,
    true,
    schema::<AgentSealed>,
);

pub const CORE_AGENT_PERSIST_FAILED: EventDef<AgentPersistFailed> = EventDef::new(
    "core/agent.persist_failed",
    Shape::Observe,
    false,
    schema::<AgentPersistFailed>,
);

pub const CORE_AGENT_TREE: EventDef<AgentTree> =
    EventDef::new("core/agent.tree", Shape::Observe, true, schema::<AgentTree>);

pub fn directory() -> Directory {
    Directory::from_metas([
        CORE_AGENT_CREATE.meta(),
        CORE_AGENT_TERMINATE.meta(),
        CORE_AGENT_CANCEL.meta(),
        CORE_AGENT_SPAWN.meta(),
        CORE_AGENT_STATUS.meta(),
        CORE_AGENT_SEALED.meta(),
        CORE_AGENT_PERSIST_FAILED.meta(),
        CORE_AGENT_TREE.meta(),
    ])
}
