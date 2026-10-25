use super::domain::CallDecision;
use serde::{Deserialize, Serialize};
use wind_event::def::{Directory, EventDef, Mount, Shape};

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
    /// 运行模式
    pub mode: String,
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

/// 实例出错
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentError {
    /// 实例标识
    pub instance_id: i64,
    /// 出错时所在的消息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<i64>,
    /// 失败原因
    pub error: String,
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

/// 一个实例转入等外部答复
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ApprovalRequest {
    /// 实例标识
    pub instance_id: i64,
    /// 挂起句柄
    pub handle: String,
    /// 待放行的调用
    pub calls: Vec<PendingCall>,
}

/// 一次等待被答复
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ApprovalResolved {
    /// 实例标识
    pub instance_id: i64,
    /// 挂起句柄
    pub handle: String,
    /// 逐项判定
    pub decisions: Vec<CallDecision>,
    /// 答复者
    pub by: String,
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

fn schema<T: schemars::JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("the generated schema is valid JSON")
}

pub const CORE_AGENT_CREATE: EventDef<AgentCreate> = EventDef::new(
    "core/agent.create",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<AgentCreate>,
);

pub const CORE_AGENT_TERMINATE: EventDef<AgentTerminate> = EventDef::new(
    "core/agent.terminate",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<AgentTerminate>,
);

pub const CORE_AGENT_CANCEL: EventDef<AgentCancel> = EventDef::new(
    "core/agent.cancel",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<AgentCancel>,
);

pub const CORE_AGENT_SPAWN: EventDef<AgentSpawn> = EventDef::new(
    "core/agent.spawn",
    Shape::Observe,
    true,
    Mount::TranslationLayer,
    schema::<AgentSpawn>,
);

pub const CORE_AGENT_STATUS: EventDef<AgentStatus> = EventDef::new(
    "core/agent.status",
    Shape::Observe,
    true,
    Mount::TranslationLayer,
    schema::<AgentStatus>,
);

pub const CORE_AGENT_ERROR: EventDef<AgentError> = EventDef::new(
    "core/agent.error",
    Shape::Observe,
    true,
    Mount::TranslationLayer,
    schema::<AgentError>,
);

pub const CORE_AGENT_TREE: EventDef<AgentTree> = EventDef::new(
    "core/agent.tree",
    Shape::Observe,
    true,
    Mount::TranslationLayer,
    schema::<AgentTree>,
);

pub const CORE_APPROVAL_REQUEST: EventDef<ApprovalRequest> = EventDef::new(
    "core/approval.request",
    Shape::Observe,
    true,
    Mount::Subsystem,
    schema::<ApprovalRequest>,
);

pub const CORE_APPROVAL_RESOLVED: EventDef<ApprovalResolved> = EventDef::new(
    "core/approval.resolved",
    Shape::Observe,
    true,
    Mount::Subsystem,
    schema::<ApprovalResolved>,
);

pub fn directory() -> Directory {
    Directory::from_metas([
        CORE_AGENT_CREATE.meta(),
        CORE_AGENT_TERMINATE.meta(),
        CORE_AGENT_CANCEL.meta(),
        CORE_AGENT_SPAWN.meta(),
        CORE_AGENT_STATUS.meta(),
        CORE_AGENT_ERROR.meta(),
        CORE_AGENT_TREE.meta(),
        CORE_APPROVAL_REQUEST.meta(),
        CORE_APPROVAL_RESOLVED.meta(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_three_entry_events_are_intercepts() {
        for def in [
            CORE_AGENT_CREATE.meta(),
            CORE_AGENT_TERMINATE.meta(),
            CORE_AGENT_CANCEL.meta(),
        ] {
            assert_eq!(
                def.shape,
                Shape::Intercept,
                "{} must be interceptable",
                def.name
            );
        }
    }

    #[test]
    fn everything_reported_after_the_fact_is_an_observe() {
        for def in [
            CORE_AGENT_SPAWN.meta(),
            CORE_AGENT_STATUS.meta(),
            CORE_AGENT_ERROR.meta(),
            CORE_AGENT_TREE.meta(),
            CORE_APPROVAL_REQUEST.meta(),
            CORE_APPROVAL_RESOLVED.meta(),
        ] {
            assert_eq!(def.shape, Shape::Observe, "{} is observe-only", def.name);
        }
    }

    #[test]
    fn the_directory_covers_every_event_with_its_shape_log_and_mount() {
        let dir = directory();
        assert_eq!(
            dir.len(),
            9,
            "the directory must contain exactly nine events"
        );

        for (name, shape, mount) in [
            ("core/agent.create", Shape::Intercept, Mount::UseCase),
            ("core/agent.terminate", Shape::Intercept, Mount::UseCase),
            ("core/agent.cancel", Shape::Intercept, Mount::UseCase),
            ("core/agent.spawn", Shape::Observe, Mount::TranslationLayer),
            ("core/agent.status", Shape::Observe, Mount::TranslationLayer),
            ("core/agent.error", Shape::Observe, Mount::TranslationLayer),
            ("core/agent.tree", Shape::Observe, Mount::TranslationLayer),
            ("core/approval.request", Shape::Observe, Mount::Subsystem),
            ("core/approval.resolved", Shape::Observe, Mount::Subsystem),
        ] {
            let meta = dir.get(name).expect("find all nine events by name");
            assert_eq!(meta.shape, shape, "{name} shape");
            assert_eq!(meta.mount, mount, "{name} mount point");
            assert!(meta.log, "{name} must be logged");
            assert!(
                (meta.schema)().is_object(),
                "{name} payload schema must be an object; a generation failure surfaces here"
            );
        }
    }

    #[test]
    fn a_status_payload_carries_the_phase_not_a_projected_enum() {
        let payload = AgentStatus {
            instance_id: 1,
            phase: "cancelled".into(),
            phase_payload: Some(serde_json::json!({ "ancestor": 3 })),
        };
        let value = serde_json::to_value(&payload).expect("serialize");
        let mut keys: Vec<&str> = value
            .as_object()
            .expect("read the payload object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            ["instance_id", "phase", "phase_payload"],
            "the payload carries only the discriminator and its payload; an extra projected public enum would lose the information in Phase"
        );
        assert_eq!(value["phase"], serde_json::json!("cancelled"));
        assert_eq!(value["phase_payload"]["ancestor"], serde_json::json!(3));

        let bare = AgentStatus {
            instance_id: 2,
            phase: "running".into(),
            phase_payload: None,
        };
        let value = serde_json::to_value(&bare).expect("serialize");
        let mut keys: Vec<&str> = value
            .as_object()
            .expect("read the payload object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            ["instance_id", "phase"],
            "no empty key is written when Phase has no payload"
        );
    }
}
