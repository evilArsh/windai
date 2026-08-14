use super::task_fsm::TaskEvent;
use crate::agent::{
    runtime::AgentRunConfig,
    task::TaskSpec,
    tool::{SpawnAgentRequest, SpawnAgentResponse},
};
use tokio::sync::oneshot;

pub enum FsmEvent {
    UserRequest(UserRequest),
    Signal { binding_id: i64, event: TaskEvent },
    Supervisor(SupervisorEvent),
}

/// 外部命令
pub enum UserRequest {
    /// 启动任务
    Start {
        is_main: bool,
        spec: TaskSpec,
        config: AgentRunConfig,
    },
    /// 取消任务
    CancelTask {
        binding_id: i64,
    },
    /// 提交审批
    Approval {
        binding_id: i64,
        allow_ids: Vec<i64>,
        deny_ids: Vec<i64>,
    },
    /// 审批已完成
    ApprovalApplied {
        binding_id: i64,
    },
    Shutdown,
}

/// 内部流转指令
pub enum SupervisorEvent {
    /// 请求创建子任务
    SpawnAgent {
        parent_binding_id: i64,
        call_id: String,
        request: SpawnAgentRequest,
        reply: oneshot::Sender<SpawnAgentResponse>,
    },
    /// 创建子任务成功
    ChildStarted {
        parent_binding_id: i64,
        spec: TaskSpec,
        config: AgentRunConfig,
    },
    /// 父任务所有子任务都完成
    ChildResolved { parent_binding_id: i64 },
}
