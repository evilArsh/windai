use crate::{
    agent::{
        runtime::AgentRunConfig,
        task::TaskSpec,
        tool::{SpawnAgentRequest, SpawnAgentResponse},
    },
    models::{Message, ToolApprovalRequest},
};
use tokio::sync::oneshot;

pub enum FsmEvent {
    UserRequest(UserRequest),
    Signal(TaskSignal),
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

pub enum TaskSignal {
    /// 模型请求工具审批
    AwaitApproval {
        binding_id: i64,
        data: Message,
        requests: Vec<ToolApprovalRequest>,
    },
    /// 正常完成。
    Completed { binding_id: i64, data: Message },
    /// 失败。
    Failed {
        binding_id: i64,
        error: String,
        message_id: Option<i64>,
    },
    /// 已取消。
    Cancelled { binding_id: i64 },
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
