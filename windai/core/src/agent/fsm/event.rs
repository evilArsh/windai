use super::task_fsm::TaskEvent;
use crate::agent::{
    event::{TopicEvent, TopicMsg},
    runtime::AgentRunConfig,
    task::TaskSpec,
    tool::{SpawnAgentRequest, SpawnAgentResponse},
};

pub enum FsmEvent {
    Topic(TopicMsg),
    /// 主 Agent 开始运行
    Start {
        spec: TaskSpec,
        config: AgentRunConfig,
    },
    /// 子 Agent 开始运行
    StartChild {
        parent_binding_id: i64,
        spec: TaskSpec,
        config: AgentRunConfig,
    },
    ChildResolved {
        parent_binding_id: i64,
    },
    /// 统一规约业务事件
    Emit(TopicEvent),
    Signal {
        binding_id: i64,
        event: TaskEvent,
    },
}
// /// 启动任务
// Start {
//     is_main: bool,
//     spec: TaskSpec,
//     config: AgentRunConfig,
// },
// /// 取消任务
// Cancel {
//     binding_id: i64,
// },
// /// 提交审批
// Approval {
//     binding_id: i64,
//     allow_ids: Vec<i64>,
//     deny_ids: Vec<i64>,
// },
// /// 审批已完成
// ApprovalResolved {
//     binding_id: i64,
// },
// /// 终止运行
// Shutdown,

// /// 请求创建子任务
// SpawnAgent {
//     parent_binding_id: i64,
//     call_id: String,
//     request: SpawnAgentRequest,
//     reply: oneshot::Sender<SpawnAgentResponse>,
// },
// /// 创建子任务成功
// ChildStarted {
//     parent_binding_id: i64,
//     spec: TaskSpec,
//     config: AgentRunConfig,
// },
// /// 父任务所有子任务都完成
// ChildResolved {
//     parent_binding_id: i64,
// },

// Signal {
//     binding_id: i64,
//     event: TaskEvent,
// },
