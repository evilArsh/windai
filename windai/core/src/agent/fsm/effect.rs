use super::state::TaskState;
use crate::agent::{
    event::TopicEvent,
    runtime::AgentRunConfig,
    task::TaskSpec,
    tool::{SpawnAgentRequest, SpawnAgentResponse},
};
use tokio::sync::oneshot;
use wind_ai::message::Content;

#[derive(Debug)]
pub enum Effect {
    /// 写 DB 状态
    PersistStatus { binding_id: i64, status: TaskState },
    /// 广播业务事件
    Emit(TopicEvent),
    /// 启动 AgentRuntime
    StartAgent {
        binding_id: i64,
        spec: TaskSpec,
        config: AgentRunConfig,
    },
    /// 审批后恢复运行。
    ResumeAgent { binding_id: i64 },
    /// 取消 Agent。
    CancelAgent { binding_id: i64 },
    /// 解析 pending 子任务并回复父任务。
    SendChildResponse {
        binding_id: i64,
        status: TaskState,
        output: Vec<Content>,
    },
    /// 创建子 Agent
    SpawnChild {
        parent_binding_id: i64,
        call_id: String,
        request: SpawnAgentRequest,
        reply: oneshot::Sender<SpawnAgentResponse>,
    },
    /// 批量写审批状态
    ApplyApprovals {
        binding_id: i64,
        allow_ids: Vec<i64>,
        deny_ids: Vec<i64>,
    },
    /// 关闭当前对话的事件流
    CloseEventStream,
    /// 终止整个 topic runtime。
    StopRuntime,
}
