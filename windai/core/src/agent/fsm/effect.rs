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
impl std::fmt::Display for Effect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (name, args) = match self {
            Effect::PersistStatus { binding_id, status } => (
                "PersistStatus",
                format!("(binding_id = {binding_id}, status = {status})"),
            ),
            Effect::Emit(topic_event) => ("Emit", format!("(topic_event = {}", topic_event)),
            Effect::StartAgent {
                binding_id, spec, ..
            } => (
                "StartAgent",
                format!(
                    "(binding_id = {binding_id}, spec = {})",
                    spec.assistant
                        .content
                        .last()
                        .and_then(|c| Some(Content::arr_to_string(&c.content)))
                        .unwrap_or_default()
                ),
            ),
            Effect::ResumeAgent { binding_id } => {
                ("ResumeAgent", format!("(binding_id = {binding_id})"))
            }
            Effect::CancelAgent { binding_id } => {
                ("CancelAgent", format!("(binding_id = {binding_id})"))
            }
            Effect::SendChildResponse {
                binding_id, status, ..
            } => (
                "SendChildResponse",
                format!("(binding_id = {binding_id}, status = {status}))"),
            ),
            Effect::SpawnChild {
                parent_binding_id,
                call_id,
                request,
                ..
            } => (
                "SpawnChild",
                format!(
                    "(parent_binding_id = {parent_binding_id}, call_id = {call_id}, agent-key = {}, mode = {}))",
                    request.agent_key, request.mode
                ),
            ),
            Effect::ApplyApprovals { binding_id, .. } => {
                ("ApplyApprovals", format!("(binding_id = {binding_id})"))
            }
            Effect::CloseEventStream => ("CloseEventStream", String::new()),
            Effect::StopRuntime => ("StopRuntime", String::new()),
        };
        write!(f, "[Effect {name}]\n{}", args)
    }
}
