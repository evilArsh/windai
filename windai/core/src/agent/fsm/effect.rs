use crate::{
    agent::{
        event::TopicEvent,
        runtime::AgentRunConfig,
        task::TaskSpec,
        tool::{SpawnAgentRequest, SpawnAgentResponse},
    },
    models::{AgentStatus, Message},
};
use tokio::sync::oneshot;
use wind_ai::{message::Content, tool::FunctionCall};

#[derive(Debug, strum::AsRefStr)]
pub enum Effect {
    /// 保存任务状态
    PersistStatus {
        binding_id: i64,
        status: AgentStatus,
    },
    /// 广播业务事件
    Emit(TopicEvent),
    /// 启动 AgentRuntime
    Start {
        binding_id: i64,
        spec: TaskSpec,
        config: AgentRunConfig,
    },
    /// 审批后恢复运行。
    Resume {
        binding_id: i64,
    },
    /// 取消 Agent 任务。
    Cancel {
        binding_id: i64,
    },
    /// 解析 pending 子任务并回复父任务。
    SendChildResponse {
        binding_id: i64,
        status: AgentStatus,
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
    Approval {
        binding_id: i64,
        allow_ids: Vec<i64>,
        deny_ids: Vec<i64>,
    },
    /// 关闭当前对话的事件流
    CloseEventStream,
    /// 终止整个 topic runtime。
    StopRuntime,
    PrepareMain {
        user_input: Vec<Content>,
    },
    ApprovalRequest {
        binding_id: i64,
        data: Message,
        calls: Vec<FunctionCall>,
    },
    Finish {
        binding_id: i64,
        data: Message,
    },
    Failed {
        binding_id: i64,
        // message_id: Option<i64>,
        data: Option<Message>,
        error: String,
    },
}
impl std::fmt::Display for Effect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name_ref = self.as_ref();
        let (name, args) = match self {
            Effect::PersistStatus { binding_id, status } => (
                name_ref,
                format!("(binding_id = {binding_id}, status = {status})"),
            ),
            Effect::Emit(topic_event) => (name_ref, format!("(topic_event = {})", topic_event)),
            Effect::Start {
                binding_id, spec, ..
            } => (
                name_ref,
                format!(
                    "(binding_id = {binding_id}, spec = {})",
                    spec.assistant
                        .content
                        .last()
                        .and_then(|c| Some(Content::arr_to_string(&c.content)))
                        .unwrap_or_default()
                ),
            ),
            Effect::Resume { binding_id } => (name_ref, format!("(binding_id = {binding_id})")),
            Effect::Cancel { binding_id } => (name_ref, format!("(binding_id = {binding_id})")),
            Effect::SendChildResponse {
                binding_id, status, ..
            } => (
                name_ref,
                format!("(binding_id = {binding_id}, status = {status}))"),
            ),
            Effect::SpawnChild {
                parent_binding_id,
                call_id,
                request,
                ..
            } => (
                name_ref,
                format!(
                    "(parent_binding_id = {parent_binding_id}, call_id = {call_id}, agent-key = {}, mode = {}))",
                    request.agent_key, request.mode
                ),
            ),
            Effect::Approval { binding_id, .. } => {
                (name_ref, format!("(binding_id = {binding_id})"))
            }
            Effect::CloseEventStream => (name_ref, String::new()),
            Effect::StopRuntime => (name_ref, String::new()),
            Effect::PrepareMain { .. } => (name_ref, String::new()),
            Effect::ApprovalRequest { .. } => (name_ref, String::new()),
            Effect::Finish { binding_id, .. } => (name_ref, format!("(binding_id = {binding_id})")),
            Effect::Failed { binding_id, .. } => (name_ref, format!("(binding_id = {binding_id})")),
        };
        write!(f, "[Effect {name}]\n{}", args)
    }
}
