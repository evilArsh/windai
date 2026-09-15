use crate::{
    agent::{
        event::TopicEvent,
        task::TaskSpec,
        tool::{SpawnAgentRequest, SpawnAgentResponse},
    },
    models::{AgentMode, AgentStatus, Message},
};
use tokio::sync::oneshot;
use wind_ai::{message::Content, tool::FunctionCall};

#[derive(Debug, strum::AsRefStr)]
pub enum Effect {
    /// 保存任务状态
    PersistStatus {
        instance_id: i64,
        status: AgentStatus,
        mode: AgentMode,
        agent_id: Option<i64>,
    },
    /// 广播业务事件
    Emit(TopicEvent),
    /// 启动 AgentRuntime
    Start { spec: TaskSpec },
    /// 审批后恢复运行
    Resume { instance_id: i64 },
    /// 取消 Agent 任务
    Cancel { instance_id: i64 },
    /// 创建子 Agent
    SpawnChild {
        /// 发出此命令的 instance_id
        instance_id: i64,
        call_id: String,
        request: SpawnAgentRequest,
        reply: oneshot::Sender<SpawnAgentResponse>,
    },
    /// 批量写审批状态
    Approval {
        instance_id: i64,
        allow_ids: Vec<i64>,
        deny_ids: Vec<i64>,
    },
    /// 终止整个 topic runtime
    StopRuntime,
    /// 关闭 topic 任务事件流
    CloseEventStream,
    /// 初始化任务
    Init { user_input: Vec<Content> },
    /// agent 发出审批请求
    ApprovalRequest {
        instance_id: i64,
        data: Message,
        calls: Vec<FunctionCall>,
    },
    Completed {
        instance_id: i64,
        data: Message,
        status: AgentStatus,
    },
    Failed {
        instance_id: i64,
        data: Option<Message>,
        status: AgentStatus,
        error: String,
    },
    Canceled {
        instance_id: i64,
        status: AgentStatus,
        error: String,
    },
}
impl std::fmt::Display for Effect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name_ref = self.as_ref();
        let (name, args) = match self {
            Effect::PersistStatus {
                instance_id,
                status,
                mode,
                agent_id,
            } => (
                name_ref,
                format!(
                    "(instance_id = {instance_id}, status = {status}, mode = {mode}), agent_id = {:?}",
                    agent_id,
                ),
            ),
            Effect::Emit(topic_event) => (name_ref, format!("(topic_event = {})", topic_event)),
            Effect::Start { spec } => {
                let instance_id = spec.instance.id;
                (
                    name_ref,
                    format!(
                        "(instance_id = {instance_id}, spec = {})",
                        spec.assistant
                            .content
                            .last()
                            .and_then(|c| Some(Content::arr_to_string(&c.content)))
                            .unwrap_or_default()
                    ),
                )
            }
            Effect::Resume { instance_id } => (name_ref, format!("(instance_id = {instance_id})")),
            Effect::Cancel { instance_id } => (name_ref, format!("(instance_id = {instance_id})")),
            Effect::Canceled {
                instance_id,
                status,
                ..
            } => (
                name_ref,
                format!("(instance_id = {instance_id}, status = {status}))"),
            ),
            Effect::SpawnChild {
                instance_id,
                call_id,
                request,
                ..
            } => (
                name_ref,
                format!(
                    "(from_instance_id = {instance_id}, call_id = {call_id}, agent-key = {}, mode = {}))",
                    request.agent_key, request.mode
                ),
            ),
            Effect::Approval { instance_id, .. } => {
                (name_ref, format!("(instance_id = {instance_id})"))
            }
            Effect::StopRuntime => (name_ref, String::new()),
            Effect::CloseEventStream => (name_ref, String::new()),
            Effect::Init { .. } => (name_ref, String::new()),
            Effect::ApprovalRequest { .. } => (name_ref, String::new()),
            Effect::Completed { instance_id, .. } => {
                (name_ref, format!("(instance_id = {instance_id})"))
            }
            Effect::Failed { instance_id, .. } => {
                (name_ref, format!("(instance_id = {instance_id})"))
            }
        };
        write!(f, "[Effect {name}]\n{}", args)
    }
}
