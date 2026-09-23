use super::task::SupervisorRequest;
use super::task::TaskNotification;
use crate::error::CoreError;
use crate::error::Result;
use crate::models::AgentInstance;
use crate::models::AgentMode;
use crate::models::AgentStatus;
use crate::models::Message;
use crate::models::ToolApprovalRequest;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};
use wind_ai::message::Content;
use wind_ai::message::Message as AiMessage;

/// 外部通知事件
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone, strum::AsRefStr)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum TopicEvent {
    /// 错误消息
    Error {
        /// 来自 Agent 实例的错误
        instance_id: Option<i64>,
        /// 话题 id
        topic_id: i64,
        /// 消息 id
        message_id: Option<i64>,
        /// 错误信息
        error: String,
    },
    /// 全量快照消息
    Snapshot {
        /// agent 实例 id
        instance_id: i64,
        /// 话题 id
        topic_id: i64,
        /// 全量消息
        messages: Vec<Message>,
    },
    /// 消息已创建
    MessageCreated {
        /// 话题 id
        topic_id: i64,
        /// instance id
        instance_id: i64,
        /// 初始化消息
        data: Message,
    },
    /// 流式分片消息
    Message {
        /// agent 实例 id
        instance_id: i64,
        /// 话题 id
        topic_id: i64,
        /// 消息 id
        message_id: i64,
        /// 消息索引，用于标识消息顺序
        index: i32,
        /// 消息内容
        data: AiMessage,
    },
    /// 消息完成
    MessageFinished {
        /// agent 实例 id
        instance_id: i64,
        /// 话题 id
        topic_id: i64,
        // 消息 id
        message_id: i64,
    },
    /// 任务状态变更
    TaskStatusChanged {
        /// agent 实例 id
        instance_id: i64,
        /// 话题 id
        topic_id: i64,
        /// 任务状态
        status: AgentStatus,
        /// 任务模式
        mode: AgentMode,
    },
    /// 需要用户审批
    ApprovalRequired {
        /// agent 实例 id
        instance_id: i64,
        /// 话题 id
        topic_id: i64,
        /// 消息 id
        message_id: i64,
        /// 审批请求
        requests: Vec<ToolApprovalRequest>,
    },
    // 实例已创建
    InstanceCreated {
        data: AgentInstance,
    },
}

/// 外部调用命令
#[derive(Debug, strum::AsRefStr)]
pub enum TopicCommand {
    /// 启动一个对话
    Start {
        user_input: Vec<Content>,
        reply: oneshot::Sender<Result<()>>,
    },
    Cancel {
        instance_id: i64,
    },
    Shutdown,
    Approval {
        instance_id: i64,
        deny_ids: Vec<i64>,
        allow_ids: Vec<i64>,
    },
    Subscribe {
        reply: oneshot::Sender<broadcast::Receiver<TopicEvent>>,
    },
}

impl std::fmt::Display for TopicEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (name, args) = match self {
            TopicEvent::Error {
                instance_id,
                topic_id,
                error,
                ..
            } => (
                self.as_ref(),
                format!(
                    "(topic_id = {}, instance_id = {}, error = {})",
                    topic_id.to_string(),
                    instance_id.map(|t| t.to_string()).unwrap_or_default(),
                    error
                ),
            ),
            TopicEvent::Snapshot {
                instance_id,
                topic_id,
                ..
            } => (
                self.as_ref(),
                format!(
                    "(topic_id = {}, instance_id = {})",
                    topic_id.to_string(),
                    instance_id.to_string(),
                ),
            ),
            TopicEvent::MessageCreated {
                instance_id,
                topic_id,
                ..
            } => (
                self.as_ref(),
                format!(
                    "(topic_id = {}, instance_id = {})",
                    topic_id.to_string(),
                    instance_id.to_string(),
                ),
            ),
            TopicEvent::Message {
                instance_id,
                topic_id,
                ..
            } => (
                self.as_ref(),
                format!(
                    "(topic_id = {}, instance_id = {})",
                    topic_id.to_string(),
                    instance_id.to_string(),
                ),
            ),
            TopicEvent::MessageFinished {
                instance_id,
                topic_id,
                ..
            } => (
                self.as_ref(),
                format!(
                    "(topic_id = {}, instance_id = {})",
                    topic_id.to_string(),
                    instance_id.to_string(),
                ),
            ),
            TopicEvent::TaskStatusChanged {
                instance_id,
                topic_id,
                ..
            } => (
                self.as_ref(),
                format!(
                    "(topic_id = {}, instance_id = {})",
                    topic_id.to_string(),
                    instance_id.to_string(),
                ),
            ),
            TopicEvent::ApprovalRequired {
                instance_id,
                topic_id,
                ..
            } => (
                self.as_ref(),
                format!(
                    "(topic_id = {}, instance_id = {})",
                    topic_id.to_string(),
                    instance_id.to_string(),
                ),
            ),
            TopicEvent::InstanceCreated { data } => (
                self.as_ref(),
                format!(
                    "(topic_id = {}, instance_id = {}, parent_instance_id = {:?})",
                    data.topic_id.to_string(),
                    data.id.to_string(),
                    data.parent_id
                ),
            ),
        };
        write!(f, "[TopicEvent {name}] {args}")
    }
}

impl std::fmt::Display for TopicCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.as_ref();
        write!(f, "[TopicCommand {name}]")
    }
}

pub enum TopicMsg {
    Command(TopicCommand),
    Task(TaskNotification),
    Supervisor(SupervisorRequest),
}
impl std::fmt::Display for TopicMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicMsg::Command(topic_command) => write!(f, "{}", topic_command),
            TopicMsg::Task(task_notification) => write!(f, "{}", task_notification),
            TopicMsg::Supervisor(supervisor_request) => write!(f, "{}", supervisor_request),
        }
    }
}
#[derive(Clone)]
pub struct TopicMailbox {
    tx: mpsc::Sender<TopicMsg>,
}
impl TopicMailbox {
    pub fn new(tx: mpsc::Sender<TopicMsg>) -> Self {
        Self { tx }
    }

    pub async fn send(&self, msg: TopicMsg) -> Result<()> {
        self.tx
            .send(msg)
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))
    }

    pub async fn notify_task(&self, event: TaskNotification) -> Result<()> {
        self.send(TopicMsg::Task(event)).await
    }

    pub async fn request_supervisor(&self, req: SupervisorRequest) -> Result<()> {
        self.send(TopicMsg::Supervisor(req)).await
    }
}
