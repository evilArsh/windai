use crate::error::CoreError;
use crate::error::Result;
use crate::models::AgentStatus;
use crate::models::Message;
use crate::models::ToolApprovalRequest;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};
use wind_ai::message::Content;
use wind_ai::message::Message as AiMessage;

use super::task::SupervisorRequest;
use super::task::TaskNotification;

/// 外部通知事件
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum TopicEvent {
    /// 错误消息
    Error {
        binding_id: i64,
        topic_id: i64,
        parent_topic_id: i64,
        message_id: Option<i64>,
        error: String,
    },
    /// 全量快照消息
    Snapshot {
        binding_id: i64,
        topic_id: i64,
        parent_topic_id: i64,
        messages: Vec<Message>,
    },
    /// 消息已创建
    MessageCreated { topic_id: i64, data: Message },
    /// 流式分片消息
    Message {
        binding_id: i64,
        topic_id: i64,
        parent_topic_id: i64,
        message_id: i64,
        index: i32,
        data: AiMessage,
    },
    /// 消息完成
    MessageFinished {
        binding_id: i64,
        parent_topic_id: i64,
        topic_id: i64,
        message_id: i64,
    },
    TaskStatusChanged {
        binding_id: i64,
        topic_id: i64,
        parent_topic_id: i64,
        status: AgentStatus,
    },
    ApprovalRequired {
        binding_id: i64,
        topic_id: i64,
        parent_topic_id: i64,
        message_id: i64,
        requests: Vec<ToolApprovalRequest>,
    },
}

/// 外部调用命令
#[derive(Debug)]
pub enum TopicCommand {
    /// 启动一个对话
    CreateChat {
        user_input: Vec<Content>,
        reply: oneshot::Sender<Result<()>>,
    },
    CancelTask {
        binding_id: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<()>>,
    },
    Approval {
        binding_id: i64,
        deny_ids: Vec<i64>,
        allow_ids: Vec<i64>,
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        reply: oneshot::Sender<broadcast::Receiver<TopicEvent>>,
    },
}

impl std::fmt::Display for TopicEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (name, topic_id, binding_id) = match self {
            TopicEvent::Error {
                binding_id,
                topic_id,
                ..
            } => ("Error", topic_id.to_string(), binding_id.to_string()),
            TopicEvent::Snapshot {
                binding_id,
                topic_id,
                ..
            } => ("Snapshot", topic_id.to_string(), binding_id.to_string()),
            TopicEvent::MessageCreated { topic_id, .. } => {
                ("MessageCreated", topic_id.to_string(), String::new())
            }
            TopicEvent::Message {
                binding_id,
                topic_id,
                ..
            } => ("Message", topic_id.to_string(), binding_id.to_string()),
            TopicEvent::MessageFinished {
                binding_id,
                topic_id,
                ..
            } => (
                "MessageFinished",
                topic_id.to_string(),
                binding_id.to_string(),
            ),
            TopicEvent::TaskStatusChanged {
                binding_id,
                topic_id,
                ..
            } => (
                "TaskStatusChanged",
                topic_id.to_string(),
                binding_id.to_string(),
            ),
            TopicEvent::ApprovalRequired {
                binding_id,
                topic_id,
                ..
            } => (
                "ApprovalRequired",
                topic_id.to_string(),
                binding_id.to_string(),
            ),
        };
        write!(
            f,
            "[TopicEvent {name}]\n(topic_id = {topic_id} , binding_id = {binding_id})"
        )
    }
}

impl std::fmt::Display for TopicCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            TopicCommand::CreateChat { .. } => "CreateChat",
            TopicCommand::CancelTask { .. } => "CancelTask",
            TopicCommand::Shutdown { .. } => "Shutdown",
            TopicCommand::Approval { .. } => "Approval",
            TopicCommand::Subscribe { .. } => "Subscribe",
        };
        write!(f, "[TopicCommand {name}]")
    }
}

pub enum TopicMsg {
    Command(TopicCommand),
    Task(TaskNotification),
    Supervisor(SupervisorRequest),
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
