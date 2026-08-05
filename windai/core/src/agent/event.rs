use crate::error::CoreError;
use crate::error::Result;
use crate::models::AgentStatus;
use crate::models::Message;
use crate::models::ToolApprovalRequest;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
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
