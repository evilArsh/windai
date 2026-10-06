use crate::models::AgentInstance;
use crate::models::AgentMode;
use crate::models::AgentStatus;
use crate::models::Message;
use crate::models::ToolApprovalRequest;
use serde::{Deserialize, Serialize};
use wind_ai::message::Message as AiMessage;

/// 外部通知事件
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone, strum::AsRefStr)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TopicEvent {
    /// 错误消息
    Error {
        /// 来自 Agent 实例的错误
        instance_id: Option<i64>,
        /// 消息 id
        message_id: Option<i64>,
        /// 错误信息
        error: String,
    },
    /// 消息已创建
    MessageCreated {
        /// instance id
        instance_id: i64,
        /// 初始化消息
        data: Message,
    },
    /// 内容块消息
    Message {
        /// agent 实例 id
        instance_id: i64,
        /// 消息 id
        message_id: i64,
        /// 本次运行内的块序号
        index: i64,
        /// 消息内容
        data: AiMessage,
    },
    /// 消息完成
    MessageFinished {
        /// agent 实例 id
        instance_id: i64,
        // 消息 id
        message_id: i64,
    },
    /// 任务状态变更
    TaskStatusChanged {
        /// agent 实例 id
        instance_id: i64,
        /// 任务状态
        status: AgentStatus,
        /// 任务模式
        mode: AgentMode,
    },
    /// 需要用户审批的消息
    ApprovalRequired {
        /// agent 实例 id
        instance_id: i64,
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

impl std::fmt::Display for TopicEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (name, args) = match self {
            TopicEvent::Error {
                instance_id, error, ..
            } => (
                self.as_ref(),
                format!(
                    "(instance_id = {}, error = {})",
                    instance_id.map(|t| t.to_string()).unwrap_or_default(),
                    error
                ),
            ),
            TopicEvent::MessageCreated { instance_id, .. } => (
                self.as_ref(),
                format!("(instance_id = {})", instance_id.to_string(),),
            ),
            TopicEvent::Message { instance_id, .. } => (
                self.as_ref(),
                format!("(instance_id = {})", instance_id.to_string(),),
            ),
            TopicEvent::MessageFinished { instance_id, .. } => (
                self.as_ref(),
                format!("(instance_id = {})", instance_id.to_string(),),
            ),
            TopicEvent::TaskStatusChanged { instance_id, .. } => (
                self.as_ref(),
                format!("(instance_id = {})", instance_id.to_string(),),
            ),
            TopicEvent::ApprovalRequired { instance_id, .. } => (
                self.as_ref(),
                format!("(instance_id = {})", instance_id.to_string(),),
            ),
            TopicEvent::InstanceCreated { data } => (
                self.as_ref(),
                format!(
                    "(instance_id = {}, parent_instance_id = {:?})",
                    data.id.to_string(),
                    data.parent_id
                ),
            ),
        };
        write!(f, "[TopicEvent {name}] {args}")
    }
}
