use super::effect::Effect;
use crate::agent::runtime::AgentRunConfig;
use crate::agent::task::TaskSpec;
use crate::models::{AgentStatus, Message};
use wind_ai::message::Content;
use wind_ai::tool::FunctionCall;

/// Agent 任务事件
#[derive(Debug, strum::AsRefStr)]
pub enum TaskEvent {
    /// 工具调用需要审批
    ApprovalRequired {
        data: Message,
        calls: Vec<FunctionCall>,
    },
    /// 任务完成
    Finish { data: Message },
    /// 任务失败
    Failed {
        /// 任务失败时会携带原 Message
        data: Option<Message>,
        error: String,
    },
    /// 任务已取消
    Cancelled,
    /// 启动任务
    Start {
        spec: TaskSpec,
        config: AgentRunConfig,
    },
    /// 子任务创建成功。
    ChildSpawned,
    /// 已审批，恢复运行。
    ApprovalResolved,
    /// 子任务完成，恢复运行。
    ChildResolved,
    /// 收到取消指令
    Cancel,
}
impl std::fmt::Display for TaskEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name_ref = self.as_ref();
        let (name, args) = match self {
            TaskEvent::ApprovalRequired { data, calls } => (
                name_ref,
                format!("(message_id = {}, calls_len = {})", data.id, calls.len()),
            ),
            TaskEvent::Finish { data } => (name_ref, format!("(message_id = {})", data.id)),
            TaskEvent::Failed { data, error } => (
                name_ref,
                format!(
                    "(message_id = {}, error = {})",
                    data.as_ref().map(|d| d.id).unwrap_or_default(),
                    error
                ),
            ),
            TaskEvent::Cancelled => (name_ref, String::new()),
            TaskEvent::Start { spec, .. } => {
                (name_ref, format!("(binding_id = {})", spec.binding_id))
            }
            TaskEvent::ChildSpawned => (name_ref, String::new()),
            TaskEvent::ApprovalResolved => (name_ref, String::new()),
            TaskEvent::ChildResolved => (name_ref, String::new()),
            TaskEvent::Cancel => (name_ref, String::new()),
        };
        write!(f, "[TaskEvent {name}] {}", args)
    }
}

/// Agent 任务状态
pub struct TaskFsm {
    binding_id: i64,
    parent_topic_id: i64,
    topic_id: i64,
    state: AgentStatus,
}

impl TaskFsm {
    pub fn new(binding_id: i64, parent_topic_id: i64, topic_id: i64) -> Self {
        Self {
            binding_id,
            parent_topic_id,
            topic_id,
            state: AgentStatus::Idle,
        }
    }

    pub fn binding_id(&self) -> i64 {
        self.binding_id
    }

    pub fn state(&self) -> AgentStatus {
        self.state
    }

    /// 迁移任务状态并生成副作用。
    pub fn reduce(&mut self, new_event: TaskEvent) -> Vec<Effect> {
        use AgentStatus as S;
        use TaskEvent as E;
        let binding_id = self.binding_id;
        let from = self.state;
        // 借用取出判别值，避免后续 match 按值 move 后无法再访问 new_event。
        let is_cancel = matches!(&new_event, E::Cancel);
        match (from, new_event) {
            (S::Idle | S::Finished | S::Failed | S::Cancelled, E::Start { spec, config }) => {
                self.state = S::Running;
                vec![
                    Effect::Start {
                        binding_id,
                        spec,
                        config,
                    },
                    Effect::PersistStatus {
                        binding_id,
                        status: self.state,
                    },
                ]
            }
            (S::WaitingApproval, E::ApprovalResolved) => {
                self.state = S::Running;
                vec![
                    Effect::PersistStatus {
                        binding_id,
                        status: self.state,
                    },
                    Effect::Resume { binding_id },
                ]
            }
            (S::WaitingChild, E::ChildResolved) => {
                self.state = S::Running;
                vec![Effect::PersistStatus {
                    binding_id,
                    status: self.state,
                }]
            }
            (S::Running, E::ApprovalRequired { data, calls }) => {
                self.state = S::WaitingApproval;
                vec![
                    Effect::PersistStatus {
                        binding_id,
                        status: self.state,
                    },
                    Effect::ApprovalRequest {
                        binding_id: self.binding_id,
                        data,
                        calls,
                    },
                ]
            }
            (S::Running, E::ChildSpawned) => {
                self.state = S::WaitingChild;
                vec![Effect::PersistStatus {
                    binding_id,
                    status: self.state,
                }]
            }
            (S::Running, E::Finish { data }) => {
                self.state = S::Finished;
                let output = Self::finished_output(&data);
                vec![
                    Effect::PersistStatus {
                        binding_id,
                        status: self.state,
                    },
                    Effect::Finish { binding_id, data },
                    Effect::SendChildResponse {
                        binding_id,
                        status: self.state,
                        output,
                    },
                ]
            }
            (S::Running, E::Failed { data, error }) => {
                self.state = S::Failed;
                vec![
                    Effect::PersistStatus {
                        binding_id,
                        status: self.state,
                    },
                    Effect::Failed {
                        binding_id,
                        data,
                        error: error.clone(),
                    },
                    Effect::SendChildResponse {
                        binding_id,
                        status: self.state,
                        output: vec![Content::new_text(error)],
                    },
                ]
            }
            (S::Running, E::Cancel)
            | (S::WaitingApproval, E::Cancel)
            | (S::WaitingChild, E::Cancel)
            | (S::Running, E::Cancelled)
            | (S::WaitingApproval, E::Cancelled)
            | (S::WaitingChild, E::Cancelled) => {
                self.state = S::Cancelled;
                let mut effects = vec![
                    Effect::PersistStatus {
                        binding_id,
                        status: self.state,
                    },
                    Effect::SendChildResponse {
                        binding_id,
                        status: self.state,
                        output: vec![Content::new_text("Task was cancelled".to_string())],
                    },
                ];
                // "取消指令"需要额外下发 CancelAgent；
                if is_cancel {
                    effects.insert(1, Effect::Cancel { binding_id });
                }
                effects
            }
            (other_s, other_e) => {
                log::warn!(
                    "[TaskFsm] Task {} cannot transition from {} to {}",
                    binding_id,
                    other_s,
                    other_e
                );
                vec![]
            }
        }
    }
    fn finished_output(data: &Message) -> Vec<Content> {
        data.content
            .last()
            .and_then(|c| {
                if c.is_simple() {
                    Some(c.content.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| vec![Content::new_text("Task has no valid result".to_string())])
    }
}
