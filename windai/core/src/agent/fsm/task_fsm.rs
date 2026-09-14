use super::super::task::TaskSpec;
use super::effect::Effect;
use crate::models::{AgentMode, AgentStatus, Message};
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
    Start { spec: TaskSpec },
    /// 子任务创建成功
    ChildSpawned,
    /// 已审批，恢复运行
    ApprovalResolved,
    /// 子任务完成，恢复运行
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
                (name_ref, format!("(instance_id = {})", spec.instance.id))
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
    instance_id: i64,
    state: AgentStatus,
    mode: AgentMode,
}

impl TaskFsm {
    pub fn new(instance_id: i64) -> Self {
        Self {
            instance_id,
            state: AgentStatus::Idle,
            mode: AgentMode::Sync,
        }
    }

    pub fn instance_id(&self) -> i64 {
        self.instance_id
    }

    pub fn state(&self) -> AgentStatus {
        self.state
    }

    /// 迁移任务状态并生成副作用
    pub fn reduce(&mut self, new_event: TaskEvent) -> Vec<Effect> {
        use AgentStatus as S;
        use TaskEvent as E;
        let instance_id = self.instance_id;
        let from = self.state;
        let is_cancel = matches!(&new_event, E::Cancel);
        match (from, new_event) {
            (S::Idle | S::Finished | S::Failed | S::Cancelled, E::Start { spec }) => {
                self.state = S::Running;
                self.mode = spec.instance.mode.unwrap_or(AgentMode::Sync);
                let agent_id = spec.instance.agent_id;
                vec![
                    Effect::PersistStatus {
                        instance_id,
                        status: self.state,
                        mode: self.mode,
                        agent_id,
                    },
                    Effect::Start { spec },
                ]
            }
            (S::WaitingApproval, E::ApprovalResolved) => {
                self.state = S::Running;
                vec![
                    Effect::PersistStatus {
                        instance_id,
                        status: self.state,
                        mode: self.mode,
                        agent_id: None,
                    },
                    Effect::Resume { instance_id },
                ]
            }
            (S::WaitingChild, E::ChildResolved) => {
                self.state = S::Running;
                vec![Effect::PersistStatus {
                    instance_id,
                    status: self.state,
                    mode: self.mode,
                    agent_id: None,
                }]
            }
            (S::Running, E::ApprovalRequired { data, calls }) => {
                self.state = S::WaitingApproval;
                vec![
                    Effect::PersistStatus {
                        instance_id,
                        status: self.state,
                        mode: self.mode,
                        agent_id: None,
                    },
                    Effect::ApprovalRequest {
                        instance_id: self.instance_id,
                        data,
                        calls,
                    },
                ]
            }
            (S::Running, E::ChildSpawned) => {
                self.state = S::WaitingChild;
                vec![Effect::PersistStatus {
                    instance_id,
                    status: self.state,
                    mode: self.mode,
                    agent_id: None,
                }]
            }
            (S::Running, E::Finish { data }) => {
                self.state = S::Finished;
                vec![
                    Effect::PersistStatus {
                        instance_id,
                        status: self.state,
                        mode: self.mode,
                        agent_id: None,
                    },
                    Effect::Completed {
                        instance_id,
                        data,
                        status: self.state,
                    },
                ]
            }
            (S::Running, E::Failed { data, error }) => {
                self.state = S::Failed;
                vec![
                    Effect::PersistStatus {
                        instance_id,
                        status: self.state,
                        mode: self.mode,
                        agent_id: None,
                    },
                    Effect::Failed {
                        instance_id,
                        data,
                        error: error.clone(),
                        status: self.state,
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
                        instance_id,
                        status: self.state,
                        mode: self.mode,
                        agent_id: None,
                    },
                    Effect::Canceled {
                        instance_id,
                        status: self.state,
                        error: "Task was cancelled".into(),
                    },
                ];
                // "取消指令"需要额外下发 CancelAgent；
                if is_cancel {
                    effects.insert(1, Effect::Cancel { instance_id });
                }
                effects
            }
            (other_s, other_e) => {
                log::warn!(
                    "[TaskFsm] Task {} cannot transition from {} to {}",
                    instance_id,
                    other_s,
                    other_e
                );
                vec![]
            }
        }
    }
}
