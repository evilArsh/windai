use super::effect::Effect;
use super::state::TaskState;
use crate::agent::event::TopicEvent;
use crate::agent::runtime::AgentRunConfig;
use crate::agent::task::TaskSpec;
use crate::models::{Message, ToolApprovalRequest};
use wind_ai::message::Content;

/// Agent 任务事件
#[derive(Debug)]
pub enum TaskEvent {
    /// 启动任务
    Start {
        spec: TaskSpec,
        config: AgentRunConfig,
    },
    /// 模型请求工具审批
    AwaitApproval {
        data: Message,
        requests: Vec<ToolApprovalRequest>,
    },
    /// 子任务创建成功。
    ChildSpawned,
    /// 已审批，恢复运行。
    ApprovalResolved,
    /// 子任务完成，恢复运行。
    ChildResolved,
    /// 正常完成。
    Completed { data: Message },
    /// 失败。
    Failed {
        error: String,
        message_id: Option<i64>,
    },
    /// 收到取消指令
    Cancel,
    /// 任务已取消
    Cancelled,
}

/// Agent 任务状态
pub struct TaskFsm {
    binding_id: i64,
    parent_topic_id: i64,
    topic_id: i64,
    state: TaskState,
}

impl TaskFsm {
    pub fn new(binding_id: i64, parent_topic_id: i64, topic_id: i64) -> Self {
        Self {
            binding_id,
            parent_topic_id,
            topic_id,
            state: TaskState::Idle,
        }
    }

    pub fn binding_id(&self) -> i64 {
        self.binding_id
    }

    pub fn state(&self) -> TaskState {
        self.state
    }

    /// 状态转移
    /// 返回合法转移的目标状态，非法转移返回 `None`。
    pub fn target(from_state: TaskState, new_event: &TaskEvent) -> Option<TaskState> {
        use TaskEvent as E;
        use TaskState as S;
        Some(match (from_state, new_event) {
            // 首次启动（Idle）或终态重启都进入 Running。
            (S::Idle | S::Finished | S::Failed | S::Cancelled, E::Start { .. }) => S::Running,
            (S::Running, E::AwaitApproval { .. }) => S::WaitingApproval,
            (S::Running, E::ChildSpawned) => S::WaitingChild,
            // 同一父任务可连续生成多个子任务：保持 WaitingChild。
            (S::WaitingChild, E::ChildSpawned) => S::WaitingChild,
            (S::Running, E::Completed { .. }) => S::Finished,
            (S::Running, E::Failed { .. }) => S::Failed,
            (S::Running, E::Cancel) | (S::Running, E::Cancelled) => S::Cancelled,
            (S::WaitingApproval, E::ApprovalResolved) => S::Running,
            (S::WaitingApproval, E::Cancel) | (S::WaitingApproval, E::Cancelled) => S::Cancelled,
            (S::WaitingChild, E::ChildResolved) => S::Running,
            (S::WaitingChild, E::Cancel) | (S::WaitingChild, E::Cancelled) => S::Cancelled,
            _ => return None,
        })
    }

    /// 迁移任务状态并生成副作用。
    pub fn reduce(&mut self, new_event: TaskEvent) -> Vec<Effect> {
        let from = self.state;
        let Some(to) = Self::target(from, &new_event) else {
            log::warn!(
                "[TaskFsm] illegal transition: {from:?} -- {new_event:?}, (binding_id = {})",
                self.binding_id
            );
            return vec![];
        };
        if to == from {
            return vec![];
        }
        // Start 事件可能携带新的子主题 id（如主任务重跑）。
        // if let TaskEvent::Start { config, .. } = &event {
        //     self.topic_id = config.topic_id;
        // }
        self.state = to;
        Self::effects(
            self.binding_id,
            self.parent_topic_id,
            self.topic_id,
            from,
            to,
            &new_event,
        )
    }

    fn effects(
        binding_id: i64,
        parent_topic_id: i64,
        topic_id: i64,
        from: TaskState,
        to: TaskState,
        event: &TaskEvent,
    ) -> Vec<Effect> {
        use TaskEvent as E;
        use TaskState as S;
        match (from, to) {
            // 启动（含终态重启）：先 StartAgent（actor 会注册 registry 条目），再持久化状态。
            (S::Idle | S::Finished | S::Failed | S::Cancelled, S::Running) => match event {
                E::Start { spec, config } => vec![
                    Effect::StartAgent {
                        binding_id,
                        spec: spec.clone(),
                        config: config.clone(),
                    },
                    Effect::PersistStatus {
                        binding_id,
                        status: S::Running,
                    },
                ],
                _ => vec![],
            },
            (S::Running, S::WaitingApproval) => match event {
                E::AwaitApproval { data, requests } => {
                    let message_id = data.id;
                    let agent_topic_id = data.topic_id;
                    vec![
                        Effect::PersistStatus {
                            binding_id,
                            status: S::WaitingApproval,
                        },
                        Effect::Emit(TopicEvent::ApprovalRequired {
                            binding_id,
                            topic_id: agent_topic_id,
                            parent_topic_id,
                            message_id,
                            requests: requests.clone(),
                        }),
                    ]
                }
                _ => vec![],
            },
            (S::Running, S::WaitingChild) => {
                vec![Effect::PersistStatus {
                    binding_id,
                    status: S::WaitingChild,
                }]
            }
            (S::Running, S::Finished) => match event {
                E::Completed { data } => {
                    let message_id = data.id;
                    let agent_topic_id = data.topic_id;
                    vec![
                        Effect::PersistStatus {
                            binding_id,
                            status: S::Finished,
                        },
                        Effect::Emit(TopicEvent::MessageFinished {
                            binding_id,
                            parent_topic_id,
                            topic_id: agent_topic_id,
                            message_id,
                        }),
                        Effect::SendChildResponse {
                            binding_id,
                            status: S::Finished,
                            output: Self::finished_output(data),
                        },
                    ]
                }
                _ => vec![],
            },
            (S::Running, S::Failed) => match event {
                E::Failed { error, message_id } => vec![
                    Effect::PersistStatus {
                        binding_id,
                        status: S::Failed,
                    },
                    Effect::Emit(TopicEvent::Error {
                        binding_id,
                        topic_id,
                        parent_topic_id,
                        message_id: *message_id,
                        error: error.clone(),
                    }),
                    Effect::SendChildResponse {
                        binding_id,
                        status: S::Failed,
                        output: vec![Content::new_text(error.clone())],
                    },
                ],
                _ => vec![],
            },
            // 任何可取消状态收到 Cancel（指令）或 Cancelled（上报）→ 终态 Cancelled。
            (S::Running | S::WaitingApproval | S::WaitingChild, S::Cancelled) => {
                let mut effects = vec![
                    Effect::PersistStatus {
                        binding_id,
                        status: S::Cancelled,
                    },
                    Effect::SendChildResponse {
                        binding_id,
                        status: S::Cancelled,
                        output: vec![Content::new_text("Task was cancelled".to_string())],
                    },
                ];
                // 只有"取消指令"需要额外下发 CancelAgent；任务自报 Cancelled 无需再取消。
                if matches!(event, E::Cancel) {
                    effects.insert(1, Effect::CancelAgent { binding_id });
                }
                effects
            }
            (S::WaitingApproval, S::Running) => vec![
                Effect::PersistStatus {
                    binding_id,
                    status: S::Running,
                },
                Effect::ResumeAgent { binding_id },
            ],
            (S::WaitingChild, S::Running) => {
                vec![Effect::PersistStatus {
                    binding_id,
                    status: S::Running,
                }]
            }
            _ => vec![],
        }
    }

    /// 从完成的 Assistant 消息中提取可读的最终结果。
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::event::TopicEvent;
    use crate::agent::fsm::testutil::*;

    fn start() -> TaskEvent {
        TaskEvent::Start {
            spec: sample_spec(),
            config: sample_config(),
        }
    }

    fn completed() -> TaskEvent {
        TaskEvent::Completed {
            data: sample_message(),
        }
    }

    fn failed() -> TaskEvent {
        TaskEvent::Failed {
            error: "boom".into(),
            message_id: Some(10),
        }
    }

    fn awaiting() -> TaskEvent {
        TaskEvent::AwaitApproval {
            data: sample_message(),
            requests: vec![sample_request()],
        }
    }

    #[test]
    fn start_transition_order() {
        let mut f = TaskFsm::new(1, 100, 200);
        let effects = f.reduce(start());
        assert_eq!(f.state(), TaskState::Running);
        // 先 StartAgent（注册 registry），再持久化状态
        assert_eq!(effect_names(&effects), vec!["StartAgent", "PersistStatus"]);
        assert!(matches!(
            effects[1],
            Effect::PersistStatus {
                status: TaskState::Running,
                ..
            }
        ));
    }

    #[test]
    fn restart_from_terminal() {
        // 主任务跑完后，同一 binding 可以重新 Start
        let mut f = TaskFsm::new(1, 100, 200);
        f.reduce(start());
        f.reduce(completed());
        assert_eq!(f.state(), TaskState::Finished);
        let effects = f.reduce(start());
        assert_eq!(f.state(), TaskState::Running);
        assert_eq!(effect_names(&effects), vec!["StartAgent", "PersistStatus"]);
    }

    #[test]
    fn approval_roundtrip() {
        let mut f = TaskFsm::new(1, 100, 200);
        f.reduce(start());

        let effects = f.reduce(awaiting());
        assert_eq!(f.state(), TaskState::WaitingApproval);
        assert_eq!(effect_names(&effects), vec!["PersistStatus", "Emit"]);
        assert!(matches!(
            effects[1],
            Effect::Emit(TopicEvent::ApprovalRequired { .. })
        ));

        let effects = f.reduce(TaskEvent::ApprovalResolved);
        assert_eq!(f.state(), TaskState::Running);
        assert_eq!(effect_names(&effects), vec!["PersistStatus", "ResumeAgent"]);
    }

    #[test]
    fn completion_effect_order() {
        let mut f = TaskFsm::new(1, 100, 200);
        f.reduce(start());

        let effects = f.reduce(completed());
        assert_eq!(f.state(), TaskState::Finished);
        // 持久化 → 广播 MessageFinished → 解析 pending 子任务
        assert_eq!(
            effect_names(&effects),
            vec!["PersistStatus", "Emit", "SendChildResponse"]
        );
        assert!(matches!(
            effects[1],
            Effect::Emit(TopicEvent::MessageFinished { .. })
        ));
    }

    #[test]
    fn failure_emits_error() {
        let mut f = TaskFsm::new(1, 100, 200);
        f.reduce(start());

        let effects = f.reduce(failed());
        assert_eq!(f.state(), TaskState::Failed);
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::Emit(TopicEvent::Error { .. })))
        );
    }

    #[test]
    fn cancel_command_cancels_agent() {
        let mut f = TaskFsm::new(1, 100, 200);
        f.reduce(start());

        let effects = f.reduce(TaskEvent::Cancel);
        assert_eq!(f.state(), TaskState::Cancelled);
        assert_eq!(
            effect_names(&effects),
            vec!["PersistStatus", "CancelAgent", "SendChildResponse"]
        );

        // 任务随后自报 Cancelled：终态幂等，不再产生副作用
        assert!(f.reduce(TaskEvent::Cancelled).is_empty());
    }

    #[test]
    fn child_spawn_and_resolve() {
        let mut f = TaskFsm::new(1, 100, 200);
        f.reduce(start());

        // 父任务生成子任务
        let effects = f.reduce(TaskEvent::ChildSpawned);
        assert_eq!(f.state(), TaskState::WaitingChild);
        assert_eq!(effect_names(&effects), vec!["PersistStatus"]);

        // 同一父任务连续生成子任务：保持 WaitingChild，幂等
        assert!(f.reduce(TaskEvent::ChildSpawned).is_empty());

        // 子任务完成 → 恢复 Running
        let effects = f.reduce(TaskEvent::ChildResolved);
        assert_eq!(f.state(), TaskState::Running);
        assert_eq!(effect_names(&effects), vec!["PersistStatus"]);
    }

    #[test]
    fn illegal_transitions_are_rejected() {
        let mut f = TaskFsm::new(1, 100, 200);
        // Idle 下不能直接完成/失败/审批/取消
        assert!(f.reduce(completed()).is_empty());
        assert!(f.reduce(failed()).is_empty());
        assert!(f.reduce(awaiting()).is_empty());
        assert!(f.reduce(TaskEvent::Cancel).is_empty());
        assert_eq!(f.state(), TaskState::Idle);

        // 终态之后不能收到生命周期信号
        f.reduce(start());
        f.reduce(completed());
        assert!(f.reduce(awaiting()).is_empty());
        assert!(f.reduce(TaskEvent::ChildResolved).is_empty());
        assert!(f.reduce(TaskEvent::ApprovalResolved).is_empty());
        assert_eq!(f.state(), TaskState::Finished);
    }

    #[test]
    fn approve_requires_waiting_approval() {
        let mut f = TaskFsm::new(1, 100, 200);
        // Running 状态下不能直接 ApprovalResolved
        f.reduce(start());
        assert!(f.reduce(TaskEvent::ApprovalResolved).is_empty());
        assert_eq!(f.state(), TaskState::Running);
    }

    #[test]
    fn finished_output_extracts_simple_content() {
        let data = sample_message();
        let output = TaskFsm::finished_output(&data);
        assert_eq!(output.len(), 1);
    }
}
