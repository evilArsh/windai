pub mod effect;
pub mod event;
pub mod state;
pub mod task_fsm;
use super::{
    event::{
        TopicCommand, TopicEvent,
        TopicMsg::{Command, Supervisor, Task},
    },
    task::{SupervisorRequest, TaskNotification},
};
use crate::models::AgentStatus;
pub use effect::*;
pub use event::*;
pub use state::*;
use std::collections::HashMap;
pub use task_fsm::*;

pub struct TopicFsm {
    topic_id: i64,
    state: TopicState,
    main_binding_id: Option<i64>,
    tasks: HashMap<i64, TaskFsm>,
}

impl TopicFsm {
    pub fn new(topic_id: i64) -> Self {
        Self {
            topic_id,
            state: TopicState::Idle,
            main_binding_id: None,
            tasks: HashMap::new(),
        }
    }

    pub fn topic_state(&self) -> TopicState {
        self.state
    }

    pub fn main_binding_id(&self) -> Option<i64> {
        self.main_binding_id
    }

    pub fn is_main_binding(&self, binding_id: i64) -> bool {
        self.main_binding_id.is_some() && self.main_binding_id == Some(binding_id)
    }

    pub fn task_state(&self, binding_id: i64) -> Option<AgentStatus> {
        self.tasks.get(&binding_id).map(|t| t.state())
    }

    pub fn is_task_busy(&self, binding_id: i64) -> bool {
        matches!(
            self.task_state(binding_id),
            Some(AgentStatus::Running | AgentStatus::WaitingApproval | AgentStatus::WaitingChild)
        )
    }

    pub fn is_main_busy(&self) -> bool {
        self.main_binding_id
            .map(|id| self.is_task_busy(id))
            .unwrap_or(false)
    }

    /// 归约事件并返回副作用。
    pub fn reduce(&mut self, event: FsmEvent) -> Vec<Effect> {
        let mut effects: Vec<Effect> = vec![];
        match event {
            FsmEvent::Topic(topic_msg) => match topic_msg {
                Command(topic_command) => self.reduce_topic_command(&mut effects, topic_command),
                Task(notify) => self.reduce_task_notification(&mut effects, notify),
                Supervisor(request) => self.reduce_supervisor_request(&mut effects, request),
            },
            FsmEvent::Start { spec, config } => {
                let binding_id = spec.binding_id;
                self.main_binding_id = Some(binding_id);
                self.state = TopicState::Running;
                let task = self.fetch_task(binding_id, config.topic_id);
                effects.extend(task.reduce(TaskEvent::Start { spec, config }));
            }
            FsmEvent::StartChild {
                parent_binding_id,
                spec,
                config,
            } => {
                let child_binding_id = spec.binding_id;
                self.apply_task(&mut effects, parent_binding_id, TaskEvent::ChildSpawned);
                let task = self.fetch_task(child_binding_id, config.topic_id);
                effects.extend(task.reduce(TaskEvent::Start { spec, config }));
            }
            FsmEvent::ChildResolved { parent_binding_id } => {
                self.apply_task(&mut effects, parent_binding_id, TaskEvent::ChildResolved);
            }
            FsmEvent::Emit(topic_event) => {
                effects.push(Effect::Emit(topic_event));
            }
            FsmEvent::Signal { binding_id, event } => {
                self.apply_task(&mut effects, binding_id, event);
            }
        }
        self.sync_topic_state(&mut effects);

        effects
    }

    fn reduce_supervisor_request(&mut self, effects: &mut Vec<Effect>, req: SupervisorRequest) {
        match req {
            SupervisorRequest::SpawnAgent {
                binding_id,
                call_id,
                request,
                reply,
            } => {
                effects.push(Effect::SpawnChild {
                    parent_binding_id: binding_id,
                    call_id,
                    request,
                    reply,
                });
            }
        }
    }

    fn reduce_task_notification(&mut self, effects: &mut Vec<Effect>, notify: TaskNotification) {
        match notify {
            TaskNotification::Started { .. } => {}
            TaskNotification::Message {
                binding_id,
                topic_id,
                message_id,
                index,
                delta,
            } => {
                effects.push(Effect::Emit(TopicEvent::Message {
                    topic_id,
                    message_id,
                    index,
                    binding_id,
                    parent_topic_id: self.topic_id,
                    data: delta,
                }));
            }
            TaskNotification::ApprovalRequired {
                binding_id,
                data,
                calls,
            } => {
                self.apply_task(
                    effects,
                    binding_id,
                    TaskEvent::ApprovalRequired { data, calls },
                );
            }
            TaskNotification::Finish { binding_id, data } => {
                self.apply_task(effects, binding_id, TaskEvent::Finish { data });
            }
            TaskNotification::Failed {
                binding_id,
                data,
                error,
            } => {
                self.apply_task(
                    effects,
                    binding_id,
                    TaskEvent::Failed {
                        data: Some(data),
                        error,
                    },
                );
            }
            TaskNotification::Cancelled { binding_id } => {
                self.apply_task(effects, binding_id, TaskEvent::Cancelled);
            }
        }
    }

    fn reduce_topic_command(&mut self, effects: &mut Vec<Effect>, cmd: TopicCommand) {
        match cmd {
            TopicCommand::Start { user_input } => effects.push(Effect::PrepareMain { user_input }),
            TopicCommand::Cancel { binding_id } => {
                self.apply_task(effects, binding_id, TaskEvent::Cancel);
            }
            TopicCommand::Shutdown => {
                if self.state != TopicState::Stopped {
                    self.state = TopicState::Stopped;
                    effects.push(Effect::StopRuntime);
                }
            }
            TopicCommand::Approval {
                binding_id,
                deny_ids,
                allow_ids,
            } => {
                // 只有 WaitingApproval 的任务才允许提交审批。
                if self.task_state(binding_id) != Some(AgentStatus::WaitingApproval) {
                    log::warn!(
                        "[TopicCommand::Approval] approval rejected, task not waiting: {binding_id}"
                    );
                    return;
                }
                effects.push(Effect::Approval {
                    binding_id,
                    allow_ids,
                    deny_ids,
                });
            }
            TopicCommand::Subscribe { .. } => {}
        }
    }

    fn sync_topic_state(&mut self, effects: &mut Vec<Effect>) {
        if self.state == TopicState::Stopped {
            return;
        }
        let prev = self.state;
        let Some(main) = self.main_binding_id else {
            return;
        };
        let next = match self.task_state(main) {
            Some(AgentStatus::Finished | AgentStatus::Failed) => TopicState::Idle,
            Some(AgentStatus::Cancelled) => TopicState::Stopped,
            _ => return,
        };
        if prev != next {
            self.state = next;
            effects.push(match next {
                TopicState::Stopped => Effect::StopRuntime,
                _ => Effect::CloseEventStream,
            });
        }
    }

    fn fetch_task(&mut self, binding_id: i64, topic_id: i64) -> &mut TaskFsm {
        self.tasks
            .entry(binding_id)
            .or_insert_with(|| TaskFsm::new(binding_id, self.topic_id, topic_id))
    }

    fn apply_task(&mut self, effects: &mut Vec<Effect>, binding_id: i64, new_event: TaskEvent) {
        if let Some(task) = self.tasks.get_mut(&binding_id) {
            effects.extend(task.reduce(new_event));
        } else {
            log::warn!("[TopicFsm] task not found: {binding_id}");
        }
    }
}
