pub mod effect;
pub mod event;
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
use std::collections::HashMap;
pub use task_fsm::*;

pub struct TopicFsm {
    topic_id: i64,
    main_instance_id: Option<i64>,
    tasks: HashMap<i64, TaskFsm>,
}

impl TopicFsm {
    pub fn new(topic_id: i64) -> Self {
        Self {
            topic_id,
            main_instance_id: None,
            tasks: HashMap::new(),
        }
    }
    pub fn main_status(&self) -> Option<AgentStatus> {
        self.main_instance_id.and_then(|id| self.task_state(id))
    }
    pub fn main_instance_id(&self) -> Option<i64> {
        self.main_instance_id
    }
    pub fn task_state(&self, instance_id: i64) -> Option<AgentStatus> {
        self.tasks.get(&instance_id).map(|t| t.state())
    }
    pub fn is_main_instance(&self, instance_id: i64) -> bool {
        self.main_instance_id.is_some() && self.main_instance_id == Some(instance_id)
    }
    pub fn is_task_busy(&self, instance_id: i64) -> bool {
        matches!(
            self.task_state(instance_id),
            Some(AgentStatus::Running | AgentStatus::WaitingApproval | AgentStatus::WaitingChild)
        )
    }
    pub fn is_main_busy(&self) -> bool {
        self.main_instance_id
            .map(|id| self.is_task_busy(id))
            .unwrap_or(false)
    }

    /// 归约事件并返回副作用
    pub fn reduce(&mut self, event: FsmEvent) -> Vec<Effect> {
        let mut effects: Vec<Effect> = Vec::with_capacity(5);
        match event {
            FsmEvent::Topic(topic_msg) => match topic_msg {
                Command(topic_command) => self.reduce_topic_command(&mut effects, topic_command),
                Task(notify) => self.reduce_task_notification(&mut effects, notify),
                Supervisor(request) => self.reduce_supervisor_request(&mut effects, request),
            },
            FsmEvent::Start { spec } => {
                let instance_id = spec.instance.id;
                if let Some(parent_instance_id) = spec.instance.parent_id {
                    self.apply_task(&mut effects, parent_instance_id, TaskEvent::ChildSpawned);
                } else {
                    self.main_instance_id = Some(instance_id);
                }
                let task = self.fetch_task(instance_id);
                effects.extend(task.reduce(TaskEvent::Start { spec }));
            }
            FsmEvent::ChildResolved { instance_id } => {
                self.apply_task(&mut effects, instance_id, TaskEvent::ChildResolved);
            }
            FsmEvent::Emit(topic_event) => {
                effects.push(Effect::Emit(topic_event));
            }
            FsmEvent::Signal { instance_id, event } => {
                self.apply_task(&mut effects, instance_id, event);
            }
        }
        effects
    }

    fn reduce_supervisor_request(&mut self, effects: &mut Vec<Effect>, req: SupervisorRequest) {
        match req {
            SupervisorRequest::SpawnAgent {
                instance_id,
                call_id,
                request,
                reply,
            } => {
                effects.push(Effect::SpawnChild {
                    instance_id,
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
                instance_id,
                message_id,
                index,
                delta,
            } => {
                effects.push(Effect::Emit(TopicEvent::Message {
                    topic_id: self.topic_id,
                    message_id,
                    index,
                    instance_id,
                    data: delta,
                }));
            }
            TaskNotification::ApprovalRequired {
                instance_id,
                data,
                calls,
            } => {
                self.apply_task(
                    effects,
                    instance_id,
                    TaskEvent::ApprovalRequired { data, calls },
                );
            }
            TaskNotification::Finish { instance_id, data } => {
                self.apply_task(effects, instance_id, TaskEvent::Finish { data });
            }
            TaskNotification::Failed {
                instance_id,
                data,
                error,
            } => {
                self.apply_task(
                    effects,
                    instance_id,
                    TaskEvent::Failed {
                        data: Some(data),
                        error,
                    },
                );
            }
            TaskNotification::Cancelled { instance_id } => {
                self.apply_task(effects, instance_id, TaskEvent::Cancelled);
            }
        }
    }

    fn reduce_topic_command(&mut self, effects: &mut Vec<Effect>, cmd: TopicCommand) {
        match cmd {
            TopicCommand::Start { user_input } => effects.push(Effect::Init { user_input }),
            TopicCommand::Cancel { instance_id } => {
                self.apply_task(effects, instance_id, TaskEvent::Cancel);
            }
            TopicCommand::Approval {
                instance_id,
                deny_ids,
                allow_ids,
            } => {
                // 只有 WaitingApproval 的任务才允许提交审批
                if self.task_state(instance_id) != Some(AgentStatus::WaitingApproval) {
                    log::warn!(
                        "[TopicCommand::Approval] approval rejected, task not waiting: {instance_id}"
                    );
                    return;
                }
                effects.push(Effect::Approval {
                    instance_id,
                    allow_ids,
                    deny_ids,
                });
            }
            TopicCommand::Shutdown => {
                effects.push(Effect::StopRuntime);
            }
            TopicCommand::Subscribe { .. } => {}
        }
    }
    fn fetch_task(&mut self, instance_id: i64) -> &mut TaskFsm {
        self.tasks
            .entry(instance_id)
            .or_insert_with(|| TaskFsm::new(instance_id))
    }

    fn apply_task(&mut self, effects: &mut Vec<Effect>, instance_id: i64, new_event: TaskEvent) {
        let close_effect: Option<Effect> = self.close_main_stream_guard(&new_event, instance_id);
        if let Some(task) = self.tasks.get_mut(&instance_id) {
            effects.extend(task.reduce(new_event));
            effects.extend(close_effect);
        } else {
            log::warn!("[TopicFsm] task not found: {instance_id}");
        }
    }

    fn close_main_stream_guard(&mut self, event: &TaskEvent, instance_id: i64) -> Option<Effect> {
        if self.is_main_instance(instance_id)
            && matches!(
                event,
                TaskEvent::Cancelled
                    | TaskEvent::ApprovalRequired { .. }
                    | TaskEvent::Failed { .. }
                    | TaskEvent::Finish { .. }
            )
        {
            Some(Effect::CloseEventStream)
        } else {
            None
        }
    }
}
