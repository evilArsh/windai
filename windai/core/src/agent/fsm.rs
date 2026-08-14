pub mod effect;
pub mod event;
pub mod state;
pub mod task_fsm;

pub use effect::*;
pub use event::*;
pub use state::*;
pub use task_fsm::*;

use std::collections::HashMap;

use crate::models::AgentStatus;

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

    /// 任务是否处于忙碌状态。
    pub fn is_task_busy(&self, binding_id: i64) -> bool {
        matches!(
            self.task_state(binding_id),
            Some(AgentStatus::Running | AgentStatus::WaitingApproval | AgentStatus::WaitingChild)
        )
    }

    /// 主任务是否忙碌
    pub fn is_main_busy(&self) -> bool {
        self.main_binding_id
            .map(|id| self.is_task_busy(id))
            .unwrap_or(false)
    }

    /// 归约事件并返回副作用。
    pub fn reduce(&mut self, event: FsmEvent) -> Vec<Effect> {
        let mut effects = vec![];
        match event {
            FsmEvent::UserRequest(req) => self.reduce_user_request(req, &mut effects),
            FsmEvent::Signal(sig) => self.reduce_signal(sig, &mut effects),
            FsmEvent::Supervisor(sup) => self.reduce_supervisor(sup, &mut effects),
        }
        effects
    }

    fn reduce_user_request(&mut self, req: UserRequest, effects: &mut Vec<Effect>) {
        match req {
            UserRequest::Start {
                is_main,
                spec,
                config,
            } => {
                let binding_id = spec.binding_id;
                // // 取消主任务后 runtime 停止，拒绝再次启动。
                // if self.state == TopicState::Stopped {
                //     log::warn!("[TopicFsm] topic stopped, reject start: {binding_id}");
                //     return;
                // }
                if is_main && self.is_main_busy() {
                    log::warn!("[TopicFsm] main agent is busy, reject start: {binding_id}");
                    return;
                }
                if is_main {
                    self.main_binding_id = Some(binding_id);
                    self.state = TopicState::Running;
                }
                let task = self.enter_task(binding_id, config.topic_id);
                effects.extend(task.reduce(TaskEvent::Start { spec, config }));
            }
            UserRequest::CancelTask { binding_id } => {
                self.task_apply(binding_id, TaskEvent::Cancel, effects);
            }
            UserRequest::Approval {
                binding_id,
                allow_ids,
                deny_ids,
            } => {
                // 只有 WaitingApproval 的任务才允许提交审批。
                if self.task_state(binding_id) != Some(AgentStatus::WaitingApproval) {
                    log::warn!("[TopicFsm] approval rejected, task not waiting: {binding_id}");
                    return;
                }
                effects.push(Effect::ApplyApprovals {
                    binding_id,
                    allow_ids,
                    deny_ids,
                });
            }
            UserRequest::ApprovalApplied { binding_id } => {
                self.task_apply(binding_id, TaskEvent::ApprovalResolved, effects);
            }
            UserRequest::Shutdown => {
                if self.state != TopicState::Stopped {
                    self.state = TopicState::Stopped;
                    effects.push(Effect::StopRuntime);
                }
            }
        }
        self.sync_topic_state(effects);
    }

    fn reduce_signal(&mut self, sig: TaskSignal, effects: &mut Vec<Effect>) {
        match sig {
            TaskSignal::AwaitApproval {
                binding_id,
                data,
                requests,
            } => {
                self.task_apply(
                    binding_id,
                    TaskEvent::AwaitApproval { data, requests },
                    effects,
                );
            }
            TaskSignal::Completed { binding_id, data } => {
                self.task_apply(binding_id, TaskEvent::Completed { data }, effects);
            }
            TaskSignal::Failed {
                binding_id,
                error,
                message_id,
            } => {
                self.task_apply(binding_id, TaskEvent::Failed { error, message_id }, effects);
            }
            TaskSignal::Cancelled { binding_id } => {
                self.task_apply(binding_id, TaskEvent::Cancelled, effects);
            }
        }
        self.sync_topic_state(effects);
    }

    fn reduce_supervisor(&mut self, sup: SupervisorEvent, effects: &mut Vec<Effect>) {
        match sup {
            SupervisorEvent::SpawnAgent {
                parent_binding_id,
                call_id,
                request,
                reply,
            } => {
                effects.push(Effect::SpawnChild {
                    parent_binding_id,
                    call_id,
                    request,
                    reply,
                });
            }
            SupervisorEvent::ChildStarted {
                parent_binding_id,
                spec,
                config,
            } => {
                let child_binding_id = spec.binding_id;
                // 父任务: Running → WaitingChild。
                self.task_apply(parent_binding_id, TaskEvent::ChildSpawned, effects);
                // 子任务: Idle → Running。
                let task = self.enter_task(child_binding_id, config.topic_id);
                effects.extend(task.reduce(TaskEvent::Start { spec, config }));
            }
            SupervisorEvent::ChildResolved { parent_binding_id } => {
                self.task_apply(parent_binding_id, TaskEvent::ChildResolved, effects);
            }
        }
        self.sync_topic_state(effects);
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

    fn enter_task(&mut self, binding_id: i64, topic_id: i64) -> &mut TaskFsm {
        self.tasks
            .entry(binding_id)
            .or_insert_with(|| TaskFsm::new(binding_id, self.topic_id, topic_id))
    }

    fn task_apply(&mut self, binding_id: i64, new_event: TaskEvent, effects: &mut Vec<Effect>) {
        if let Some(task) = self.tasks.get_mut(&binding_id) {
            effects.extend(task.reduce(new_event));
        } else {
            log::warn!("[TopicFsm] task not found: {binding_id}");
        }
    }
}

#[cfg(test)]
pub(crate) mod testutil {
    use crate::agent::runtime::AgentRunConfig;
    use crate::agent::task::TaskSpec;
    use crate::models::{
        AgentDefinition, AgentDefinitionData, AgentMode, AgentScope, Credentials, Message, Model,
        Provider, ToolApprovalRequest, ToolApprovalStatus,
    };
    use wind_ai::message::{Content, Message as AiMessage, ReqConfig, Role};
    use wind_ai::model::AdapterType;

    pub fn sample_message() -> Message {
        Message {
            id: 10,
            from_id: None,
            stream: false,
            content: vec![AiMessage::new_simple(
                Role::Assistant,
                vec![Content::new_text("hello".to_string())],
                None,
            )],
            model_id: 1,
            topic_id: 200,
            is_boundary: false,
            is_excluded: false,
            input_tokens: 0,
            output_tokens: 0,
            created_at: 0,
        }
    }

    pub fn sample_request() -> ToolApprovalRequest {
        ToolApprovalRequest {
            id: 1,
            binding_id: 1,
            topic_id: 200,
            parent_topic_id: 100,
            message_id: 10,
            tool_call_id: "call_1".into(),
            tool_name: "agent_list_agents".into(),
            arguments: serde_json::Value::Null,
            status: ToolApprovalStatus::Pending,
            created_at: 0,
            updated_at: 0,
        }
    }

    pub fn sample_spec() -> TaskSpec {
        let assistant = sample_message();
        TaskSpec {
            binding_id: 1,
            agent: AgentDefinition {
                id: 1,
                key: "test".into(),
                name: "Test".into(),
                description: String::new(),
                scope: AgentScope::Global,
                owner_topic_id: None,
                cloned_from_agent_id: None,
                active: true,
                data: AgentDefinitionData::default(),
                created_at: 0,
            },
            model: Model {
                id: 1,
                name: "test-model".into(),
                provider_id: 1,
                alias: None,
                adapter: AdapterType::OpenAICompletion,
                modalities: None,
                active: true,
                icon: None,
                endpoint: None,
                frequency: None,
                created_at: 0,
            },
            provider: Provider {
                id: 1,
                name: "test-provider".into(),
                base_url: "http://localhost".into(),
                description: None,
                doc: None,
                alias: None,
                active: true,
                created_at: 0,
            },
            credential: Credentials {
                id: 1,
                provider_id: 1,
                key: "test-key".into(),
                created_at: 0,
                active: true,
            },
            req_config: ReqConfig::default(),
            rule_set: None,
            tools: None,
            assistant,
            contexts: vec![],
        }
    }

    pub fn sample_config() -> AgentRunConfig {
        AgentRunConfig {
            binding_id: 1,
            topic_id: 200,
            parent_topic_id: 100,
            tool_approval_policy: None,
            mode: AgentMode::Sync,
        }
    }

    /// 副作用种类名列表，用于断言副作用的有序性。
    pub fn effect_names(effects: &[crate::agent::fsm::Effect]) -> Vec<&'static str> {
        use crate::agent::fsm::Effect;
        effects
            .iter()
            .map(|e| match e {
                Effect::PersistStatus { .. } => "PersistStatus",
                Effect::Emit(_) => "Emit",
                Effect::StartAgent { .. } => "StartAgent",
                Effect::ResumeAgent { .. } => "ResumeAgent",
                Effect::CancelAgent { .. } => "CancelAgent",
                Effect::SendChildResponse { .. } => "SendChildResponse",
                Effect::SpawnChild { .. } => "SpawnChild",
                Effect::ApplyApprovals { .. } => "ApplyApprovals",
                Effect::CloseEventStream => "CloseEventStream",
                Effect::StopRuntime => "StopRuntime",
            })
            .collect()
    }

    /// 指定 binding_id 的任务 spec（默认给子任务用）。
    pub fn sample_spec_with_binding(binding_id: i64) -> crate::agent::task::TaskSpec {
        let mut spec = sample_spec();
        spec.binding_id = binding_id;
        spec
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::fsm::testutil::*;
    use crate::agent::tool::SpawnAgentRequest;
    use crate::models::AgentMode;

    fn start_main() -> FsmEvent {
        FsmEvent::UserRequest(UserRequest::Start {
            is_main: true,
            spec: sample_spec(),
            config: sample_config(),
        })
    }

    fn completed(binding_id: i64) -> FsmEvent {
        FsmEvent::Signal(TaskSignal::Completed {
            binding_id,
            data: sample_message(),
        })
    }

    #[test]
    fn main_start_marks_topic_running() {
        let mut f = TopicFsm::new(100);
        assert_eq!(f.topic_state(), TopicState::Idle);
        assert!(!f.is_main_busy());

        let effects = f.reduce(start_main());
        assert_eq!(f.topic_state(), TopicState::Running);
        assert_eq!(f.main_binding_id(), Some(1));
        assert!(f.is_main_busy());
        assert_eq!(f.task_state(1), Some(AgentStatus::Running));
        assert_eq!(effect_names(&effects), vec!["StartAgent", "PersistStatus"]);
    }

    #[test]
    fn main_busy_guard_rejects_second_start() {
        let mut f = TopicFsm::new(100);
        f.reduce(start_main());
        // 主任务 busy 时拒绝再次启动
        let effects = f.reduce(start_main());
        assert!(effects.is_empty());
    }

    #[test]
    fn main_finish_closes_stream_and_stays_idle() {
        let mut f = TopicFsm::new(100);
        f.reduce(start_main());
        assert_eq!(f.topic_state(), TopicState::Running);

        let effects = f.reduce(completed(1));
        // 自然完成 → Idle，事件流关闭（runtime 继续存活）
        assert_eq!(f.topic_state(), TopicState::Idle);
        assert!(!f.is_main_busy());
        assert_eq!(
            effect_names(&effects),
            vec![
                "PersistStatus",
                "Emit",
                "SendChildResponse",
                "CloseEventStream"
            ]
        );

        // 主任务 Idle 后可重新启动（runtime 等待下次输入）
        f.reduce(start_main());
        assert_eq!(f.topic_state(), TopicState::Running);
    }

    #[test]
    fn main_cancel_stops_topic() {
        let mut f = TopicFsm::new(100);
        f.reduce(start_main());
        let effects = f.reduce(FsmEvent::UserRequest(UserRequest::CancelTask {
            binding_id: 1,
        }));
        // 用户取消主任务 → Stopped + StopRuntime
        assert_eq!(f.topic_state(), TopicState::Stopped);
        assert_eq!(
            effect_names(&effects),
            vec![
                "PersistStatus",
                "CancelAgent",
                "SendChildResponse",
                "StopRuntime"
            ]
        );
        // FSM 层不再阻止重启：StopRuntime 会令 actor 退出，实际不会有新的 Start 到达。
    }

    #[test]
    fn approval_rejected_when_not_waiting() {
        let mut f = TopicFsm::new(100);
        f.reduce(start_main());
        // Running 状态下提交审批 → 拒绝
        let effects = f.reduce(FsmEvent::UserRequest(UserRequest::Approval {
            binding_id: 1,
            allow_ids: vec![1],
            deny_ids: vec![],
        }));
        assert!(effects.is_empty());
    }

    #[test]
    fn child_spawn_flow() {
        let mut f = TopicFsm::new(100);
        f.reduce(start_main());

        // 父任务请求生成子任务：只生成 SpawnChild 副作用，父状态暂不变
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let effects = f.reduce(FsmEvent::Supervisor(SupervisorEvent::SpawnAgent {
            parent_binding_id: 1,
            call_id: "call_1".into(),
            request: SpawnAgentRequest {
                agent_key: "child".into(),
                mode: AgentMode::Sync,
                task: "do something".into(),
            },
            reply: tx,
        }));
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::SpawnChild { .. }))
        );
        assert_eq!(f.task_state(1), Some(AgentStatus::Running));

        // 子任务创建成功：父 → WaitingChild，子 → Running
        let effects = f.reduce(FsmEvent::Supervisor(SupervisorEvent::ChildStarted {
            parent_binding_id: 1,
            spec: sample_spec_with_binding(2),
            config: sample_config(),
        }));
        assert_eq!(f.task_state(1), Some(AgentStatus::WaitingChild));
        assert_eq!(f.task_state(2), Some(AgentStatus::Running));
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::StartAgent { binding_id: 2, .. }))
        );

        // 子任务完成：解析 pending 前父任务先恢复 Running（由 actor 在 SendChildResponse 后注入）
        f.reduce(completed(2));
        assert_eq!(f.task_state(2), Some(AgentStatus::Finished));
        f.reduce(FsmEvent::Supervisor(SupervisorEvent::ChildResolved {
            parent_binding_id: 1,
        }));
        assert_eq!(f.task_state(1), Some(AgentStatus::Running));
    }

    #[test]
    fn shutdown_stops_topic() {
        let mut f = TopicFsm::new(100);
        f.reduce(start_main());
        let effects = f.reduce(FsmEvent::UserRequest(UserRequest::Shutdown));
        assert_eq!(f.topic_state(), TopicState::Stopped);
        assert_eq!(effect_names(&effects), vec!["StopRuntime"]);
    }
}
