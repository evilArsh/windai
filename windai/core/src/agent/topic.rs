use super::event::{TopicCommand, TopicEvent, TopicMailbox};
use super::fsm::{Effect, FsmEvent, TaskEvent, TopicFsm};
use super::helper::{self};
use super::runtime::AgentRunConfig;
use super::task::sync::SyncTask;
use super::task::{PendingChild, TaskEntry, TaskRegistry, TaskSpec};
use super::tool::{SpawnAgentRequest, SpawnAgentResponse};
use crate::agent::event::TopicMsg;
use crate::error::{CoreError, Result};
use crate::models::{
    AgentMode, AgentStatus, ApprovalRecord, ToolApprovalStatus, UpdateAgentBinding,
};
use crate::storage::Storage;
use futures::future::try_join;
use std::collections::VecDeque;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use wind_ai::message::Content;
use wind_mcp::client::registry::RegistryHandle;

macro_rules! try_send_log {
    ($sender:expr, $value:expr, $prefix:expr $(,)?) => {
        if $sender.send($value).is_err() {
            log::error!("{} reply dropped", $prefix);
        }
    };
}

#[derive(Clone)]
pub struct TopicRuntimeHandle {
    mailbox: TopicMailbox,
    ctx: CancellationToken,
}
impl TopicRuntimeHandle {
    fn ensure_alive(&self) -> Result<()> {
        if self.is_stopped() {
            Err(CoreError::Internal(format!("topic runtime has stopped")))
        } else {
            Ok(())
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.ctx.is_cancelled()
    }

    /// 订阅当前对话的事件流
    ///
    /// 主任务进入 Idle 或 runtime 停止时，该 channel 被关闭。
    /// 每次新对话需重新订阅
    pub async fn subscribe(&self) -> Result<broadcast::Receiver<TopicEvent>> {
        self.ensure_alive()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mailbox
            .send(TopicMsg::Command(TopicCommand::Subscribe {
                reply: reply_tx,
            }))
            .await?;
        reply_rx
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))
    }

    /// 创建新的对话
    pub async fn create_chat(&self, user_input: Vec<Content>) -> Result<()> {
        self.ensure_alive()?;
        self.mailbox
            .send(TopicMsg::Command(TopicCommand::Start { user_input }))
            .await
    }
    /// 取消任务
    pub async fn cancel_task(&self, binding_id: i64) -> Result<()> {
        self.ensure_alive()?;
        self.mailbox
            .send(TopicMsg::Command(TopicCommand::Cancel { binding_id }))
            .await
    }
    /// 审批任务
    pub async fn approve(
        &self,
        binding_id: i64,
        allow_ids: Vec<i64>,
        deny_ids: Vec<i64>,
    ) -> Result<()> {
        self.ensure_alive()?;
        self.mailbox
            .send(TopicMsg::Command(TopicCommand::Approval {
                binding_id,
                deny_ids,
                allow_ids,
            }))
            .await
    }
    /// 关闭当前运行时
    pub async fn shutdown(&self) -> Result<()> {
        if self.is_stopped() {
            return Ok(());
        }
        self.mailbox
            .send(TopicMsg::Command(TopicCommand::Shutdown))
            .await
    }
}

pub struct TopicRuntime {
    ctx: CancellationToken,
    topic_id: i64,
    mailbox: TopicMailbox,
    mailbox_rx: mpsc::Receiver<TopicMsg>,
    app_rx: Option<broadcast::Sender<TopicEvent>>,
    storage: Storage,
    mcp_registry: RegistryHandle,
    registry: TaskRegistry,
    fsm: TopicFsm,
}

impl TopicRuntime {
    /// 创建顶层Topic运行时
    pub fn spawn(
        ctx: CancellationToken,
        topic_id: i64,
        mcp_registry: RegistryHandle,
        storage: Storage,
    ) -> TopicRuntimeHandle {
        let (tx, rx) = mpsc::channel(256);
        let mailbox = TopicMailbox::new(tx);
        let runtime = Self {
            ctx: ctx.clone(),
            topic_id,
            mailbox: mailbox.clone(),
            mailbox_rx: rx,
            app_rx: None,
            storage,
            mcp_registry,
            registry: TaskRegistry::new(),
            fsm: TopicFsm::new(topic_id),
        };

        tokio::spawn(runtime.run());

        TopicRuntimeHandle { mailbox, ctx }
    }

    fn emit(&self, event: TopicEvent) {
        if let Some(tx) = self.app_rx.as_ref() {
            if let Err(err) = tx.send(event) {
                log::error!("[emit] error: {err}");
            }
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                _ = self.ctx.cancelled() => {
                    let _ = self.shutdown().await;
                    break;
                }
                Some(msg) = self.mailbox_rx.recv() => {
                    self.handle_topic_msg(msg).await;
                }
                else => {
                    break;
                }
            }
        }
    }

    /// 归约 FSM 事件并执行副作用。
    ///
    /// 副作用执行过程中可能产生新的 FSM 事件。
    ///
    /// 采用深度优先策略：一个 effect 执行后产生的 follow_up 事件会立即归约
    /// 执行，直到其副作用链全部完成，才继续处理下一个同级 effect。这保证每个
    /// effect 的完整副作用（含广播事件）都在后续 effect（如 CloseEventStream）
    /// 生效前全部发出。
    async fn apply(&mut self, event: FsmEvent) {
        let mut queue = VecDeque::new();
        queue.push_back(event);
        let mut stack: Vec<Effect> = vec![];

        loop {
            if let Some(effect) = stack.pop() {
                match self.execute(effect).await {
                    None => {}
                    Some(follow_ups) => {
                        for fu in follow_ups.into_iter().rev() {
                            for e in self.fsm.reduce(fu).into_iter().rev() {
                                stack.push(e);
                            }
                        }
                    }
                }
            } else if let Some(ev) = queue.pop_front() {
                for e in self.fsm.reduce(ev).into_iter().rev() {
                    stack.push(e);
                }
            } else {
                break;
            }
        }
    }

    /// 执行副作用seam
    async fn execute(&mut self, effect: Effect) -> Option<Vec<FsmEvent>> {
        log::debug!("{}", effect);
        match effect {
            Effect::SendChildResponse {
                binding_id,
                status,
                output,
            } => Some(self.resolve_pending_child(binding_id, status, output).await),
            Effect::SpawnChild {
                parent_binding_id,
                call_id,
                request,
                reply,
            } => {
                let mode = request.mode;
                match self.spawn_child(parent_binding_id, request).await {
                    Ok((binding_id, res)) => {
                        self.registry.insert_pending(PendingChild {
                            call_id: call_id.clone(),
                            mode,
                            reply,
                            binding_id,
                            parent_binding_id,
                        });
                        Some(res)
                    }
                    Err(err) => {
                        try_send_log!(
                            reply,
                            SpawnAgentResponse {
                                call_id,
                                mode,
                                status: AgentStatus::Failed,
                                output: vec![Content::new_text(err.to_string())],
                            },
                            "SpawnAgent"
                        );
                        None
                    }
                }
            }
            Effect::Approval {
                binding_id,
                allow_ids,
                deny_ids,
            } => match self.apply_approvals(binding_id, allow_ids, deny_ids).await {
                Ok(res) => Some(res),
                Err(err) => Some(vec![FsmEvent::Signal {
                    binding_id,
                    event: TaskEvent::Failed {
                        data: None,
                        error: err.to_string(),
                    },
                }]),
            },
            Effect::ApprovalRequest {
                binding_id,
                data,
                calls,
            } => {
                let message_id = data.id;
                let agent_topic_id = data.topic_id;
                match helper::save_approval_state(
                    &self.storage,
                    binding_id,
                    self.topic_id,
                    data.clone(),
                    calls,
                )
                .await
                {
                    Ok(requests) => Some(vec![FsmEvent::Emit(TopicEvent::ApprovalRequired {
                        binding_id,
                        topic_id: agent_topic_id,
                        parent_topic_id: self.topic_id,
                        message_id,
                        requests,
                    })]),
                    Err(err) => Some(vec![FsmEvent::Signal {
                        binding_id,
                        event: TaskEvent::Failed {
                            error: err.to_string(),
                            data: Some(data),
                        },
                    }]),
                }
            }
            Effect::Finish { binding_id, data } => {
                let message_id = data.id;
                let agent_topic_id = data.topic_id;
                match helper::save_message(&self.storage, data.clone()).await {
                    Ok(_) => Some(vec![FsmEvent::Emit(TopicEvent::MessageFinished {
                        binding_id,
                        parent_topic_id: self.topic_id,
                        topic_id: agent_topic_id,
                        message_id,
                    })]),
                    Err(err) => Some(vec![FsmEvent::Signal {
                        binding_id,
                        event: TaskEvent::Failed {
                            error: err.to_string(),
                            data: Some(data),
                        },
                    }]),
                }
            }
            Effect::Failed {
                binding_id,
                data,
                error,
            } => {
                let mut error = error;
                let topic_id = data.as_ref().map(|d| d.topic_id);
                let message_id = data.as_ref().map(|d| d.id);
                if let Some(data) = data {
                    error = match helper::save_message(&self.storage, data).await {
                        Ok(_) => error,
                        Err(e) => {
                            log::error!("[Failed] save message error: {}", e);
                            format!("{error}: {e}")
                        }
                    };
                }
                Some(vec![FsmEvent::Emit(TopicEvent::Error {
                    binding_id: Some(binding_id),
                    topic_id,
                    parent_topic_id: self.topic_id,
                    message_id,
                    error: error.clone(),
                })])
            }
            Effect::CloseEventStream => {
                self.app_rx = None;
                None
            }
            Effect::StopRuntime => {
                self.ctx.cancel();
                None
            }
            Effect::Cancel { binding_id } => match self.cancel_task(binding_id).await {
                Err(err) => Some(vec![FsmEvent::Signal {
                    binding_id,
                    event: TaskEvent::Failed {
                        data: None,
                        error: err.to_string(),
                    },
                }]),
                Ok(_) => None,
            },
            Effect::Resume { binding_id } => match self.resume_task(binding_id).await {
                Err(err) => Some(vec![FsmEvent::Signal {
                    binding_id,
                    event: TaskEvent::Failed {
                        data: None,
                        error: err.to_string(),
                    },
                }]),
                Ok(_) => None,
            },
            Effect::Emit(event) => {
                self.emit(event);
                None
            }
            Effect::Start {
                binding_id,
                spec,
                config,
            } => match self.start_agent_task(spec, config).await {
                Err(err) => Some(vec![FsmEvent::Signal {
                    binding_id,
                    event: TaskEvent::Failed {
                        data: None,
                        error: err.to_string(),
                    },
                }]),
                Ok(_) => None,
            },
            Effect::PersistStatus { binding_id, status } => {
                match self.persist_status(binding_id, status).await {
                    Ok(res) => Some(res),
                    Err(err) => Some(vec![FsmEvent::Emit(TopicEvent::Error {
                        binding_id: Some(binding_id),
                        message_id: None,
                        topic_id: None,
                        parent_topic_id: self.topic_id,
                        error: err.to_string(),
                    })]),
                }
            }
            Effect::PrepareMain { user_input } => match self.prepare_main_agent(user_input).await {
                Ok(ev) => Some(ev),
                Err(err) => Some(vec![FsmEvent::Emit(TopicEvent::Error {
                    binding_id: None,
                    topic_id: None,
                    parent_topic_id: self.topic_id,
                    message_id: None,
                    error: err.to_string(),
                })]),
            },
        }
    }

    async fn handle_topic_msg(&mut self, msg: TopicMsg) {
        log::debug!("{}", msg);
        match msg {
            TopicMsg::Command(command) => {
                let name = command.to_string();
                match command {
                    TopicCommand::Subscribe { reply } => {
                        let sender = self
                            .app_rx
                            .get_or_insert_with(|| broadcast::channel(1024).0);
                        try_send_log!(reply, sender.subscribe(), name);
                    }
                    others => {
                        self.apply(FsmEvent::Topic(TopicMsg::Command(others))).await;
                    }
                }
            }
            others => {
                self.apply(FsmEvent::Topic(others)).await;
            }
        }
    }
    /// 准备新的 Main Agent 对话配置
    async fn prepare_main_agent(&mut self, user_input: Vec<Content>) -> Result<Vec<FsmEvent>> {
        if self.fsm.is_main_busy() {
            return Err(CoreError::Internal(format!("main agent is running")));
        }
        let binding = helper::get_main_binding(&self.storage, self.topic_id).await?;
        log::debug!("[start_main_agent] get binding: {:#?}", binding);
        let agent = helper::get_def_by_id(&self.storage, binding.agent_id).await?;
        log::debug!("[start_main_agent] get agent: {:#?}", agent);
        let chat_ctx =
            helper::get_base_info(&self.storage, &self.mcp_registry, &binding, &agent).await?;
        log::debug!("[start_main_agent] get chat_ctx: {:#?}", chat_ctx);

        let tx = self.storage.begin().await?;
        let agent_topic = match helper::get_topic_by_binding_id(
            &self.storage,
            self.topic_id,
            binding.id,
        )
        .await?
        {
            Some(topic) => topic,
            None => {
                helper::create_sub_topic(
                    &tx.storage(),
                    self.topic_id,
                    binding.id,
                    format!("#main-agent"),
                )
                .await?
            }
        };
        let (user, assistant, contexts) =
            helper::create_contexts(&tx.storage(), agent_topic.id, user_input, &agent, &chat_ctx)
                .await?;
        tx.commit().await?;

        let spec = TaskSpec {
            binding_id: binding.id,
            agent,
            model: chat_ctx.model,
            provider: chat_ctx.provider,
            credential: chat_ctx.credential,
            req_config: chat_ctx.req_config,
            rule_set: chat_ctx.rule_set,
            tools: chat_ctx.tools,
            assistant: assistant.clone(),
            contexts,
        };
        let config = AgentRunConfig {
            binding_id: binding.id,
            topic_id: agent_topic.id,
            parent_topic_id: binding.parent_topic_id,
            tool_approval_policy: binding.tool_approval_policy,
            mode: AgentMode::Sync,
        };

        Ok(vec![
            FsmEvent::Emit(TopicEvent::MessageCreated {
                topic_id: agent_topic.id,
                data: user,
            }),
            FsmEvent::Emit(TopicEvent::MessageCreated {
                topic_id: agent_topic.id,
                data: assistant,
            }),
            FsmEvent::Start { spec, config },
        ])
    }

    async fn resume_task(&mut self, binding_id: i64) -> Result<()> {
        let binding = helper::get_binding_by_id(&self.storage, binding_id).await?;
        let agent = helper::get_def_by_id(&self.storage, binding.agent_id).await?;
        let agent_topic = match helper::get_topic_by_binding_id(
            &self.storage,
            self.topic_id,
            binding_id,
        )
        .await?
        {
            Some(topic) => topic,
            None => {
                return Err(CoreError::RowNotFound(format!(
                    "topic by binding_id: {}",
                    binding_id
                )));
            }
        };

        let (chat_ctx, contexts) = try_join(
            helper::get_base_info(&self.storage, &self.mcp_registry, &binding, &agent),
            helper::get_message_contexts(&self.storage, agent_topic.id),
        )
        .await?;

        let assistant = contexts.last().cloned().ok_or_else(|| {
            CoreError::Internal(format!("no assistant in this topic: {}", agent_topic.id))
        })?;
        let contexts = helper::transfer_contexts(contexts)?;

        let spec = TaskSpec {
            binding_id: binding.id,
            agent,
            model: chat_ctx.model,
            provider: chat_ctx.provider,
            credential: chat_ctx.credential,
            req_config: chat_ctx.req_config,
            rule_set: chat_ctx.rule_set,
            tools: chat_ctx.tools,
            assistant,
            contexts,
        };
        let config = AgentRunConfig {
            binding_id: binding.id,
            topic_id: agent_topic.id,
            parent_topic_id: binding.parent_topic_id,
            tool_approval_policy: binding.tool_approval_policy,
            mode: binding.mode.unwrap_or(AgentMode::Sync),
        };

        if let Some(entry) = self.registry.get_entry(binding_id) {
            entry.handler.start(spec, config).await
        } else {
            self.start_agent_task(spec, config).await
        }
    }

    /// 启动一个 SyncTask 并注册到 registry
    async fn start_agent_task(&mut self, spec: TaskSpec, config: AgentRunConfig) -> Result<()> {
        let topic_id = spec.assistant.topic_id;
        let binding = helper::get_binding_by_id(&self.storage, spec.binding_id).await?;

        let sync_handle = SyncTask::spawn(
            self.ctx.child_token(),
            spec.binding_id,
            self.topic_id,
            topic_id,
            self.mailbox.clone(),
            self.storage.clone(),
            self.mcp_registry.clone(),
        );

        let entry = self.registry.upsert(TaskEntry::new(
            spec.binding_id,
            topic_id,
            binding.role,
            sync_handle,
        ));
        entry.mode = Some(config.mode);
        entry.handler.start(spec, config).await?;

        Ok(())
    }

    async fn cancel_task(&mut self, binding_id: i64) -> Result<()> {
        if let Some(entry) = self.registry.get_entry(binding_id) {
            entry.handler.cancel().await
        } else {
            Err(CoreError::Internal(format!(
                "task not found, binding_id: {}",
                binding_id
            )))
        }
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.app_rx = None;
        self.registry.clear();
        Ok(())
    }

    async fn persist_status(
        &mut self,
        binding_id: i64,
        status: AgentStatus,
    ) -> Result<Vec<FsmEvent>> {
        if let Some(entry) = self.registry.get_entry(binding_id) {
            let mode = entry.mode;
            let topic_id = entry.topic_id;
            helper::update_binding(
                &self.storage,
                binding_id,
                UpdateAgentBinding {
                    agent_id: None,
                    role: None,
                    model_id: None,
                    chat_config_id: None,
                    enabled: None,
                    status: Some(status),
                    mode,
                    tool_approval_policy: None,
                },
            )
            .await?;
            Ok(vec![FsmEvent::Emit(TopicEvent::TaskStatusChanged {
                binding_id,
                topic_id,
                parent_topic_id: self.topic_id,
                status,
            })])
        } else {
            log::warn!(
                "[persist_status] task not found, binding_id: {}",
                binding_id
            );
            Ok(vec![])
        }
    }

    /// 解析 pending 子任务并回复父任务;
    async fn resolve_pending_child(
        &mut self,
        binding_id: i64,
        status: AgentStatus,
        output: Vec<Content>,
    ) -> Vec<FsmEvent> {
        let mut follow_ups = vec![];
        if let Some(pending) = self.registry.take_pending(binding_id) {
            try_send_log!(
                pending.reply,
                SpawnAgentResponse {
                    call_id: pending.call_id,
                    mode: pending.mode,
                    status,
                    output,
                },
                "resolve pending child"
            );
            let parent = pending.parent_binding_id;
            if !self.registry.has_pending_for(parent) {
                follow_ups.push(FsmEvent::ChildResolved {
                    parent_binding_id: parent,
                });
            }
        }
        follow_ups
    }

    /// 创建子 Agent
    async fn spawn_child(
        &mut self,
        parent_binding_id: i64,
        request: SpawnAgentRequest,
    ) -> Result<(i64, Vec<FsmEvent>)> {
        let mode = request.mode;
        let agent = helper::get_def_by_key(&self.storage, &request.agent_key).await?;
        let binding =
            helper::get_binding_by_agent_id(&self.storage, self.topic_id, agent.id).await?;
        let binding_id = binding.id;
        log::debug!(
            "[spawn agent] parent_binding_id = {}, binding_id = {}",
            parent_binding_id,
            binding_id
        );
        if self.fsm.is_task_busy(binding.id) {
            return Err(CoreError::Internal(format!(
                "Agent is busy, binding_id: {}",
                binding_id
            )));
        }

        let chat_ctx =
            helper::get_base_info(&self.storage, &self.mcp_registry, &binding, &agent).await?;

        let tx = self.storage.begin().await?;
        let agent_topic = helper::create_sub_topic(
            &tx.storage(),
            self.topic_id,
            binding_id,
            format!("#sub-{}-agent", mode),
        )
        .await?;

        let user_input = vec![Content::new_text(request.task)];
        let (user, assistant, contexts) = match mode {
            AgentMode::Fork => match self.registry.main_entry() {
                Some(entry) => {
                    helper::create_fork_contexts(
                        &tx.storage(),
                        entry.topic_id,
                        agent_topic.id,
                        user_input,
                        &agent,
                        &chat_ctx,
                    )
                    .await?
                }
                None => {
                    return Err(CoreError::Validation(format!(
                        "Main task not found, cannot fork"
                    )));
                }
            },
            AgentMode::Sync | AgentMode::Background => {
                helper::create_contexts(
                    &tx.storage(),
                    agent_topic.id,
                    user_input,
                    &agent,
                    &chat_ctx,
                )
                .await?
            }
        };
        tx.commit().await?;

        let spec = TaskSpec {
            binding_id,
            agent,
            model: chat_ctx.model,
            provider: chat_ctx.provider,
            credential: chat_ctx.credential,
            req_config: chat_ctx.req_config,
            rule_set: chat_ctx.rule_set,
            tools: chat_ctx.tools,
            assistant: assistant.clone(),
            contexts,
        };
        let config = AgentRunConfig {
            binding_id,
            topic_id: agent_topic.id,
            parent_topic_id: binding.parent_topic_id,
            tool_approval_policy: binding.tool_approval_policy,
            mode,
        };

        Ok((
            binding_id,
            vec![
                FsmEvent::Emit(TopicEvent::MessageCreated {
                    topic_id: agent_topic.id,
                    data: user,
                }),
                FsmEvent::Emit(TopicEvent::MessageCreated {
                    topic_id: agent_topic.id,
                    data: assistant,
                }),
                FsmEvent::StartChild {
                    parent_binding_id,
                    spec,
                    config,
                },
            ],
        ))
    }

    /// 批量审批
    async fn apply_approvals(
        &mut self,
        binding_id: i64,
        allow_ids: Vec<i64>,
        deny_ids: Vec<i64>,
    ) -> Result<Vec<FsmEvent>> {
        let mut records = Vec::with_capacity(deny_ids.len() + allow_ids.len());
        for id in deny_ids {
            records.push(ApprovalRecord {
                id,
                status: ToolApprovalStatus::Denied,
            });
        }
        for id in allow_ids {
            records.push(ApprovalRecord {
                id,
                status: ToolApprovalStatus::Approved,
            });
        }
        self.storage.approval().batch_set_status(records).await?;
        Ok(vec![FsmEvent::Signal {
            binding_id,
            event: TaskEvent::ApprovalResolved,
        }])
    }
}

impl Drop for TopicRuntime {
    fn drop(&mut self) {
        self.ctx.cancel();
    }
}
