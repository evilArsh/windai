use std::collections::VecDeque;

use super::event::{TopicCommand, TopicEvent, TopicMailbox};
use super::fsm::{Effect, FsmEvent, SupervisorEvent, TaskSignal, TaskState, TopicFsm, UserRequest};
use super::helper::{self};
use super::runtime::AgentRunConfig;
use super::task::sync::SyncTask;
use super::task::{
    PendingChild, SupervisorRequest, TaskEntry, TaskNotification, TaskRegistry, TaskSpec,
};
use super::tool::{SpawnAgentRequest, SpawnAgentResponse};
use crate::agent::event::TopicMsg;
use crate::error::{CoreError, Result};
use crate::models::{
    AgentMode, AgentStatus, ApprovalRecord, ToolApprovalStatus, UpdateAgentBinding,
};
use crate::storage::Storage;
use futures::future::try_join;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use wind_ai::message::Content;
use wind_mcp::client::registry::RegistryHandle;

macro_rules! try_send_log {
    ($sender:expr, $value:expr, $prefix:expr $(,)?) => {
        if $sender.send($value).is_err() {
            log::error!("[{}] reply dropped", $prefix);
        }
    };
}

#[derive(Clone)]
pub struct TopicRuntimeHandle {
    mailbox: TopicMailbox,
    ctx: CancellationToken,
}
impl TopicRuntimeHandle {
    /// 已停止的运行时不允许再下发命令，避免 reply 永久悬挂。
    fn ensure_alive(&self) -> Result<()> {
        if self.is_stopped() {
            Err(CoreError::Internal("topic runtime has stopped".into()))
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
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mailbox
            .send(TopicMsg::Command(TopicCommand::CreateChat {
                user_input,
                reply: reply_tx,
            }))
            .await?;
        reply_rx
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?
    }
    /// 取消任务
    pub async fn cancel_task(&self, binding_id: i64) -> Result<()> {
        self.ensure_alive()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mailbox
            .send(TopicMsg::Command(TopicCommand::CancelTask {
                binding_id,
                reply: reply_tx,
            }))
            .await?;
        reply_rx
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?
    }
    /// 审批任务
    pub async fn approve(
        &self,
        binding_id: i64,
        allow_ids: Vec<i64>,
        deny_ids: Vec<i64>,
    ) -> Result<()> {
        self.ensure_alive()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mailbox
            .send(TopicMsg::Command(TopicCommand::Approval {
                reply: reply_tx,
                binding_id,
                deny_ids,
                allow_ids,
            }))
            .await?;
        reply_rx
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?
    }
    /// 关闭当前运行时
    pub async fn shutdown(&self) -> Result<()> {
        if self.is_stopped() {
            return Ok(());
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mailbox
            .send(TopicMsg::Command(TopicCommand::Shutdown {
                reply: reply_tx,
            }))
            .await?;
        reply_rx
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?
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
        log::debug!("[emit] {}", event);
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
                    match msg {
                        TopicMsg::Command(command) => {
                            log::debug!("{}", command);
                            self.handle_command(command).await;
                        }
                        TopicMsg::Task(notification) => {
                            log::debug!("{}", notification);
                            self.handle_task_notification(notification).await;
                        }
                        TopicMsg::Supervisor(request) => {
                            log::debug!("{}", request);
                            self.handle_supervisor_request(request).await;
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }
    }

    /// 归约 FSM 事件并执行副作用。
    ///
    /// 副作用执行过程中可能产生新的 FSM 事件
    async fn apply(&mut self, event: FsmEvent) -> Result<()> {
        let mut queue = VecDeque::new();
        queue.push_back(event);
        let mut first_error: Option<CoreError> = None;

        while let Some(ev) = queue.pop_front() {
            let effects = self.fsm.reduce(ev);
            for effect in effects {
                match self.execute(effect).await {
                    Ok(Some(follow_ups)) => queue.extend(follow_ups),
                    Ok(None) => {}
                    Err(err) => {
                        log::error!("[apply] effect failed: {}", err);
                        first_error.get_or_insert(err);
                    }
                }
            }
        }

        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// 执行副作用seam
    async fn execute(&mut self, effect: Effect) -> Result<Option<Vec<FsmEvent>>> {
        match effect {
            Effect::PersistStatus { binding_id, status } => {
                self.persist_status(binding_id, status.into()).await?;
                Ok(None)
            }
            Effect::Emit(event) => {
                self.emit(event);
                Ok(None)
            }
            Effect::StartAgent {
                binding_id,
                spec,
                config,
            } => {
                if let Err(err) = self.start_agent_task(spec, config).await {
                    let msg = err.to_string();
                    log::error!("[StartAgent] start task error: {}", msg);
                    // 主任务启动失败 → 直接向调用方传播错误；
                    // 子任务启动失败 → 生成 Failed 事件回写 FSM，从而解析 pending 子任务。
                    if self.fsm.is_main_binding(binding_id) {
                        return Err(err);
                    }
                    return Ok(Some(vec![FsmEvent::Signal(TaskSignal::Failed {
                        binding_id,
                        error: msg,
                        message_id: None,
                    })]));
                }
                Ok(None)
            }
            Effect::ResumeAgent { binding_id } => {
                self.resume_task(binding_id).await?;
                Ok(None)
            }
            Effect::CancelAgent { binding_id } => {
                self.cancel_task(binding_id).await?;
                Ok(None)
            }
            Effect::SendChildResponse {
                binding_id,
                status,
                output,
            } => self
                .resolve_pending_child(binding_id, status.into(), output)
                .await
                .map(Some),
            Effect::SpawnChild {
                parent_binding_id,
                call_id,
                request,
                reply,
            } => self
                .spawn_child(parent_binding_id, call_id, request, reply)
                .await
                .map(Some),
            Effect::ApplyApprovals {
                binding_id,
                allow_ids,
                deny_ids,
            } => self
                .apply_approvals(binding_id, allow_ids, deny_ids)
                .await
                .map(Some),
            Effect::CloseEventStream => {
                self.app_rx = None;
                Ok(None)
            }
            Effect::StopRuntime => {
                self.ctx.cancel();
                Ok(None)
            }
        }
    }

    async fn handle_command(&mut self, command: TopicCommand) {
        match command {
            TopicCommand::CreateChat { user_input, reply } => {
                let result = self.start_main_agent(user_input).await;
                try_send_log!(reply, result, "CreateChat");
            }
            TopicCommand::CancelTask { binding_id, reply } => {
                let result = if self.fsm.task_state(binding_id).is_some() {
                    self.apply(FsmEvent::UserRequest(UserRequest::CancelTask {
                        binding_id,
                    }))
                    .await
                } else {
                    Err(CoreError::Internal(format!(
                        "task not found, binding_id: {}",
                        binding_id
                    )))
                };
                try_send_log!(reply, result, "CancelTask");
            }
            TopicCommand::Shutdown { reply } => {
                let result = self
                    .apply(FsmEvent::UserRequest(UserRequest::Shutdown))
                    .await;
                try_send_log!(reply, result, "Shutdown");
            }
            TopicCommand::Approval {
                binding_id,
                deny_ids,
                allow_ids,
                reply,
            } => {
                let result = if self.fsm.task_state(binding_id) == Some(TaskState::WaitingApproval)
                {
                    self.apply(FsmEvent::UserRequest(UserRequest::Approval {
                        binding_id,
                        allow_ids,
                        deny_ids,
                    }))
                    .await
                } else {
                    Err(CoreError::Internal(format!(
                        "Task is not waiting approval, current status: {:?}",
                        self.fsm.task_state(binding_id)
                    )))
                };
                try_send_log!(reply, result, "Approval");
            }
            TopicCommand::Subscribe { reply } => {
                let sender = self
                    .app_rx
                    .get_or_insert_with(|| broadcast::channel(1024).0);
                try_send_log!(reply, sender.subscribe(), "Subscribe");
            }
        }
    }

    async fn handle_task_notification(&mut self, notification: TaskNotification) {
        match notification {
            TaskNotification::Started { .. } => {}
            TaskNotification::Message {
                binding_id,
                message_id,
                topic_id,
                index,
                delta,
            } => {
                self.emit(TopicEvent::Message {
                    topic_id,
                    message_id,
                    index,
                    binding_id,
                    parent_topic_id: self.topic_id,
                    data: delta,
                });
            }
            TaskNotification::WaitingApproval {
                data,
                binding_id,
                calls,
            } => {
                match helper::save_approval_state(
                    &self.storage,
                    binding_id,
                    self.topic_id,
                    data.clone(),
                    calls,
                )
                .await
                {
                    Ok(requests) => {
                        let _ = self
                            .apply(FsmEvent::Signal(TaskSignal::AwaitApproval {
                                binding_id,
                                data,
                                requests,
                            }))
                            .await;
                    }
                    Err(err) => {
                        log::error!(
                            "[WaitingApproval] save approval state error: {}, (topic_id = {})",
                            &err,
                            self.topic_id
                        );
                        let _ = self
                            .apply(FsmEvent::Signal(TaskSignal::Failed {
                                binding_id,
                                error: err.to_string(),
                                message_id: Some(data.id),
                            }))
                            .await;
                    }
                }
            }
            TaskNotification::Cancelled { binding_id } => {
                let _ = self
                    .apply(FsmEvent::Signal(TaskSignal::Cancelled { binding_id }))
                    .await;
            }
            TaskNotification::Completed { binding_id, data } => {
                match helper::save_message(&self.storage, data.clone()).await {
                    Ok(()) => {
                        let _ = self
                            .apply(FsmEvent::Signal(TaskSignal::Completed { binding_id, data }))
                            .await;
                    }
                    Err(err) => {
                        log::error!("[Completed] save message error: {}", err);
                        let _ = self
                            .apply(FsmEvent::Signal(TaskSignal::Failed {
                                binding_id,
                                error: err.to_string(),
                                message_id: Some(data.id),
                            }))
                            .await;
                    }
                }
            }
            TaskNotification::Failed {
                binding_id,
                data,
                error,
            } => {
                let message_id = data.id;
                let error = match helper::save_message(&self.storage, data).await {
                    Ok(()) => error,
                    Err(e) => {
                        log::error!("[Failed] save message error: {}", e);
                        format!("{error}: {e}")
                    }
                };
                let _ = self
                    .apply(FsmEvent::Signal(TaskSignal::Failed {
                        binding_id,
                        error,
                        message_id: Some(message_id),
                    }))
                    .await;
            }
        }
    }

    //  内部流转指令
    async fn handle_supervisor_request(&mut self, request: SupervisorRequest) {
        match request {
            SupervisorRequest::SpawnAgent {
                binding_id,
                call_id,
                request,
                reply,
            } => {
                let _ = self
                    .apply(FsmEvent::Supervisor(SupervisorEvent::SpawnAgent {
                        parent_binding_id: binding_id,
                        call_id,
                        request,
                        reply,
                    }))
                    .await;
            }
        }
    }

    /// 启动一个 Agent 任务
    async fn spawn_agent(
        &mut self,
        request: SpawnAgentRequest,
    ) -> Result<(i64, TaskSpec, AgentRunConfig)> {
        let agent = helper::get_def_by_key(&self.storage, &request.agent_key).await?;
        let binding =
            helper::get_binding_by_agent_id(&self.storage, self.topic_id, agent.id).await?;
        if self.fsm.is_task_busy(binding.id) {
            return Err(CoreError::Internal(format!(
                "Agent is busy, binding_id: {}",
                binding.id
            )));
        }

        let mode = request.mode;
        let chat_ctx =
            helper::get_base_info(&self.storage, &self.mcp_registry, &binding, &agent).await?;
        let binding_id = binding.id;

        let tx = self.storage.begin().await?;
        let agent_topic = helper::create_sub_topic(
            &tx.storage(),
            self.topic_id,
            binding.id,
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

        self.emit(TopicEvent::MessageCreated {
            topic_id: agent_topic.id,
            data: user,
        });

        self.emit(TopicEvent::MessageCreated {
            topic_id: agent_topic.id,
            data: assistant.clone(),
        });

        let spec = TaskSpec {
            binding_id,
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
            binding_id,
            topic_id: agent_topic.id,
            parent_topic_id: binding.parent_topic_id,
            tool_approval_policy: binding.tool_approval_policy,
            mode,
        };
        Ok((binding_id, spec, config))
    }

    /// 开始新的 Main Agent 对话。
    async fn start_main_agent(&mut self, user_input: Vec<Content>) -> Result<()> {
        if self.fsm.is_main_busy() {
            return Err(CoreError::Internal("main agent is running".into()));
        }

        let binding = helper::get_main_binding(&self.storage, self.topic_id).await?;
        log::debug!("[start_main_agent] get binding: {:#?}", binding);
        let agent = helper::get_def_by_id(&self.storage, binding.agent_id).await?;
        log::debug!("[start_main_agent] get agent: {:#?}", agent);
        let chat_ctx =
            helper::get_base_info(&self.storage, &self.mcp_registry, &binding, &agent).await?;
        log::debug!("[start_main_agent] get chat_ctx: {:#?}", chat_ctx);

        let tx = self.storage.begin().await?;
        let agent_topic = helper::create_sub_topic(
            &tx.storage(),
            self.topic_id,
            binding.id,
            format!("#main-agent"),
        )
        .await?;
        log::debug!("[start_main_agent] create agent_topic: {:#?}", agent_topic);
        let (user, assistant, contexts) =
            helper::create_contexts(&tx.storage(), agent_topic.id, user_input, &agent, &chat_ctx)
                .await?;
        tx.commit().await?;

        self.emit(TopicEvent::MessageCreated {
            topic_id: agent_topic.id,
            data: user,
        });

        self.emit(TopicEvent::MessageCreated {
            topic_id: agent_topic.id,
            data: assistant.clone(),
        });

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
            mode: AgentMode::Sync,
        };

        self.apply(FsmEvent::UserRequest(UserRequest::Start {
            is_main: true,
            spec,
            config,
        }))
        .await
    }

    async fn resume_task(&mut self, binding_id: i64) -> Result<()> {
        let binding = helper::get_binding_by_id(&self.storage, binding_id).await?;
        let agent = helper::get_def_by_id(&self.storage, binding.agent_id).await?;
        let agent_topic =
            helper::get_topic_by_binding_id(&self.storage, self.topic_id, binding_id).await?;

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
        self.registry.close().await;
        self.app_rx = None;
        Ok(())
    }

    async fn persist_status(&mut self, binding_id: i64, status: AgentStatus) -> Result<()> {
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
            self.emit(TopicEvent::TaskStatusChanged {
                binding_id,
                topic_id,
                parent_topic_id: self.topic_id,
                status,
            });
            Ok(())
        } else {
            log::warn!(
                "[persist_status] task not found, binding_id: {}",
                binding_id
            );
            Ok(())
        }
    }

    /// 解析 pending 子任务并回复父任务;
    /// 返回可选的 `ChildResolved` 后续事件。
    async fn resolve_pending_child(
        &mut self,
        binding_id: i64,
        status: AgentStatus,
        output: Vec<Content>,
    ) -> Result<Vec<FsmEvent>> {
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
            // 父任务若没有其它 pending 子任务，则恢复为 Running
            let parent = pending.parent_binding_id;
            if !self.registry.has_pending_for(parent) {
                follow_ups.push(FsmEvent::Supervisor(SupervisorEvent::ChildResolved {
                    parent_binding_id: parent,
                }));
            }
        }
        Ok(follow_ups)
    }

    /// 创建子 Agent
    async fn spawn_child(
        &mut self,
        parent_binding_id: i64,
        call_id: String,
        request: SpawnAgentRequest,
        reply: oneshot::Sender<SpawnAgentResponse>,
    ) -> Result<Vec<FsmEvent>> {
        let mode = request.mode;
        match self.spawn_agent(request).await {
            Ok((child_binding_id, spec, config)) => {
                self.registry.insert_pending(PendingChild {
                    call_id: call_id.clone(),
                    mode,
                    reply,
                    binding_id: child_binding_id,
                    parent_binding_id,
                });
                Ok(vec![FsmEvent::Supervisor(SupervisorEvent::ChildStarted {
                    parent_binding_id,
                    spec,
                    config,
                })])
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
                Ok(vec![])
            }
        }
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
        Ok(vec![FsmEvent::UserRequest(UserRequest::ApprovalApplied {
            binding_id,
        })])
    }
}

impl Drop for TopicRuntime {
    fn drop(&mut self) {
        self.ctx.cancel();
    }
}
