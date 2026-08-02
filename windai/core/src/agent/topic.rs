use super::event::{TopicCommand, TopicEvent, TopicMailbox};
use super::helper::{self};
use super::runtime::AgentRunConfig;
use super::task::sync::SyncTask;
use super::task::{
    PendingChild, SupervisorRequest, TaskEntry, TaskNotification, TaskRegistry, TaskSpec,
};
use super::tool::{SpawnAgentRequest, SpawnAgentResponse};
use crate::agent::event::TopicMsg;
use crate::chat::loops::ChatContext;
use crate::error::{CoreError, Result};
use crate::models::{
    AgentBinding, AgentDefinition, AgentMode, AgentStatus, ApprovalRecord, Message,
    ToolApprovalStatus, UpdateAgentBinding,
};
use crate::storage::Storage;
use futures::future::{join_all, try_join};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use wind_ai::message::{Content, Message as AiMessage};
use wind_mcp::client::registry::RegistryHandle;

macro_rules! try_send_log {
    ($sender:expr, $value:expr, $prefix:expr $(,)?) => {
        if let Err(_) = $sender.send($value) {
            log::error!("[{}] reply dropped", $prefix);
        }
    };
}

pub struct TopicRuntimeHandle {
    mailbox: TopicMailbox,
    events: broadcast::Sender<TopicEvent>,
}
impl TopicRuntimeHandle {
    /// 订阅Topic事件
    pub fn subscribe(&self) -> broadcast::Receiver<TopicEvent> {
        self.events.subscribe()
    }
    /// 创建新的对话
    pub async fn create_chat(&self, user_input: Vec<Content>) -> Result<()> {
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
    pub async fn cancel_task(&self, binding_id: i64) -> Result<()> {
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

    pub async fn shutdown(&self) -> Result<()> {
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
    app_rx: broadcast::Sender<TopicEvent>,
    storage: Storage,
    mcp_registry: RegistryHandle,
    registry: TaskRegistry,
}

impl TopicRuntime {
    /// 创建顶层Topic运行时
    pub fn spawn(
        topic_id: i64,
        mcp_registry: RegistryHandle,
        storage: Storage,
    ) -> TopicRuntimeHandle {
        let (tx, rx) = mpsc::channel(256);
        let mailbox = TopicMailbox::new(tx);
        let (events, _) = broadcast::channel(1024);
        let root_ctx = CancellationToken::new();
        let runtime = Self {
            ctx: root_ctx.clone(),
            topic_id,
            mailbox: mailbox.clone(),
            mailbox_rx: rx,
            app_rx: events.clone(),
            storage,
            mcp_registry,
            registry: TaskRegistry::new(),
        };

        tokio::spawn(runtime.run());

        TopicRuntimeHandle { mailbox, events }
    }

    fn emit(&self, event: TopicEvent) {
        let _ = self.app_rx.send(event);
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;

                _ = self.ctx.cancelled() => {
                    // TODO: more
                    break;
                }

                Some(msg) = self.mailbox_rx.recv() => {
                    match msg {
                        TopicMsg::Command(command) => {
                            self.handle_command(command).await;
                        }
                        TopicMsg::Task(notification) => {
                            self.handle_task_notification(notification).await;
                        }
                        TopicMsg::Supervisor(request) => {
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
    async fn handle_command(&mut self, command: TopicCommand) {
        match command {
            TopicCommand::CreateChat { user_input, reply } => {
                let result = self.start_main_chat(user_input).await;
                try_send_log!(reply, result, "CreateChat");
            }
            TopicCommand::CancelTask { binding_id, reply } => {
                let result = self.cancel_task(binding_id).await;
                try_send_log!(reply, result, "CancelTask");
            }
            TopicCommand::Shutdown { reply } => {
                let result = self.shutdown().await;
                try_send_log!(reply, result, "Shutdown");
            }
            TopicCommand::Approval {
                binding_id,
                deny_ids,
                allow_ids,
                reply,
            } => {
                let result = self.apply_approvals(binding_id, deny_ids, allow_ids).await;
                try_send_log!(reply, result, "Approval");
            }
        }
    }

    async fn handle_task_notification(&mut self, event: TaskNotification) {
        match event {
            TaskNotification::Started { binding_id } => {
                self.update_task_status(binding_id, AgentStatus::Running)
                    .await;
            }
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
                let message_id = data.id;
                let agent_topic_id = data.topic_id;
                match helper::save_approval_state(
                    &self.storage,
                    binding_id,
                    self.topic_id,
                    data,
                    calls,
                )
                .await
                {
                    Ok(requests) => {
                        self.emit(TopicEvent::ApprovalRequired {
                            binding_id,
                            topic_id: agent_topic_id,
                            parent_topic_id: self.topic_id,
                            message_id,
                            requests,
                        });
                        self.update_task_status(binding_id, AgentStatus::WaitingApproval)
                            .await;
                    }
                    Err(err) => {
                        log::error!("[AgentOutput::ApprovalRequired] {}", &err);
                        self.emit(TopicEvent::Error {
                            binding_id,
                            topic_id: agent_topic_id,
                            parent_topic_id: self.topic_id,
                            message_id: Some(message_id),
                            error: err.to_string(),
                        });
                        self.update_task_status(binding_id, AgentStatus::Failed)
                            .await;
                    }
                }
            }
            TaskNotification::Cancelled { binding_id } => {
                let s = AgentStatus::Cancelled;
                self.walk_task(binding_id, s, || {
                    vec![Content::new_text("Task was cancelled".to_string())]
                });
                self.update_task_status(binding_id, s).await;
            }
            TaskNotification::Completed { binding_id, data } => {
                let message_id = data.id;
                let topic_id = data.topic_id;
                self.walk_task(binding_id, AgentStatus::Finished, || {
                    data.content
                        .last()
                        .and_then(|c| {
                            if c.is_simple() {
                                Some(c.content.clone())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_else(|| {
                            vec![Content::new_text(format!("Task has no valid result"))]
                        })
                });

                if let Err(err) = helper::save_message(&self.storage, data).await {
                    self.emit(TopicEvent::Error {
                        binding_id,
                        topic_id,
                        parent_topic_id: self.topic_id,
                        message_id: Some(message_id),
                        error: err.to_string(),
                    });
                    self.update_task_status(binding_id, AgentStatus::Failed)
                        .await;
                } else {
                    self.emit(TopicEvent::MessageFinished {
                        binding_id,
                        topic_id,
                        parent_topic_id: self.topic_id,
                        message_id,
                    });
                    self.update_task_status(binding_id, AgentStatus::Finished)
                        .await;
                }
            }
            TaskNotification::Failed {
                binding_id,
                data,
                error,
            } => {
                let message_id = data.id;
                let topic_id = data.topic_id;
                let mut err = error;
                let s = AgentStatus::Failed;
                self.walk_task(binding_id, s, || vec![Content::new_text(err.to_string())]);

                match helper::save_message(&self.storage, data).await {
                    Err(error) => err = error.to_string(),
                    _ => {}
                };
                self.emit(TopicEvent::Error {
                    binding_id,
                    topic_id,
                    parent_topic_id: self.topic_id,
                    message_id: Some(message_id),
                    error: err.clone(),
                });
                self.update_task_status(binding_id, s).await;
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
                let mode = request.mode;
                let mut resp = None;
                let mut child_binding_id = None;
                match self.spawn_sync(request).await {
                    Ok(binding_id) => child_binding_id = Some(binding_id),
                    Err(err) => {
                        resp = Some(SpawnAgentResponse {
                            call_id: call_id.clone(),
                            mode,
                            status: AgentStatus::Failed,
                            output: vec![Content::new_text(err.to_string())],
                        });
                    }
                }
                if resp.is_none()
                    && let Some(child_binding_id) = child_binding_id
                {
                    self.update_task_status(binding_id, AgentStatus::WaitingChild)
                        .await;
                    self.registry.insert_pending(PendingChild {
                        call_id: call_id.clone(),
                        mode,
                        reply,
                        binding_id: child_binding_id,
                        parent_binding_id: binding_id,
                    });
                    return;
                } else {
                    resp = Some(SpawnAgentResponse {
                        call_id,
                        mode,
                        status: AgentStatus::Failed,
                        output: vec![Content::new_text("Parent task not found".to_string())],
                    });
                }
                if let Some(resp) = resp {
                    try_send_log!(reply, resp, "SpawnAgent");
                }
            }
        }
    }

    /// 启动一个同步任务并且返回任务ID(binding_id)
    async fn spawn_sync(&mut self, request: SpawnAgentRequest) -> Result<i64> {
        let agent_topic =
            helper::create_sub_topic(&self.storage, self.topic_id, format!("#sub-sync-agent"))
                .await?;
        let agent = helper::get_def_by_key(&self.storage, &request.agent_key).await?;
        let binding =
            helper::get_binding_by_agent_id(&self.storage, self.topic_id, agent.id).await?;
        if let Some(entry) = self.registry.get_entry(binding.id)
            && entry.is_busy()
        {
            return Err(CoreError::Internal(format!(
                "Agent is busy, status: {}",
                entry.get_status()
            )));
        }

        let chat_ctx =
            helper::get_base_info(&self.storage, &self.mcp_registry, &binding, &agent).await?;
        let binding_id = binding.id;

        let user_input = vec![Content::new_text(request.task)];
        let (user, assistant, contexts) =
            helper::create_contexts(&self.storage, user_input, &agent_topic, &agent, &chat_ctx)
                .await?;

        self.emit(TopicEvent::MessageCreated {
            topic_id: agent_topic.id,
            data: user,
        });

        self.emit(TopicEvent::MessageCreated {
            topic_id: agent_topic.id,
            data: assistant.clone(),
        });

        self.start_task(
            AgentMode::Sync,
            chat_ctx,
            assistant,
            contexts,
            agent,
            binding,
        )
        .await
        .and_then(|_| Ok(binding_id))
    }

    /// 开始新的对话。
    /// 加载对话需要的所有上下文，
    /// 消息发送到Agent并开始对话循环
    async fn start_main_chat(&mut self, user_input: Vec<Content>) -> Result<()> {
        // 1. 查询当前是否有主Agent运行
        // 2. 如果主Agent没有结束，则等待，否则创建一个主Agent开始运行,TODO: Queued状态
        if let Some(entry) = self.registry.main_entry()
            && entry.is_busy()
        {
            return Err(CoreError::Internal("main agent is running".into()));
        }
        let agent_topic =
            helper::create_sub_topic(&self.storage, self.topic_id, format!("#main-agent")).await?;
        let binding = helper::get_main_binding(&self.storage, self.topic_id).await?;
        let agent = helper::get_def_by_id(&self.storage, binding.agent_id).await?;
        let chat_ctx =
            helper::get_base_info(&self.storage, &self.mcp_registry, &binding, &agent).await?;

        let (user, assistant, contexts) =
            helper::create_contexts(&self.storage, user_input, &agent_topic, &agent, &chat_ctx)
                .await?;

        self.emit(TopicEvent::MessageCreated {
            topic_id: agent_topic.id,
            data: user,
        });

        self.emit(TopicEvent::MessageCreated {
            topic_id: agent_topic.id,
            data: assistant.clone(),
        });

        self.start_task(
            AgentMode::Sync,
            chat_ctx,
            assistant,
            contexts,
            agent,
            binding,
        )
        .await
    }

    async fn resume_task(&mut self, binding_id: i64) -> Result<()> {
        let entry = self.registry.get_entry(binding_id);
        if let Some(entry) = entry
            && entry.get_status() != AgentStatus::WaitingApproval
        {
            return Err(CoreError::Internal(format!(
                "Task is not waiting approval, current status: {}",
                entry.get_status()
            )));
        }

        let binding = helper::get_binding_by_id(&self.storage, binding_id).await?;
        let agent = helper::get_def_by_id(&self.storage, binding.agent_id).await?;

        let (chat_ctx, contexts) = try_join(
            helper::get_base_info(&self.storage, &self.mcp_registry, &binding, &agent),
            helper::get_message_contexts(&self.storage, binding.topic_id),
        )
        .await?;

        let assistant = contexts.last().cloned().ok_or_else(|| {
            CoreError::Internal(format!("no assistant in this topic: {}", binding.topic_id))
        })?;
        let contexts = helper::transfer_contexts(contexts)?;

        if let Some(entry) = entry {
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
            entry
                .handler
                .start(
                    spec,
                    AgentRunConfig {
                        binding_id: binding.id,
                        topic_id: binding.topic_id,
                        parent_topic_id: binding.parent_topic_id,
                        tool_approval_policy: binding.tool_approval_policy,
                    },
                )
                .await
        } else {
            self.start_task(
                binding.mode.unwrap_or(AgentMode::Sync),
                chat_ctx,
                assistant,
                contexts,
                agent,
                binding,
            )
            .await
        }
    }

    async fn start_task(
        &mut self,
        mode: AgentMode,
        chat_ctx: ChatContext,
        assistant: Message,
        contexts: Vec<AiMessage>,
        agent: AgentDefinition,
        binding: AgentBinding,
    ) -> Result<()> {
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

        // TODO: 使用mode区分
        let sync_handle = SyncTask::spawn(
            self.ctx.child_token(),
            binding.id,
            binding.topic_id,
            self.mailbox.clone(),
            self.storage.clone(),
            self.mcp_registry.clone(),
        );

        let entry = self.registry.upsert(TaskEntry::new(
            binding.id,
            binding.topic_id,
            binding.role,
            sync_handle,
        ));
        entry.mode = Some(mode);
        entry
            .handler
            .start(
                spec,
                AgentRunConfig {
                    binding_id: binding.id,
                    topic_id: binding.topic_id,
                    parent_topic_id: binding.parent_topic_id,
                    tool_approval_policy: binding.tool_approval_policy,
                },
            )
            .await?;

        self.update_task_status(binding.id, AgentStatus::Created)
            .await;

        Ok(())
    }
    async fn cancel_task(&mut self, binding_id: i64) -> Result<()> {
        if let Some(entry) = self.registry.get_entry_mut(binding_id) {
            entry.handler.cancel().await?;
            Ok(())
        } else {
            Err(CoreError::Internal(format!(
                "task not found, binding_id: {}",
                binding_id
            )))
        }
    }

    async fn shutdown(&mut self) -> Result<()> {
        let handles: Vec<_> = self
            .registry
            .get_entries()
            .into_iter()
            .map(|entry| entry.handler.clone())
            .collect();

        join_all(handles.into_iter().map(|handle| async move {
            if let Err(e) = handle.cancel().await {
                log::error!("shutdown cancel error: {}", e);
            }
        }))
        .await;

        self.ctx.cancel();

        Ok(())
    }

    async fn apply_approvals(
        &mut self,
        binding_id: i64,
        deny_ids: Vec<i64>,
        allow_ids: Vec<i64>,
    ) -> Result<()> {
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
        self.resume_task(binding_id).await
    }

    fn walk_task(
        &mut self,
        binding_id: i64,
        status: AgentStatus,
        build_result: impl FnOnce() -> Vec<Content>,
    ) {
        if let Some(pending) = self.registry.take_pending(binding_id) {
            try_send_log!(
                pending.reply,
                SpawnAgentResponse {
                    call_id: pending.call_id,
                    mode: pending.mode,
                    status,
                    output: build_result(),
                },
                "walk task"
            );
        }
    }

    async fn update_task_status(&mut self, binding_id: i64, status: AgentStatus) {
        if let Some(entry) = self.registry.get_entry_mut(binding_id) {
            entry.set_status(status);
            let mode = entry.mode;
            let topic_id = entry.topic_id;
            match helper::update_binding(
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
            .await
            {
                Ok(_) => {
                    self.emit(TopicEvent::TaskStatusChanged {
                        binding_id,
                        topic_id,
                        parent_topic_id: self.topic_id,
                        status,
                    });
                }
                Err(e) => {
                    log::error!("update binding status error: {}", e);
                    self.emit(TopicEvent::Error {
                        binding_id,
                        topic_id,
                        parent_topic_id: self.topic_id,
                        message_id: None,
                        error: e.to_string(),
                    });
                }
            };
        } else {
            log::warn!("task not found, binding_id: {}", binding_id);
        }
    }
}
