use super::event::{TopicCommand, TopicEvent, TopicMailbox, TopicMsg};
use super::fsm::{Effect, FsmEvent, TaskEvent, TopicFsm};
use super::task::{PendingChild, TaskManager, TaskSpec};
use super::tool::{SpawnAgentRequest, SpawnAgentResponse};
use crate::env::app_dirs;
use crate::error::{CoreError, Result};
use crate::models::AgentStatus;
use crate::storage::Storage;
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
    /// 主任务进入终态（Finished / Failed / Cancelled）、等待审批，或 runtime 停止时，
    /// 该 channel 被关闭。每次新对话需重新订阅
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

    /// 创建新的任务
    pub async fn create_task(&self, user_input: Vec<Content>) -> Result<()> {
        self.ensure_alive()?;
        self.mailbox
            .send(TopicMsg::Command(TopicCommand::Start { user_input }))
            .await
    }
    /// 取消任务
    pub async fn cancel_task(&self, instance_id: i64) -> Result<()> {
        self.ensure_alive()?;
        self.mailbox
            .send(TopicMsg::Command(TopicCommand::Cancel { instance_id }))
            .await
    }
    /// 审批任务
    pub async fn approve(
        &self,
        instance_id: i64,
        allow_ids: Vec<i64>,
        deny_ids: Vec<i64>,
    ) -> Result<()> {
        self.ensure_alive()?;
        self.mailbox
            .send(TopicMsg::Command(TopicCommand::Approval {
                instance_id,
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
    fsm: TopicFsm,
    task_mgr: TaskManager,
}

impl TopicRuntime {
    /// 创建顶层 Topic 运行时
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
            fsm: TopicFsm::new(topic_id),
            task_mgr: TaskManager::new(
                ctx.child_token(),
                storage,
                mcp_registry,
                app_dirs().topic_dir().join(topic_id.to_string()),
            ),
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

    /// 归约 FSM 事件并执行副作用
    ///
    /// 副作用执行过程中可能产生新的 FSM 事件
    ///
    /// 采用深度优先策略：一个 effect 执行后产生的 follow_up 事件会立即归约
    /// 执行，直到其副作用链全部完成，才继续处理下一个同级 effect
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

    /// 执行副作用 seam
    async fn execute(&mut self, effect: Effect) -> Option<Vec<FsmEvent>> {
        log::debug!("{}", effect);
        match effect {
            Effect::SpawnChild {
                instance_id,
                call_id,
                request,
                reply,
            } => {
                match self
                    .handle_spawn_child(instance_id, call_id, request, reply)
                    .await
                {
                    Some(spec) => Some(vec![
                        FsmEvent::Emit(TopicEvent::MessageCreated {
                            data: spec.user.clone(),
                            topic_id: self.topic_id,
                            instance_id: spec.instance.id,
                        }),
                        FsmEvent::Emit(TopicEvent::MessageCreated {
                            data: spec.assistant.clone(),
                            topic_id: self.topic_id,
                            instance_id: spec.instance.id,
                        }),
                        FsmEvent::Start { spec },
                    ]),
                    None => None,
                }
            }
            Effect::Approval {
                instance_id,
                allow_ids,
                deny_ids,
            } => match self.handle_approval(instance_id, allow_ids, deny_ids).await {
                Ok(res) => Some(res),
                Err(err) => Some(vec![FsmEvent::Signal {
                    instance_id,
                    event: TaskEvent::Failed {
                        data: None,
                        error: err.to_string(),
                    },
                }]),
            },
            Effect::ApprovalRequest {
                instance_id,
                data,
                calls,
            } => {
                let message_id = data.id;
                let event = match self
                    .task_mgr
                    .persist_approval_state(self.topic_id, instance_id, data.clone(), calls)
                    .await
                {
                    Ok(requests) => Some(vec![FsmEvent::Emit(TopicEvent::ApprovalRequired {
                        instance_id,
                        topic_id: self.topic_id,
                        message_id,
                        requests,
                    })]),
                    Err(err) => Some(vec![FsmEvent::Signal {
                        instance_id,
                        event: TaskEvent::Failed {
                            error: err.to_string(),
                            data: Some(data),
                        },
                    }]),
                };
                event
            }
            Effect::Completed {
                instance_id,
                data,
                status,
            } => {
                let message_id = data.id;
                let mut event =
                    self.handle_pending(instance_id, status, TaskManager::get_output(&data));
                match self.task_mgr.persist_message(data.clone()).await {
                    Ok(_) => event.push(FsmEvent::Emit(TopicEvent::MessageFinished {
                        instance_id,
                        topic_id: self.topic_id,
                        message_id,
                    })),
                    Err(err) => event.push(FsmEvent::Signal {
                        instance_id,
                        event: TaskEvent::Failed {
                            error: err.to_string(),
                            data: Some(data),
                        },
                    }),
                }
                Some(event)
            }
            Effect::Failed {
                instance_id,
                data,
                error,
                status,
            } => {
                let mut event = self.handle_pending(
                    instance_id,
                    status,
                    vec![Content::new_text(error.clone())],
                );
                let mut error = error;
                let message_id = data.as_ref().map(|d| d.id);
                if let Some(data) = data {
                    error = match self.task_mgr.persist_message(data).await {
                        Ok(_) => error,
                        Err(e) => format!("{error}: {e}"),
                    };
                }
                event.push(FsmEvent::Emit(TopicEvent::Error {
                    instance_id: Some(instance_id),
                    topic_id: self.topic_id,
                    message_id,
                    error: error.clone(),
                }));
                Some(event)
            }
            Effect::Canceled {
                instance_id,
                status,
                error,
            } => {
                let event =
                    self.handle_pending(instance_id, status, vec![Content::new_text(error)]);
                Some(event)
            }
            Effect::StopRuntime => {
                self.cancel_all();
                None
            }
            Effect::CloseEventStream => {
                self.close_event_stream();
                None
            }
            Effect::Cancel { instance_id } => match self.task_mgr.cancel(instance_id).await {
                Err(err) => Some(vec![FsmEvent::Signal {
                    instance_id,
                    event: TaskEvent::Failed {
                        data: None,
                        error: err.to_string(),
                    },
                }]),
                Ok(_) => None,
            },
            Effect::Resume { instance_id } => match self.task_mgr.resume(instance_id).await {
                Ok(Some(spec)) => Some(vec![FsmEvent::Start { spec }]),
                Ok(_) => None,
                Err(err) => Some(vec![FsmEvent::Signal {
                    instance_id,
                    event: TaskEvent::Failed {
                        data: None,
                        error: err.to_string(),
                    },
                }]),
            },
            Effect::Emit(event) => {
                self.emit(event);
                None
            }
            Effect::Start { spec } => {
                let instance_id = spec.instance.id;
                match self.task_mgr.start(spec, self.mailbox.clone()).await {
                    Err(err) => Some(vec![FsmEvent::Signal {
                        instance_id,
                        event: TaskEvent::Failed {
                            data: None,
                            error: err.to_string(),
                        },
                    }]),
                    Ok(_) => None,
                }
            }
            Effect::PersistStatus {
                instance_id,
                status,
                mode,
                agent_id,
            } => match self
                .task_mgr
                .persist_status(instance_id, status, mode, agent_id)
                .await
            {
                Ok(_) => Some(vec![FsmEvent::Emit(TopicEvent::TaskStatusChanged {
                    instance_id,
                    topic_id: self.topic_id,
                    status,
                    mode,
                })]),
                Err(err) => Some(vec![FsmEvent::Emit(TopicEvent::Error {
                    instance_id: Some(instance_id),
                    message_id: None,
                    topic_id: self.topic_id,
                    error: err.to_string(),
                })]),
            },
            Effect::Init { user_input } => {
                match self.task_mgr.init(self.topic_id, user_input).await {
                    Ok(spec) => Some(vec![
                        FsmEvent::Emit(TopicEvent::MessageCreated {
                            data: spec.user.clone(),
                            topic_id: self.topic_id,
                            instance_id: spec.instance.id,
                        }),
                        FsmEvent::Emit(TopicEvent::MessageCreated {
                            data: spec.assistant.clone(),
                            topic_id: self.topic_id,
                            instance_id: spec.instance.id,
                        }),
                        FsmEvent::Start { spec },
                    ]),
                    Err(err) => Some(vec![
                        FsmEvent::Emit(TopicEvent::Error {
                            instance_id: None,
                            topic_id: self.topic_id,
                            message_id: None,
                            error: err.to_string(),
                        }),
                        FsmEvent::Topic(TopicMsg::Command(TopicCommand::Shutdown)),
                    ]),
                }
            }
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

    async fn shutdown(&mut self) -> Result<()> {
        self.close_event_stream();
        self.task_mgr.clear();
        Ok(())
    }

    async fn handle_spawn_child(
        &mut self,
        parent_instance_id: i64,
        call_id: String,
        request: SpawnAgentRequest,
        reply: oneshot::Sender<SpawnAgentResponse>,
    ) -> Option<TaskSpec> {
        let mode = request.mode;
        match self
            .task_mgr
            .spawn_child(self.topic_id, parent_instance_id, request)
            .await
        {
            Ok(spec) => {
                self.task_mgr.insert_pending(PendingChild {
                    call_id,
                    mode,
                    reply,
                    instance_id: spec.instance.id,
                    parent_instance_id,
                });
                Some(spec)
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

    /// 任务结束，处理状态
    fn handle_pending(
        &mut self,
        instance_id: i64,
        status: AgentStatus,
        output: Vec<Content>,
    ) -> Vec<FsmEvent> {
        let mut event = vec![];
        if let Some(pending) = self.task_mgr.take_pending(instance_id) {
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
            if !self.task_mgr.has_pending_for(pending.parent_instance_id) {
                event.push(FsmEvent::ChildResolved {
                    instance_id: pending.parent_instance_id,
                });
            }
        }

        event
    }

    /// 批量审批
    async fn handle_approval(
        &mut self,
        instance_id: i64,
        allow_ids: Vec<i64>,
        deny_ids: Vec<i64>,
    ) -> Result<Vec<FsmEvent>> {
        self.task_mgr
            .persist_approval_record(allow_ids, deny_ids)
            .await?;
        Ok(vec![FsmEvent::Signal {
            instance_id,
            event: TaskEvent::ApprovalResolved,
        }])
    }

    fn close_event_stream(&mut self) {
        self.app_rx = None;
    }
    fn cancel_all(&self) {
        self.ctx.cancel();
    }
}

impl Drop for TopicRuntime {
    fn drop(&mut self) {
        self.cancel_all();
    }
}
