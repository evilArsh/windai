use super::{AgentOutput, SupervisorRequest, TaskCommand, TaskNotification, TaskSpec};
use crate::{
    agent::{
        event::TopicMailbox,
        function_call::execute_tool_calls,
        helper::{self},
        host::AgentHost,
        runtime::{AgentRunConfig, AgentRuntime},
        tool::{ListAgentsResponse, SpawnAgentRequest, SpawnAgentResponse},
    },
    chat::loops::ChatContext,
    error::{CoreError, Result},
    models::ToolApprovalRequest,
    storage::Storage,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use wind_ai::{
    message::{Content, Message as AiMessage},
    tool::{FunctionCall, FunctionCallOutput},
};
use wind_mcp::client::registry::RegistryHandle;

struct SyncHost {
    binding_id: i64,
    topic_id: i64,
    topic_tx: TopicMailbox,
    storage: Storage,
    mcp_registry: RegistryHandle,
}
impl SyncHost {
    pub fn new(
        binding_id: i64,
        topic_id: i64,
        topic_tx: TopicMailbox,
        storage: Storage,
        mcp_registry: RegistryHandle,
    ) -> Self {
        Self {
            binding_id,
            topic_id,
            topic_tx,
            storage,
            mcp_registry,
        }
    }

    async fn notify_task(&self, notification: TaskNotification) {
        if let Err(err) = self.topic_tx.notify_task(notification).await {
            log::error!("[SyncHost] {}", err);
        }
    }
}
#[async_trait]
impl AgentHost for SyncHost {
    async fn emit(&self, output: AgentOutput) {
        match output {
            AgentOutput::Started => {
                self.notify_task(TaskNotification::Started {
                    binding_id: self.binding_id,
                })
                .await;
            }
            AgentOutput::MessageDelta {
                message_id,
                index,
                delta,
            } => {
                self.notify_task(TaskNotification::Message {
                    binding_id: self.binding_id,
                    topic_id: self.topic_id,
                    message_id,
                    index,
                    delta,
                })
                .await;
            }
            AgentOutput::Finish { data, error } => match error {
                Some(err) => {
                    self.notify_task(TaskNotification::Failed {
                        binding_id: self.binding_id,
                        data,
                        error: err.to_string(),
                    })
                    .await;
                }
                None => {
                    self.notify_task(TaskNotification::Completed {
                        binding_id: self.binding_id,
                        data,
                    })
                    .await;
                }
            },
            AgentOutput::ApprovalRequired {
                data,
                contexts: _,
                calls,
            } => {
                self.notify_task(TaskNotification::WaitingApproval {
                    binding_id: self.binding_id,
                    data,
                    calls,
                })
                .await;
            }
        }
    }

    async fn list_agents(&self) -> Result<ListAgentsResponse> {
        helper::list_agents(&self.storage, self.topic_id).await
    }

    async fn list_approvals(&self, message_id: i64) -> Result<Vec<ToolApprovalRequest>> {
        helper::list_approval_requests(&self.storage, message_id).await
    }

    async fn spawn_agent(
        &self,
        call_id: String,
        request: SpawnAgentRequest,
    ) -> Result<SpawnAgentResponse> {
        let (tx, rx) = oneshot::channel();
        if let Err(err) = self
            .topic_tx
            .request_supervisor(SupervisorRequest::SpawnAgent {
                binding_id: self.binding_id,
                call_id,
                request,
                reply: tx,
            })
            .await
        {
            log::error!("[spawn_agent] {}", err);
            return Err(CoreError::Internal(err.to_string()));
        };

        rx.await.map_err(|e| CoreError::Internal(e.to_string()))
    }

    async fn execute_tool_calls(&self, calls: &[FunctionCall]) -> Result<AiMessage> {
        let outputs = execute_tool_calls(&self.mcp_registry, calls)
            .await?
            .content
            .into_iter()
            .filter_map(|content| {
                if let Content::FunctionCall { data } = content {
                    return Some(data);
                } else {
                    log::warn!("Unexpected content type: {:?}", content);
                }
                None
            })
            .collect::<Vec<FunctionCallOutput>>();

        Ok(AiMessage::new_tool_result(outputs))
    }
}

#[derive(Clone)]
pub struct SyncTaskHandle {
    pub binding_id: i64,
    cmd_tx: mpsc::Sender<TaskCommand>,
}

impl SyncTaskHandle {
    pub async fn cancel(&self) -> Result<()> {
        if let Err(err) = self.cmd_tx.send(TaskCommand::Cancel).await {
            log::error!(
                "error when cancel task: {:?}. (binding_id = {})",
                err,
                self.binding_id
            );
            return Err(CoreError::Internal(err.to_string()));
        }
        Ok(())
    }
    pub async fn start(&self, task: TaskSpec, config: AgentRunConfig) -> Result<()> {
        if let Err(err) = self.cmd_tx.send(TaskCommand::Start { task, config }).await {
            log::error!(
                "error when start task: {:?}. (binding_id = {})",
                err,
                self.binding_id
            );
            return Err(CoreError::Internal(err.to_string()));
        }
        Ok(())
    }
}

pub struct SyncTask {
    ctx: CancellationToken,
    cmd_rx: mpsc::Receiver<TaskCommand>,
    host: Arc<dyn AgentHost>,
    binding_id: i64,
    topic_tx: TopicMailbox,
}

impl SyncTask {
    pub fn spawn(
        ctx: CancellationToken,
        binding_id: i64,
        topic_id: i64,
        topic_tx: TopicMailbox,
        storage: Storage,
        mcp_registry: RegistryHandle,
    ) -> SyncTaskHandle {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let handle = SyncTaskHandle { binding_id, cmd_tx };

        let task = Self {
            cmd_rx,
            ctx,
            binding_id,
            topic_tx: topic_tx.clone(),
            host: Arc::new(SyncHost::new(
                binding_id,
                topic_id,
                topic_tx,
                storage,
                mcp_registry,
            )),
        };

        tokio::spawn(task.run());

        handle
    }

    fn start_agent(&self, task: TaskSpec, config: AgentRunConfig) {
        let agent = AgentRuntime::new(self.host.clone(), config);
        tokio::spawn(agent.run(
            self.ctx.child_token(),
            ChatContext {
                model: task.model,
                provider: task.provider,
                credential: task.credential,
                req_config: task.req_config,
                rule_set: task.rule_set,
                tools: task.tools,
            },
            task.assistant,
            task.contexts,
            None,
        ));
    }
    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;

                _ = self.ctx.cancelled() => {
                    let _ = self.topic_tx.notify_task(TaskNotification::Cancelled {
                        binding_id: self.binding_id,
                    }).await;
                    return
                },

                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_command(cmd);
                }
                else => {
                    return
                }
            }
        }
    }
    fn handle_command(&mut self, cmd: TaskCommand) {
        match cmd {
            TaskCommand::Cancel => {
                self.ctx.cancel();
            }
            TaskCommand::Start { task, config } => {
                self.start_agent(task, config);
            }
        }
    }
}
