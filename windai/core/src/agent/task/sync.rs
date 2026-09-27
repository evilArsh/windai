use super::{SupervisorRequest, TaskCommand, TaskNotification, TaskSpec};
use crate::{
    agent::{
        event::TopicMailbox,
        function_call::execute_tool_calls,
        helper::{self},
        host::AgentHost,
        runtime::AgentRuntime,
        tool::{ListAgentsResponse, SpawnAgentRequest, SpawnAgentResponse},
    },
    error::{CoreError, Result},
    models::ToolApprovalRequest,
    storage::Storage,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use wind_ai::tool::{FunctionCall, FunctionCallOutput};
use wind_mcp::client::registry::RegistryHandle;

struct SyncHost {
    instance_id: i64,
    topic_id: i64,
    topic_tx: TopicMailbox,
    storage: Storage,
    mcp_registry: RegistryHandle,
}
impl SyncHost {
    pub fn new(
        instance_id: i64,
        topic_id: i64,
        topic_tx: TopicMailbox,
        storage: Storage,
        mcp_registry: RegistryHandle,
    ) -> Self {
        Self {
            instance_id,
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
    async fn emit(&self, output: TaskNotification) {
        self.notify_task(output).await;
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
                instance_id: self.instance_id,
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

    async fn execute_tool_calls(&self, calls: &[FunctionCall]) -> Result<Vec<FunctionCallOutput>> {
        Ok(execute_tool_calls(&self.mcp_registry, calls).await?)
    }
}

#[derive(Clone)]
pub struct SyncTaskHandler {
    instance_id: i64,
    cmd_tx: mpsc::Sender<TaskCommand>,
}

impl SyncTaskHandler {
    pub async fn cancel(&self) -> Result<()> {
        if let Err(err) = self.cmd_tx.send(TaskCommand::Cancel).await {
            let err = err.to_string();
            log::error!(
                "error when cancel task: {}. (instance_id = {})",
                err,
                self.instance_id
            );
            return Err(CoreError::Internal(err));
        }
        Ok(())
    }
    pub async fn start(&self, task: TaskSpec) -> Result<()> {
        if let Err(err) = self.cmd_tx.send(TaskCommand::Start { task }).await {
            let err = err.to_string();
            log::error!(
                "error when start task: {}. (instance_id = {})",
                err,
                self.instance_id
            );
            return Err(CoreError::Internal(err));
        }
        Ok(())
    }
}

pub struct SyncTask {
    ctx: CancellationToken,
    cmd_rx: mpsc::Receiver<TaskCommand>,
    host: Arc<dyn AgentHost>,
    instance_id: i64,
    topic_tx: TopicMailbox,
}

impl SyncTask {
    pub fn spawn(
        ctx: CancellationToken,
        instance_id: i64,
        topic_id: i64,
        topic_tx: TopicMailbox,
        storage: Storage,
        mcp_registry: RegistryHandle,
    ) -> SyncTaskHandler {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let handle = SyncTaskHandler {
            instance_id,
            cmd_tx,
        };

        let task = Self {
            cmd_rx,
            ctx,
            instance_id,
            topic_tx: topic_tx.clone(),
            host: Arc::new(SyncHost::new(
                instance_id,
                topic_id,
                topic_tx,
                storage,
                mcp_registry,
            )),
        };

        tokio::spawn(task.run());

        handle
    }

    fn start_agent(&self, task: TaskSpec) {
        let agent = AgentRuntime::new(self.ctx.child_token(), self.host.clone(), task);
        tokio::spawn(agent.run());
    }
    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;

                _ = self.ctx.cancelled() => {
                    let _ = self.topic_tx.notify_task(TaskNotification::Cancelled {
                        instance_id: self.instance_id,
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
            TaskCommand::Start { task } => {
                self.start_agent(task);
            }
        }
    }
}
