use self::sync::{SyncTask, SyncTaskHandler};
use super::event::TopicMailbox;
use super::tool::{SpawnAgentRequest, SpawnAgentResponse};
use crate::agent::helper;
use crate::chat::runner::ChatContext;
use crate::error::{CoreError, Result};
use crate::models::{
    AgentDefinition, AgentInstance, AgentMode, AgentRole, AgentStatus, ApprovalRecord, Message,
    ToolApprovalRequest, ToolApprovalStatus, UpdateInstance,
};
use crate::storage::Storage;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::PathBuf;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use wind_ai::message::{Content, Message as AiMessage, Role};
use wind_ai::tool::FunctionCall;
use wind_mcp::client::registry::RegistryHandle;

pub mod background;
pub mod sync;

#[derive(Debug)]
pub enum AgentOutput {
    Started,
    /// 流式分片消息
    Message {
        message_id: i64,
        index: i32,
        delta: AiMessage,
    },
    /// Agent 运行完成
    ///
    /// 如果运行失败，error 字段会保存错误信息；
    /// 错误信息同时会保存至 data 的消息上下文中
    Finish {
        data: Message,
        error: Option<String>,
    },
    /// 该轮对话的部分调用需要审批
    ApprovalRequired {
        data: Message,
        contexts: Vec<AiMessage>,
        calls: Vec<FunctionCall>,
    },
}

pub enum SupervisorRequest {
    SpawnAgent {
        instance_id: i64,
        call_id: String,
        request: SpawnAgentRequest,
        reply: oneshot::Sender<SpawnAgentResponse>,
    },
}

/// Task 任务命令
pub enum TaskCommand {
    Cancel,
    Start { task: TaskSpec },
}

#[derive(Debug, strum::AsRefStr)]
/// 任务消息通知
pub enum TaskNotification {
    Started {
        instance_id: i64,
    },
    Message {
        instance_id: i64,
        message_id: i64,
        index: i32,
        delta: AiMessage,
    },
    ApprovalRequired {
        instance_id: i64,
        data: Message,
        calls: Vec<FunctionCall>,
    },
    Finish {
        instance_id: i64,
        data: Message,
    },
    Failed {
        instance_id: i64,
        data: Message,
        error: String,
    },
    Cancelled {
        instance_id: i64,
    },
}

impl std::fmt::Display for TaskNotification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.as_ref();
        let instance_id = match self {
            TaskNotification::Started { instance_id } => instance_id,
            TaskNotification::Message { instance_id, .. } => instance_id,
            TaskNotification::ApprovalRequired { instance_id, .. } => instance_id,
            TaskNotification::Finish { instance_id, .. } => instance_id,
            TaskNotification::Failed { instance_id, .. } => instance_id,
            TaskNotification::Cancelled { instance_id } => instance_id,
        };
        write!(f, "[TaskNotification {name}] (instance_id = {instance_id})")
    }
}

impl std::fmt::Display for SupervisorRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let instance_id = match self {
            SupervisorRequest::SpawnAgent { instance_id, .. } => instance_id,
        };
        write!(f, "[SupervisorRequest] (instance_id = {instance_id})")
    }
}

#[derive(Debug, Clone)]
pub struct TaskSpec {
    pub chat_context: ChatContext,
    pub instance: AgentInstance,
    pub agent: Option<AgentDefinition>,
    /// 用户任务
    pub user: Message,
    pub assistant: Message,
    pub contexts: Vec<AiMessage>,
}

/// 待完成的子任务记录
///
/// 当父实例 spawn 出一个子实例后，会登记一条该记录，用于在子实例
/// 完成任务时，通过 `reply` 通道把结果回传给等待中的父实例
/// 父实例（`parent_instance_id`）在此等待子实例（`instance_id`）完成
pub struct PendingChild {
    pub parent_instance_id: i64,
    pub instance_id: i64,
    /// MCP call id
    pub call_id: String,
    pub mode: AgentMode,
    pub reply: oneshot::Sender<SpawnAgentResponse>,
}

/// 任务的运行时元数据旁表
pub struct TaskEntry {
    instance_id: i64,
    pub role: AgentRole,
    // TODO: 通用抽象句柄
    pub handler: SyncTaskHandler,
}

impl TaskEntry {
    pub fn new(instance_id: i64, role: AgentRole, handler: SyncTaskHandler) -> Self {
        TaskEntry {
            instance_id,
            role,
            handler,
        }
    }
}

pub struct TaskManager {
    ctx: CancellationToken,
    /// 任务工作目录
    cwd: PathBuf,
    storage: Storage,
    mcp_registry: RegistryHandle,
    /// instance_id -> TaskEntry
    instance_map: HashMap<i64, TaskEntry>,
    pending: Vec<PendingChild>,
}

impl TaskManager {
    pub fn new(
        ctx: CancellationToken,
        storage: Storage,
        mcp_registry: RegistryHandle,
        cwd: PathBuf,
    ) -> Self {
        return Self {
            ctx,
            storage,
            mcp_registry,
            cwd,

            instance_map: Default::default(),
            pending: vec![],
        };
    }

    pub fn get_output(src: &Message) -> Vec<Content> {
        src.content
            .last()
            .and_then(|c| {
                if c.is_simple() && c.role == Role::Assistant {
                    Some(c.content.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| vec![Content::new_text("Task has no valid result".to_string())])
    }

    /// 登记子任务记录
    pub fn insert_pending(&mut self, child: PendingChild) {
        if !self
            .pending
            .iter()
            .any(|x| x.instance_id == child.instance_id)
        {
            self.pending.push(child);
        } else {
            log::warn!(
                "Duplicate pending child ignored: parent={}, child={}, call_id={}",
                child.parent_instance_id,
                child.instance_id,
                child.call_id
            );
        }
    }

    /// 取出并移除一条子任务记录
    pub fn take_pending(&mut self, instance_id: i64) -> Option<PendingChild> {
        self.pending
            .iter()
            .position(|p| p.instance_id == instance_id)
            .map(|i| self.pending.remove(i))
    }

    /// 该任务是否仍有未完成的 pending 子任务
    pub fn has_pending_for(&self, instance_id: i64) -> bool {
        self.pending
            .iter()
            .any(|p| p.parent_instance_id == instance_id)
    }

    fn upsert_entry(&mut self, data: TaskEntry) -> &mut TaskEntry {
        match self.instance_map.entry(data.instance_id) {
            Entry::Occupied(entry) => {
                log::debug!("Task already exists, instance_id: {}", data.instance_id);
                let entry = entry.into_mut();
                entry.handler = data.handler;
                entry.role = data.role;

                entry
            }
            Entry::Vacant(entry) => entry.insert(data),
        }
    }

    pub fn get_entry(&self, instance_id: i64) -> Option<&TaskEntry> {
        self.instance_map.get(&instance_id)
    }

    /// 初始化任务
    pub async fn init(&self, topic_id: i64, user_input: Vec<Content>) -> Result<TaskSpec> {
        let tx = self.storage.begin().await?;
        let mut instance = helper::get_or_create_main_instance(&tx.storage(), topic_id).await?;
        instance.mode = Some(AgentMode::Sync);

        let agent = match instance.agent_id {
            Some(agent_id) => Some(helper::get_def_by_id(&tx.storage(), agent_id).await?),
            None => None,
        };

        let tools =
            helper::build_agent_tools(&tx.storage(), &self.mcp_registry, &instance, agent.as_ref())
                .await?;

        let mut chat_ctx = helper::get_base_info(&tx.storage(), topic_id).await?;
        chat_ctx.tools = tools;
        log::debug!("Task init, chat_ctx: {:#?}", chat_ctx);

        let (user, assistant, contexts) = helper::create_contexts(
            &tx.storage(),
            &self.cwd,
            &chat_ctx,
            instance.id,
            user_input,
            agent.as_ref(),
        )
        .await?;

        // 创建主 Agent 工作空间
        std::fs::create_dir_all(&self.cwd)?;
        tx.commit().await?;

        let spec = TaskSpec {
            chat_context: chat_ctx,
            instance,
            agent,
            user,
            assistant,
            contexts,
        };
        Ok(spec)
    }

    /// 恢复任务执行
    pub async fn resume(&self, instance_id: i64) -> Result<Option<TaskSpec>> {
        let mut instance = helper::get_instance_by_id(&self.storage, instance_id).await?;
        instance.mode.get_or_insert(AgentMode::Sync);

        let agent = match instance.agent_id {
            Some(agent_id) => Some(helper::get_def_by_id(&self.storage, agent_id).await?),
            None => None,
        };

        let mut chat_ctx = helper::get_base_info(&self.storage, instance.topic_id).await?;
        let tools =
            helper::build_agent_tools(&self.storage, &self.mcp_registry, &instance, agent.as_ref())
                .await?;
        chat_ctx.tools = tools;

        let contexts = helper::get_message_contexts(&self.storage, instance.id).await?;

        let mut it = contexts.iter().rev();
        let assistant = it.next().cloned().ok_or_else(|| {
            CoreError::Validation(format!("Assistant message not found: {}", instance.id))
        })?;
        let user = it.next().cloned().ok_or_else(|| {
            CoreError::Validation(format!("User message not found: {}", instance.id))
        })?;

        if !matches!(&assistant.from_id, Some(id) if id == &user.id) {
            return Err(CoreError::Validation(format!(
                "Mismatched user-assistant pair: instance={}, user_id={}, assistant_from_id={:?}",
                instance.id, user.id, assistant.from_id
            )));
        }

        let spec = TaskSpec {
            chat_context: chat_ctx,
            agent,
            assistant,
            contexts: helper::transfer_contexts(contexts)?,
            instance,
            user,
        };
        if let Some(entry) = self.get_entry(instance_id) {
            entry.handler.start(spec).await?;
            Ok(None)
        } else {
            Ok(Some(spec))
        }
    }

    pub async fn cancel(&self, instance_id: i64) -> Result<()> {
        if let Some(entry) = self.get_entry(instance_id) {
            entry.handler.cancel().await
        } else {
            Err(CoreError::Internal(format!(
                "Task not found, instance_id: {}",
                instance_id
            )))
        }
    }

    /// 启动子任务
    pub async fn spawn_child(
        &mut self,
        topic_id: i64,
        parent_instance_id: i64,
        request: SpawnAgentRequest,
    ) -> Result<TaskSpec> {
        if request.mode == AgentMode::Background {
            // TODO: 后台任务
            return Err(CoreError::Validation(
                "background mode is not supported yet".into(),
            ));
        }

        let tx = self.storage.begin().await?;

        let agent = helper::get_def_by_key(&tx.storage(), &request.agent_key).await?;

        let instance = helper::create_child_instance(
            &tx.storage(),
            topic_id,
            parent_instance_id,
            agent.id,
            request.mode,
        )
        .await?;
        let instance_id = instance.id;

        let mut chat_ctx = helper::get_base_info(&tx.storage(), topic_id).await?;
        let tools =
            helper::build_agent_tools(&tx.storage(), &self.mcp_registry, &instance, Some(&agent))
                .await?;
        chat_ctx.tools = tools;

        log::debug!(
            "Spawn new task, parent_instance_id = {}, new_instance_id = {}",
            parent_instance_id,
            instance_id
        );

        let user_input = vec![Content::new_text(request.task)];
        let (user, assistant, contexts) = match request.mode {
            AgentMode::Fork => match self.get_entry(parent_instance_id) {
                Some(entry) => {
                    helper::create_fork_contexts(
                        &self.cwd,
                        &tx.storage(),
                        entry.instance_id,
                        instance_id,
                        user_input,
                        &chat_ctx,
                        Some(&agent),
                    )
                    .await?
                }
                None => {
                    return Err(CoreError::Validation(format!(
                        "Parent task not found, cannot spawn"
                    )));
                }
            },
            AgentMode::Sync | AgentMode::Background => {
                helper::create_contexts(
                    &tx.storage(),
                    &self.cwd,
                    &chat_ctx,
                    instance_id,
                    user_input,
                    Some(&agent),
                )
                .await?
            }
        };
        tx.commit().await?;

        let spec = TaskSpec {
            chat_context: chat_ctx,
            assistant: assistant.clone(),
            contexts,
            instance,
            agent: Some(agent),
            user,
        };

        Ok(spec)
    }

    /// 启动一个 SyncTask
    ///
    /// TODO: 启动 background 任务
    pub async fn start(&mut self, spec: TaskSpec, topic_mailbox: TopicMailbox) -> Result<()> {
        let sync_handle = SyncTask::spawn(
            self.ctx.child_token(),
            spec.instance.id,
            spec.instance.topic_id,
            topic_mailbox,
            self.storage.clone(),
            self.mcp_registry.clone(),
        );

        let entry = self.upsert_entry(TaskEntry::new(
            spec.instance.id,
            spec.instance.role,
            sync_handle,
        ));
        entry.handler.start(spec).await
    }

    /// 持久化任务状态
    pub async fn persist_status(
        &mut self,
        instance_id: i64,
        status: AgentStatus,
        mode: AgentMode,
        agent_id: Option<i64>,
    ) -> Result<()> {
        self.storage
            .agent()
            .update_instance(
                instance_id,
                UpdateInstance {
                    agent_id: agent_id,
                    status: Some(status),
                    mode: Some(mode),
                },
            )
            .await
    }

    /// 持久化任务执行结果
    pub async fn persist_message(&mut self, data: Message) -> Result<()> {
        self.storage.message().update(data.id, data.into()).await
    }

    pub async fn persist_approval_state(
        &mut self,
        topic_id: i64,
        instance_id: i64,
        assistant: Message,
        calls: Vec<FunctionCall>,
    ) -> Result<Vec<ToolApprovalRequest>> {
        helper::save_approval_state(&self.storage, topic_id, instance_id, assistant, calls).await
    }

    pub async fn persist_approval_record(
        &mut self,
        allow_ids: Vec<i64>,
        deny_ids: Vec<i64>,
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
        self.storage.approval().batch_set_status(records).await
    }

    pub fn clear(&mut self) {
        self.instance_map.clear();
        self.pending.clear();
    }
}
