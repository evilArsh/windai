use self::sync::SyncTaskHandler;
use super::runtime::AgentRunConfig;
use super::tool::{SpawnAgentRequest, SpawnAgentResponse};
use crate::models::{
    AgentDefinition, AgentMode, AgentRole, Credentials, JsonRule, Message, Model, Provider,
};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use tokio::sync::oneshot;
use wind_ai::message::Message as AiMessage;
use wind_ai::message::ReqConfig;
use wind_ai::tool::{FunctionCall, Tools};

pub mod background;
pub mod sync;

#[derive(Debug)]
pub enum AgentOutput {
    Started,
    /// 流式分片消息
    MessageDelta {
        message_id: i64,
        index: i32,
        delta: AiMessage,
    },
    /// Agent 运行完成
    ///
    /// 如果运行失败，error字段会保存错误信息；
    /// 错误信息同时会保存至data的消息上下文中
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
        binding_id: i64,
        call_id: String,
        request: SpawnAgentRequest,
        reply: oneshot::Sender<SpawnAgentResponse>,
    },
}

/// Task 任务命令
pub enum TaskCommand {
    Cancel,
    Start {
        task: TaskSpec,
        config: AgentRunConfig,
    },
}

#[derive(Debug)]
/// 任务消息通知
pub enum TaskNotification {
    Started {
        binding_id: i64,
    },
    Message {
        binding_id: i64,
        topic_id: i64,
        message_id: i64,
        index: i32,
        delta: AiMessage,
    },
    WaitingApproval {
        binding_id: i64,
        data: Message,
        calls: Vec<FunctionCall>,
    },
    Completed {
        binding_id: i64,
        data: Message,
    },
    Failed {
        binding_id: i64,
        data: Message,
        error: String,
    },
    Cancelled {
        binding_id: i64,
    },
}

impl std::fmt::Display for TaskNotification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (name, binding_id) = match self {
            TaskNotification::Started { binding_id } => ("Started", binding_id),
            TaskNotification::Message { binding_id, .. } => ("Message", binding_id),
            TaskNotification::WaitingApproval { binding_id, .. } => ("WaitingApproval", binding_id),
            TaskNotification::Completed { binding_id, .. } => ("Completed", binding_id),
            TaskNotification::Failed { binding_id, .. } => ("Failed", binding_id),
            TaskNotification::Cancelled { binding_id } => ("Cancelled", binding_id),
        };
        write!(f, "[TaskNotification {name}] (binding_id = {binding_id})")
    }
}

impl std::fmt::Display for SupervisorRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let binding_id = match self {
            SupervisorRequest::SpawnAgent { binding_id, .. } => binding_id,
        };
        write!(f, "[SupervisorRequest] (binding_id = {binding_id})")
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TaskSpec {
    pub binding_id: i64,
    /// Agent 能力定义
    pub agent: AgentDefinition,
    /// 模型
    pub model: Model,
    /// 提供商
    pub provider: Provider,
    /// 请求凭证
    pub credential: Credentials,
    /// 请求配置
    pub req_config: ReqConfig,
    pub rule_set: Option<JsonRule>,
    pub tools: Option<Vec<Tools>>,
    pub assistant: Message,
    pub contexts: Vec<AiMessage>,
}

#[allow(dead_code)]
pub struct PendingChild {
    pub parent_binding_id: i64,
    pub binding_id: i64,
    pub call_id: String,
    pub mode: AgentMode,
    pub reply: oneshot::Sender<SpawnAgentResponse>,
}

/// 任务的运行时元数据旁表。
pub struct TaskEntry {
    binding_id: i64,
    pub topic_id: i64,
    pub role: AgentRole,
    pub mode: Option<AgentMode>,
    // TODO: 通用抽象句柄
    pub handler: SyncTaskHandler,
}

impl TaskEntry {
    pub fn new(binding_id: i64, topic_id: i64, role: AgentRole, handler: SyncTaskHandler) -> Self {
        TaskEntry {
            binding_id,
            topic_id,
            role,
            mode: None,
            handler,
        }
    }
}

pub struct TaskRegistry {
    /// binding_id -> TaskEntry
    binding_map: HashMap<i64, TaskEntry>,
    pending: Vec<PendingChild>,
    main_binding_id: Option<i64>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        TaskRegistry {
            binding_map: Default::default(),
            pending: vec![],
            main_binding_id: None,
        }
    }
    pub fn insert_pending(&mut self, child: PendingChild) {
        if !self
            .pending
            .iter()
            .any(|x| x.binding_id == child.binding_id)
        {
            self.pending.push(child);
        }
    }

    /// 取出并移除等待该子任务的父任务
    pub fn take_pending(&mut self, child_binding_id: i64) -> Option<PendingChild> {
        self.pending
            .iter()
            .position(|p| p.binding_id == child_binding_id)
            .map(|i| self.pending.remove(i))
    }

    pub fn upsert(&mut self, data: TaskEntry) -> &mut TaskEntry {
        if data.role == AgentRole::Main {
            self.main_binding_id = Some(data.binding_id);
        }

        match self.binding_map.entry(data.binding_id) {
            Entry::Occupied(entry) => {
                log::debug!(
                    "[upsert task]: task already exists, binding_id: {}",
                    data.binding_id
                );
                let entry = entry.into_mut();
                entry.mode = data.mode;
                entry.handler = data.handler;
                entry.role = data.role;
                entry.topic_id = data.topic_id;

                entry
            }
            Entry::Vacant(entry) => entry.insert(data),
        }
    }

    pub fn get_entry(&self, binding_id: i64) -> Option<&TaskEntry> {
        self.binding_map.get(&binding_id)
    }

    pub fn main_entry(&self) -> Option<&TaskEntry> {
        self.main_binding_id.and_then(|id| self.get_entry(id))
    }

    /// 该父任务是否仍有未完成的 pending 子任务。
    pub fn has_pending_for(&self, parent_binding_id: i64) -> bool {
        self.pending
            .iter()
            .any(|p| p.parent_binding_id == parent_binding_id)
    }

    /// 并发批量取消所有运行中的任务并清空注册表
    pub async fn close(&mut self) {
        let cancels = self
            .binding_map
            .values_mut()
            .map(|entry| async move {
                if let Err(e) = entry.handler.cancel().await {
                    log::error!("shutdown cancel error: {}", e);
                }
            })
            .collect::<Vec<_>>();
        futures::future::join_all(cancels).await;
        // self.binding_map.clear();
        self.pending.clear();
        self.main_binding_id = None;
    }
}
