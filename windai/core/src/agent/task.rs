use self::sync::SyncTaskHandler;
use super::runtime::AgentRunConfig;
use super::tool::{SpawnAgentRequest, SpawnAgentResponse};
use crate::models::{
    AgentDefinition, AgentMode, AgentRole, AgentStatus, Credentials, JsonRule, Message, Model,
    Provider,
};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use tokio::sync::oneshot;
use wind_ai::message::Message as AiMessage;
use wind_ai::message::ReqConfig;
use wind_ai::tool::{FunctionCall, Tools};

pub mod background;
pub mod fork;
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

#[derive(Debug, Clone)]
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

pub struct PendingChild {
    pub parent_binding_id: i64,
    pub binding_id: i64,
    pub call_id: String,
    pub mode: AgentMode,
    pub reply: oneshot::Sender<SpawnAgentResponse>,
}

pub struct TaskEntry {
    binding_id: i64,
    status: AgentStatus,
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
            status: AgentStatus::Created,
            mode: None,
            handler,
        }
    }

    /// 当前任务是否忙
    pub fn is_busy(&self) -> bool {
        matches!(
            self.status,
            |AgentStatus::Running| AgentStatus::WaitingApproval | AgentStatus::WaitingChild
        )
    }

    pub fn get_status(&self) -> AgentStatus {
        self.status
    }
    pub fn set_status(&mut self, status: AgentStatus) {
        self.status = status;
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
                entry.status = data.status;
                entry.topic_id = data.topic_id;

                entry
            }
            Entry::Vacant(entry) => entry.insert(data),
        }
    }

    pub fn get_entry_mut(&mut self, binding_id: i64) -> Option<&mut TaskEntry> {
        self.binding_map.get_mut(&binding_id)
    }

    pub fn get_entry(&self, binding_id: i64) -> Option<&TaskEntry> {
        self.binding_map.get(&binding_id)
    }

    pub fn get_entries(&self) -> impl Iterator<Item = &TaskEntry> {
        self.binding_map.values()
    }

    pub fn main_entry(&self) -> Option<&TaskEntry> {
        self.main_binding_id.and_then(|id| self.get_entry(id))
    }
}
