pub mod agent;
pub mod approval;
mod executor;
pub mod mcp;
pub mod message;
pub mod model;
pub mod prompt;
pub mod provider;
pub mod topic;
pub mod utils;

use self::{
    agent::AgentStorage, approval::ToolApprovalStorage, mcp::McpStorage, message::MessageStorage,
    model::ModelStorage, prompt::PromptStorage, provider::ProviderStorage, topic::TopicStorage,
};
use super::db::DbPool;
use crate::error::Result;
use crate::storage::executor::StorageExecutor;
use ferroid::{
    generator::AtomicSnowflakeGenerator,
    id::SnowflakeTwitterId,
    time::{MonotonicClock, TWITTER_EPOCH},
};
use std::{future::Future, sync::OnceLock};

#[derive(Clone)]
pub struct Storage {
    executor: StorageExecutor,
    provider: provider::ProviderStorage,
    topic: topic::TopicStorage,
    model: model::ModelStorage,
    message: message::MessageStorage,
    mcp: mcp::McpStorage,
    agent: agent::AgentStorage,
    prompt: prompt::PromptStorage,
    approval: approval::ToolApprovalStorage,
}

pub(crate) struct TableName;

impl TableName {
    pub const TOPICS: &'static str = "topics";
    pub const CHAT_CONFIGS: &'static str = "chat_configs";
    pub const PROVIDERS: &'static str = "providers";
    pub const CREDENTIALS: &'static str = "credentials";
    pub const JSONRULE: &'static str = "json_rule";
    pub const MODELS: &'static str = "models";
    pub const MESSAGES: &'static str = "messages";
    pub const MCP_SERVERS: &'static str = "mcp_servers";
    pub const TOOL_APPROVAL_REQUESTS: &'static str = "tool_approval_requests";
    pub const PROMPT_MODULES: &'static str = "prompt_modules";
    pub const AGENT_DEFINITION: &'static str = "agent_definitions";
    pub const TOPIC_AGENT_BINDINGS: &'static str = "topic_agent_bindings";
}

impl Storage {
    pub fn new(db: DbPool) -> Self {
        Self::from_executor(StorageExecutor::new(db))
    }

    fn from_executor(executor: StorageExecutor) -> Self {
        Self {
            provider: ProviderStorage::new(executor.clone()),
            topic: TopicStorage::new(executor.clone()),
            model: ModelStorage::new(executor.clone()),
            message: MessageStorage::new(executor.clone()),
            mcp: McpStorage::new(executor.clone()),
            agent: AgentStorage::new(executor.clone()),
            prompt: PromptStorage::new(executor.clone()),
            approval: ToolApprovalStorage::new(executor.clone()),
            executor,
        }
    }

    pub async fn with_tx<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Storage) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        self.executor
            .with_tx(|executor| f(Self::from_executor(executor)))
            .await
    }

    pub async fn begin(&self) -> Result<StorageTx> {
        Ok(StorageTx {
            storage: Self::from_executor(
                StorageExecutor::new_transaction(self.executor.pool().clone()).await?,
            ),
        })
    }
    pub fn provider(&self) -> &ProviderStorage {
        &self.provider
    }
    pub fn topic(&self) -> &TopicStorage {
        &self.topic
    }
    pub fn model(&self) -> &ModelStorage {
        &self.model
    }
    pub fn message(&self) -> &MessageStorage {
        &self.message
    }

    pub fn mcp(&self) -> &McpStorage {
        &self.mcp
    }

    pub fn agent(&self) -> &AgentStorage {
        &self.agent
    }

    pub fn prompt(&self) -> &PromptStorage {
        &self.prompt
    }

    pub fn approval(&self) -> &ToolApprovalStorage {
        &self.approval
    }

    pub async fn close(&self) {
        let pool = self.executor.pool();
        if !pool.is_closed() {
            pool.close().await;
        }
    }
}

pub struct StorageTx {
    storage: Storage,
}

impl StorageTx {
    pub fn storage(&self) -> Storage {
        self.storage.clone()
    }

    pub async fn commit(self) -> Result<()> {
        self.storage.executor.commit().await
    }

    pub async fn rollback(self) -> Result<()> {
        self.storage.executor.rollback().await
    }
}

type SnowflakeGen = AtomicSnowflakeGenerator<SnowflakeTwitterId, MonotonicClock<1>>;
static ID_GENERATOR: OnceLock<SnowflakeGen> = OnceLock::new();

/// 初始化 ID 生成器
pub fn init_id_generator(machine_id: u16) {
    let clock = MonotonicClock::<1>::with_epoch(TWITTER_EPOCH);
    let generator = AtomicSnowflakeGenerator::new(machine_id as u64, clock);
    let _ = ID_GENERATOR.set(generator).map_err(|_| {
        log::warn!("ID generator has already been initialized");
    });
}
