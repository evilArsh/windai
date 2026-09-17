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
    agent::AgentStorage, approval::ToolApprovalStorage, executor::StorageExecutor, mcp::McpStorage,
    message::MessageStorage, model::ModelStorage, prompt::PromptStorage, provider::ProviderStorage,
    topic::TopicStorage,
};
use super::db::DbPool;
use crate::error::Result;
use std::future::Future;

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
    pub const PROVIDERS: &'static str = "providers";
    pub const CREDENTIALS: &'static str = "credentials";
    pub const JSONRULE: &'static str = "json_rule";
    pub const MODELS: &'static str = "models";
    pub const MESSAGES: &'static str = "messages";
    pub const MCP_SERVERS: &'static str = "mcp_servers";
    pub const TOOL_APPROVAL_REQUESTS: &'static str = "tool_approval_requests";
    pub const PROMPT_MODULES: &'static str = "prompt_modules";
    pub const AGENT_DEFINITION: &'static str = "agent_definitions";
    pub const AGENT_INSTANCES: &'static str = "agent_instances";
    pub const TOPIC_AGENT_MAPS: &'static str = "topic_agent_maps";
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
