use self::executor::StorageExecutor;
use super::db::{Db, DbPool};
use super::error::Result;
use std::future::Future;

pub mod agent;
pub mod config;
pub mod conversation;
pub(crate) mod executor;
pub mod log;
pub mod module;
pub mod pending;
pub mod rule;
pub mod spawn;
pub mod statement_count;
pub mod utils;
pub mod wait;

pub(crate) struct TableName;

impl TableName {
    pub const TURN: &'static str = "turn";
    pub const MESSAGE: &'static str = "message";
    pub const BLOCK: &'static str = "block";
    pub const AGENT_INSTANCES: &'static str = "agent_instances";
    pub const AGENT_PENDING: &'static str = "agent_pending";
    pub const SPAWN_CALLS: &'static str = "spawn_calls";
    pub const EXTERNAL_WAITS: &'static str = "external_waits";
    pub const TOPICS: &'static str = "topics";
    pub const PROVIDERS: &'static str = "providers";
    pub const CREDENTIALS: &'static str = "credentials";
    pub const MODELS: &'static str = "models";
    pub const JSON_RULE: &'static str = "json_rule";
    #[allow(dead_code)]
    pub const MCP_SERVERS: &'static str = "mcp_servers";
    pub const MODS: &'static str = "mods";
    pub const MOD_VERSIONS: &'static str = "mod_versions";
    pub const TOPIC_MODS: &'static str = "topic_mods";
    pub const MOD_STATE: &'static str = "mod_state";
    pub const EVENT_LOG: &'static str = "event_log";
}

#[derive(Clone)]
pub struct Storage {
    executor: StorageExecutor,
}

impl Storage {
    /// 用数据库句柄建立存储
    pub fn new(db: Db) -> Self {
        Self::from_executor(StorageExecutor::new(db.pool().clone()))
    }

    fn from_executor(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    /// 在单个事务中执行闭包，闭包收到事务内的存储
    pub async fn with_tx<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Storage) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        self.executor
            .with_tx(|executor| f(Self::from_executor(executor)))
            .await
    }

    /// 开启事务并返回事务句柄
    pub async fn begin(&self) -> Result<StorageTx> {
        Ok(StorageTx {
            storage: Self::from_executor(
                StorageExecutor::new_transaction(
                    self.executor.pool().clone(),
                    self.executor.statements(),
                )
                .await?,
            ),
        })
    }

    /// 取出底层连接池
    pub fn pool(&self) -> &DbPool {
        self.executor.pool()
    }

    /// 累计执行的 SQL 语句数
    pub fn statement_count(&self) -> u64 {
        self.executor.statement_count()
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
