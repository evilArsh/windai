pub mod agent;
pub mod chat;
pub mod db;
mod env;
pub mod error;
pub mod models;
pub mod schema;
pub mod storage;

use crate::agent::topic::{TopicRuntime, TopicRuntimeHandle};

use self::storage::Storage;
use db::DbPool;
use error::Result;
use std::{
    collections::{HashMap, hash_map::Entry},
    path::Path,
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;
use wind_mcp::client::registry::{Registry, RegistryHandle};

pub struct WindCore {
    ctx: CancellationToken,
    mcp: RegistryHandle,
    storage: Storage,
    topic_handler: Mutex<HashMap<i64, TopicRuntimeHandle>>,
}

impl WindCore {
    /// 使用本地文件数据库初始化。
    ///
    /// `path` 为可选的数据库文件路径，传 `None` 则使用默认路径
    /// `~/.windai/windai.db`（可通过 `WINDAI_ROOT_DIR` 环境变量覆盖）。
    pub async fn init_local(path: Option<&str>) -> Result<Self> {
        let db_url = match path {
            Some(p) => {
                let p = Path::new(p);
                if p.as_os_str().is_empty() {
                    return Err(error::CoreError::Validation(
                        "database path is empty".into(),
                    ));
                }
                if let Some(parent) = p.parent() {
                    if !parent.as_os_str().is_empty() && !parent.exists() {
                        return Err(error::CoreError::Validation(format!(
                            "directory does not exist: {}",
                            parent.display()
                        )));
                    }
                }
                p.to_string_lossy().to_string()
            }
            None => env::db_path().to_string_lossy().to_string(),
        };
        Self::init(&db_url).await
    }

    /// 使用内存数据库初始化
    pub async fn init_memory() -> Result<Self> {
        Self::init("sqlite::memory:").await
    }

    async fn init(db_url: &str) -> Result<Self> {
        let db = db::init_db(db_url)
            .await
            .map_err(|e| error::CoreError::Database(e))?;
        Self::init_with_pool(db).await
    }
    /// 使用外部构建的连接池初始化，供测试使用。
    pub async fn init_with_pool(pool: DbPool) -> Result<Self> {
        Self::init_with_pool_and_registry(pool, Registry::new()).await
    }

    /// 使用外部构建的连接池和 MCP registry 初始化，供测试复用 MCP 服务。
    pub async fn init_with_pool_and_registry(pool: DbPool, mcp: RegistryHandle) -> Result<Self> {
        schema::init_schema(&pool).await?;
        storage::init_id_generator(0);
        Ok(Self {
            ctx: CancellationToken::new(),
            mcp,
            storage: Storage::new(pool),
            topic_handler: Mutex::new(HashMap::new()),
        })
    }
    pub fn storage(&self) -> &Storage {
        &self.storage
    }
    pub fn registry(&self) -> &RegistryHandle {
        &self.mcp
    }

    /// 获取一个 topic 的运行时句柄
    ///
    /// # Panic
    /// 如果 core 已经被关闭，将会 panic
    pub fn fetch_topic(&self, topic_id: i64) -> TopicRuntimeHandle {
        if self.ctx.is_cancelled() {
            panic!("core is shutdown")
        }

        let mut map = self.topic_handler.lock().unwrap();
        match map.entry(topic_id) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let handler = TopicRuntime::spawn(
                    self.ctx.child_token(),
                    topic_id,
                    self.mcp.clone(),
                    self.storage.clone(),
                );
                let cloned = handler.clone();
                entry.insert(handler);
                cloned
            }
        }
    }
    /// 关闭所有服务
    /// - 关闭所有 MCP 客户端
    pub async fn shutdown(&self) {
        self.ctx.cancel();
        self.mcp.shutdown().await;
        self.storage.close().await;
    }
}
