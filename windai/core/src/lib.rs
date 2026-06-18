pub mod agent;
pub mod chat;
pub mod db;
mod env;
pub mod error;
pub mod models;
pub mod schema;
pub mod storage;

use self::storage::Storage;
use chat::ChatEngine;
use db::DbPool;
use error::Result;
use std::path::Path;
use wind_mcp::client::registry::{Registry, RegistryHandle};

pub struct WindCore {
    mcp: RegistryHandle,
    storage: Storage,
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
            mcp,
            storage: Storage::new(pool),
        })
    }
    pub fn storage(&self) -> &Storage {
        &self.storage
    }
    pub fn chat(&self) -> ChatEngine<'_> {
        ChatEngine::new(self.mcp.clone(), self.storage())
    }
    pub fn registry(&self) -> RegistryHandle {
        self.mcp.clone()
    }
    /// 关闭所有服务
    /// - 关闭所有 MCP 客户端
    pub async fn shutdown(&self) {
        self.mcp.shutdown().await;
        self.storage.close().await;
    }
}
