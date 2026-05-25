pub mod chat;
pub mod db;
mod env;
pub mod error;
pub mod models;
pub mod schema;
pub mod storage;

use chat::ChatEngine;
use error::Result;
use sqlx::{Pool, Sqlite};
use std::path::Path;
use storage::message::service::MessageService;
use storage::model::service::ModelService;
use storage::provider::service::ProviderService;
use storage::topic::service::TopicService;
use wind_mcp::client::registry::{Registry, RegistryHandle};

/// Wind Core 模块的主入口点。
///
/// 持有私有数据库连接池、共享的 JS 引擎以及全局 MCP 客户端。
pub struct WindCore {
    db: Pool<Sqlite>,
    mcp: RegistryHandle,
    provider_svc: ProviderService,
    topic_svc: TopicService,
    model_svc: ModelService,
    message_svc: MessageService,
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
        let db = db::connect(db_url)
            .await
            .map_err(|e| error::CoreError::Database(e))?;

        schema::init_schema(&db).await?;

        let mcp = Registry::new();

        Ok(Self {
            db: db.clone(),
            mcp,
            provider_svc: ProviderService::new(db.clone()),
            topic_svc: TopicService::new(db.clone()),
            model_svc: ModelService::new(db.clone()),
            message_svc: MessageService::new(db.clone()),
        })
    }
    pub fn provider(&self) -> &ProviderService {
        &self.provider_svc
    }
    pub fn model(&self) -> &ModelService {
        &self.model_svc
    }
    pub fn topic(&self) -> &TopicService {
        &self.topic_svc
    }
    pub fn message(&self) -> &MessageService {
        &self.message_svc
    }
    pub fn chat(&self) -> ChatEngine<'_> {
        ChatEngine::new(
            self.topic(),
            self.provider(),
            self.model(),
            self.message(),
            self.mcp.clone(),
        )
    }
    /// 关闭所有服务
    /// - 关闭所有 MCP 客户端
    pub async fn shutdown(&self) {
        self.mcp.shutdown().await;
        if !self.db.is_closed() {
            self.db.close().await;
        }
    }
}
