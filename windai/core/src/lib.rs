pub mod chat;
pub mod db;
pub mod error;
pub mod models;
pub mod schema;
pub mod storage;

use chat::ChatEngine;
use error::Result;
use sqlx::SqlitePool;
use std::sync::Arc;
use storage::message::service::MessageService;
use storage::model::service::ModelService;
use storage::provider::service::ProviderService;
use storage::topic::service::TopicService;
use wind_js::JsEngine;
use wind_mcp::client::registry::{Registry, RegistryHandle};

/// Wind Core 模块的主入口点。
///
/// 持有私有数据库连接池、共享的 JS 引擎以及全局 MCP 客户端。
pub struct WindCore {
    db: SqlitePool,
    js_engine: Arc<JsEngine>,
    mcp: RegistryHandle,
}

impl WindCore {
    /// 初始化 Wind Core 模块。
    /// - 目前只支持SQLite数据库
    pub async fn init(db_url: &str) -> Result<Self> {
        let db = db::connect(db_url)
            .await
            .map_err(|e| error::CoreError::Database(e))?;

        schema::init_schema(&db).await?;

        let js_engine = Arc::new(JsEngine::new().map_err(|e| error::CoreError::Js(e.to_string()))?);

        let mcp = Registry::new();

        Ok(Self { db, js_engine, mcp })
    }

    pub fn provider(&self) -> ProviderService {
        ProviderService::new(self.db.clone())
    }

    pub fn model(&self) -> ModelService {
        ModelService::new(self.db.clone())
    }

    pub fn topic(&self) -> TopicService {
        TopicService::new(self.db.clone())
    }

    pub fn message(&self) -> MessageService {
        MessageService::new(self.db.clone())
    }

    pub fn chat(&self) -> ChatEngine {
        ChatEngine::new(self.db.clone(), self.js_engine.clone(), self.mcp.clone())
    }

    /// 关闭所有服务
    /// - 关闭所有 MCP 客户端
    pub async fn shutdown(&self) {
        self.mcp.shutdown().await;
    }
}
