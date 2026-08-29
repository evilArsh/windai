use serde::{Deserialize, Serialize};
use std::env;

/// 启动配置，从环境变量读取，带默认值。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub db_path: Option<String>,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let host = env::var("WIND_HTTP_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        let port = env::var("WIND_HTTP_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(7324);
        let db_path = env::var("WINDAI_DB_PATH").ok().filter(|s| !s.is_empty());
        Self {
            host,
            port,
            db_path,
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 7324,
            db_path: None,
        }
    }
}
