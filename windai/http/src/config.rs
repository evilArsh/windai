use serde::{Deserialize, Serialize};
use std::env;

/// 启动配置，从环境变量读取，带默认值。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
}

impl AppConfig {
    /// 从环境变量加载配置，环境变量缺失时使用默认值
    pub fn from_env() -> Self {
        Self {
            host: env::var("WIND_HTTP_HOST").unwrap_or_else(|_| Self::default_host()),
            port: env::var("WIND_HTTP_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(Self::default_port()),
        }
    }
    fn default_host() -> String {
        String::from("127.0.0.1")
    }
    fn default_port() -> u16 {
        7324
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            host: Self::default_host(),
            port: Self::default_port(),
        }
    }
}
