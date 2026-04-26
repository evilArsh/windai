use serde::{Deserialize, Serialize};
use std::env;

/// 提供商账号信息
#[derive(Serialize, Deserialize)]
pub struct Credentials {
    pub id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<i64>,
    pub key: String,
}
impl Credentials {
    pub fn from_env() -> Self {
        let api_key = env::var("API_KEY").unwrap_or(String::new());
        Credentials {
            id: 0,
            provider_id: None,
            key: api_key,
        }
    }
}

/// 提供商
#[derive(Serialize, Deserialize)]
pub struct Provider {
    pub id: i64,
    /// 唯一的提供商名字
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 提供商 base api 地址
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// 提供商官方文档地址
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// 提供商别名
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    pub active: bool,
}
