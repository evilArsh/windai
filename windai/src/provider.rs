use std::env;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Credentials {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<i64>,
    pub url: String,
    pub key: String,
}
impl Credentials {
    pub fn from_env() -> Self {
        let api_key = env::var("API_KEY").unwrap_or("".to_string());
        let api_url = env::var("API_BASE_URL").unwrap_or("".to_string());
        Credentials {
            id: None,
            provider_id: None,
            url: api_url,
            key: api_key,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Provider {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    /// 唯一的提供商名字
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 提供商官方文档地址
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// 提供商别名
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active: Option<u8>,
}
