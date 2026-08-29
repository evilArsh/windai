use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::db::DbRow;

/// 提供商账号
#[derive(utoipa::ToSchema, Serialize, Deserialize, Clone)]
pub struct Credentials {
    /// 唯一id
    pub id: i64,
    /// 提供商id
    pub provider_id: i64,
    /// 密钥
    pub key: String,
    /// 创建时间
    pub created_at: i64,
    /// 账号是否启用
    pub active: bool,
}

impl<'s> sqlx::FromRow<'s, DbRow> for Credentials {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Credentials {
            id: row.get("id"),
            active: row.get("active"),
            provider_id: row.get("provider_id"),
            key: row.get("key"),
            created_at: row.get("created_at"),
        })
    }
}
impl std::fmt::Display for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Credentials {{ id: {}, provider_id: {}, key: {}, created_at: {}, active: {} }}",
            self.id,
            self.provider_id,
            "*".repeat(self.key.len()),
            self.created_at,
            self.active
        )
    }
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("id", &self.id)
            .field("provider_id", &self.provider_id)
            .field("key", &"*".repeat(self.key.len())) // 脱敏
            .field("created_at", &self.created_at)
            .field("active", &self.active)
            .finish()
    }
}

/// 提供商
#[derive(utoipa::ToSchema, Serialize, Deserialize, Debug, Clone)]
pub struct Provider {
    /// 唯一id
    pub id: i64,
    /// 唯一的提供商名字
    pub name: String,
    /// 提供商 base api 地址
    pub base_url: String,
    /// 提供商描述
    pub description: Option<String>,
    /// 提供商官方文档地址
    pub doc: Option<String>,
    /// 提供商别名
    pub alias: Option<String>,
    /// 账号是否启用
    pub active: bool,
    /// 创建时间
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for Provider {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Provider {
            id: row.get("id"),
            name: row.get("name"),
            alias: row.get("alias"),
            created_at: row.get("created_at"),
            base_url: row.get("base_url"),
            description: row.get("description"),
            doc: row.get("doc"),
            active: row.get("active"),
        })
    }
}

/// 新建提供商
#[derive(utoipa::ToSchema, Serialize, Deserialize, Debug, Clone)]
pub struct CreateProvider {
    /// 提供商名字
    pub name: String,
    /// 提供商描述
    pub description: Option<String>,
    /// 提供商 base api 地址
    pub base_url: String,
    /// 提供商官方文档地址
    pub doc: Option<String>,
    /// 提供商别名
    pub alias: Option<String>,
}

/// 更新提供商
#[derive(utoipa::ToSchema, Serialize, Deserialize, Debug, Clone)]
pub struct UpdateProvider {
    /// 提供商名字
    pub name: Option<String>,
    /// 提供商描述
    pub description: Option<String>,
    /// 提供商 base api 地址
    pub base_url: Option<String>,
    /// 提供商官方文档地址
    pub doc: Option<String>,
    /// 提供商别名
    pub alias: Option<String>,
    /// 账号是否启用
    pub active: Option<bool>,
}

impl Default for UpdateProvider {
    fn default() -> Self {
        Self {
            name: None,
            description: None,
            base_url: None,
            doc: None,
            alias: None,
            active: None,
        }
    }
}

/// 新建凭证
#[derive(utoipa::ToSchema, Serialize, Deserialize, Debug, Clone)]
pub struct CreateCredentials {
    /// 提供商id
    pub provider_id: i64,
    /// 凭证密钥
    pub key: String,
}
