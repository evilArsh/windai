use serde::{Deserialize, Serialize};
use sqlx::Row;
use wind_ai::model::AdapterType;

use crate::db::DbRow;
use crate::storage::utils;

/// JSON 规则，用于用户手动处理模型请求配置
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JsonRule {
    /// 唯一ID
    pub id: i64,
    /// 提供商ID
    pub provider_id: i64,
    /// 适配器类型
    pub adapter: AdapterType,
    /// JSON 规则。JSON对象字符串
    pub json_rule: String,
    /// 是否启用
    pub active: bool,
    /// 创建时间
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for JsonRule {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(JsonRule {
            id: row.get("id"),
            provider_id: row.get("provider_id"),
            adapter: utils::parse_str_to(&row.get::<String, _>("adapter")).map_err(|e| {
                sqlx::Error::Decode(format!("Failed to deserialize adapter type: {}", e).into())
            })?,
            active: row.get("active"),
            created_at: row.get("created_at"),
            json_rule: row.get("json_rule"),
        })
    }
}

/// 创建 JSON 规则
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateJsonRule {
    /// 提供商ID
    pub provider_id: i64,
    /// 适配器类型
    pub adapter: AdapterType,
    /// JSON 配置。JSON对象字符串
    pub json_rule: String,
}

/// 更新 JSON 配置
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UpdateJsonRule {
    /// 唯一ID
    pub provider_id: Option<i64>,
    /// 适配器类型
    pub adapter: Option<AdapterType>,
    /// JSON 配置。JSON对象字符串
    pub json_rule: Option<String>,
    /// 是否启用
    pub active: Option<bool>,
}
