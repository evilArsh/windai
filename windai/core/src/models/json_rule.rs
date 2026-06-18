use serde::Serialize;
use sqlx::Row;
use wind_ai::model::AdapterType;

use crate::db::DbRow;
use crate::storage::utils;

/// JSON 规则，用于用户手动处理模型请求配置
#[derive(Debug, Serialize, Clone)]
pub struct JsonRule {
    pub id: i64,
    pub provider_id: i64,
    pub adapter: AdapterType,
    pub json_rule: String,
    pub active: bool,
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

#[derive(Debug, Serialize, Clone)]
pub struct CreateJsonRule {
    pub provider_id: i64,
    pub adapter: AdapterType,
    pub json_rule: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct UpdateJsonRule {
    pub provider_id: Option<i64>,
    pub adapter: Option<AdapterType>,
    pub json_rule: Option<String>,
    pub active: Option<bool>,
}
