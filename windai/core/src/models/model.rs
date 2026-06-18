use serde::{Deserialize, Serialize};
use sqlx::Row;
use wind_ai::model::AdapterType;

use crate::db::DbRow;
use crate::storage::utils;

/// 模态类型, 用于UI展示
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, strum::EnumString, strum::Display)]
pub enum ModelType {
    Chat,
    Embedding,
    Reranker,
    Audio,
    Video,
}

/// 模型结构
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Model {
    pub id: i64,
    /// 提供商提供的模型名称
    pub name: String,
    pub provider_id: i64,
    /// 自定义模型别名
    pub alias: Option<String>,
    /// 当前模型的适配器类型。
    /// 该类型决定了模型请求和响应结果的处理方式
    pub adapter: AdapterType,
    /// 标注模态类型
    pub modalities: Option<Vec<ModelType>>,
    /// 模型是否启用
    pub active: bool,
    /// 模型图标
    pub icon: Option<String>,
    /// 模型专属端点地址
    ///
    /// 默认使用[AdapterType]类型下的不同提供商的默认端点。
    pub endpoint: Option<String>,
    /// 模型使用次数统计
    pub frequency: Option<i32>,
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for Model {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Model {
            id: row.get("id"),
            name: row.get("name"),
            provider_id: row.get("provider_id"),
            alias: row.get("alias"),
            adapter: utils::parse_str_to(&row.get::<String, _>("adapter")).map_err(|e| {
                sqlx::Error::Decode(format!("Failed to deserialize adapter type: {}", e).into())
            })?,
            modalities: utils::de_str_to(&row.get::<String, _>("modalities")).map_err(|e| {
                sqlx::Error::Decode(format!("Failed to deserialize modalities: {}", e).into())
            })?,
            active: row.get("active"),
            icon: row.get("icon"),
            endpoint: row.get("endpoint"),
            frequency: row.get("frequency"),
            created_at: row.get("created_at"),
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateModel {
    pub name: String,
    pub provider_id: i64,
    pub alias: Option<String>,
    pub adapter: AdapterType,
    pub modalities: Option<Vec<ModelType>>,
    pub active: Option<bool>,
    pub icon: Option<String>,
    pub endpoint: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateModel {
    pub name: Option<String>,
    pub alias: Option<String>,
    pub adapter: Option<AdapterType>,
    pub modalities: Option<Vec<ModelType>>,
    pub active: Option<bool>,
    pub icon: Option<String>,
    pub endpoint: Option<String>,
    pub frequency: Option<i32>,
}

impl Default for UpdateModel {
    fn default() -> Self {
        Self {
            name: None,
            alias: None,
            adapter: None,
            modalities: None,
            active: None,
            icon: None,
            endpoint: None,
            frequency: None,
        }
    }
}
