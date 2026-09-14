use crate::db::DbRow;
use crate::storage::utils;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use wind_ai::{JsonObject, model::AdapterType};

/// 模态类型, 用于UI展示
#[derive(
    utoipa::ToSchema,
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    strum::EnumString,
    strum::Display,
)]
pub enum ModelType {
    /// 聊天模型
    Chat,
    /// 嵌入模型
    Embedding,
    /// 重排序模型
    Reranker,
    /// 音频模型
    Audio,
    /// 视频模型
    Video,
}

/// 推理级别
#[derive(
    utoipa::ToSchema,
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    strum::EnumString,
    strum::Display,
)]
#[serde(rename_all = "lowercase")]
pub enum ReasonEffort {
    None,
    Low,
    Medium,
    High,
    Xhigh,
}
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone)]
pub struct ModelConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<ReasonEffort>,
}
impl ModelConfig {
    pub fn to_json_obj(&self) -> Result<JsonObject, serde_json::Error> {
        match serde_json::to_value(self)? {
            Value::Object(map) => Ok(map),
            _ => Err(serde::de::Error::custom(format!("expected a json object"))),
        }
    }
}

/// 模型结构
#[derive(utoipa::ToSchema, Debug, Serialize, Deserialize, Clone)]
pub struct Model {
    /// 唯一id
    pub id: i64,
    /// 提供商提供的模型名称
    pub name: String,
    /// 提供商id
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
    /// 模型请求配置
    pub config: Option<ModelConfig>,
    /// 模型使用次数统计
    pub frequency: Option<i32>,
    /// 创建时间
    pub created_at: i64,
}

impl<'s> sqlx::FromRow<'s, DbRow> for Model {
    fn from_row(row: &'s DbRow) -> Result<Self, sqlx::Error> {
        Ok(Model {
            id: row.get("id"),
            name: row.get("name"),
            provider_id: row.get("provider_id"),
            alias: row.get("alias"),
            adapter: utils::parse_str_to(&row.get::<String, _>("adapter"))
                .map_err(|e| sqlx::Error::Decode(e.into()))?,
            modalities: utils::de_str_to(&row.get::<String, _>("modalities"))
                .map_err(|e| sqlx::Error::Decode(e.into()))?,
            config: utils::de_str_to(&row.get::<String, _>("config"))
                .map_err(|e| sqlx::Error::Decode(e.into()))?,
            active: row.get("active"),
            icon: row.get("icon"),
            endpoint: row.get("endpoint"),
            frequency: row.get("frequency"),
            created_at: row.get("created_at"),
        })
    }
}

/// 创建模型参数
#[derive(utoipa::ToSchema, Serialize, Deserialize, Debug, Clone)]
pub struct CreateModel {
    /// 模型名称
    pub name: String,
    /// 提供商id
    pub provider_id: i64,
    /// 自定义模型别名
    pub alias: Option<String>,
    /// 模型适配器类型
    pub adapter: AdapterType,
    /// 模型支持的模态类型
    pub modalities: Option<Vec<ModelType>>,
    /// 模型是否启用
    pub active: Option<bool>,
    /// 模型图标
    pub icon: Option<String>,
    /// 模型专属端点地址
    pub endpoint: Option<String>,
    /// 模型配置
    pub config: Option<ModelConfig>,
}

/// 更新模型参数
#[derive(utoipa::ToSchema, Serialize, Deserialize, Debug, Clone)]
pub struct UpdateModel {
    /// 模型名称
    pub name: Option<String>,
    /// 自定义模型别名
    pub alias: Option<String>,
    /// 模型适配器类型
    pub adapter: Option<AdapterType>,
    /// 模型支持的模态类型
    pub modalities: Option<Vec<ModelType>>,
    /// 模型是否启用
    pub active: Option<bool>,
    /// 模型图标
    pub icon: Option<String>,
    /// 模型专属端点地址
    pub endpoint: Option<String>,
    /// 模型使用次数统计
    pub frequency: Option<i32>,
    /// 模型配置
    pub config: Option<ModelConfig>,
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
            config: None,
        }
    }
}
