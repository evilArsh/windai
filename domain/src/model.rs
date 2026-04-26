use super::adaptor::AdaptorType;
use serde::{Deserialize, Serialize};

/// 模型的模态类型
#[derive(Serialize, Deserialize, Clone, PartialEq, strum::EnumString, strum::Display)]
pub enum ModelType {
    Chat,
    Embedding,
    Reranker,
    Audio,
    Video,
}

#[derive(Serialize, Deserialize)]
pub struct Model {
    pub id: i64,
    /// 提供商提供的模型名称
    pub name: String,
    /// 自定义模型别名
    pub alias: Option<String>,
    /// 模型所属的提供商id
    pub provider_id: i64,
    /// 当前模型的适配器类型。
    /// 该类型决定了模型请求和响应结果的处理方式
    pub adaptor: AdaptorType,
    /// 模型的模态类型
    pub modalities: Vec<ModelType>,
    /// 模型是否使用
    pub active: bool,
    /// 模型图标
    pub icon: Option<String>,
    /// 模型专属端点地址
    ///
    /// 默认使用[AdaptorType]类型下的不同提供商的默认端点。
    pub endpoint: Option<String>,
    /// 模型使用次数统计
    pub frequency: Option<i32>,
}
