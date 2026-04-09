use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::adaptor::AdaptorType;

#[derive(Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ModelType {
    Chat,
    Embedding,
    Reranker,
    Audio,
    Video,
}

#[derive(Serialize, Deserialize, Builder)]
pub struct Model {
    #[builder(default)]
    id: String,
    /// 当前模型的适配器类型。
    /// 该类型决定了模型请求结果的处理方式
    #[builder(default)]
    adaptor_type: Option<AdaptorType>,
    /// 模型的模态类型
    #[builder(default)]
    r#type: Vec<ModelType>,
    /// 模型名称
    #[builder(default)]
    name: String,
    /// 模型所属的提供商id
    #[builder(default)]
    provider_id: String,
}
