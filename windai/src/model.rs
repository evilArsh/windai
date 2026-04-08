use derive_builder::Builder;
use serde::{Deserialize, Serialize};

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
pub struct ModelInfo {
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<i64>,
    /// modalities of the model
    #[builder(default)]
    r#type: Vec<ModelType>,
    /// model name
    #[builder(default)]
    name: String,
    /// the model belongs to which provider
    ///
    /// if the providers have their own models, this value is equal to `sub_provider_name`.
    #[builder(default)]
    provider_name: String,
    /// the sub-provider name, some providers does not own models,
    /// they are just the agents for example: Siliconflow has DeepSeek models and other models.
    ///
    /// if the providers have their own models, this value is equal to `provider_name`.
    #[builder(default)]
    sub_provider_name: String,
}
