use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Hash,
    Copy,
    PartialEq,
    Eq,
    Clone,
    strum::EnumString,
    strum::Display,
)]
pub enum AdapterType {
    OpenAICompletion,
    OpenAIResponse,
}

#[derive(Debug, Serialize, Clone)]
pub struct Model {
    /// 提供商提供的模型名称
    pub name: String,
    /// 当前模型的适配器类型。
    /// 该类型决定了模型请求和响应结果的处理方式
    pub adapter: AdapterType,
    /// 模型专属端点地址
    ///
    /// 默认使用与 [AdapterType] 关联的提供商的默认端点。
    pub endpoint: Option<String>,
}
