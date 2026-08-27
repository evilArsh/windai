use serde::{Deserialize, Serialize};

/// 模型适配器类型
///
/// 决定模型请求和响应结果处理方式。
/// 每一种适配器按照官方标准API格式处理，但会加入不影响标准API的额外参数。
///
/// 对于复杂的标准API变体，用户可以使用 wind-rule 模块进行适配和拓展
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
    /// OpenAI chat completion API 适配器
    OpenAICompletion,
    /// OpenAI response api 适配器
    OpenAIResponse,
}

/// 模型信息
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
