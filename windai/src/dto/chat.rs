use crate::models::{chat::{ContentType, Message}, model::AdaptorType};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

/// LLM 对话请求配置
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct RequestConfig {
    /// 采样温度，范围 0~2。较高值使输出更随机，较低值使输出更聚焦。
    /// 通常建议只调 temperature 或 top_p 之一。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    /// 核采样阈值。模型只考虑累积概率达到 top_p 的候选 token。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    /// 最大输出 token 数。
    /// #TODO:
    /// - OpenAI Chat → max_completion_tokens
    /// - OpenAI Response → max_output_tokens
    /// - 国内厂商 → max_tokens
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<i32>,
    /// 是否启用流式输出
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    /// 存在性惩罚，-2.0 ~ 2.0。正值增加模型讨论新话题的可能性。
    /// - OpenAI Chat 支持，Response API 不支持
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f64>,
    /// 频率惩罚，-2.0 ~ 2.0。正值降低模型逐字重复的可能性。
    /// - OpenAI Chat 支持，Response API 不支持
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f64>,
    /// 是否在工具调用期间启用并行函数调用。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    /// 是否开启推理模式。
    ///
    /// #TODO:
    /// - DeepSeek: `{"thinking": {"type": "enabled"}}`
    /// - SiliconFlow: `{"enable_thinking": true}`
    /// - OpenAI: 转换为 reasoning_effort
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<bool>,
    // /// 推理努力程度，仅 OpenAI 有效。
    // /// 可选值: `"none"`, `"minimal"`, `"low"`, `"medium"`, `"high"`, `"xhigh"`
    // ///
    // /// 当 reasoning_enabled=true 但未设置此值时，适配器默认设为 `"medium"`
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub reasoning_effort: Option<String>,

    // /// 额外参数
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub extra: Option<Value>,
}

/// adaptor 解析对话消息响应结构体
#[derive(Debug, Serialize, Deserialize)]
pub struct MessageCommon {
    pub stream: bool,
    pub role: String,
    pub raw_content: String,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcript: Option<String>,
    pub content_type: ContentType,
    pub created_at: i64,
    pub input_tokens: i32,
    pub output_tokens: i32,
}

/// 统一对话消息响应结构体
#[derive(Debug, Serialize, Deserialize, Builder)]
#[builder(setter(into))]
pub struct MessageResponse {
    #[serde(flatten)]
    pub base: Message,
    /// 模型名称
    #[builder(default)]
    pub model_name: String,
    /// 提供商名字
    #[builder(default)]
    pub provider_name: String,
    /// 提供商id
    #[builder(default)]
    pub provider_id: i64,
    /// 消息适配器类型
    pub adaptor: AdaptorType,
}

/// 过滤上下文消息，排除非文本消息，处理消息边界
pub fn filter_context(messages: Vec<MessageResponse>) -> Vec<MessageResponse> {
    todo!()
}
