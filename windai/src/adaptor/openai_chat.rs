//! OpenAI Chat API 数据结构
//! https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
//!
//! 国内模型大多使用该结构，不同厂商有细微差别

use super::is_none_or_empty_vec;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
    Developer,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContentObject {
    pub r#type: String,
    pub content: String,
}
impl ContentObject {
    pub fn to_text(self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum Content {
    Text(String),
    Texts(Vec<String>),
    Object(ContentObject),
    Objects(Vec<ContentObject>),
}
impl Content {
    /// convert content to text
    pub fn to_text(self) -> String {
        match self {
            Content::Text(text) => text,
            Content::Texts(texts) => texts.join(","),
            Content::Object(object) => object.content,
            Content::Objects(mut objects) => match objects.len() {
                1 => objects.remove(0).content,
                _ => objects
                    .into_iter()
                    .map(|object| object.to_text())
                    .collect::<Vec<_>>()
                    .join("\n"),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TokenUsage {
    pub completion_tokens: i32,
    pub prompt_tokens: i32,
    pub total_tokens: i32,
}

// ======================================================
// ChatCompletion 请求
// ======================================================

#[derive(Debug, Serialize, Deserialize, Builder, Clone, Default)]
#[builder(setter(strip_option, into))]
pub struct ChatCompletionRequest {
    /// 对话消息列表
    #[builder(default)]
    pub messages: Vec<ChatCompletionRequestMessage>,

    /// 用于生成响应的模型ID
    #[builder(default)]
    pub model: String,

    /// 数值范围在-2.0到2.0之间。正值会根据新词在文本中已有的出现频率进行惩罚，从而降低模型逐字重复相同内容的可能性。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f64>,

    /// 聊天完成中可以生成的最大token数量
    ///
    /// OpenAI 已弃用，使用[Self::max_completion_tokens]。
    /// 国内模型使用该字段
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<i32>,

    /// 聊天完成中可以生成的最大token数量
    ///
    /// OpenAI 弃用[max_tokens]并改用该字段
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<i32>,

    /// 数值在 -2.0 到 2.0 之间。正值会根据新标记是否已在文本中出现过进行惩罚，从而增加模型讨论新话题的可能性。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f64>,

    /// 开启推理模式。
    ///
    /// DeepSeek中需要转换为
    /// ```json
    /// {thinking:{type:"enabled"|"disabled"}}
    /// ```
    ///Siliconflow中为
    /// ```json
    /// {enable_thinking:boolean}
    /// ```
    /// OpenAI中为[Self::reasoning_effort]字段
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<bool>,

    /// 开启推理模式，该字段只在OpenAI中生效
    ///
    /// OpenAI中可选值为[none], [minimal], [low], [medium], [high],[xhigh]
    /// [Self::reasoning] 为[true] 但不设置该值时，发送到OpenAI时该值应设置为[medium]
    #[builder(default)]
    pub reasoning_effort: Option<String>,

    /// 响应格式，默认为 text
    ///
    /// ```json
    /// {type: "text"|"json_object"}
    /// ```
    #[builder(default = Some(json!({ "type": "text" })))]
    pub response_format: Option<Value>,

    /// 若设置为 true，模型生成的响应数据将通过服务器发送事件（server-sent events）实时流式传输至客户端。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,

    /// 采样温度应在0到2之间选择。较高的数值（如0.8）会使输出更具随机性，而较低的数值（如0.2）则会使输出更加聚焦和确定。
    /// 通常建议调整温度参数或top_p参数，但不要同时调整两者。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,

    /// 模型可以调用的工具列表
    #[builder(default)]
    #[serde(skip_serializing_if = "is_none_or_empty_vec")]
    pub tools: Option<Vec<ToolCallRequest>>,

    /// 一种替代温度采样的方法是核采样，在这种方法中，模型仅考虑那些累积概率达到 top_p 的候选token。
    /// 例如，当 top_p 设置为 0.1 时，模型只会考虑那些累计概率质量达到前 10% 的token。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Builder, Clone)]
#[builder(setter(strip_option, into))]
pub struct ChatCompletionRequestMessage {
    /// 消息作者的角色
    #[builder(default = "Role::User")]
    pub role: Role,

    /// 消息具体内容
    #[builder(default = "Content::Text(String::new())")]
    pub content: Content,

    /// 参与者的可选名称。为模型提供信息，以区分同一角色的不同参与者。
    #[builder(default)]
    pub name: Option<String>,

    /// 此消息所响应的工具调用标识符
    #[builder(default)]
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Builder, Clone, Default)]
#[builder(setter(strip_option, into))]
pub struct ToolCallRequestParams {
    /// 要调用的函数名称
    #[builder(default)]
    pub name: String,

    /// 函数描述。模型根据此描述决定是否调用该函数。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// 描述函数参数的 JSON schema 对象
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,

    /// 是否强制执行严格的参数验证
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

/// 发送给模型的工具调用参数
#[derive(Debug, Serialize, Deserialize, Builder, Clone)]
#[builder(setter(strip_option, into))]
pub struct ToolCallRequest {
    /// 工具调用类型，固定值为 "function"
    #[builder(default = "\"function\"".to_string())]
    pub r#type: String,

    /// 工具调用请求参数
    #[builder(default)]
    pub function: ToolCallRequestParams,
}

// ======================================================
// ChatCompletion 响应
// ======================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatCompletion {
    /// 聊天补全的唯一标识符。
    pub id: String,

    /// 聊天补全选项列表。
    pub choices: Vec<ChatCompletionChoice>,

    /// 聊天补全创建时间的 Unix 时间戳（秒级）。
    pub created: u64,

    /// 用于聊天补全的模型名称。
    pub model: String,

    /// 此指纹代表模型运行时的后端配置。
    /// 可与 seed 请求参数结合使用，以判断后端变更是否可能影响确定性。
    /// 注意：OpenAI 已弃用此字段
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_fingerprint: Option<String>,

    /// 补全请求的用量统计信息。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<TokenUsage>,
}

/// 实时流式传输聊天完成。使用服务器发送事件接收模型返回的完成片段。
#[derive(Debug, Serialize, Deserialize)]
pub struct ChatStreamCompletion {
    /// 聊天补全的唯一标识符。
    pub id: String,

    /// 聊天补全选项列表。
    pub choices: Vec<ChatStreamCompletionChoice>,

    /// 聊天补全创建时间的 Unix 时间戳（秒级）。
    pub created: u64,

    /// 用于聊天补全的模型名称。
    pub model: String,

    /// 此指纹代表模型运行时的后端配置。
    /// 可与 seed 请求参数结合使用，以判断后端变更是否可能影响确定性。
    /// 注意：OpenAI 已弃用此字段
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_fingerprint: Option<String>,

    /// 补全请求的用量统计信息。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<TokenUsage>,
}

/// 对话消息
#[derive(Debug, Serialize, Deserialize, Builder, Clone)]
#[builder(setter(strip_option, into))]
pub struct ChatCompletionMessage {
    /// 消息内容
    /// 流式消息中该字段可能为空
    pub content: Option<String>,

    /// 模型可能返回的推理消息
    pub reasoning_content: Option<String>,

    /// 消息作者的角色。
    /// 流式消息中该字段可能为空
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,

    /// 模型返回的工具调用参数
    #[serde(skip_serializing_if = "is_none_or_empty_vec")]
    #[builder(default)]
    pub tool_calls: Option<Vec<ToolCallResponse>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ToolCallResponseParam {
    /// 模型选择的本地函数名称
    pub name: String,

    /// 调用函数时使用的参数，由模型生成的 JSON 格式字符串。
    /// 注意：模型生成的 JSON 不一定总是有效的，且可能产生函数模式中未定义的参数。
    /// 在调用函数前，请务必在代码中验证这些参数。
    /// 需要通过 JSON schema 进行验证
    pub arguments: String,
}

/// 模型返回的工具调用参数
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ToolCallResponse {
    /// 工具调用的唯一标识符
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    /// 模型调用的函数信息
    pub function: ToolCallResponseParam,

    /// 工具调用类型，固定值为 "function"
    pub r#type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatCompletionChoice {
    pub finish_reason: String,
    pub index: i32,
    pub message: ChatCompletionMessage,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatStreamCompletionChoice {
    pub finish_reason: Option<String>,
    pub index: i32,
    pub delta: ChatCompletionMessage,
}
