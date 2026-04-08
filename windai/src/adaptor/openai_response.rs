//! OpenAI Response API 数据结构
//! https://developers.openai.com/api/reference/resources/responses/methods/create

use super::is_none_or_empty_vec;
use super::openai_chat::{Role, ToolCallRequest};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ======================================================
// Responses 请求
// ======================================================

/// 响应创建请求的主结构体
#[derive(Debug, Serialize, Deserialize, Default, Clone, Builder)]
pub struct CreateResponseRequest {
    /// 是否在后台运行模型响应。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub background: Option<bool>,

    /// 此请求的上下文管理配置。
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub context_management: Option<Vec<ContextManagementConfig>>,

    /// 此响应所属的会话。
    /// 该会话的项目将自动添加并更新。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation: Option<ConversationParam>,

    /// 指定要在模型响应中包含的附加输出数据。
    #[builder(default)]
    #[serde(skip_serializing_if = "is_none_or_empty_vec")]
    pub include: Option<Vec<ResponseIncludable>>,

    /// 输入内容，可以是简单的字符串，也可以是消息对象数组。
    #[builder(default)]
    #[serde(skip_serializing_if = "is_none_or_empty_vec")]
    pub input: Option<Vec<InputItem>>,

    /// 响应可生成的token数量上限，包括可见输出token和推理token。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<i32>,

    /// 内置工具在单个响应中可处理的总调用次数上限。
    /// 此上限适用于所有内置工具调用的总和，而非单个工具的调用次数。
    /// 若模型尝试进行超出此限制的工具调用，后续调用将被忽略。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tool_calls: Option<i32>,

    /// 模型名称。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// 模型推理开关
    pub reasoning: Option<CreateResponseReasoning>,

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

#[derive(Debug, Serialize, Deserialize, Default, Clone, Builder)]
pub struct CreateResponseReasoning {
    /// 可选值： none, minimal, low, medium, high, xhigh
    ///
    /// 限制推理模型在推理上的努力程度。降低推理努力可带来更快的响应速度，并减少响应中用于推理的token数量。
    ///
    /// gpt-5.1 默认值为 none，即不进行推理。gpt-5.1 支持的推理值为：none, low, medium, and high。所有推理值在 gpt-5.1 中均支持工具调用。
    ///
    /// gpt-5.1 之前的所有模型默认推理努力为中，且不支持无值。
    ///
    /// gpt-5-pro 模型默认（且仅支持）高推理努力。
    ///
    /// xhigh 在所有 gpt-5.1-codex-max 之后的模型中均受支持。
    effort: Option<String>,
}

/// 上下文管理配置
#[derive(Debug, Serialize, Default, Deserialize, Clone, Builder)]
pub struct ContextManagementConfig {
    ///  entry 类型。目前仅支持 'compaction'（压缩）。
    #[builder(default="\"compaction\"".to_string())]
    pub r#type: String,

    /// 触发压缩的 Token 阈值。最小值通常为 1000。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compact_threshold: Option<f64>,
}

/// 会话参数，支持直接传入会话 ID 或对象
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum ConversationParam {
    /// 唯一的会话 ID 字符串
    Id(String),
    /// 包含 ID 字段的结构化对象
    Object { id: String },
}

/// 可选包含的附加输出项枚举
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ResponseIncludable {
    #[serde(rename = "web_search_call.action.sources")]
    WebSearchSources,
    #[serde(rename = "web_search_call.results")]
    WebSearchResults,
    #[serde(rename = "code_interpreter_call.outputs")]
    CodeInterpreterOutputs,
    #[serde(rename = "computer_call_output.output.image_url")]
    ComputerCallImageUrl,
    #[serde(rename = "file_search_call.results")]
    FileSearchResults,
    #[serde(rename = "message.input_image.image_url")]
    InputImageUrl,
    #[serde(rename = "message.output_text.logprobs")]
    Logprobs,
    #[serde(rename = "reasoning.encrypted_content")]
    ReasoningEncryptedContent,
}

/// 输入内容的变体
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum InputItem {
    /// 简单文本输入
    TextInput(String),
    /// 结构化消息对象数组
    EasyInput {
        /// 模型接收的文本、图像或音频输入，用于生成回应。也可包含先前的助手回应。
        content: EasyInputContent,
        /// 角色
        role: Role,
        // /// 将助手消息标记为中间评论（commentary）或最终答案（final_answer）。
        // /// 对于像gpt-5.3-codex及更高版本的模型，在发送后续请求时，需保留并重新发送所有助手消息的阶段标签——删除它们可能会降低性能。
        // /// 此标签不用于用户消息。
        // #[serde(skip_serializing_if = "Option::is_none")]
        // pub phase: Option<String>,
        // /// 总是 "message"
        // #[serde(skip_serializing_if = "Option::is_none")]
        // pub r#type: Option<String>,
    },
    /// 模型返回的函数调用信息
    ///
    /// 模型选择需要用户调用的函数后，用户需要将选择的函数以及生成的参数放入上下文中。
    FunctionCall {
        /// 传递给函数的参数JSON字符串
        arguments: String,
        /// 模型生成的函数工具调用的唯一ID
        call_id: String,
        /// 要运行的函数名称
        name: String,
        /// 函数工具调用的类型。始终为 "function_call"
        r#type: String,
        /// 函数工具调用的唯一ID（可选）
        id: Option<String>,
        /// 要运行的函数的命名空间（可选）
        namespace: Option<String>,
    },
    /// 本地函数调用结果。
    ///
    /// 当本地函数调用结束后，用户需要将函数调用结果放入到上下文中。
    FunctionCallOutput {
        /// 模型生成的函数工具调用的唯一ID
        call_id: String,
        /// 本地函数调用生成的结果
        output: Value,
        /// 函数工具调用的类型。始终为 "function_call_output"
        r#type: String,
        /// 函数工具调用的唯一ID（可选）
        id: Option<String>,
    },
}

/// 易用的输入消息结构
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum EasyInputContent {
    TextInput(String),
    ResponseInputText {
        text: String,
        /// 总是 "input_text"
        r#type: String,
    },
    ResponseInputImage {
        /// One of `high`, `low`, `auto`, or `original`. Defaults to `auto`.
        detail: Option<String>,
        /// "input_image"
        r#type: String,
        file_id: Option<String>,
        /// 要发送给模型的图像的URL。可以是完全限定的URL，也可以是数据URL中base64编码的图像。
        image_url: Option<String>,
    },
    ResponseInputFile {
        /// 输入项的类型。始终为 "input_file"。
        r#type: String,
        /// 要发送给模型的文件内容。
        file_data: Option<String>,
        /// 要发送给模型的文件ID。
        file_id: Option<String>,
        /// 要发送给模型的文件URL。
        file_url: Option<String>,
        /// 要发送给模型的文件名。
        filename: Option<String>,
    },
}

// ======================================================
// Responses 响应
// ======================================================

/// Responses 响应结构体
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateResponse {
    /// Responses 的唯一标识符。
    pub id: String,
    /// 聊天补全创建时间的 Unix 时间戳（秒级）。
    pub created: u64,
    /// 模型未能生成响应时返回的错误对象。
    ///
    /// 返回格式：
    /// ```json
    /// {
    ///   code: "server_error",
    ///   message: "Invalid request: Invalid input text."
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
    /// 模型名称。
    pub model: String,
    /// 模型输出内容
    pub output: Vec<ResponseOutputMessage>,
}

/// 模型响应内容
#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseOutputMessage {
    /// 模型输出的类型
    pub r#type: String,
    /// 状态："in_progress" or "completed" or "incomplete"
    pub status: Option<String>,

    /// type == "message"
    ///
    /// 响应消息的唯一标识符。
    /// 或者 function tool call 唯一id
    pub id: Option<String>,
    /// type == "message"
    ///
    /// 模型响应的文本内容
    pub content: ResponseOutputText,
    /// type == "message"
    ///
    /// 模型输出的角色。
    pub role: Role,
    /// type == "message"
    ///
    ///  "commentary" or "final_answer"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// type == "function_call"
    ///
    /// 传递给函数的参数JSON字符串
    pub arguments: Option<String>,
    /// type == "function_call" | "function_call_output"
    ///
    /// 模型生成的函数工具调用的唯一ID
    pub call_id: Option<String>,
    /// type == "function_call"
    ///
    /// 要运行的函数名称
    pub name: Option<String>,
    /// type == "function_call"
    ///
    /// 要运行的函数的命名空间
    pub namespace: Option<String>,
    /// type == "function_call_output"
    ///
    /// 你的代码生成的函数调用的输出。可以是字符串或输出内容的列表。
    /// 在执行函数调用时(MCP)，需要将调用结果发送给模型
    pub output: Option<Value>,
    pub top_p: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub background: Option<bool>,
    /// 完成时间：秒
    pub completed_at: Option<u64>,
    pub max_output_tokens: Option<i32>,
    pub max_tool_calls: Option<i32>,
    pub output_text: Option<String>,
    pub previous_response_id: Option<String>,
}

/// 模型的文本输出。
/// 参考：https://developers.openai.com/api/reference/resources/responses/methods/create
#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseOutputText {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Value::String 类型
    pub r#type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Value::Object 类型
    pub annotations: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Value::String 类型
    pub logprobs: Option<Value>,
    /// Value::String 类型
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<Value>,
}
