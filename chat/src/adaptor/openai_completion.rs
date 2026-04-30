//! OpenAI Chat Completion API 数据结构
//! https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create

use super::is_none_or_empty_vec;
use crate::Role;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ======================================================
// ChatCompletion 请求
// ======================================================

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ChatCompletionRequest {
    /// 对话消息列表
    pub messages: Vec<ChatCompletionMessageParam>,
    /// 用于生成响应的模型ID
    pub model: String,
    /// 音频输出参数。当请求音频输出且 modalities 字段设为["audio"]时必需。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<ChatCompletionAudioParam>,
    /// 数值范围在-2.0到2.0之间。正值会根据新词在文本中已有的出现频率进行惩罚，从而降低模型逐字重复相同内容的可能性。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f64>,
    /// Hash<String, f64>
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logit_bias: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<bool>,
    /// 聊天完成中可以生成的最大token数量
    ///
    /// OpenAI 弃用[max_tokens]并改用该字段
    /// # TODO
    /// 适配器调整
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<i32>,
    // /// 聊天完成中可以生成的最大token数量
    // ///
    // /// OpenAI 已弃用，使用[Self::max_completion_tokens]。
    // /// 国内模型使用该字段
    // /// # TODO
    // /// 适配器调整
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub max_tokens: Option<i32>,
    /// Hash<String, String>
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    /// "text" or "audio", 语音多模态需要填此参数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modalities: Option<Vec<String>>,
    /// 为每条输入消息生成多少个聊天完成选项。
    /// 最小值：1
    /// 最大值：128
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<i32>,
    /// 是否在工具使用期间启用并行函数调用。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    /// 静态预测输出内容，例如正在重新生成的文本文件的内容。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prediction: Option<Content>,
    /// 数值在 -2.0 到 2.0 之间。正值会根据新标记是否已在文本中出现过进行惩罚，从而增加模型讨论新话题的可能性。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_key: Option<String>,
    /// 提示缓存的保留策略。设置为24小时以启用扩展提示缓存，该功能可使缓存的提示前缀保持更长的活动时间，最长可达24小时。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_retention: Option<String>,
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
    ///
    /// OpenAI中可选值为[none], [minimal], [low], [medium], [high],[xhigh]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// 响应格式，默认为 text
    ///
    /// ```json
    /// {type: "text"|"json_object"}
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safety_identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    /// String 或 Vec<String>
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store: Option<bool>,
    /// 若设置为 true，模型生成的响应数据将通过服务器发送事件（server-sent events）实时流式传输至客户端。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    /// stream:true 时生效
    ///
    /// 值为
    /// ```json
    /// {
    ///   include_obfuscation?:boolean,
    ///   // 如果设置此选项，在数据流结束前会额外传输一个数据块：[DONE]消息。该数据块中的usage字段会显示整个请求的令牌使用统计信息，而choices字段将始终为空数组。
    ///   // 所有其他数据块也会包含usage字段，但其值为null。注意：如果数据流中断，您可能无法接收到包含请求总令牌使用量的最终usage数据块。
    ///   include_usage?:boolean
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<Value>,
    /// 采样温度应在0到2之间选择。较高的数值（如0.8）会使输出更具随机性，而较低的数值（如0.2）则会使输出更加聚焦和确定。
    /// 通常建议调整温度参数或top_p参数，但不要同时调整两者。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    /// 控制模型调用何种工具（如有）
    ///
    /// none 表示模型不会调用任何工具，而是生成一条消息。
    ///
    /// auto 表示模型可以选择生成消息或调用一个或多个工具。
    ///
    /// required 表示模型必须调用一个或多个工具。
    ///
    /// 通过指定特定工具（如 {"type": "function", "function": {"name": "my_function"}}）可强制模型调用该工具。
    ///
    /// 当未提供工具时，默认值为 none。若存在可用工具，则默认值为 auto。
    ///
    /// 可能的值：
    /// ```json
    /// 1. "none" or "auto" or "required"
    /// 2.
    /// {
    ///   allowed_tools: {
    ///      mode: "auto|required",
    ///   }
    ///   type: "allowed_tools"
    /// }
    /// 3.
    /// {
    ///   function: {
    ///      name: "function name",
    ///   }
    ///   type: "function"
    /// }
    /// 4.
    /// {
    ///   custom: {
    ///      name: "function name",
    ///   }
    ///   type: "custom"
    /// }
    ///
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
    /// 模型可以调用的工具列表
    #[serde(skip_serializing_if = "is_none_or_empty_vec")]
    pub tools: Option<Vec<ToolCallRequest>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_logprobs: Option<i32>,
    /// 一种替代温度采样的方法是核采样，在这种方法中，模型仅考虑那些累积概率达到 top_p 的候选token。
    /// 例如，当 top_p 设置为 0.1 时，模型只会考虑那些累计概率质量达到前 10% 的token。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    /// "low" 或 "medium" 或 "high"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verbosity: Option<String>,
    /// 此工具可在网络上搜索相关结果以用于生成回应
    ///
    /// 结构：
    /// ```json
    /// {
    ///   search_context_size?: "low|medium|high",
    ///   user_location?: {
    ///     approximate：{
    ///       city?: String,
    ///       country?: String,
    ///       region?: String,
    ///       timezone?: String,
    ///     },
    ///     type: "approximate",
    ///   },
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub web_search_options: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatCompletionContentPartImage {
    /// 图片url或者base64编码的图片
    pub url: String,
    /// 可选值："auto","low","high"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatCompletionContentPartInputAudio {
    pub data: String,
    /// 可选值："wav","mp3"
    pub format: String,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatCompletionAudioParam {
    /// 指定输出音频格式。必须是 wav、mp3、flac、opus或pcm16中的一种。
    pub format: String,
    /// `String` 或者 `{id:String}`
    ///
    /// 模型用于回应的声音。
    /// 支持的内置语音有 alloy、ash、ballad、coral、echo、fable、nova、onyx、sage、shimmer、marin 和 cedar。
    /// 您也可以提供一个自定义的语音对象，其中包含一个 id，例如 { "id": "voice_1234" }。
    pub voice: Value,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileContentPart {
    /// base64编码的文件数据，当作为字符串将文件传递给模型时使用。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_data: Option<String>,
    /// 已上传文件的ID，用作输入。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_id: Option<String>,
    /// 文件名，当以字符串形式将文件传递给模型时使用。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContentObject {
    pub r#type: String,

    /// [Self::type] == "text" 时传入
    ///
    /// 文本内容
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,

    /// [Self::type] == "image_url" 时传入
    ///
    /// 图片url或者base64编码的图片
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_url: Option<ChatCompletionContentPartImage>,

    /// [Self::type] == "input_audio" 时传入
    ///
    /// 音频数据
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_audio: Option<ChatCompletionContentPartInputAudio>,

    /// [Self::type] == "file" 时传入
    ///
    /// 文件内容
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<FileContentPart>,

    /// [Self::type] == "refusal" 时传入.
    /// 存在 role: "assistant" 中。
    ///
    /// 模型生成的拒绝消息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum Content {
    Text(String),
    Objects(Vec<ContentObject>),
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TokenUsage {
    pub completion_tokens: i32,
    pub prompt_tokens: i32,
    pub total_tokens: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatCompletionMessageParam {
    /// 消息具体内容
    pub content: Content,

    /// 消息作者的角色
    pub role: Role,

    /// 参与者的可选名称。为模型提供信息，以区分同一角色的不同参与者。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// role: "assistant"
    ///
    /// 格式
    /// ```json
    /// {id: String}
    /// ```
    /// 关于模型先前音频响应的数据。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<Value>,

    /// role: "assistant"
    ///
    /// 模型生成的工具调用。在函数调用中，需要将该消息放到消息上下文中
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ChatCompletionMessageToolCall>>,

    /// role: "tool"
    ///
    /// 用于标识工具调用的 id，此时 content 为本地函数调用的返回值，需要将该消息放到消息上下文中。
    /// 模型将调用结果转换为自然语言输出
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ToolCallRequestParams {
    /// 要调用的函数名称
    pub name: String,

    /// 函数描述。模型根据此描述决定是否调用该函数。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// 描述函数参数的 JSON schema 对象
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,

    /// 是否强制执行严格的参数验证
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

/// 发送给模型的工具调用参数
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ToolCallRequest {
    /// 工具调用类型，固定值为 "function"
    pub r#type: String,

    /// 工具调用请求参数
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
    pub created: i64,

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
    pub created: i64,

    /// 用于聊天补全的模型名称。
    pub model: String,
    pub object: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,

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
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatCompletionMessage {
    /// 消息内容
    /// 流式消息中该字段可能为空
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    /// 消息作者的角色。
    /// 流式消息中该字段可能为空
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<Role>,
    /// 模型可能返回的推理消息
    ///
    /// TODO: 非标准，OpenAI无此字段
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    /// 消息的注释（如适用），例如在使用网络搜索工具时。
    /// # JSON 结构
    /// ```json
    /// {
    ///     "type": "url_citation",
    ///     "url_citation": {
    ///         "end_index": number,
    ///         "start_index": number,
    ///         "title": "string",
    ///         "url": "string"
    ///     }
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Value>,
    /// 如果请求了音频输出模式，此对象包含模型音频响应的相关数据。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<ChatCompletionMessageAudio>,
    /// 模型返回的工具调用参数
    #[serde(skip_serializing_if = "is_none_or_empty_vec")]
    pub tool_calls: Option<Vec<ChatCompletionMessageToolCall>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatCompletionMessageFunctionToolCallFunction {
    /// 模型选择的本地函数名称
    pub name: String,

    /// 调用函数时使用的参数，由模型生成的 JSON 格式字符串。
    /// 注意：模型生成的 JSON 不一定总是有效的，且可能产生函数模式中未定义的参数。
    /// 在调用函数前，请务必在代码中验证这些参数。
    /// 需要通过 JSON schema 进行验证
    pub arguments: String,
}

/// 模型生成的音频数据
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatCompletionMessageAudio {
    /// 唯一标识符
    pub id: String,
    /// 由模型生成的Base64编码音频字节，格式遵循请求中的规定。
    pub data: String,
    /// 此音频响应在服务器上不再可用于多轮对话的Unix时间戳（以秒为单位）。
    pub expires_at: i32,
    /// 模型生成的音频转录文本
    pub transcript: Option<String>,
}

/// 对模型创建的函数工具的一次调用。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatCompletionMessageFunctionToolCall {
    /// 工具调用的唯一标识符
    pub id: String,
    pub function: ChatCompletionMessageFunctionToolCallFunction,
    /// 模型调用的函数信息
    /// 工具调用类型，固定值为 "function"
    pub r#type: String,
}
/// 模型生成的工具调用
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum ChatCompletionMessageToolCall {
    Function(ChatCompletionMessageFunctionToolCall),
    Custom(ChatCompletionMessageCustomToolCall),
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatCompletionMessageCustomToolCallFunction {
    /// 模型生成的自定义工具调用的输入。
    pub input: String,
    /// 要调用的自定义工具的名称。
    pub name: String,
}

/// 对模型创建的自定义工具的调用。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatCompletionMessageCustomToolCall {
    /// 工具调用的唯一标识符
    pub id: String,
    /// 模型调用的函数信息
    pub custom: ChatCompletionMessageCustomToolCallFunction,
    /// 工具调用类型，固定值为 "custom"
    pub r#type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatCompletionChoice {
    pub finish_reason: String,
    pub index: i32,
    pub message: ChatCompletionMessage,
    /// # JSON 结构
    /// ```json
    /// {
    ///     "content": [
    ///         {
    ///             "token": "string",
    ///             "bytes": [number] | null,
    ///             "logprob": number,
    ///             "top_logprobs": [
    ///                 {
    ///                     "token": "string",
    ///                     "bytes": [number] | null,
    ///                     "logprob": number
    ///                 }
    ///             ]
    ///         }
    ///     ],
    ///     "refusal": [
    ///         {
    ///             "token": "string",
    ///             "bytes": [number] | null,
    ///             "logprob": number,
    ///             "top_logprobs": [
    ///                 {
    ///                     "token": "string",
    ///                     "bytes": [number] | null,
    ///                     "logprob": number
    ///                 }
    ///             ]
    ///         }
    ///     ]
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatStreamCompletionChoice {
    /// 由流式模型响应生成的聊天完成增量片段
    pub delta: ChatCompletionMessage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    pub index: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<Value>,
}
