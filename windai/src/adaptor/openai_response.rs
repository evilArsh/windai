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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<InputItem>,

    /// 插入到模型上下文中的系统（或开发者）消息。
    /// 与 `previous_response_id` 一起使用时，先前响应的指令不会延续到下一个响应。
    /// 这使得在新响应中替换系统（或开发者）消息变得简单。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,

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

    /// 可附加到对象的16个键值对集合。
    /// 这对于以结构化格式存储有关对象的附加信息非常有用，
    /// 并且可以通过API或仪表板查询对象。
    /// 键是最大长度为64个字符的字符串。值是最大长度为512个字符的字符串。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,

    /// 模型名称。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// 是否允许模型并行运行工具调用。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,

    /// 模型先前响应的唯一ID。用于创建多轮对话。
    /// 不能与 `conversation` 同时使用。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_response_id: Option<String>,

    /// 对提示模板及其变量的引用。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<Value>,

    /// 由OpenAI用于缓存类似请求的响应以优化缓存命中率。替换 `user` 字段。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_key: Option<String>,

    /// 提示缓存的保留策略。设置为 `24h` 以启用扩展提示缓存，
    /// 这将使缓存的前缀保持更长时间，最长可达24小时。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_retention: Option<String>,

    /// 模型推理开关
    pub reasoning: Option<CreateResponseReasoning>,

    /// 用于帮助检测可能违反OpenAI使用政策的应用程序用户的稳定标识符。
    /// ID应为唯一标识每个用户的字符串，最大长度为64个字符。
    /// 我们建议对其用户名或电子邮件地址进行哈希处理，以避免向我们发送任何识别信息。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safety_identifier: Option<String>,

    /// 指定用于服务请求的处理类型。
    /// - 如果设置为 'auto'，则请求将使用项目设置中配置的服务层级进行处理。
    ///   除非另有配置，否则项目将使用 'default'。
    /// - 如果设置为 'default'，则请求将使用所选模型的标准定价和性能进行处理。
    /// - 如果设置为 '[flex](/docs/guides/flex-processing)' 或 '[priority](https://openai.com/api-priority-processing/)'，
    ///   则请求将使用相应的服务层级进行处理。
    /// - 未设置时，默认行为是 'auto'。
    /// 当设置 `service_tier` 参数时，响应体将包含基于实际用于服务请求的处理模式的 `service_tier` 值。
    /// 此响应值可能与参数中设置的值不同。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,

    /// 是否存储生成的模型响应以供以后通过API检索。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store: Option<bool>,

    /// 若设置为 true，模型生成的响应数据将通过服务器发送事件（server-sent events）实时流式传输至客户端。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,

    /// stream:true 时生效
    ///
    /// 值为
    /// ```json
    /// {
    ///   include_obfuscation?:boolean,
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<Value>,

    /// 采样温度应在0到2之间选择。较高的数值（如0.8）会使输出更具随机性，而较低的数值（如0.2）则会使输出更加聚焦和确定。
    /// 通常建议调整温度参数或top_p参数，但不要同时调整两者。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,

    /// 模型文本响应的配置选项。可以是纯文本或结构化JSON数据
    ///
    /// JSON 结构示例：
    /// ```json
    /// {
    ///   "format": {
    ///     "type": "json_schema",
    ///     "name": "weather_response",
    ///     "schema": {
    ///       "type": "object",
    ///       "properties": {
    ///         "temperature": { "type": "number" },
    ///         "condition": { "type": "string" }
    ///       },
    ///       "required": ["temperature", "condition"]
    ///     },
    ///     "description": "天气信息响应格式",
    ///     "strict": true
    ///   },
    ///   "verbosity": "medium"
    /// }
    /// ```
    ///
    /// 或使用默认文本格式：
    /// ```json
    /// {
    ///   "format": {
    ///     "type": "text"
    ///   },
    ///   "verbosity": "low"
    /// }
    /// ```
    ///
    /// 或使用旧版 JSON 对象格式：
    /// ```json
    /// {
    ///   "format": {
    ///     "type": "json_object"
    ///   }
    /// }
    /// ```
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<Value>,

    /// 模型可以调用的工具列表
    #[builder(default)]
    #[serde(skip_serializing_if = "is_none_or_empty_vec")]
    pub tools: Option<Vec<ToolCallRequest>>,

    /// 一种替代温度采样的方法是核采样，在这种方法中，模型仅考虑那些累积概率达到 top_p 的候选token。
    /// 例如，当 top_p 设置为 0.1 时，模型只会考虑那些累计概率质量达到前 10% 的token。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,

    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_logprobs: Option<f64>,

    /// - auto: 如果此响应的输入超出模型的上下文窗口大小，模型将通过丢弃对话开头的项目来截断响应，以适应上下文窗口。
    /// - disabled（默认）：如果输入大小将超出模型的上下文窗口大小，请求将以400错误失败。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation: Option<String>,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
/// 输入模型的消息带有指示遵循角色层次结构的指令。
/// 以开发者或系统角色给出的指令优先于以用户角色给出的指令。
/// 带有助手角色的消息被假定为模型在先前交互中生成的。
pub struct EasyInputMessage {
    /// 模型接收的文本、图像或音频输入，用于生成回应。也可包含先前的助手回应。
    pub content: EasyInputContent,
    /// 角色
    ///
    /// role: "user" or "assistant" or "system" or "developer"
    pub role: Role,
    /// 将助手消息标记为中间评论（commentary）或最终答案（final_answer）。
    /// 对于像gpt-5.3-codex及更高版本的模型，在发送后续请求时，需保留并重新发送所有助手消息的阶段标签——删除它们可能会降低性能。
    /// 此标签不用于用户消息。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// 总是 "message"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
/// 输入模型的消息带有指示遵循角色层级的指令。
/// 以开发者或系统角色给出的指令优先于以用户角色给出的指令。
pub struct Message {
    /// 模型接收的文本、图像或音频输入，用于生成回应。也可包含先前的助手回应。
    ///
    /// ! 不包含 `EasyInputContent::TextInput`
    pub content: EasyInputContent,
    /// 角色
    ///
    /// role: "user" or "system" or "developer"
    pub role: Role,
    /// 项目状态。可选值为 in_progress, completed, incomplete
    /// 通过API返回项目列表时，该字段将被填充。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// 总是 "message"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

/// 输入内容的变体
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum InputItem {
    TextInput(String),
    EasyInput(Vec<EasyInputMessage>),
    Message(Vec<Message>),
    ResponseOutputMessage(Vec<ResponseOutputMessage>),
    FileSearchCall(Vec<FileSearchCall>),
    ComputerCall(Vec<ComputerCall>),
    ComputerCallOutput(Vec<ComputerCallOutput>),
    WebSearchCall(Vec<WebSearchCall>),
    FunctionCall(Vec<FunctionCall>),
    FunctionCallOutput(Vec<FunctionCallOutput>),
    ToolSearchCall(Vec<ToolSearchCall>),
    ToolSearchOutput(Vec<ToolSearchOutput>),
    Reasoning(Vec<Reasoning>),
    Compaction(Vec<Compaction>),
    ImageGenerationCall(Vec<ImageGenerationCall>),
    CodeInterpreterCall(Vec<CodeInterpreterCall>),
    LocalShellCall(Vec<LocalShellCall>),
    LocalShellCallOutput(Vec<LocalShellCallOutput>),
    ShellCall(Vec<ShellCall>),
    ShellCallOutput(Vec<ShellCallOutput>),
    ApplyPatchCall(Vec<ApplyPatchCall>),
    ApplyPatchCallOutput(Vec<ApplyPatchCallOutput>),
    McpListTools(Vec<McpListTools>),
    McpApprovalRequest(Vec<McpApprovalRequest>),
    McpApprovalResponse(Vec<McpApprovalResponse>),
    McpCall(Vec<McpCall>),
    CustomToolCallOutput(Vec<CustomToolCallOutput>),
    CustomToolCall(Vec<CustomToolCall>),
    ItemReference(Vec<ItemReference>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseInputText {
    text: String,
    /// 总是 "input_text"
    r#type: String,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseInputImage {
    /// One of `high`, `low`, `auto`, or `original`. Defaults to `auto`.
    detail: Option<String>,
    /// "input_image"
    r#type: String,
    file_id: Option<String>,
    /// 要发送给模型的图像的URL。可以是完全限定的URL，也可以是数据URL中base64编码的图像。
    image_url: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseInputFile {
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
}
/// 易用的输入消息结构
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum EasyInputContent {
    TextInput(String),
    ResponseInputText(Vec<ResponseInputText>),
    ResponseInputImage(Vec<ResponseInputImage>),
    ResponseInputFile(Vec<ResponseInputFile>),
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
/// 文件搜索工具调用结果对象
/// 文件搜索工具调用的结果。有关更多信息，请参阅文件搜索指南。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FileSearchCall {
    /// 文件搜索工具调用的唯一ID
    pub id: String,
    /// 用于搜索文件的查询数组
    pub queries: Vec<String>,
    /// 文件搜索工具调用的状态。可能为in_progress、searching、completed、incomplete或failed之一
    pub status: String,
    /// 文件搜索工具调用的类型。始终为file_search_call
    #[serde(rename = "type")]
    pub type_field: String,
    /// 文件搜索工具调用的结果
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<Value>,
}
/// 计算机使用工具调用对象
/// 计算机使用工具的调用。有关更多信息，请参阅计算机使用指南。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ComputerCall {
    /// 计算机调用的唯一ID
    pub id: String,
    /// 使用输出响应工具调用时使用的标识符
    pub call_id: String,
    /// 计算机调用的待处理安全检查数组
    pub pending_safety_checks: Value,
    /// 项目的状态。可能为in_progress、completed或incomplete之一。通过API返回项目时填充
    pub status: String,
    /// 计算机调用的类型。始终为computer_call
    #[serde(rename = "type")]
    pub type_field: String,
    /// 点击操作
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<Value>,
    /// 计算机使用的扁平化批处理操作。每个操作包括类型鉴别器和特定于操作的字段
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actions: Option<Value>,
}
/// 计算机工具调用输出对象
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ComputerCallOutput {
    /// 产生输出的计算机工具调用的ID
    pub call_id: String,
    /// 与计算机使用工具一起使用的计算机截图图像
    pub output: Value,
    /// 计算机工具调用输出的类型。始终为computer_call_output
    #[serde(rename = "type")]
    pub type_field: String,
    /// 计算机工具调用输出的ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 开发者已确认的API报告的安全检查
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acknowledged_safety_checks: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}
/// 网络搜索工具调用结果对象
/// 网络搜索工具调用的结果。有关更多信息，请参阅[网络搜索指南](/docs/guides/tools-web-search)。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WebSearchCall {
    /// 网络搜索工具调用的唯一ID
    pub id: String,
    /// 描述此网络搜索调用中采取的具体操作的对象
    /// 包括模型如何使用网络（搜索、打开页面、在页面中查找）的详细信息
    pub action: Value,
    /// 网络搜索工具调用的状态
    pub status: String,
    /// 网络搜索工具调用的类型。始终为web_search_call
    #[serde(rename = "type")]
    pub type_field: String,
}
/// 函数调用工具调用对象
/// 用于运行函数的工具调用。有关更多信息，请参阅函数调用指南。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FunctionCall {
    /// 要传递给函数的参数的JSON字符串
    pub arguments: String,
    /// 模型生成的函数工具调用的唯一ID
    pub call_id: String,
    /// 要运行的函数名称
    pub name: String,
    /// 函数工具调用的类型。始终为function_call
    #[serde(rename = "type")]
    pub type_field: String,
    /// 函数工具调用的唯一ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 要运行的函数的命名空间
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// 项目的状态。可能为in_progress、completed或incomplete之一。通过API返回项目时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}
/// 函数工具调用的输出
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FunctionCallOutput {
    /// 模型生成的函数工具调用的唯一ID
    pub call_id: String,
    /// 函数工具调用的文本、图像或文件输出
    /// ```json
    /// string | array<ResponseInputTextContent | ResponseInputImageContent | ResponseInputFileContent>
    /// ```
    pub output: Value,
    /// 函数工具调用输出的类型。始终为function_call_output
    #[serde(rename = "type")]
    pub type_field: String,
    /// 函数工具调用输出的唯一ID。通过API返回此项时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 项目的状态。in_progress、completed或incomplete之一。通过API返回项目时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// 工具搜索调用
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ToolSearchCall {
    /// 提供给工具搜索调用的参数
    pub arguments: Value,
    /// 项目类型。始终为tool_search_call
    #[serde(rename = "type")]
    pub type_field: String,
    /// 此工具搜索调用的唯一ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 模型生成的工具搜索调用的唯一ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    /// 工具搜索是由服务器执行还是由客户端执行
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution: Option<String>,
    /// 工具搜索调用的状态
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// 工具搜索输出
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ToolSearchOutput {
    /// 工具搜索输出返回的加载的工具定义
    /// ```json
    /// array<Function
    /// | FileSearch
    /// | Computer
    /// | ComputerUsePreview
    /// | WebSearch
    /// | Mcp
    /// | CodeInterpreter
    /// | ImageGeneration
    /// | LocalShell
    /// | Shell
    /// | Custom
    /// | Namespace
    /// | ToolSearch
    /// | WebSearchPreview
    /// | ApplyPatch>
    /// ```
    pub tools: Value,
    /// 项目类型。始终为tool_search_output
    #[serde(rename = "type")]
    pub type_field: String,
    /// 此工具搜索输出的唯一ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 模型生成的工具搜索调用的唯一ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    /// 工具搜索是由服务器执行还是由客户端执行
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution: Option<String>,
    /// 工具搜索输出的状态
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// 推理模型生成响应时使用的思维链描述
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Reasoning {
    /// 推理内容的唯一标识符
    pub id: String,
    /// 推理摘要内容
    /// ```json
    /// array<SummaryTextContent>
    /// ```
    pub summary: Value,
    /// 对象的类型。始终为reasoning
    #[serde(rename = "type")]
    pub type_field: String,
    /// 推理文本内容
    /// ```json
    /// array<{ text: string, type: "reasoning_text" }>
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<Value>,
    /// 推理项目的加密内容 - 当响应在include参数中包含reasoning.encrypted_content时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encrypted_content: Option<String>,
    /// 项目的状态。in_progress、completed或incomplete之一。通过API返回项目时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// 由v1/responses/compact API生成的压缩项目
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Compaction {
    /// 压缩摘要的加密内容
    pub encrypted_content: String,
    /// 项目的类型。始终为compaction
    #[serde(rename = "type")]
    pub type_field: String,
    /// 压缩项目的ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// 模型发出的图像生成请求
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ImageGenerationCall {
    /// 图像生成调用的唯一ID
    pub id: String,
    /// 以base64编码的生成图像
    pub result: String,
    /// 图像生成调用的状态
    pub status: String,
    /// 图像生成调用的类型。始终为image_generation_call
    #[serde(rename = "type")]
    pub type_field: String,
}

/// 运行代码的工具调用
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CodeInterpreterCall {
    /// 代码解释器工具调用的唯一ID
    pub id: String,
    /// 要运行的代码，如果不可用则为null
    pub code: String,
    /// 用于运行代码的容器的ID
    pub container_id: String,
    /// 代码解释器生成的输出，例如日志或图像。如果没有可用输出，则可以为null
    /// ```json
    /// array<Logs | Image>
    /// ```
    pub outputs: Value,
    /// 代码解释器工具调用的状态。有效值为in_progress、completed、incomplete、interpreting和failed
    pub status: String,
    /// 代码解释器工具调用的类型。始终为code_interpreter_call
    #[serde(rename = "type")]
    pub type_field: String,
}

/// 在本地shell上运行命令的工具调用
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct LocalShellCall {
    /// 本地shell调用的唯一ID
    pub id: String,
    /// 在服务器上执行shell命令
    /// ```json
    /// {
    ///   "command": array<string>,
    ///   "env": map<string>,
    ///   "type": "exec",
    ///   "timeout_ms": number?,
    ///   "user": string?,
    ///   "working_directory": string?
    /// }
    /// ```
    pub action: Value,
    /// 模型生成的本地shell工具调用的唯一ID
    pub call_id: String,
    /// 本地shell调用的状态
    pub status: String,
    /// 本地shell调用的类型。始终为local_shell_call
    #[serde(rename = "type")]
    pub type_field: String,
}
/// 本地shell工具调用的输出
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct LocalShellCallOutput {
    /// 模型生成的本地shell工具调用的唯一ID
    pub id: String,
    /// 本地shell工具调用的输出JSON字符串
    pub output: String,
    /// 本地shell工具调用输出的类型。始终为local_shell_call_output
    #[serde(rename = "type")]
    pub type_field: String,
    /// 项目的状态。in_progress、completed或incomplete之一
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// 表示执行一个或多个shell命令请求的工具
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ShellCall {
    /// 描述如何运行工具调用的shell命令和限制
    /// ```json
    /// {
    ///   "commands": array<string>,
    ///   "max_output_length": number?,
    ///   "timeout_ms": number?
    /// }
    /// ```
    pub action: Value,
    /// 模型生成的shell工具调用的唯一ID
    pub call_id: String,
    /// 项目的类型。始终为shell_call
    #[serde(rename = "type")]
    pub type_field: String,
    /// shell工具调用的唯一ID。通过API返回此项时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 执行shell命令的环境
    /// ```json
    /// LocalEnvironment | ContainerReference
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment: Option<Value>,
    /// shell调用的状态。in_progress、completed或incomplete之一
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// shell工具调用发出的流式输出项目
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ShellCallOutput {
    /// 模型生成的shell工具调用的唯一ID
    pub call_id: String,
    /// 捕获的stdout和stderr输出块及其相关结果
    /// ```json
    /// array<ResponseFunctionShellCallOutputContent>
    /// ```
    pub output: Value,
    /// 项目的类型。始终为shell_call_output
    #[serde(rename = "type")]
    pub type_field: String,
    /// shell工具调用输出的唯一ID。通过API返回此项时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 为此shell调用的组合输出捕获的最大UTF-8字符数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_length: Option<Value>,
    /// shell调用输出的状态
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// 表示使用差异补丁创建、删除或更新文件请求的工具调用
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ApplyPatchCall {
    /// 模型生成的apply patch工具调用的唯一ID
    pub call_id: String,
    /// apply_patch工具调用的特定创建、删除或更新指令
    /// ```json
    /// CreateFile | DeleteFile | UpdateFile
    /// ```
    pub operation: Value,
    /// apply patch工具调用的状态。in_progress或completed之一
    pub status: String,
    /// 项目的类型。始终为apply_patch_call
    #[serde(rename = "type")]
    pub type_field: String,
    /// apply patch工具调用的唯一ID。通过API返回此项时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// apply patch工具调用发出的流式输出
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ApplyPatchCallOutput {
    /// 模型生成的apply patch工具调用的唯一ID
    pub call_id: String,
    /// apply patch工具调用输出的状态。completed或failed之一
    pub status: String,
    /// 项目的类型。始终为apply_patch_call_output
    #[serde(rename = "type")]
    pub type_field: String,
    /// apply patch工具调用输出的唯一ID。通过API返回此项时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 来自apply patch工具的可选人类可读日志文本（例如，补丁结果或错误）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
}

/// MCP服务器上可用工具的列表
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct McpListTools {
    /// 列表的唯一ID
    pub id: String,
    /// MCP服务器的标签
    pub server_label: String,
    /// 服务器上可用的工具
    /// ```json
    /// array<{
    ///   "input_schema": unknown,
    ///   "name": string,
    ///   "annotations": unknown?,
    ///   "description": string?
    /// }>
    /// ```
    pub tools: Value,
    /// 项目的类型。始终为mcp_list_tools
    #[serde(rename = "type")]
    pub type_field: String,
    /// 如果服务器无法列出工具，则为错误消息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// 工具调用的人工批准请求
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct McpApprovalRequest {
    /// 批准请求的唯一ID
    pub id: String,
    /// 工具参数的JSON字符串
    pub arguments: String,
    /// 要运行的工具的名称
    pub name: String,
    /// 发出请求的MCP服务器的标签
    pub server_label: String,
    /// 项目的类型。始终为mcp_approval_request
    #[serde(rename = "type")]
    pub type_field: String,
}

/// MCP批准请求的响应
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct McpApprovalResponse {
    /// 正在回答的批准请求的ID
    pub approval_request_id: String,
    /// 请求是否被批准
    pub approve: bool,
    /// 项目的类型。始终为mcp_approval_response
    #[serde(rename = "type")]
    pub type_field: String,
    /// 批准响应的唯一ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 决策的可选原因
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// MCP服务器上工具的调用
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct McpCall {
    /// 工具调用的唯一ID
    pub id: String,
    /// 传递给工具的参数的JSON字符串
    pub arguments: String,
    /// 运行的工具体名称
    pub name: String,
    /// 运行工具的MCP服务器的标签
    pub server_label: String,
    /// 项目的类型。始终为mcp_call
    #[serde(rename = "type")]
    pub type_field: String,
    /// MCP工具调用批准请求的唯一标识符
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_request_id: Option<String>,
    /// 工具调用的错误（如果有）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// 工具调用的输出
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// 工具调用的状态。in_progress、completed、incomplete、calling或failed之一
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// 从您的代码发送回模型的自定义工具调用的输出
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CustomToolCallOutput {
    /// 用于将此自定义工具调用输出映射到自定义工具调用的调用ID
    pub call_id: String,
    /// 由您的代码生成的自定义工具调用的输出
    /// ```json
    /// string | array<ResponseInputText | ResponseInputImage | ResponseInputFile>
    /// ```
    pub output: Value,
    /// 自定义工具调用输出的类型。始终为custom_tool_call_output
    #[serde(rename = "type")]
    pub type_field: String,
    /// 自定义工具调用输出在OpenAI平台中的唯一ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// 模型创建的自定义工具的调用
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CustomToolCall {
    /// 用于将此自定义工具调用映射到工具调用输出的标识符
    pub call_id: String,
    /// 模型生成的自定义工具调用的输入
    pub input: String,
    /// 被调用的自定义工具的名称
    pub name: String,
    /// 自定义工具调用的类型。始终为custom_tool_call
    #[serde(rename = "type")]
    pub type_field: String,
    /// 自定义工具调用在OpenAI平台中的唯一ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 被调用的自定义工具的命名空间
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// 用于引用项目的内部标识符
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ItemReference {
    /// 要引用的项目的ID
    pub id: String,
    /// 要引用的项目的类型。始终为item_reference
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_field: Option<String>,
}
/// 模型响应内容
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseOutputMessage {
    /// 响应消息的唯一标识符。
    pub id: Option<String>,
    /// 模型响应的文本内容
    pub content: ResponseOutput,
    /// 模型输出的角色。
    pub role: Role,
    /// 状态："in_progress" or "completed" or "incomplete"
    pub status: Option<String>,
    /// 模型输出的类型, 总是 message
    pub r#type: String,
    ///  "commentary" or "final_answer"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum ResponseOutput {
    ResponseOutputText(ResponseOutputText),
    ResponseOutputRefusal(ResponseOutputRefusal),
}
/// 模型的文本输出。
/// 参考：https://developers.openai.com/api/reference/resources/responses/methods/create
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseOutputText {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Value::Object 类型
    pub annotations: Option<Annotations>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Value::Array
    pub logprobs: Option<Value>,
    /// 模型的文本输出
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Value::String 类型
    pub r#type: Option<String>,
    /// 模型拒绝响应。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<ResponseOutputRefusal>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseOutputRefusal {
    pub r#type: String,
    pub refusal: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Annotations {
    FileCitation(FileCitation),
    URLCitation(URLCitation),
    ContainerFileCitation(ContainerFileCitation),
    FilePath(FilePath),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCitation {
    /// The ID of the file.
    pub file_id: String,
    /// The filename of the file cited.
    pub filename: String,
    /// The index of the file in the list of files.
    pub index: i32,
    /// The type of the file citation. Always "file_citation".
    #[serde(rename = "type")]
    pub citation_type: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct URLCitation {
    /// URL引用在消息中最后一个字符的索引
    pub end_index: i32,
    /// URL引用在消息中第一个字符的索引
    pub start_index: i32,
    /// 网络资源的标题
    pub title: String,
    /// URL引用的类型。始终为 "url_citation"
    #[serde(rename = "type")]
    pub citation_type: String,
    /// 网络资源的URL
    pub url: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerFileCitation {
    /// 容器文件的ID
    pub container_id: String,
    /// 容器文件引用在消息中最后一个字符的索引
    pub end_index: i32,
    /// 文件的ID
    pub file_id: String,
    /// 被引用的容器文件名
    pub filename: String,
    /// 容器文件引用在消息中第一个字符的索引
    pub start_index: i32,
    /// 容器文件引用的类型。始终为 "container_file_citation"
    #[serde(rename = "type")]
    pub citation_type: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilePath {
    /// 文件的ID
    pub file_id: String,
    /// 文件在文件列表中的索引
    pub index: i32,
    /// 文件路径的类型。始终为 "file_path"
    #[serde(rename = "type")]
    pub path_type: String,
}
