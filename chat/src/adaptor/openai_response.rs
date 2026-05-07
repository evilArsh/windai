//! OpenAI Response API 数据结构
//! https://developers.openai.com/api/reference/resources/responses/methods/create

use super::is_none_or_empty_vec;
use crate::Role;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ======================================================
// Responses 请求
// ======================================================

/// 响应创建请求的主结构体
#[derive(Debug, Serialize, Default, Clone, Builder)]
pub struct ResponseRequest {
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
    pub input: Vec<InputItem>,

    /// 插入到模型上下文中的系统（或开发者）消息。
    /// 与 `previous_response_id` 一起使用时，先前响应的指令不会延续到下一个响应。
    /// 这使得在新响应中替换系统（或开发者）消息变得简单。
    #[builder(default)]
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
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,

    /// 模型名称。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// 是否允许模型并行运行工具调用。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,

    /// 模型先前响应的唯一ID。用于创建多轮对话。
    /// 不能与 `conversation` 同时使用。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_response_id: Option<String>,

    /// 对提示模板及其变量的引用。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<Value>,

    /// 由OpenAI用于缓存类似请求的响应以优化缓存命中率。替换 `user` 字段。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_key: Option<String>,

    /// 提示缓存的保留策略。设置为 `24h` 以启用扩展提示缓存，
    /// 这将使缓存的前缀保持更长时间，最长可达24小时。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_retention: Option<String>,

    /// 模型推理开关
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<ResponseReasoning>,

    /// 用于帮助检测可能违反OpenAI使用政策的应用程序用户的稳定标识符。
    /// ID应为唯一标识每个用户的字符串，最大长度为64个字符。
    /// 我们建议对其用户名或电子邮件地址进行哈希处理，以避免向我们发送任何识别信息。
    #[builder(default)]
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
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,

    /// 是否存储生成的模型响应以供以后通过API检索。
    #[builder(default)]
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
    #[builder(default)]
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

    /// 模型在生成响应时应如何选择使用哪个工具（或哪些工具）
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,

    /// 模型可以调用的工具列表
    #[builder(default)]
    #[serde(skip_serializing_if = "is_none_or_empty_vec")]
    pub tools: Option<Vec<Tools>>,

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

/// 模型在生成响应时应如何选择使用哪个工具（或哪些工具）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ToolChoice {
    /// 工具选择选项
    Options(String),
    /// 允许的工具配置
    Allowed(ToolChoiceAllowed),
    /// 内置工具类型
    Types(ToolChoiceTypes),
    /// 函数工具选择
    Function(ToolChoiceFunction),
    /// MCP工具选择
    Mcp(ToolChoiceMcp),
    /// 自定义工具选择
    Custom(ToolChoiceCustom),
    /// 应用补丁工具选择
    ApplyPatch(ToolChoiceApplyPatch),
    /// Shell工具选择
    Shell(ToolChoiceShell),
}

/// 将模型可用的工具约束到预定义集合
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolChoiceAllowed {
    /// 将模型可用的工具约束到预定义集合
    /// auto允许模型从允许的工具中选择并生成消息
    /// required要求模型调用一个或多个允许的工具
    pub mode: String,
    /// 模型应被允许调用的工具定义列表
    /// ```json
    /// array<map<unknown>>
    /// ```
    pub tools: Value,
    /// 允许的工具配置类型。始终为 allowed_tools
    pub r#type: String,
}

/// 表示模型应使用内置工具生成响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolChoiceTypes {
    /// 模型应使用的托管工具类型
    pub r#type: String,
}

/// 使用此选项强制模型调用特定函数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolChoiceFunction {
    /// 要调用的函数的名称
    pub name: String,
    /// 对于函数调用，类型始终为function
    pub r#type: String,
}

/// 使用此选项强制模型调用远程MCP服务器上的特定工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolChoiceMcp {
    /// 要使用的MCP服务器的标签
    pub server_label: String,
    /// 对于MCP工具，类型始终为mcp
    pub r#type: String,
    /// 要在服务器上调用的工具的名称
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// 使用此选项强制模型调用特定的自定义工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolChoiceCustom {
    /// 要调用的自定义工具的名称
    pub name: String,
    /// 对于自定义工具调用，类型始终为custom
    pub r#type: String,
}

/// 强制模型在执行工具调用时调用apply_patch工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolChoiceApplyPatch {
    /// 要调用的工具。始终为apply_patch
    pub r#type: String,
}

/// 强制模型在需要工具调用时调用shell工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolChoiceShell {
    /// 要调用的工具。始终为shell
    pub r#type: String,
}

#[derive(Debug, Serialize, Default, Clone, Builder)]
pub struct ResponseReasoning {
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
    pub effort: Option<String>,
}

/// 上下文管理配置
#[derive(Debug, Serialize, Default, Clone, Builder)]
pub struct ContextManagementConfig {
    ///  entry 类型。目前仅支持 'compaction'（压缩）。
    #[builder(default = "String::from(\"compaction\")")]
    pub r#type: String,

    /// 触发压缩的 Token 阈值。最小值通常为 1000。
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compact_threshold: Option<f64>,
}

/// 会话参数，支持直接传入会话 ID 或对象
#[derive(Debug, Serialize, Clone)]
#[serde(untagged)]
pub enum ConversationParam {
    /// 唯一的会话 ID 字符串
    Id(String),
    /// 包含 ID 字段的结构化对象
    Object { id: String },
}

/// 可选包含的附加输出项枚举
#[derive(Debug, Serialize, Clone)]
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

#[derive(Debug, Serialize, Clone)]
/// 输入模型的消息带有指示遵循角色层级的指令。
/// 以开发者或系统角色给出的指令优先于以用户角色给出的指令。
///
/// Message 和 EasyInputMessage 合并
pub struct Message {
    /// 模型接收的文本、图像或音频输入，用于生成回应。也可包含先前的助手回应。
    ///
    pub content: Vec<InputContent>,
    /// 角色
    ///
    /// role: "user" or "system" or "developer"
    pub role: Role,
    /// 将助手消息标记为中间评论（commentary）或最终答案（final_answer）。
    /// 对于像gpt-5.3-codex及更高版本的模型，在发送后续请求时，需保留并重新发送所有助手消息的阶段标签——删除它们可能会降低性能。
    /// 此标签不用于用户消息。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// 项目状态。可选值为 in_progress, completed, incomplete
    /// 通过API返回项目列表时，该字段将被填充。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// 总是 "message"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

/// 输入内容的变体
#[derive(Debug, Serialize, Clone)]
#[serde(untagged)]
pub enum InputItem {
    // TextInput(String),
    // EasyInput(Vec<EasyInputMessage>),
    Message(Message),
    ResponseOutputMessage(ResponseOutputMessage),
    FileSearchCall(FileSearchCall),
    ComputerCall(ComputerCall),
    ComputerCallOutput(ComputerCallOutput),
    WebSearchCall(WebSearchCall),
    FunctionCall(FunctionCall),
    FunctionCallOutput(FunctionCallOutput),
    ToolSearchCall(ToolSearchCall),
    ToolSearchOutput(ToolSearchOutput),
    Reasoning(Reasoning),
    Compaction(Compaction),
    ImageGenerationCall(ImageGenerationCall),
    CodeInterpreterCall(CodeInterpreterCall),
    LocalShellCall(LocalShellCall),
    LocalShellCallOutput(LocalShellCallOutput),
    ShellCall(ShellCall),
    ShellCallOutput(ShellCallOutput),
    ApplyPatchCall(ApplyPatchCall),
    ApplyPatchCallOutput(ApplyPatchCallOutput),
    McpListTools(McpListTools),
    McpApprovalRequest(McpApprovalRequest),
    McpApprovalResponse(McpApprovalResponse),
    McpCall(McpCall),
    CustomToolCallOutput(CustomToolCallOutput),
    CustomToolCall(CustomToolCall),
    ItemReference(ItemReference),
}

/// 输出内容的变体
#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum OutputItem {
    ResponseOutputMessage(ResponseOutputMessage),
    FileSearchCall(FileSearchCall),
    ComputerCall(ComputerCall),
    ComputerCallOutput(ComputerCallOutput),
    WebSearchCall(WebSearchCall),
    FunctionCall(FunctionCall),
    FunctionCallOutput(FunctionCallOutput),
    ToolSearchCall(ToolSearchCall),
    ToolSearchOutput(ToolSearchOutput),
    Reasoning(Reasoning),
    Compaction(Compaction),
    ImageGenerationCall(ImageGenerationCall),
    CodeInterpreterCall(CodeInterpreterCall),
    LocalShellCall(LocalShellCall),
    LocalShellCallOutput(LocalShellCallOutput),
    ShellCall(ShellCall),
    ShellCallOutput(ShellCallOutput),
    ApplyPatchCall(ApplyPatchCall),
    ApplyPatchCallOutput(ApplyPatchCallOutput),
    McpListTools(McpListTools),
    McpApprovalRequest(McpApprovalRequest),
    McpApprovalResponse(McpApprovalResponse),
    McpCall(McpCall),
    CustomToolCallOutput(CustomToolCallOutput),
    CustomToolCall(CustomToolCall),
    ItemReference(ItemReference),
}

#[derive(Debug, Serialize, Clone)]
pub struct ResponseInputText {
    pub text: String,
    /// 总是 "input_text"
    pub r#type: String,
}
#[derive(Debug, Serialize, Clone)]
pub struct ResponseInputImage {
    /// One of `high`, `low`, `auto`, or `original`. Defaults to `auto`.
    pub detail: Option<String>,
    /// "input_image"
    pub r#type: String,
    pub file_id: Option<String>,
    /// 要发送给模型的图像的URL。可以是完全限定的URL，也可以是数据URL中base64编码的图像。
    pub image_url: Option<String>,
}
#[derive(Debug, Serialize, Clone)]
pub struct ResponseInputFile {
    /// 输入项的类型。始终为 "input_file"。
    pub r#type: String,
    /// 要发送给模型的文件内容。
    pub file_data: Option<String>,
    /// 要发送给模型的文件ID。
    pub file_id: Option<String>,
    /// 要发送给模型的文件URL。
    pub file_url: Option<String>,
    /// 要发送给模型的文件名。
    pub filename: Option<String>,
}
/// 易用的输入消息结构
#[derive(Debug, Serialize, Clone)]
#[serde(untagged)]
pub enum InputContent {
    ResponseInputText(ResponseInputText),
    ResponseInputImage(ResponseInputImage),
    ResponseInputFile(ResponseInputFile),
}

// ======================================================
// Responses 响应
// ======================================================

#[derive(Debug, Deserialize, Clone)]
pub struct TokenUsage {
    pub input_tokens: i32,
    pub output_tokens: i32,
    pub total_tokens: i32,
}

/// Responses 响应结构体
#[derive(Debug, Deserialize)]
pub struct Response {
    /// Responses 的唯一标识符。
    pub id: String,
    /// 聊天补全创建时间的 Unix 时间戳（秒级）。
    pub created_at: i64,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incomplete_details: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    /// 模型名称。
    pub model: String,
    pub object: String,
    /// 模型输出内容
    pub output: Vec<OutputItem>,
    pub parallel_tool_calls: bool,
    pub temperature: f64,
    /// 模型在生成响应时应如何选择使用哪个工具（或哪些工具）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,
    /// 模型可以调用的工具列表
    #[serde(skip_serializing_if = "is_none_or_empty_vec")]
    pub tools: Option<Vec<Tools>>,
    pub top_p: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub background: Option<bool>,
    /// 秒
    pub completed_at: Option<i32>,
    /// 此响应所属的对话
    /// ```json
    /// {
    ///   "id": string
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation: Option<Value>,

    /// 响应可以生成的 token 数量的上限，包括可见输出 token 和推理 token
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<Value>,

    /// 响应中可以处理的内置工具调用的最大总数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tool_calls: Option<Value>,

    /// SDK专用便利属性，包含输出数组中所有output_text项的聚合文本输出
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_text: Option<String>,

    /// 模型先前响应的唯一ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_response_id: Option<String>,

    /// 对提示模板及其变量的引用
    /// ```json
    /// {
    ///   "id": string,
    ///   "variables": map<string | ResponseInputText | ResponseInputImage | ResponseInputFile>?,
    ///   "version": string?
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<Value>,

    /// 用于缓存类似请求的响应以优化缓存命中率
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_key: Option<String>,

    /// 提示缓存的保留策略
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_retention: Option<String>,

    /// 推理模型的配置选项
    /// ```json
    /// {
    ///   "effort": "none" | "minimal" | "low" | "medium" | "high" | "xhigh"?,
    ///   "generate_summary": "auto" | "concise" | "detailed"?,
    ///   "summary": "auto" | "concise" | "detailed"?
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<Value>,

    /// 用于帮助检测可能违反OpenAI使用政策的应用程序用户的稳定标识符
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safety_identifier: Option<String>,

    /// 指定用于服务请求的处理类型
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,

    /// 响应生成的状态
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,

    /// 模型文本响应的配置选项
    /// ```json
    /// {
    ///   "format": ResponseFormatText | ResponseFormatTextJSONSchemaConfig | ResponseFormatJSONObject?,
    ///   "verbosity": "low" | "medium" | "high"?
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<Value>,

    /// 指定在每个 token 位置返回的最可能 token 数量，每个 token 都有相关的对数概率
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_logprobs: Option<u16>,

    /// 模型响应的截断策略
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation: Option<String>,

    /// 表示 token 使用详情，包括输入 token 、输出 token 、输出 token 的细分和使用的总 token
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<TokenUsage>,
}
#[derive(Debug, Deserialize)]
pub struct ResponseStream {
    /// 所有事件的响应
    pub r#type: String,
    /// 所有事件的响应
    pub sequence_number: i32,
    /// 事件
    /// - response.created
    /// - response.in_progress
    /// - response.in_progress
    /// - response.completed
    /// - response.failed
    /// - response.incomplete
    /// - response.queued
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<Response>,
    /// 事件
    /// - response.output_item.added
    /// - response.output_item.done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item: Option<OutputItem>,
    /// 事件
    /// - response.content_part.added
    /// - response.content_part.done
    /// - response.content_part.added
    /// - response.output_text.delta
    /// - response.output_text.done
    /// - response.refusal.delta
    /// - response.refusal.done
    /// - response.function_call_arguments.delta
    /// - response.function_call_arguments.done
    /// - response.file_search_call.in_progress
    /// - response.file_search_call.searching
    /// - response.file_search_call.completed
    /// - response.web_search_call.in_progress
    /// - response.web_search_call.searching
    /// - response.web_search_call.completed
    /// - response.reasoning_summary_part.added
    /// - response.reasoning_summary_part.done
    /// - response.reasoning_summary_text.delta
    /// - response.reasoning_summary_text.done
    /// - response.reasoning_text.delta
    /// - response.reasoning_text.done
    /// - response.image_generation_call.completed
    /// - response.image_generation_call.generating
    /// - response.image_generation_call.in_progress
    /// - response.image_generation_call.partial_image
    /// - response.mcp_call_arguments.delta
    /// - response.mcp_call_arguments.done
    /// - response.mcp_call.completed
    /// - response.mcp_call.failed
    /// - response.mcp_call.in_progress
    /// - response.mcp_list_tools.completed
    /// - response.mcp_list_tools.failed
    /// - response.mcp_list_tools.in_progress
    /// - response.code_interpreter_call.in_progress
    /// - response.code_interpreter_call.interpreting
    /// - response.code_interpreter_call.completed
    /// - response.code_interpreter_call_code.delta
    /// - response.code_interpreter_call_code.done
    /// - response.output_text.annotation.added
    /// - response.custom_tool_call_input.delta
    /// - response.custom_tool_call_input.done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    /// Vec 结构
    ///
    /// 事件
    /// - response.output_text.delta
    /// - response.output_text.done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_probs: Option<Value>,
    /// 事件
    /// - response.output_text.delta
    /// - response.refusal.delta
    /// - response.function_call_arguments.delta
    /// - response.reasoning_summary_text.delta
    /// - response.reasoning_text.delta
    /// - response.mcp_call_arguments.delta
    /// - response.code_interpreter_call_code.delta
    /// - response.custom_tool_call_input.delta
    /// - response.audio.transcript.delta
    /// - response.audio.delta
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<String>,
    /// - response.function_call_arguments.done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// - response.output_text.done
    /// - response.reasoning_text.done
    /// - response.reasoning_summary_text.done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// - response.refusal.done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    /// 事件
    /// - response.code_interpreter_call_code.delta
    /// - response.code_interpreter_call_code.done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// 事件
    /// - response.mcp_call_arguments.done
    /// - response.function_call_arguments.done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<String>,
    /// 事件
    /// - response.output_item.added
    /// - response.output_item.done
    /// - response.content_part.added
    /// - response.content_part.done
    /// - response.output_text.delta
    /// - response.output_text.done
    /// - response.refusal.delta
    /// - response.refusal.done
    /// - response.function_call_arguments.delta
    /// - response.function_call_arguments.done
    /// - response.file_search_call.in_progress
    /// - response.file_search_call.searching
    /// - response.file_search_call.completed
    /// - response.web_search_call.in_progress
    /// - response.web_search_call.searching
    /// - response.web_search_call.completed
    /// - response.reasoning_summary_part.added
    /// - response.reasoning_summary_part.done
    /// - response.reasoning_summary_text.delta
    /// - response.reasoning_summary_text.done
    /// - response.reasoning_text.delta
    /// - response.reasoning_text.done
    /// - response.image_generation_call.completed
    /// - response.image_generation_call.generating
    /// - response.image_generation_call.in_progress
    /// - response.image_generation_call.partial_image
    /// - response.mcp_call_arguments.delta
    /// - response.mcp_call_arguments.done
    /// - response.mcp_call.completed
    /// - response.mcp_call.failed
    /// - response.mcp_call.in_progress
    /// - response.mcp_list_tools.completed
    /// - response.mcp_list_tools.failed
    /// - response.mcp_list_tools.in_progress
    /// - response.code_interpreter_call.in_progress
    /// - response.code_interpreter_call.interpreting
    /// - response.code_interpreter_call.completed
    /// - response.code_interpreter_call_code.delta
    /// - response.code_interpreter_call_code.done
    /// - response.output_text.annotation.added
    /// - response.custom_tool_call_input.delta
    /// - response.custom_tool_call_input.done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_index: Option<i32>,
    /// 事件
    /// - response.image_generation_call.completed
    /// - response.image_generation_call.generating
    /// - response.image_generation_call.in_progress
    /// - response.image_generation_call.partial_image
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_image_b64: Option<String>,
    /// 事件
    /// - response.image_generation_call.completed
    /// - response.image_generation_call.generating
    /// - response.image_generation_call.in_progress
    /// - response.image_generation_call.partial_image
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_image_index: Option<i32>,
    /// - response.content_part.added
    /// - response.output_text.delta
    /// - response.output_text.done
    /// - response.refusal.delta
    /// - response.refusal.done
    /// - response.reasoning_text.delta
    /// - response.reasoning_text.done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_index: Option<i32>,
    /// 事件
    /// - response.content_part.added
    /// - response.content_part.done
    ///
    /// 以下事件只拥有 [ResponseOutputText] 类型
    /// - response.reasoning_summary_part.added
    /// - response.reasoning_summary_part.done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub part: Option<ResponseOutput>,
    /// 事件
    /// - response.reasoning_summary_part.added
    /// - response.reasoning_summary_part.done
    /// - response.reasoning_summary_text.delta
    /// - response.reasoning_summary_text.done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary_index: Option<i32>,
    /// - response.output_text.annotation.added
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotation_index: Option<i32>,
    /// - response.custom_tool_call_input.done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    /// - error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// - error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// - error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub param: Option<String>,
}

/// 模型在生成响应时可以调用的工具数组
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Tools {
    /// 函数工具
    Function(FunctionTool),
    /// 文件搜索工具
    FileSearch(FileSearchTool),
    /// 计算机工具
    Computer(ComputerTool),
    /// 计算机使用预览工具
    ComputerUsePreview(ComputerUsePreviewTool),
    /// 网络搜索工具
    WebSearch(WebSearchTool),
    /// MCP工具
    Mcp(McpTool),
    /// 代码解释器工具
    CodeInterpreter(CodeInterpreterTool),
    /// 图像生成工具
    ImageGeneration(ImageGenerationTool),
    /// 本地Shell工具
    LocalShell(LocalShellTool),
    /// Shell工具
    Shell(ShellTool),
    /// 自定义工具
    Custom(CustomTool),
    /// 命名空间工具
    Namespace(NamespaceTool),
    /// 工具搜索工具
    ToolSearch(ToolSearchTool),
    /// 网络搜索预览工具
    WebSearchPreview(WebSearchPreviewTool),
    /// 应用补丁工具
    ApplyPatch(ApplyPatchTool),
}

/// 定义模型可以选择调用的您自己代码中的函数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionTool {
    /// 要调用的函数的名称
    pub name: String,
    /// 描述函数参数的JSON模式对象
    /// ```json
    /// map<unknown>
    /// ```
    pub parameters: Option<Value>,
    /// 是否强制执行严格的参数验证。默认为true
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
    /// 函数工具的类型。始终为function
    pub r#type: String,
    /// 此函数是否被延迟并通过工具搜索加载
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defer_loading: Option<bool>,
    /// 函数的描述。用于模型确定是否调用该函数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// 从上传的文件中搜索相关内容的工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSearchTool {
    /// 文件搜索工具的类型。始终为file_search
    pub r#type: String,
    /// 要搜索的向量存储的ID
    pub vector_store_ids: Vec<String>,
    /// 要应用的过滤器
    /// ```json
    /// ComparisonFilter | CompoundFilter
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Value>,
    /// 要返回的最大结果数。此数字应在1到50之间（含）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_num_results: Option<Value>,
    /// 搜索的排名选项
    /// ```json
    /// {
    ///   "hybrid_search": {
    ///     "embedding_weight": number,
    ///     "text_weight": number
    ///   }?,
    ///   "ranker": "auto" | "default-2024-11-15"?,
    ///   "score_threshold": number?
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ranking_options: Option<Value>,
}

/// 控制虚拟计算机的工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputerTool {
    /// 计算机工具的类型。始终为 computer
    pub r#type: String,
}

/// 控制虚拟计算机的工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputerUsePreviewTool {
    /// 计算机显示器的高度
    pub display_height: i32,
    /// 计算机显示器的宽度
    pub display_width: i32,
    /// 要控制的计算机环境类型
    pub environment: String,
    /// 计算机使用工具的类型。始终为 computer_use_preview
    pub r#type: String,
}

/// 搜索与提示相关的互联网资源的工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSearchTool {
    /// 网络搜索工具的类型。web_search或web_search_2025_08_26之一
    pub r#type: String,
    /// 搜索的过滤器
    /// ```json
    /// {
    ///   "allowed_domains": array<string>?
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Value>,
    /// 用于搜索的上下文窗口空间量的高级指导。low、medium或high之一。默认为medium
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_context_size: Option<String>,
    /// 用户的近似位置
    /// ```json
    /// {
    ///   "city": string?,
    ///   "country": string?,
    ///   "region": string?,
    ///   "timezone": string?,
    ///   "type": "approximate"?
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_location: Option<Value>,
}

/// 通过远程模型上下文协议（MCP）服务器为模型提供额外工具访问权限
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTool {
    /// 此MCP服务器的标签，用于在工具调用中标识它
    pub server_label: String,
    /// MCP工具的类型。始终为 mcp
    pub r#type: String,
    /// 允许的工具名称列表或过滤器对象
    /// ```json
    /// array<string> | {
    ///   "read_only": boolean?,
    ///   "tool_names": array<string>?
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_tools: Option<Value>,
    /// 可用于远程MCP服务器的OAuth访问 token
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization: Option<String>,
    /// 服务连接器的标识符
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connector_id: Option<String>,
    /// 此MCP工具是否被延迟并通过工具搜索发现
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defer_loading: Option<bool>,
    /// 发送到MCP服务器的可选HTTP标头
    /// ```json
    /// map<string>
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<Value>,
    /// 指定MCP服务器的哪些工具需要批准
    /// ```json
    /// {
    ///   "always": {
    ///     "read_only": boolean?,
    ///     "tool_names": array<string>?
    ///   }?,
    ///   "never": {
    ///     "read_only": boolean?,
    ///     "tool_names": array<string>?
    ///   }?
    /// } | "always" | "never"
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub require_approval: Option<Value>,
    /// MCP服务器的可选描述，用于提供更多上下文
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_description: Option<String>,
    /// MCP服务器的URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_url: Option<String>,
}

/// 运行Python代码以帮助生成对提示的响应的工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeInterpreterTool {
    /// 代码解释器容器
    /// ```json
    /// string | {
    ///   "type": "auto",
    ///   "file_ids": array<string>?,
    ///   "memory_limit": "1g" | "4g" | "16g" | "64g"?,
    ///   "network_policy": ContainerNetworkPolicyDisabled | ContainerNetworkPolicyAllowlist?
    /// }
    /// ```
    pub container: Value,
    /// 代码解释器工具的类型。始终为 code_interpreter
    pub r#type: String,
}

/// 使用GPT图像模型生成图像的工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageGenerationTool {
    /// 图像生成工具的类型。始终为 image_generation
    pub r#type: String,
    /// 是生成新图像还是编辑现有图像。默认为auto
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    /// 生成图像的背景类型。transparent、opaque或auto之一。默认为auto
    #[serde(skip_serializing_if = "Option::is_none")]
    pub background: Option<String>,
    /// 控制模型在匹配输入图像的风格和特征（尤其是面部特征）方面付出多少努力
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_fidelity: Option<String>,
    /// 修复的可选蒙版
    /// ```json
    /// {
    ///   "file_id": string?,
    ///   "image_url": string?
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_image_mask: Option<Value>,
    /// 要使用的图像生成模型。默认为gpt-image-1
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<Value>,
    /// 生成图像的审核级别。默认为auto
    #[serde(skip_serializing_if = "Option::is_none")]
    pub moderation: Option<String>,
    /// 输出图像的压缩级别。默认为100
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_compression: Option<Value>,
    /// 生成图像的输出格式。png、webp或jpeg之一。默认为png
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_format: Option<String>,
    /// 在流式模式下生成的部分图像数量，从0（默认值）到3
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_images: Option<Value>,
    /// 生成图像的质量。low、medium、high或auto之一。默认为auto
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality: Option<String>,
    /// 生成图像的尺寸。1024x1024、1024x1536、1536x1024或auto之一。默认为auto
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
}

/// 允许模型在本地环境中执行shell命令的工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalShellTool {
    /// 本地shell工具的类型。始终为 local_shell
    pub r#type: String,
}

/// 允许模型执行shell命令的工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShellTool {
    /// shell工具的类型。始终为 shell
    pub r#type: String,
    /// 执行shell命令的环境
    /// ```json
    /// ContainerAuto | LocalEnvironment | ContainerReference
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment: Option<Value>,
}

/// 使用指定格式处理输入的自定义工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomTool {
    /// 自定义工具的名称，用于在工具调用中标识它
    pub name: String,
    /// 自定义工具的类型。始终为 custom
    pub r#type: String,
    /// 此工具是否应被延迟并通过工具搜索发现
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defer_loading: Option<bool>,
    /// 自定义工具的可选描述，用于提供更多上下文
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 自定义工具的输入格式。默认为无约束文本
    /// ```json
    /// {
    ///   "type": "text"
    /// } | {
    ///   "definition": string,
    ///   "syntax": "lark" | "regex",
    ///   "type": "grammar"
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<Value>,
}

/// 在共享命名空间下分组函数/自定义工具
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceTool {
    /// 显示给模型的命名空间描述
    pub description: String,
    /// 工具调用中使用的命名空间名称（例如，crm）
    pub name: String,
    /// 此命名空间内可用的函数/自定义工具
    /// ```json
    /// array<Function | Custom>
    /// ```
    pub tools: Value,
    /// 工具的类型。始终为 namespace
    pub r#type: String,
}

/// 延迟工具的托管或BYOT工具搜索配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSearchTool {
    /// 工具的类型。始终为 tool_search
    pub r#type: String,
    /// 为客户端执行的工具搜索工具显示给模型的描述
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 工具搜索是由服务器执行还是由客户端执行
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution: Option<String>,
    /// 客户端执行的工具搜索工具的参数模式
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
}

/// 此工具搜索网络以获取相关结果以在响应中使用
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSearchPreviewTool {
    /// 网络搜索工具的类型。web_search_preview或web_search_preview_2025_03_11之一
    pub r#type: String,
    /// 搜索内容类型
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_content_types: Option<Value>,
    /// 用于搜索的上下文窗口空间量的高级指导。low、medium或high之一。默认为medium
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_context_size: Option<String>,
    /// 用户的位置
    /// ```json
    /// {
    ///   "type": "approximate",
    ///   "city": string?,
    ///   "country": string?,
    ///   "region": string?,
    ///   "timezone": string?
    /// }
    /// ```
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_location: Option<Value>,
}

/// 允许助手使用统一差异创建、删除或更新文件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyPatchTool {
    /// 工具的类型。始终为 apply_patch
    pub r#type: String,
}
/// 文件搜索工具调用结果对象
/// 文件搜索工具调用的结果。有关更多信息，请参阅文件搜索指南。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSearchCall {
    /// 文件搜索工具调用的唯一ID
    pub id: String,
    /// 用于搜索文件的查询数组
    pub queries: Vec<String>,
    /// 文件搜索工具调用的状态。可能为in_progress、searching、completed、incomplete或failed之一
    pub status: String,
    /// 文件搜索工具调用的类型。始终为 file_search_call
    pub r#type: String,
    /// 文件搜索工具调用的结果
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<Value>,
}
/// 计算机使用工具调用对象
/// 计算机使用工具的调用。有关更多信息，请参阅计算机使用指南。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputerCall {
    /// 计算机调用的唯一ID
    pub id: String,
    /// 使用输出响应工具调用时使用的标识符
    pub call_id: String,
    /// 计算机调用的待处理安全检查数组
    pub pending_safety_checks: Value,
    /// 项目的状态。可能为in_progress、completed或incomplete之一。通过API返回项目时填充
    pub status: String,
    /// 计算机调用的类型。始终为 computer_call
    pub r#type: String,
    /// 点击操作
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<Value>,
    /// 计算机使用的扁平化批处理操作。每个操作包括类型鉴别器和特定于操作的字段
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actions: Option<Value>,
}
/// 计算机工具调用输出对象
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputerCallOutput {
    /// 产生输出的计算机工具调用的ID
    pub call_id: String,
    /// 与计算机使用工具一起使用的计算机截图图像
    pub output: Value,
    /// 计算机工具调用输出的类型。始终为 computer_call_output
    pub r#type: String,
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
pub struct WebSearchCall {
    /// 网络搜索工具调用的唯一ID
    pub id: String,
    /// 描述此网络搜索调用中采取的具体操作的对象
    /// 包括模型如何使用网络（搜索、打开页面、在页面中查找）的详细信息
    pub action: Value,
    /// 网络搜索工具调用的状态
    pub status: String,
    /// 网络搜索工具调用的类型。始终为 web_search_call
    pub r#type: String,
}
/// 函数调用工具调用对象，由模型生成并返回
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    /// 要传递给函数的参数的JSON字符串
    pub arguments: String,
    /// 模型生成的函数工具调用的唯一ID
    pub call_id: String,
    /// 要运行的函数名称
    pub name: String,
    /// 函数工具调用的类型。始终为 function_call
    pub r#type: String,
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
/// 函数工具调用的输出，本地函数调用结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCallOutput {
    /// 模型生成的函数工具调用的唯一ID
    pub call_id: String,
    /// 函数工具调用的文本、图像或文件输出
    /// ```json
    /// string | array<ResponseInputTextContent | ResponseInputImageContent | ResponseInputFileContent>
    /// ```
    pub output: Value,
    /// 函数工具调用输出的类型。始终为 function_call_output
    pub r#type: String,
    /// 函数工具调用输出的唯一ID。通过API返回此项时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 项目的状态。in_progress、completed或incomplete之一。通过API返回项目时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// 工具搜索调用
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSearchCall {
    /// 提供给工具搜索调用的参数
    pub arguments: Value,
    /// 项目类型。始终为 tool_search_call
    pub r#type: String,
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
    /// 项目类型。始终为 tool_search_output
    pub r#type: String,
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
pub struct Reasoning {
    /// 推理内容的唯一标识符
    pub id: String,
    /// 推理摘要内容
    /// ```json
    /// array<SummaryTextContent>
    /// ```
    pub summary: Value,
    /// 对象的类型。始终为 reasoning
    pub r#type: String,
    /// 推理文本内容
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<Vec<ReasoningContent>>,
    /// 推理项目的加密内容 - 当响应在include参数中包含reasoning.encrypted_content时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encrypted_content: Option<String>,
    /// 项目的状态。in_progress、completed或incomplete之一。通过API返回项目时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}
/// 推理模型生成响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReasoningContent {
    pub text: String,
    pub r#type: String,
}
/// 由v1/responses/compact API生成的压缩项目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Compaction {
    /// 压缩摘要的加密内容
    pub encrypted_content: String,
    /// 项目的类型。始终为 compaction
    pub r#type: String,
    /// 压缩项目的ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// 模型发出的图像生成请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageGenerationCall {
    /// 图像生成调用的唯一ID
    pub id: String,
    /// 以base64编码的生成图像
    pub result: String,
    /// 图像生成调用的状态
    pub status: String,
    /// 图像生成调用的类型。始终为 image_generation_call
    pub r#type: String,
}

/// 运行代码的工具调用
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// 代码解释器工具调用的类型。始终为 code_interpreter_call
    pub r#type: String,
}

/// 在本地shell上运行命令的工具调用
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// 本地shell调用的类型。始终为 local_shell_call
    pub r#type: String,
}
/// 本地shell工具调用的输出
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalShellCallOutput {
    /// 模型生成的本地shell工具调用的唯一ID
    pub id: String,
    /// 本地shell工具调用的输出JSON字符串
    pub output: String,
    /// 本地shell工具调用输出的类型。始终为 local_shell_call_output
    pub r#type: String,
    /// 项目的状态。in_progress、completed或incomplete之一
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// 表示执行一个或多个shell命令请求的工具
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// 项目的类型。始终为 shell_call
    pub r#type: String,
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
pub struct ShellCallOutput {
    /// 模型生成的shell工具调用的唯一ID
    pub call_id: String,
    /// 捕获的stdout和stderr输出块及其相关结果
    /// ```json
    /// array<ResponseFunctionShellCallOutputContent>
    /// ```
    pub output: Value,
    /// 项目的类型。始终为 shell_call_output
    pub r#type: String,
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
    /// 项目的类型。始终为 apply_patch_call
    pub r#type: String,
    /// apply patch工具调用的唯一ID。通过API返回此项时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// apply patch工具调用发出的流式输出
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyPatchCallOutput {
    /// 模型生成的apply patch工具调用的唯一ID
    pub call_id: String,
    /// apply patch工具调用输出的状态。completed或failed之一
    pub status: String,
    /// 项目的类型。始终为 apply_patch_call_output
    pub r#type: String,
    /// apply patch工具调用输出的唯一ID。通过API返回此项时填充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 来自apply patch工具的可选人类可读日志文本（例如，补丁结果或错误）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
}

/// MCP服务器上可用工具的列表
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// 项目的类型。始终为 mcp_list_tools
    pub r#type: String,
    /// 如果服务器无法列出工具，则为错误消息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// 工具调用的人工批准请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpApprovalRequest {
    /// 批准请求的唯一ID
    pub id: String,
    /// 工具参数的JSON字符串
    pub arguments: String,
    /// 要运行的工具的名称
    pub name: String,
    /// 发出请求的MCP服务器的标签
    pub server_label: String,
    /// 项目的类型。始终为 mcp_approval_request
    pub r#type: String,
}

/// MCP批准请求的响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpApprovalResponse {
    /// 正在回答的批准请求的ID
    pub approval_request_id: String,
    /// 请求是否被批准
    pub approve: bool,
    /// 项目的类型。始终为 mcp_approval_response
    pub r#type: String,
    /// 批准响应的唯一ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 决策的可选原因
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// MCP服务器上工具的调用
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpCall {
    /// 工具调用的唯一ID
    pub id: String,
    /// 传递给工具的参数的JSON字符串
    pub arguments: String,
    /// 运行的工具体名称
    pub name: String,
    /// 运行工具的MCP服务器的标签
    pub server_label: String,
    /// 项目的类型。始终为 mcp_call
    pub r#type: String,
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
pub struct CustomToolCallOutput {
    /// 用于将此自定义工具调用输出映射到自定义工具调用的调用ID
    pub call_id: String,
    /// 由您的代码生成的自定义工具调用的输出
    /// ```json
    /// string | array<ResponseInputText | ResponseInputImage | ResponseInputFile>
    /// ```
    pub output: Value,
    /// 自定义工具调用输出的类型。始终为 custom_tool_call_output
    pub r#type: String,
    /// 自定义工具调用输出在OpenAI平台中的唯一ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// 模型创建的自定义工具的调用
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomToolCall {
    /// 用于将此自定义工具调用映射到工具调用输出的标识符
    pub call_id: String,
    /// 模型生成的自定义工具调用的输入
    pub input: String,
    /// 被调用的自定义工具的名称
    pub name: String,
    /// 自定义工具调用的类型。始终为 custom_tool_call
    pub r#type: String,
    /// 自定义工具调用在OpenAI平台中的唯一ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// 被调用的自定义工具的命名空间
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// 用于引用项目的内部标识符
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemReference {
    /// 要引用的项目的ID
    pub id: String,
    /// 要引用的项目的类型。始终为 item_reference
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}
/// 模型响应内容
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseOutputMessage {
    /// 响应消息的唯一标识符。
    pub id: Option<String>,
    /// 模型响应的文本内容
    pub content: Vec<ResponseOutput>,
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
    pub annotations: Option<Vec<Annotations>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Value::Array
    pub logprobs: Option<Value>,
    /// 模型的文本输出
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ///  类型
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
    /// URL引用的类型。始终为  "url_citation"
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
    /// 容器文件引用的类型。始终为  "container_file_citation"
    pub citation_type: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilePath {
    /// 文件的ID
    pub file_id: String,
    /// 文件在文件列表中的索引
    pub index: i32,
    /// 文件路径的类型。始终为  "file_path"
    pub path_type: String,
}
