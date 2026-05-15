use rmcp::{ServiceError, service::ClientInitializeError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};
mod cmd_normalizer;
mod connector;
pub mod registry;

pub type JsonObject<F = Value> = serde_json::Map<String, F>;

const MCP_TOOL_IDENTIFIER: &str = "0m0";

#[derive(Debug, Serialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum ClientStatus {
    Connecting,
    Connected,
    Disconnected,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum TransportType {
    Stdio,
    Streamable,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum ServerParams {
    Stdio(StdioParams),
    Streamable(StreamableParams),
}
impl ServerParams {
    pub fn new_stdio(
        name: String,
        command: String,
        args: Vec<String>,
        description: Option<String>,
        env: Option<HashMap<String, String>>,
    ) -> Self {
        Self::Stdio(StdioParams {
            name,
            description,
            command,
            args,
            env,
        })
    }
    pub fn new_streamable(name: String, url: String, description: String) -> Self {
        Self::Streamable(StreamableParams {
            name,
            description,
            url,
        })
    }
    fn get_name(&self) -> Cow<'_, str> {
        match self {
            ServerParams::Stdio(params) => Cow::Borrowed(&params.name),
            ServerParams::Streamable(params) => Cow::Borrowed(&params.name),
        }
    }
    fn get_transport(&self) -> TransportType {
        match self {
            ServerParams::Stdio(_) => TransportType::Stdio,
            ServerParams::Streamable(_) => TransportType::Streamable,
        }
    }
}
/// 启动 Stdio 服务的参数
#[derive(Debug, Clone, Serialize)]
pub struct StdioParams {
    /// 服务名称
    pub name: String,
    /// 描述
    pub description: Option<String>,
    /// 启动命令
    pub command: String,
    /// 启动参数
    pub args: Vec<String>,
    /// 环境变量
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<HashMap<String, String>>,
}
/// 启动 Streamable-HTTP 服务的参数
#[derive(Debug, Clone, Serialize)]
pub struct StreamableParams {
    /// 服务名称
    pub name: String,
    /// 描述
    pub description: String,
    /// 服务地址
    pub url: String,
}

/// 客户端状态快照
#[derive(Debug, Clone, Serialize)]
pub struct ClientSnapshot {
    /// 服务名
    pub name: String,
    pub transport: TransportType,
    pub status: ClientStatus,
    pub ref_sessions: HashSet<String>,
}

/// mcp 服务广播的事件
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientEvent {
    Connecting { name: String },
    Connected { name: String },
    Disconnected { name: String, reason: String },
    Error { name: String, error: String },
}

#[derive(thiserror::Error, Debug)]
pub enum McpError {
    #[error(transparent)]
    Stdio(#[from] std::io::Error),

    #[error("Failed to initialize client: {0}")]
    ClientInitialize(#[from] ClientInitializeError),

    #[error("MCP service error: {0}")]
    Service(#[from] ServiceError),

    #[error("Server '{0}' not found")]
    ServerNotFound(String),

    #[error("Client manager has shut down")]
    ManagerShutdown,

    #[error("Unsupported stdio command: {0}")]
    UnsupportedStdioCommand(String),

    #[error("request timeout")]
    Timeout(String),

    #[error("{0}")]
    Other(String),
}

/// MCP 工具调用结果
#[derive(Debug, Serialize, Deserialize)]
pub struct CallToolResult {
    pub content: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
}
impl From<rmcp::model::CallToolResult> for CallToolResult {
    fn from(value: rmcp::model::CallToolResult) -> Self {
        Self {
            content: value
                .content
                .into_iter()
                .filter_map(|v| serde_json::to_string(&v).ok())
                .collect(),
            is_error: value.is_error,
        }
    }
}

/// MCP 工具
#[derive(Debug, Serialize)]
pub struct Tool {
    /// 工具名(函数名)
    /// - 工具名前拼接了 MCP 服务名
    pub name: String,
    /// 该工具的一个易于理解的标题
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// 工具功能的描述
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema对象定义该工具接收的参数格式
    pub input_schema: Arc<JsonObject>,
    #[serde(skip)]
    _p: u8,
}
impl Tool {
    /// 解析出MCP服务名和真实工具名
    /// - (MCP server name, tool name)
    pub fn parse_name(tool_name: &str) -> (Option<String>, String) {
        if let Some(pos) = tool_name.find(MCP_TOOL_IDENTIFIER) {
            let server_name = tool_name[..pos].to_string();
            let tool_name = tool_name[pos + MCP_TOOL_IDENTIFIER.len()..].to_string();
            (Some(server_name), tool_name)
        } else {
            (None, tool_name.to_string())
        }
    }

    fn build_name(server_name: &str, tool_name: &str) -> String {
        if server_name.is_empty() {
            tool_name.to_string()
        } else {
            format!("{}{}{}", server_name, MCP_TOOL_IDENTIFIER, tool_name)
        }
    }
    pub fn new(server_name: &str, tool: &rmcp::model::Tool) -> Self {
        Self {
            name: Self::build_name(server_name, &tool.name),
            title: tool.title.clone(),
            description: tool.description.clone().map(|cow| cow.into_owned()),
            input_schema: tool.input_schema.clone(),
            _p: 0x0,
        }
    }
}

/// 可用于从模型生成文本的提示词（Prompt）
#[derive(Debug, Serialize)]
pub struct Prompt {
    /// 提示词的名称
    pub name: String,
    /// 可选的标题
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// 可选：描述该提示词的功能
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 可选：可传递给提示词以进行自定义的参数列表
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Vec<PromptArgument>>,
}
impl From<rmcp::model::Prompt> for Prompt {
    fn from(tool: rmcp::model::Prompt) -> Self {
        Self {
            name: tool.name,
            title: tool.title,
            description: tool.description,
            arguments: tool
                .arguments
                .map(|args| args.into_iter().map(|arg| arg.into()).collect()),
        }
    }
}
/// 提示词参数
#[derive(Debug, Serialize)]
pub struct PromptArgument {
    /// 参数的名称
    pub name: String,
    /// 参数的人类可读标题
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// 参数用途的描述
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 此参数是否为必填项
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
}
impl From<rmcp::model::PromptArgument> for PromptArgument {
    fn from(value: rmcp::model::PromptArgument) -> Self {
        Self {
            name: value.name.clone(),
            title: value.title.clone(),
            description: value.description.clone(),
            required: value.required,
        }
    }
}

/// 资源
#[derive(Debug, Serialize)]
pub struct Resource {
    /// 表示资源位置的 URI（例如："file:///path/to/file" 或 "str:///content"）
    pub uri: String,
    /// 资源的名称
    pub name: String,
    /// 资源的人类可读标题
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// 可选的资源描述
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 资源内容的 MIME 类型（"text" 或 "blob"）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    /// 原始资源内容的大小（以字节为单位），如果已知的话。
    ///
    /// 此大小是在 base64 编码或任何分词处理之前计算的。
    /// 主机（Hosts）可以使用此信息来显示文件大小并估算上下文窗口的使用情况。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u32>,
}
impl From<rmcp::model::Resource> for Resource {
    fn from(r: rmcp::model::Resource) -> Self {
        Self {
            uri: r.uri.to_owned(),
            name: r.name.to_owned(),
            title: r.title.to_owned(),
            description: r.description.to_owned(),
            mime_type: r.mime_type.to_owned(),
            size: r.size,
        }
    }
}

/// 调用 MCP 工具的参数
#[derive(Debug, Clone)]
pub struct CallToolParam {
    /// MCP 服务名
    pub server_name: String,
    /// MCP 工具名
    pub tool_name: String,
    pub arguments: Option<Map<String, Value>>,
}
