use rmcp::{ServiceError, service::ClientInitializeError};
use serde::Serialize;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};
mod cmd_normalizer;
pub mod connector;
pub mod registry;

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
        id: String,
        name: String,
        command: String,
        args: Vec<String>,
        description: Option<String>,
        env: Option<HashMap<String, String>>,
    ) -> Self {
        Self::Stdio(StdioParams {
            id,
            name,
            description,
            command,
            args,
            env,
        })
    }
    pub fn new_streamable(id: String, name: String, url: String, description: String) -> Self {
        Self::Streamable(StreamableParams {
            id,
            name,
            description,
            url,
        })
    }
    fn get_id(&self) -> Cow<'_, str> {
        match self {
            ServerParams::Stdio(params) => Cow::Borrowed(&params.id),
            ServerParams::Streamable(params) => Cow::Borrowed(&params.id),
        }
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
    /// 唯一服务id
    pub id: String,
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
    /// 唯一服务id
    pub id: String,
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
    pub id: String,
    pub name: String,
    pub transport: TransportType,
    pub status: ClientStatus,
    pub ref_sessions: HashSet<String>,
}

/// mcp 服务广播的事件
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientEvent {
    Connecting {
        id: String,
        name: String,
    },
    Connected {
        id: String,
        name: String,
    },
    Disconnected {
        id: String,
        name: String,
        reason: String,
    },
    Error {
        id: String,
        name: String,
        error: String,
    },
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

    #[error("{0}")]
    Other(String),
}
