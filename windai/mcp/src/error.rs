use std::io;

#[derive(Debug, thiserror::Error)]
pub enum McpError {
    #[error("invalid service spec: {0}")]
    InvalidSpec(String),
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("cannot connect: {0}")]
    Connect(String),
    #[error("the operation timed out")]
    Timeout,
    #[error("the operation was cancelled")]
    Cancelled,
    #[error("service error: {0}")]
    Service(String),
    #[error("the server returned an unsupported response")]
    UnsupportedResponse,
    #[error("the tool is no longer available")]
    ToolUnavailable,
    #[error("the registry is shutting down")]
    ShuttingDown,
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, McpError>;
