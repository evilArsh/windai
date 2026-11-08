use std::path::PathBuf;

/// MCP 连接错误
#[derive(Debug, thiserror::Error)]
pub enum McpError {
    /// 配置不满足连接前提
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    /// 命令无法解析为可执行文件
    #[error("the command {0} is not available")]
    CommandUnavailable(PathBuf),
    /// 工具参数不是对象
    #[error("the tool arguments must be a json object")]
    ArgumentsNotObject,
    /// 服务返回了尚未支持的中间响应
    #[error("the server returned an unsupported tool response")]
    UnsupportedResponse,
    /// 建立连接失败
    #[error("cannot connect: {0}")]
    Connect(String),
    /// 服务协议错误
    #[error("service error: {0}")]
    Service(#[from] rmcp::ServiceError),
    /// 调用方取消
    #[error("the call was cancelled")]
    Cancelled,
    /// 超出时间预算
    #[error("the operation timed out")]
    Timeout,
    /// 关闭连接失败
    #[error("shutdown failed: {0}")]
    Shutdown(String),
    /// 结果无法序列化
    #[error("cannot encode the result: {0}")]
    Encode(String),
    /// 绑定声明无效
    #[error("invalid binding: {0}")]
    Binding(String),
    /// 目标已不再指向当前连接
    #[error("the tool is no longer available")]
    ToolUnavailable,
    /// 注册表已关闭
    #[error("the registry is closed")]
    Closed,
    /// 内建服务不可用
    #[error("builtin server error: {0}")]
    Builtin(String),
    /// 存储错误
    #[error("storage error: {0}")]
    Storage(#[from] wind_storage::error::StorageError),
}

pub type Result<T> = std::result::Result<T, McpError>;
