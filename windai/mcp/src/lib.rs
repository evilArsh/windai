//! MCP 客户端连接、内建服务与工具目录

pub mod builtin;
mod connector;
mod error;
pub mod events;
mod options;
mod registry;
mod runner;

use std::sync::Arc;

pub use connector::{CallOutcome, ToolDescriptor};
pub use error::{McpError, Result};
pub use options::McpOptions;
pub use registry::{BoundTool, Catalog, RegistryHandle, ToolCall};
use wind_event::Dispatcher;

/// 创建注册表；全局内建 MCP 在此启动
pub fn init(options: McpOptions, bus: Arc<Dispatcher>) -> RegistryHandle {
    RegistryHandle::spawn(options, bus)
}
