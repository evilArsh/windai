//! MCP 客户端连接、内建服务与工具目录

pub mod builtin;
mod connector;
mod error;
pub mod events;
mod registry;
mod runner;

use std::sync::Arc;
use std::path::PathBuf;
use std::time::Duration;
pub use connector::{CallOutcome, ToolDescriptor};
pub use error::{McpError, Result};
pub use registry::{BoundTool, Catalog, RegistryHandle};
use wind_event::Dispatcher;


#[derive(Clone)]
pub struct McpOptions {
    /// mod 版本目录的根
    pub mods_root: PathBuf,
    /// 建立连接并完成握手的上限
    pub connect_timeout: Duration,
    /// 读取工具清单的上限
    pub list_timeout: Duration,
    /// 单次工具调用的上限
    pub call_timeout: Duration,
    /// 关闭连接时等待在飞调用与清理的上限
    pub shutdown_grace: Duration,
}

impl McpOptions {
    pub fn new(mods_root: impl Into<PathBuf>) -> Self {
        Self {
            mods_root: runner::normalize_root(mods_root.into()),
            connect_timeout: Duration::from_secs(10),
            list_timeout: Duration::from_secs(10),
            call_timeout: Duration::from_secs(60),
            shutdown_grace: Duration::from_secs(3),
        }
    }
}



/// 创建注册表；全局内建 MCP 在此启动
pub fn init(options: McpOptions, bus: Arc<Dispatcher>) -> RegistryHandle {
    RegistryHandle::spawn(options, bus)
}
