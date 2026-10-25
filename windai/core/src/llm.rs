//! 模型请求链路
//!
//! 一条模型往返的六个可干预点：
//!
//! ```text
//! 提示词组装
//! 提供商结构侧的改写
//! 统一结构侧的请求
//! 统一结构侧的响应
//! 工具调用的采纳
//! 一批调用成为执行计划
//! ```

pub mod events;
pub mod execute;
pub mod policy;
pub mod ports;
pub mod prompt;
pub mod rules;
pub mod tools;
pub mod turn;

pub use events::directory;
pub use ports::{DeclaredTool, McpTools, ModRequests, TopicMods};
