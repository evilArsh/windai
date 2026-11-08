//! MCP 客户端连接与调用

pub use config::{Environment, McpOptions, environment_snapshot};
pub use connector::{Connection, Connector, RemoteTool};
pub use error::{McpError, Result};
pub use identity::{ConnectionKey, connection_key};
pub use registry::{
    AdmittedCall, BindingSpec, BindingState, BindingStatus, BoundTool, BuiltinRoots, Registry,
    SkillRoot, SyncReport, ToolTarget,
};
pub use schema::mcp_schema;
pub use service::{McpChange, McpService};

pub mod builtin;
pub mod config;
pub mod connector;
pub mod error;
pub mod events;
pub mod identity;
mod process;
pub mod registry;
pub mod schema;
pub mod service;
