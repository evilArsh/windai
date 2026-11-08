use serde::{Deserialize, Serialize};
use wind_event::{
    def::{Directory, EventDef, Shape},
    schema,
};

/// 一个 MCP 绑定已连接
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpStarted {
    pub topic_id: i64,
    pub source: String,
    pub key: String,
}

/// 一个 MCP 绑定已停止
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpStopped {
    pub topic_id: i64,
    pub source: String,
    pub key: String,
}

/// 一个 MCP 绑定连接或清单查询失败
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpFailed {
    pub topic_id: i64,
    pub source: String,
    pub key: String,
    pub error: String,
}

pub const CORE_MCP_STARTED: EventDef<McpStarted> = EventDef::new(
    "core/mcp.started",
    Shape::Observe,
    true,
    schema::<McpStarted>,
);

pub const CORE_MCP_STOPPED: EventDef<McpStopped> = EventDef::new(
    "core/mcp.stopped",
    Shape::Observe,
    true,
    schema::<McpStopped>,
);

pub const CORE_MCP_FAILED: EventDef<McpFailed> =
    EventDef::new("core/mcp.failed", Shape::Observe, true, schema::<McpFailed>);

pub fn directory() -> Directory {
    Directory::from_metas([
        CORE_MCP_STARTED.meta(),
        CORE_MCP_STOPPED.meta(),
        CORE_MCP_FAILED.meta(),
    ])
}
