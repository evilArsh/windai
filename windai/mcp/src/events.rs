use std::time::Duration;

use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use wind_event::{
    DispatchCx, Origin, Scope, Source,
    def::{Directory, EventDef, Shape},
    schema,
};

use crate::spec::ServiceSpec;

/// 单条 observe 事件的派发预算
pub const DEFAULT_BUDGET: Duration = Duration::from_secs(10);

/// 单条 intercept 事件的派发预算
pub const INTERCEPT_BUDGET: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum StopReason {
    Released,
    Shutdown,
    Replaced,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpLaunch {
    pub name: String,
    pub refs: Vec<String>,
    pub spec: ServiceSpec,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpReplaced {
    pub name: String,
    pub refs: Vec<String>,
    pub old: ServiceSpec,
    pub new: ServiceSpec,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpStarting {
    pub name: String,
    pub refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpStarted {
    pub name: String,
    pub refs: Vec<String>,
    pub tools: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpFailed {
    pub name: String,
    pub refs: Vec<String>,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpStopped {
    pub name: String,
    pub refs: Vec<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpLog {
    pub name: String,
    pub stream: String,
    pub line: String,
}

pub const CORE_MCP_LAUNCH: EventDef<McpLaunch> = EventDef::new(
    "core/mcp.launch",
    Shape::Intercept,
    true,
    schema::<McpLaunch>,
);

pub const CORE_MCP_REPLACED: EventDef<McpReplaced> = EventDef::new(
    "core/mcp.replaced",
    Shape::Intercept,
    true,
    schema::<McpReplaced>,
);

pub const CORE_MCP_STARTING: EventDef<McpStarting> = EventDef::new(
    "core/mcp.starting",
    Shape::Observe,
    true,
    schema::<McpStarting>,
);

pub const CORE_MCP_STARTED: EventDef<McpStarted> = EventDef::new(
    "core/mcp.started",
    Shape::Observe,
    true,
    schema::<McpStarted>,
);

pub const CORE_MCP_FAILED: EventDef<McpFailed> =
    EventDef::new("core/mcp.failed", Shape::Observe, true, schema::<McpFailed>);

pub const CORE_MCP_STOPPED: EventDef<McpStopped> = EventDef::new(
    "core/mcp.stopped",
    Shape::Observe,
    true,
    schema::<McpStopped>,
);

pub const CORE_MCP_LOG: EventDef<McpLog> =
    EventDef::new("core/mcp.log", Shape::Observe, true, schema::<McpLog>);

pub fn directory() -> Directory {
    Directory::from_metas([
        CORE_MCP_LAUNCH.meta(),
        CORE_MCP_REPLACED.meta(),
        CORE_MCP_STARTING.meta(),
        CORE_MCP_STARTED.meta(),
        CORE_MCP_FAILED.meta(),
        CORE_MCP_STOPPED.meta(),
        CORE_MCP_LOG.meta(),
    ])
}

pub(crate) fn cx() -> DispatchCx {
    root(DEFAULT_BUDGET)
}

pub(crate) fn intercept_cx() -> DispatchCx {
    root(INTERCEPT_BUDGET)
}

fn root(budget: Duration) -> DispatchCx {
    DispatchCx::root(Origin::Internal, Scope::Global, Source::core("mcp"), budget)
}
