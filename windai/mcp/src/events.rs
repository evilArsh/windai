use serde::{Deserialize, Serialize};
use std::time::Duration;
use strum::AsRefStr;
use wind_event::def::{Directory, EventDef, Shape, schema};
use wind_event::{DispatchCx, Origin, Scope, Source};
use wind_wire::mcp::McpDefinition;

/// 观察类事件的总预算
pub const DEFAULT_BUDGET: Duration = Duration::from_secs(10);
/// 改写类事件的预算
pub const INTERCEPT_BUDGET: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum StopReason {
    /// 最后一个引用被释放
    Released,
    /// 注册表关闭
    Shutdown,
    /// 同名服务的定义被替换
    Replaced,
}

/// 服务即将启动，载荷可被改写
#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpLaunch {
    pub name: String,
    /// 当前持有该服务的引用
    pub refs: Vec<String>,
    pub definition: McpDefinition,
}

/// 同名服务的定义被替换，载荷可被改写
#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpReplaced {
    pub name: String,
    pub refs: Vec<String>,
    pub old: McpDefinition,
    pub new: McpDefinition,
}

/// 服务已进入启动流程
#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpStarting {
    pub name: String,
    pub refs: Vec<String>,
}

/// 服务已启动并可调用
#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpStarted {
    pub name: String,
    pub refs: Vec<String>,
    pub tools: Vec<String>,
}

/// 服务启动失败
#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpFailed {
    pub name: String,
    pub refs: Vec<String>,
    pub error: String,
}

/// 服务已停止
#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpStopped {
    pub name: String,
    pub refs: Vec<String>,
    pub reason: String,
}

/// 服务输出了一行日志
#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpLog {
    pub name: String,
    /// 输出流，目前只有 stderr
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
    EventDef::new("core/mcp.log", Shape::Observe, false, schema::<McpLog>);

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
