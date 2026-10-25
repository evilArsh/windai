use serde::{Deserialize, Serialize};
use wind_event::def::{Directory, EventDef, Mount, Shape};
use wind_event::envelope::Scope;

/// 把一个服务绑定到一个话题
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpAttach {
    /// 话题标识
    pub topic_id: i64,
    /// 服务名。名字只是引用键，不参与连接身份
    pub server_name: String,
}

/// 把一个服务从一个话题解绑
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpDetach {
    /// 话题标识
    pub topic_id: i64,
    /// 服务名
    pub server_name: String,
}

/// 启动一个服务
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpStart {
    /// 服务名
    pub server_name: String,
    /// 启动原因。自动重连与用户主动触发都经由这个字段记录，原因必须可解释
    pub reason: String,
}

/// 停止一个服务
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpStop {
    /// 服务名
    pub server_name: String,
    /// 停止原因
    pub reason: String,
}

/// 一条连接断了
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpCrash {
    /// 服务名
    pub server_name: String,
    /// 断开的原因
    pub error: String,
}

/// 服务的概览
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpServerInfo {
    /// 服务名
    pub name: String,
    /// 用途说明
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 连接方式
    pub transport: Transport,
    /// 当前是否已连接
    pub running: bool,
}

/// 连接方式
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, strum::AsRefStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Transport {
    /// 子进程加标准输入输出
    Stdio,
    /// 远端地址
    Streamable,
    /// 宿主进程内，自带服务用它
    Builtin,
}

/// 列举服务的载荷
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpServerList {
    /// 当前可见的服务
    pub servers: Vec<McpServerInfo>,
}

/// 工具的概览
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpToolInfo {
    /// 提供该工具的服务名
    pub server_name: String,
    /// 工具名，不含服务名前缀
    pub name: String,
    /// 面向模型的说明
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 参数结构
    pub input_schema: serde_json::Value,
}

/// 列举工具的载荷
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpToolList {
    /// 当前可见的工具
    pub tools: Vec<McpToolInfo>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpToolCall {
    pub server_name: String,
    pub tool: String,
    pub arguments: serde_json::Value,
    pub scope: Scope,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpToolResult {
    pub server_name: String,
    pub tool: String,
    /// 这次调用的标识，与调用方的配对字段一致
    pub call_id: String,
    pub result: serde_json::Value,
}

fn schema<T: schemars::JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("the generated schema is valid JSON")
}

fn scope_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["kind"],
        "properties": {
            "kind": { "enum": ["global", "topic"] },
            "topic_id": { "type": "integer" }
        }
    })
}

fn tool_call_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["server_name", "tool", "arguments", "scope"],
        "properties": {
            "server_name": { "type": "string" },
            "tool": { "type": "string" },
            "arguments": { "type": "object" },
            "scope": scope_schema()
        }
    })
}

fn tool_result_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["server_name", "tool", "call_id", "result"],
        "properties": {
            "server_name": { "type": "string" },
            "tool": { "type": "string" },
            "call_id": { "type": "string" },
            "result": {}
        }
    })
}

pub const CORE_MCP_ATTACH: EventDef<McpAttach> = EventDef::new(
    "core/mcp.attach",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<McpAttach>,
);

pub const CORE_MCP_DETACH: EventDef<McpDetach> = EventDef::new(
    "core/mcp.detach",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<McpDetach>,
);

pub const CORE_MCP_START: EventDef<McpStart> = EventDef::new(
    "core/mcp.start",
    Shape::Intercept,
    true,
    Mount::Subsystem,
    schema::<McpStart>,
);

pub const CORE_MCP_STOP: EventDef<McpStop> = EventDef::new(
    "core/mcp.stop",
    Shape::Intercept,
    true,
    Mount::Subsystem,
    schema::<McpStop>,
);

pub const CORE_MCP_CRASH: EventDef<McpCrash> = EventDef::new(
    "core/mcp.crash",
    Shape::Observe,
    true,
    Mount::Subsystem,
    schema::<McpCrash>,
);

pub const CORE_MCP_SERVER_LIST: EventDef<McpServerList> = EventDef::new(
    "core/mcp.server.list",
    Shape::Intercept,
    true,
    Mount::Subsystem,
    schema::<McpServerList>,
);

pub const CORE_MCP_TOOL_LIST: EventDef<McpToolList> = EventDef::new(
    "core/mcp.tool.list",
    Shape::Intercept,
    true,
    Mount::Subsystem,
    schema::<McpToolList>,
);

pub const CORE_MCP_TOOL_CALL: EventDef<McpToolCall> = EventDef::new(
    "core/mcp.tool.call",
    Shape::Intercept,
    true,
    Mount::Subsystem,
    tool_call_schema,
);

pub const CORE_MCP_TOOL_RESULT: EventDef<McpToolResult> = EventDef::new(
    "core/mcp.tool.result",
    Shape::Intercept,
    true,
    Mount::Subsystem,
    tool_result_schema,
);

pub fn directory() -> Directory {
    Directory::from_metas([
        CORE_MCP_ATTACH.meta(),
        CORE_MCP_DETACH.meta(),
        CORE_MCP_START.meta(),
        CORE_MCP_STOP.meta(),
        CORE_MCP_CRASH.meta(),
        CORE_MCP_SERVER_LIST.meta(),
        CORE_MCP_TOOL_LIST.meta(),
        CORE_MCP_TOOL_CALL.meta(),
        CORE_MCP_TOOL_RESULT.meta(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_event_carries_a_json_schema() {
        let directory = directory();
        assert_eq!(directory.len(), 9);
        for name in directory.names() {
            let meta = directory
                .get(name)
                .expect("find every event in the directory");
            assert!(
                (meta.schema)().is_object(),
                "the schema of {name} must be an object"
            );
        }
    }

    #[test]
    fn the_schema_of_a_tool_call_asks_for_a_scope() {
        let schema = (CORE_MCP_TOOL_CALL.schema)();
        let required = schema["required"]
            .as_array()
            .expect("read required as an array");
        assert!(required.contains(&serde_json::json!("scope")));
    }
}
