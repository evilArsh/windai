use super::{McpService, ModService};
use crate::{
    agent::SPAWN_AGENT_NAME,
    error::{CoreError, Result},
};
use serde::{Deserialize, Serialize};
use wind_event::{DispatchCx, schema};
use wind_wire::{
    AgentMode, FunctionCallOutput,
    tool::{FunctionCall, FunctionTool, Tools},
};

/// 服务名与工具名拼成模型可见名字时使用的分隔串
const MCP_TOOL_IDENTIFIER: &str = "0m0";

/// Create an agent by spawning a child instance to complete a specified task.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SpawnAgentSchema {
    /// Agent run mode
    pub mode: AgentMode,
    /// Description of the task to be completed by this agent
    pub task: String,
    /// Whether to fork parent agent's contexts
    pub fork: bool,
}

fn tool_declarations() -> Vec<Tools> {
    vec![function_tool(
        SPAWN_AGENT_NAME,
        "Create an agent by spawning a child instance to complete a specified task.",
        schema::<SpawnAgentSchema>(),
    )]
}

#[derive(Debug)]
pub struct ExecutedPlan {
    pub results: Vec<FunctionCallOutput>,
    pub spawns: Vec<SpawnAgentAction>,
}

#[derive(Debug, Clone)]
pub struct DeclaredTool {
    pub origin: ToolOrigin,
    pub declaration: Tools,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpawnAgentRequest {
    pub mode: AgentMode,
    pub task: String,
    /// 继承父实例上下文
    #[serde(default)]
    pub fork: bool,
}

#[derive(Debug, Clone)]
pub struct SpawnAgentAction {
    pub call_id: String,
    pub data: SpawnAgentRequest,
}

/// 定位一个 MCP 工具：服务名加原始工具名
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpTarget {
    pub server: String,
    pub tool: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolOrigin {
    Agent { name: String },
    Mcp { target: McpTarget },
    ModRequest { mod_name: String, request: String },
}
impl ToolOrigin {
    pub fn exposed(&self) -> String {
        match self {
            ToolOrigin::Agent { name } => name.clone(),
            ToolOrigin::Mcp { target } => {
                format!("{}{MCP_TOOL_IDENTIFIER}{}", target.server, target.tool)
            }
            ToolOrigin::ModRequest { mod_name, request } => format!("m_{mod_name}__{request}"),
        }
    }
}

pub struct ToolCatalog {
    entries: Vec<(ToolOrigin, Tools)>,
}

impl ToolCatalog {
    pub fn assemble(from_mods: Vec<DeclaredTool>, from_mcp: Vec<DeclaredTool>) -> Result<Self> {
        let mut entries: Vec<(ToolOrigin, Tools)> = Vec::new();
        for declared in from_mods.into_iter().chain(from_mcp) {
            let name = declared.origin.exposed();
            if entries.iter().any(|(origin, _)| origin.exposed() == name) {
                return Err(CoreError::ToolNameConflict { name });
            }
            entries.push((declared.origin, declared.declaration));
        }
        for tool in tool_declarations() {
            let name = tool_name(&tool);
            if entries.iter().any(|(origin, _)| origin.exposed() == name) {
                continue;
            }
            entries.push((ToolOrigin::Agent { name }, tool));
        }
        Ok(Self { entries })
    }

    pub fn declarations(&self) -> Vec<Tools> {
        self.entries.iter().map(|(_, tool)| tool.clone()).collect()
    }

    pub fn lookup(&self, name: &str) -> Option<&ToolOrigin> {
        self.entries
            .iter()
            .find(|(origin, _)| origin.exposed() == name)
            .map(|(origin, _)| origin)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub async fn execute_plan(
    calls: Vec<FunctionCall>,
    catalog: &ToolCatalog,
    cx: &DispatchCx,
    mcp_service: &dyn McpService,
    mod_service: &dyn ModService,
) -> Result<ExecutedPlan> {
    let mut results = Vec::with_capacity(calls.len());
    let mut agent_calls = Vec::new();
    for call in calls {
        let Some(origin) = catalog.lookup(&call.name) else {
            results.push(unknown_result(&call.call_id, &call.name));
            continue;
        };
        match origin {
            ToolOrigin::Agent { .. } => agent_calls.push(call),
            ToolOrigin::Mcp { target } => {
                results.push(call_mcp(cx, mcp_service, &call, target).await);
            }
            ToolOrigin::ModRequest { mod_name, request } => {
                results.push(call_mod_request(cx, mod_service, &call, mod_name, request).await?);
            }
        }
    }
    let spawns = parse_spawn_agents(&agent_calls)?;
    Ok(ExecutedPlan { results, spawns })
}

pub fn parse_spawn_agents(calls: &[FunctionCall]) -> Result<Vec<SpawnAgentAction>> {
    let mut spawn_agents: Vec<SpawnAgentAction> = Vec::new();

    for call in calls {
        match call.name.as_str() {
            SPAWN_AGENT_NAME => spawn_agents.push(SpawnAgentAction {
                call_id: call.call_id.clone(),
                data: parse_args::<SpawnAgentRequest>(&call.arguments)?,
            }),
            name => log::warn!("unsupported agent tool name: {name}, ignored"),
        }
    }

    Ok(spawn_agents)
}

fn function_tool(name: &str, description: &str, parameters: serde_json::Value) -> Tools {
    Tools::Function(FunctionTool {
        name: name.to_string(),
        description: Some(description.to_string()),
        parameters: Some(parameters),
        strict: None,
    })
}

fn tool_name(tool: &Tools) -> String {
    match tool {
        Tools::Function(function) => function.name.clone(),
    }
}

fn parse_arguments(arguments: &str) -> Result<serde_json::Value> {
    Ok(serde_json::from_str(arguments)?)
}

fn parse_args<T>(arguments: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    Ok(serde_json::from_str(arguments)?)
}

fn unknown_result(call_id: &str, name: &str) -> FunctionCallOutput {
    FunctionCallOutput {
        id: call_id.to_string(),
        content: serde_json::json!({ "error": format!("unknown tool {name}") }),
    }
}

fn error_result(call_id: &str, message: impl Into<String>) -> FunctionCallOutput {
    FunctionCallOutput {
        id: call_id.to_string(),
        content: serde_json::json!({ "error": message.into() }),
    }
}

fn done_result(call_id: &str, value: serde_json::Value) -> FunctionCallOutput {
    FunctionCallOutput {
        id: call_id.to_string(),
        content: value,
    }
}

pub(crate) fn denied_result(call_id: impl Into<String>, reason: &str) -> FunctionCallOutput {
    FunctionCallOutput {
        id: call_id.into(),
        content: serde_json::json!({ "refused": true, "reason": reason }),
    }
}

async fn call_mcp(
    cx: &DispatchCx,
    mcp_service: &dyn McpService,
    call: &FunctionCall,
    target: &McpTarget,
) -> FunctionCallOutput {
    let arguments = match parse_arguments(&call.arguments) {
        Ok(arguments) if arguments.is_object() => arguments,
        Ok(_) => {
            return error_result(&call.call_id, "the tool arguments must be a json object");
        }
        Err(error) => {
            return error_result(
                &call.call_id,
                format!("cannot parse tool arguments: {error}"),
            );
        }
    };
    match mcp_service.call(cx, &call.call_id, target, arguments).await {
        Ok(value) => done_result(&call.call_id, value),
        Err(error) => error_result(&call.call_id, error.to_string()),
    }
}

async fn call_mod_request(
    cx: &DispatchCx,
    mod_service: &dyn ModService,
    call: &FunctionCall,
    mod_name: &str,
    request: &str,
) -> Result<FunctionCallOutput> {
    let version = mod_service.version_of(mod_name).await?;
    let input = parse_arguments(&call.arguments)?;
    match mod_service
        .call(&cx.scope, mod_name, version, request, input)
        .await
    {
        Ok(value) => Ok(done_result(&call.call_id, value)),
        Err(error) => Ok(denied_result(&call.call_id, &error.to_string())),
    }
}
