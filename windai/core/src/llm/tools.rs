use super::{McpService, ModService};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use wind_event::DispatchCx;
use wind_wire::{
    AgentMode, FunctionCallOutput,
    tool::{FunctionCall, FunctionTool, Tools},
};

// pub fn tool_declarations() -> Vec<Tools> {
//     vec![function_tool(
//         SPAWN_AGENT_NAME,
//         "Create an agent by spawning a child instance to complete a specified task.",
//         serde_json::json!({
//             "type": "object",
//             "properties": {
//                 "mode": {
//                     "type": "string",
//                     "enum": AgentMode::iter().map(|mode| mode.to_string()).collect::<Vec<_>>(),
//                     "description": "Agent run mode."
//                 },
//                 "task": {
//                     "type": "string",
//                     "description": "Description of the task to be completed by this agent"
//                 }
//             },
//             "required": ["mode", "task"],
//             "additionalProperties": false
//         }),
//     )]
// }

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
    pub fork: bool,
}

#[derive(Debug, Clone)]
pub struct SpawnAgentAction {
    pub call_id: String,
    pub data: SpawnAgentRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolOrigin {
    Agent { name: String },
    Mcp { server: String, tool: String },
    ModRequest { mod_name: String, request: String },
}
impl ToolOrigin {
    pub fn exposed(&self) -> String {
        match self {
            ToolOrigin::Agent { name } => name.clone(),
            ToolOrigin::Mcp { server, tool } => format!("{server}__{tool}"),
            ToolOrigin::ModRequest { mod_name, request } => format!("m_{mod_name}__{request}"),
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        if let Some(rest) = name.strip_prefix("m_") {
            let (mod_name, request) = rest.split_once("__")?;
            return Some(ToolOrigin::ModRequest {
                mod_name: mod_name.to_string(),
                request: request.to_string(),
            });
        }
        match name.split_once("__") {
            Some((server, tool)) => Some(ToolOrigin::Mcp {
                server: server.to_string(),
                tool: tool.to_string(),
            }),
            None => Some(ToolOrigin::Agent {
                name: name.to_string(),
            }),
        }
    }
}

pub struct ToolCatalog {
    entries: Vec<(ToolOrigin, Tools)>,
}

impl ToolCatalog {
    pub fn assemble(from_mods: Vec<DeclaredTool>, from_mcp: Vec<DeclaredTool>) -> Self {
        let mut entries: Vec<(ToolOrigin, Tools)> = Vec::new();
        // TODO: 内建 SPAWN_AGENT_NAME，
        // todo: mod可以覆盖该工具的实现
        // for tool in tool_declarations() {
        //     let name = match &tool {
        //         Tools::Function(function) => function.name.clone(),
        //     };
        //     entries.push((ToolOrigin::Agent { name }, tool));
        // }
        for declared in from_mods.into_iter().chain(from_mcp) {
            entries.push((declared.origin, declared.declaration));
        }
        Self { entries }
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
            results.push(unknown_result(&call.id, &call.name));
            continue;
        };
        match origin {
            ToolOrigin::Agent { .. } => agent_calls.push(call),
            ToolOrigin::Mcp { server, tool } => {
                results.push(call_mcp(cx, mcp_service, &call, server, tool).await);
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
                call_id: call.id.clone(),
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

fn parse_arguments(arguments: &str) -> serde_json::Value {
    match serde_json::from_str(arguments) {
        Ok(value) => value,
        Err(error) => {
            log::warn!("cannot parse tool arguments: {error}");
            serde_json::json!({})
        }
    }
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

fn done_result(call_id: &str, value: serde_json::Value) -> FunctionCallOutput {
    FunctionCallOutput {
        id: call_id.to_string(),
        content: value,
    }
}

fn denied_result(call_id: &str, reason: &str) -> FunctionCallOutput {
    FunctionCallOutput {
        id: call_id.to_string(),
        content: serde_json::json!({ "refused": true, "reason": reason }),
    }
}

async fn call_mcp(
    cx: &DispatchCx,
    mcp_service: &dyn McpService,
    call: &FunctionCall,
    server: &str,
    tool: &str,
) -> FunctionCallOutput {
    let arguments = parse_arguments(&call.arguments);
    match mcp_service
        .call(cx, &call.id, server, tool, arguments)
        .await
    {
        Ok(value) => done_result(&call.id, value),
        Err(error) => unknown_result(&call.id, &error.to_string()),
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
    let input = parse_arguments(&call.arguments);
    match mod_service
        .call(&cx.scope, mod_name, version, request, input)
        .await
    {
        Ok(value) => Ok(done_result(&call.id, value)),
        Err(error) => Ok(denied_result(&call.id, &error.to_string())),
    }
}
