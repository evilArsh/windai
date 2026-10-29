use crate::error::Result;
use serde::{Deserialize, Serialize};
use wind_wire::{
    AgentMode,
    tool::{FunctionCall, FunctionTool, Tools},
};

/// 系统 tool_call 预留关键字
///
/// TODO: new feature：
/// mod系统通过实现这些关键工具使 agent 拥有核心功能。
/// 系统拥有默认的实现以保证基础功能运转
pub const SPAWN_AGENT_NAME: &str = "wind_spawn_agent";

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
        // TODO: 内建 SPAWN_AGENT_NAME
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

fn parse_args<T>(arguments: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    Ok(serde_json::from_str(arguments)?)
}
