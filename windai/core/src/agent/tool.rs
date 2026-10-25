use crate::error::Result;
use crate::models::AgentMode;
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;
use wind_wire::tool::{FunctionCall, FunctionTool, Tools};

pub const SPAWN_AGENT_NAME: &str = "windai_spawn_agent";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpawnAgentRequest {
    pub mode: String,
    pub task: String,
}

#[derive(Debug, Clone)]
pub struct SpawnAgentAction {
    pub call_id: String,
    pub data: SpawnAgentRequest,
}

pub fn tool_declarations() -> Vec<Tools> {
    vec![function_tool(
        SPAWN_AGENT_NAME,
        "Create an agent by spawning a child instance to complete a specified task.",
        serde_json::json!({
            "type": "object",
            "properties": {
                "mode": {
                    "type": "string",
                    "enum": AgentMode::iter().map(|mode| mode.to_string()).collect::<Vec<_>>(),
                    "description": "Agent run mode."
                },
                "task": {
                    "type": "string",
                    "description": "Description of the task to be completed by this agent"
                }
            },
            "required": ["mode", "task"],
            "additionalProperties": false
        }),
    )]
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

fn parse_args<T>(arguments: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    Ok(serde_json::from_str(arguments)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一次调用
    fn call(id: &str, name: &str, arguments: &str) -> FunctionCall {
        FunctionCall {
            id: id.into(),
            name: name.into(),
            arguments: arguments.into(),
        }
    }

    /// 唯一声明的工具序列化之后的结果
    fn declared() -> serde_json::Value {
        let tools = tool_declarations();
        assert_eq!(tools.len(), 1, "only the spawn tool is declared");
        let tool = tools.into_iter().next().expect("find the tool declaration");
        let Tools::Function(function) = tool;
        assert_eq!(function.name, SPAWN_AGENT_NAME);
        serde_json::to_value(function).expect("serialize")
    }

    #[test]
    fn the_only_declaration_is_the_spawn_tool() {
        let spawn = declared();
        assert_eq!(spawn["parameters"]["type"], serde_json::json!("object"));
        assert_eq!(
            spawn["parameters"]["required"],
            serde_json::json!(["mode", "task"]),
            "both parameters are required"
        );
        assert!(
            spawn["parameters"]["properties"].get("agent_key").is_none(),
            "an agent has only an instance and no definition to choose from, so the declaration must not ask for an agent_key"
        );
        let modes = spawn["parameters"]["properties"]["mode"]["enum"]
            .as_array()
            .expect("read the mode enum")
            .clone();
        assert!(
            modes.contains(&serde_json::json!("background")),
            "background mode must be selectable, otherwise child instances that stay out of the barrier cannot be spawned: {modes:?}"
        );
    }

    #[test]
    fn the_list_tool_is_gone() {
        assert!(
            tool_declarations().iter().all(|tool| match tool {
                Tools::Function(function) => function.name != "agent_list_agents",
            }),
            "the tool that lists agent definitions has no producer and must not remain in the declarations"
        );
    }

    #[test]
    fn parsing_collects_the_spawn_requests_in_order() {
        let actions = parse_spawn_agents(&[
            call(
                "c-1",
                SPAWN_AGENT_NAME,
                r#"{"mode":"sync","task":"look up information"}"#,
            ),
            call(
                "c-2",
                SPAWN_AGENT_NAME,
                r#"{"mode":"background","task":"write a draft"}"#,
            ),
        ])
        .expect("parse");

        assert_eq!(actions.len(), 2);
        assert_eq!(
            actions[0].call_id, "c-1",
            "order within the batch is context order"
        );
        assert_eq!(actions[0].data.mode, "sync");
        assert_eq!(actions[0].data.task, "look up information");
        assert_eq!(actions[1].data.mode, "background");
    }

    #[test]
    fn a_malformed_spawn_argument_is_rejected() {
        assert!(
            parse_spawn_agents(&[call("c-1", SPAWN_AGENT_NAME, "{ not json")]).is_err(),
            "arguments come from model-written text; unparseable input must be rejected, not defaulted to a guess"
        );
    }

    #[test]
    fn an_unknown_agent_tool_is_ignored() {
        let actions =
            parse_spawn_agents(&[call("c-1", "agent_do_something", "{}")]).expect("parse");
        assert!(actions.is_empty());
    }
}
