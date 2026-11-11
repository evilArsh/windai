use crate::{
    agent::SPAWN_AGENT_NAME,
    error::{CoreError, Result},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use wind_event::{DispatchCx, schema};
use wind_wire::{
    AgentMode, FunctionCallOutput, McpService, ToolCallParam,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolOrigin {
    Agent,
    Mcp,
}

pub(crate) fn mcp_tool_name(server: &str, tool: &str) -> String {
    format!("{server}{MCP_TOOL_IDENTIFIER}{tool}")
}

fn split_mcp_tool_name(name: &str) -> Option<(&str, &str)> {
    let (server, tool) = name.split_once(MCP_TOOL_IDENTIFIER)?;
    (!server.is_empty() && !tool.is_empty()).then_some((server, tool))
}

pub struct ToolCatalog {
    entries: Vec<(ToolOrigin, Tools)>,
}

impl ToolCatalog {
    pub fn assemble(from_mcp: Vec<Tools>) -> Result<Self> {
        let mut entries: Vec<(ToolOrigin, Tools)> = Vec::new();
        for tool in from_mcp {
            let name = tool_name(&tool);
            if entries
                .iter()
                .any(|(_, declared)| tool_name(declared) == name)
            {
                return Err(CoreError::ToolNameConflict {
                    name: name.to_string(),
                });
            }
            entries.push((ToolOrigin::Mcp, tool));
        }
        for tool in tool_declarations() {
            let name = tool_name(&tool);
            if entries
                .iter()
                .any(|(_, declared)| tool_name(declared) == name)
            {
                continue;
            }
            entries.push((ToolOrigin::Agent, tool));
        }
        Ok(Self { entries })
    }

    pub fn declarations(&self) -> Vec<Tools> {
        self.entries.iter().map(|(_, tool)| tool.clone()).collect()
    }

    pub fn lookup(&self, name: &str) -> Option<&ToolOrigin> {
        self.entries
            .iter()
            .find(|(_, tool)| tool_name(tool) == name)
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
) -> Result<ExecutedPlan> {
    let mut slots: Vec<Option<FunctionCallOutput>> = calls.iter().map(|_| None).collect();
    let mut agent_calls = Vec::new();
    let mut batch = Vec::new();
    for (index, call) in calls.iter().enumerate() {
        let Some(origin) = catalog.lookup(&call.name) else {
            slots[index] = Some(unknown_result(&call.call_id, &call.name));
            continue;
        };
        match origin {
            ToolOrigin::Agent => agent_calls.push(call),
            ToolOrigin::Mcp => match tool_param(call) {
                Ok(param) => batch.push((index, param)),
                Err(result) => slots[index] = Some(result),
            },
        }
    }
    if !batch.is_empty() {
        let (indices, params): (Vec<usize>, Vec<ToolCallParam>) = batch.into_iter().unzip();
        let outputs = match mcp_service
            .tool_calls(cx.deadline(), cx.cancel().clone(), params)
            .await
        {
            Ok(outputs) => outputs,
            Err(reason) => {
                log::warn!("[llm] the mcp batch failed: {reason}");
                Vec::new()
            }
        };
        let mut by_id: HashMap<String, Value> = outputs
            .into_iter()
            .map(|output| (output.id, output.content))
            .collect();
        for index in indices {
            let call_id = &calls[index].call_id;
            slots[index] = Some(match by_id.remove_entry(call_id) {
                Some((id, content)) => FunctionCallOutput { id, content },
                None => error_result(call_id, "the call produced no result"),
            });
        }
    }
    let results = slots.into_iter().flatten().collect();
    let spawns = parse_spawn_agents(&agent_calls)?;
    Ok(ExecutedPlan { results, spawns })
}

pub fn parse_spawn_agents(calls: &[&FunctionCall]) -> Result<Vec<SpawnAgentAction>> {
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

fn tool_name(tool: &Tools) -> &str {
    match tool {
        Tools::Function(function) => function.name.as_str(),
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

pub(crate) fn denied_result(call_id: impl Into<String>, reason: &str) -> FunctionCallOutput {
    FunctionCallOutput {
        id: call_id.into(),
        content: serde_json::json!({ "refused": true, "reason": reason }),
    }
}

fn tool_param(call: &FunctionCall) -> std::result::Result<ToolCallParam, FunctionCallOutput> {
    let arguments = match parse_arguments(&call.arguments) {
        Ok(arguments) if arguments.is_object() => arguments,
        Ok(_) => {
            return Err(error_result(
                &call.call_id,
                "the tool arguments must be a json object",
            ));
        }
        Err(error) => {
            return Err(error_result(
                &call.call_id,
                format!("cannot parse tool arguments: {error}"),
            ));
        }
    };
    let Some((server, tool)) = split_mcp_tool_name(&call.name) else {
        return Err(error_result(
            &call.call_id,
            format!("the tool name {} names no server", call.name),
        ));
    };
    Ok(ToolCallParam {
        call_id: call.call_id.clone(),
        server: server.to_string(),
        name: tool.to_string(),
        arguments,
    })
}

#[cfg(test)]
mod tests {
    use super::{ToolCatalog, execute_plan, function_tool};
    use serde_json::{Value, json};
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;
    use wind_event::{DispatchCx, Origin, Scope, Source};
    use wind_wire::tool::{FunctionCall, FunctionCallOutput, Tools};
    use wind_wire::{McpService, ToolCallParam};

    #[derive(Default)]
    struct Fake {
        /// 每次 tool_calls 收到的 (call_id, server, tool)
        batches: Mutex<Vec<Vec<(String, String, String)>>>,
        failing: bool,
    }

    #[async_trait::async_trait]
    impl McpService for Fake {
        async fn list_tools(&self, _topic_id: i64) -> Result<Vec<Tools>, String> {
            Ok(Vec::new())
        }

        async fn tool_call(
            &self,
            _deadline: tokio::time::Instant,
            _token: CancellationToken,
            _param: ToolCallParam,
        ) -> Result<Value, String> {
            panic!("a round must go through tool_calls")
        }

        async fn tool_calls(
            &self,
            _deadline: tokio::time::Instant,
            _token: CancellationToken,
            params: Vec<ToolCallParam>,
        ) -> Result<Vec<FunctionCallOutput>, String> {
            let batch = params
                .iter()
                .map(|param| {
                    (
                        param.call_id.clone(),
                        param.server.clone(),
                        param.name.clone(),
                    )
                })
                .collect();
            self.batches
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(batch);
            if self.failing {
                return Err("the registry is shutting down".to_string());
            }
            Ok(params
                .into_iter()
                .rev()
                .map(|param| FunctionCallOutput {
                    id: param.call_id,
                    content: json!({ "tool": param.name }),
                })
                .collect())
        }
    }

    fn cx() -> DispatchCx {
        DispatchCx::root(
            Origin::Internal,
            Scope::Global,
            Source::core("test"),
            Duration::from_secs(5),
        )
    }

    fn call(call_id: &str, name: &str, arguments: &str) -> FunctionCall {
        FunctionCall {
            item_id: None,
            call_id: call_id.to_string(),
            name: name.to_string(),
            arguments: arguments.to_string(),
        }
    }

    fn catalog() -> ToolCatalog {
        let tools = vec![
            function_tool("srv0m0a", "a", json!({ "type": "object" })),
            function_tool("srv0m0b", "b", json!({ "type": "object" })),
        ];
        ToolCatalog::assemble(tools).expect("the catalog must assemble")
    }

    #[tokio::test]
    async fn a_round_of_mcp_calls_runs_once_and_keeps_the_call_order() {
        let service = Fake::default();
        let spawn = json!({ "mode": "sync", "task": "t", "fork": false }).to_string();
        let calls = vec![
            call("c1", "srv0m0a", "{}"),
            call("c2", "srv0m0b", "{}"),
            call("c3", "wind_spawn_agent", &spawn),
        ];
        let plan = execute_plan(calls, &catalog(), &cx(), &service)
            .await
            .expect("the plan must run");

        assert_eq!(plan.spawns.len(), 1, "the agent call must be spawned");
        assert_eq!(plan.spawns[0].call_id, "c3");
        assert_eq!(plan.results.len(), 2, "agent calls carry no tool result");
        assert_eq!(plan.results[0].id, "c1");
        assert_eq!(plan.results[0].content, json!({ "tool": "a" }));
        assert_eq!(plan.results[1].id, "c2");
        assert_eq!(plan.results[1].content, json!({ "tool": "b" }));

        let batches = service
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(batches.len(), 1, "the round must be a single batch");
        assert_eq!(
            batches[0],
            vec![
                ("c1".to_string(), "srv".to_string(), "a".to_string()),
                ("c2".to_string(), "srv".to_string(), "b".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn an_unknown_tool_does_not_join_the_batch() {
        let service = Fake::default();
        let calls = vec![call("c1", "missing", "{}"), call("c2", "srv0m0a", "{}")];
        let plan = execute_plan(calls, &catalog(), &cx(), &service)
            .await
            .expect("the plan must run");

        assert_eq!(plan.results.len(), 2);
        assert_eq!(plan.results[0].id, "c1");
        assert!(plan.results[0].content["error"].is_string());
        assert_eq!(plan.results[1].id, "c2");
        assert_eq!(plan.results[1].content, json!({ "tool": "a" }));

        let batches = service
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(batches[0].len(), 1, "only the known tool is batched");
    }

    #[tokio::test]
    async fn a_failed_batch_turns_every_mcp_call_into_an_error_result() {
        let service = Fake {
            failing: true,
            ..Fake::default()
        };
        let calls = vec![call("c1", "srv0m0a", "{}"), call("c2", "srv0m0b", "{}")];
        let plan = execute_plan(calls, &catalog(), &cx(), &service)
            .await
            .expect("a failed batch must not fail the round");

        assert_eq!(plan.results.len(), 2);
        assert!(
            plan.results
                .iter()
                .all(|result| result.content["error"].is_string())
        );
    }

    #[tokio::test]
    async fn arguments_that_are_not_an_object_produce_an_error_result() {
        let service = Fake::default();
        let calls = vec![call("c1", "srv0m0a", "[]"), call("c2", "srv0m0b", "{}")];
        let plan = execute_plan(calls, &catalog(), &cx(), &service)
            .await
            .expect("the plan must run");

        assert_eq!(plan.results[0].id, "c1");
        assert!(plan.results[0].content["error"].is_string());
        assert_eq!(plan.results[1].content, json!({ "tool": "b" }));
    }
}
