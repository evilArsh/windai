use crate::error::Result;
use crate::models::{AgentMode, AgentStatus};
use serde::{Deserialize, Serialize};
use wind_ai::message::Content;
use wind_ai::tool::{FunctionCall, FunctionTool, Tools};

pub const AGENT_TOOL_PREFIX: &str = "agent_";
const LIST_AGENTS_NAME: &str = "agent_list_agents";
const SPAWN_AGENT_NAME: &str = "agent_spawn_agent";

/// list_agents 的响应。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ListAgentsResponse {
    /// 当前 Topic 中可见的 Agent 绑定视图。
    pub agents: Vec<AgentBindingView>,
}

/// 暴露给 LLM 的 Agent 绑定视图。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentBindingView {
    /// AgentDefinition.key。
    pub key: String,
    /// 当前 Topic 中配置的 Agent 别名。
    pub alias: Option<String>,
    /// Agent 能力描述。
    pub description: String,
}

/// 创建子 Agent 的请求
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpawnAgentRequest {
    /// AgentDefinition.key
    pub agent_key: String,
    /// 子 Agent 运行模式。
    pub mode: AgentMode,
    /// 分配给子 Agent 的任务描述。
    pub task: String,
}
impl std::fmt::Display for SpawnAgentRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[SpawnAgentRequest] (agent_key = {}, mode = {}, task = {})",
            self.agent_key, self.mode, self.task
        )
    }
}
/// 创建子 Agent 的响应。
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub struct SpawnAgentResponse {
    pub call_id: String,
    pub mode: AgentMode,
    pub status: AgentStatus,
    pub output: Vec<Content>,
}

pub struct AgentActionPlan {
    /// 合并后的 list_agents 。
    pub list_agents: Option<Vec<String>>,
    /// 独立的 spawn_agent 调用。
    pub spawn_agents: Vec<SpawnAgentAction>,
}

/// 单个 spawn_agent 调用。
pub struct SpawnAgentAction {
    pub call_id: String,
    pub data: SpawnAgentRequest,
}

pub fn list_catalogs() -> Vec<Tools> {
    vec![
        function_tool(
            LIST_AGENTS_NAME,
            "List all available agents to finish specified tasks.",
            serde_json::json!({
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": false
            }),
        ),
        function_tool(
            SPAWN_AGENT_NAME,
            "Create an agent by specifying a pattern to complete a specified task.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "agent_key": {
                        "type": "string",
                        "description": "The unique identifier of the agent"
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["sync", "background", "fork"],
                        "description": "Operating mode after agent creation"
                    },
                    "task": {
                        "type": "string",
                        "description": "Description of the task to be completed by this agent"
                    }
                },
                "required": ["agent_key", "mode", "task"],
                "additionalProperties": false
            }),
        ),
    ]
}

/// 解析并合并 Agent 工具调用。
///
/// 多个 `list_agents` 调用会被合并为一次查询：
/// - `call_ids` 收集所有原始 call ID
/// - `include_disabled` 取逻辑或（任意 call 要求即为 true）
pub fn parse_agent_action(calls: &[FunctionCall]) -> Result<AgentActionPlan> {
    let mut list_call_ids: Vec<String> = Vec::new();
    let mut spawn_agents: Vec<SpawnAgentAction> = Vec::new();

    for call in calls {
        match call.name.as_str() {
            name if name == LIST_AGENTS_NAME => {
                list_call_ids.push(call.id.clone());
            }
            name if name == SPAWN_AGENT_NAME => {
                spawn_agents.push(SpawnAgentAction {
                    call_id: call.id.clone(),
                    data: parse_args::<SpawnAgentRequest>(&call.arguments)?,
                });
            }
            name => {
                log::warn!("unsupported agent tool name: {name}, ignored");
            }
        };
    }

    Ok(AgentActionPlan {
        list_agents: Some(list_call_ids),
        spawn_agents,
    })
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
