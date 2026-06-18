use serde::{Deserialize, Serialize};

use super::artifact::AgentArtifact;
use super::binding::TopicAgentBindingRole;
use super::context::ContextPolicy;
use super::definition::AgentMode;
use super::instance::{AgentInstanceStatus, AgentOutput};
use super::policy::OutputContract;

/// 查询当前 Topic 中可被当前主 Agent 调度的 Agent。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ListAgentsRequest {
    /// 是否包含 disabled binding。
    pub include_disabled: bool,
}

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
    /// Agent 用户可读名称。
    pub name: String,
    /// Agent 能力描述。
    pub description: String,
    /// Agent 在当前 Topic 中的绑定角色。
    pub role: TopicAgentBindingRole,
    /// 当前 Topic 中允许该 Agent 使用的运行模式。
    pub allowed_modes: Vec<AgentMode>,
    /// 当前 Topic 中该 Agent 的默认运行模式。
    pub default_mode: AgentMode,
}

/// 创建子 Agent 的请求。agent_key 来自当前 TopicAgentBinding 的 alias 或 AgentDefinition.key。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpawnAgentRequest {
    /// 目标 Agent 的 key 或 alias。
    pub agent_key: String,
    /// 子 Agent 运行模式。
    pub mode: AgentMode,
    /// 分配给子 Agent 的任务描述。
    pub task: String,
    /// 本次创建覆盖的上下文策略。
    pub context_policy: Option<ContextPolicy>,
    /// 本次创建覆盖的输出约束。
    pub output_contract: Option<OutputContract>,
    /// 传给子 Agent 的变量。
    pub variables: serde_json::Value,
    /// 本次创建显式指定的模型 id。
    pub model_id: Option<i64>,
}

/// 创建子 Agent 的响应。
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpawnAgentResponse {
    /// 同步子 Agent 已完成。
    SyncCompleted {
        /// AgentInstance id。
        instance_id: i64,
        /// 子 Agent Topic id。
        topic_id: i64,
        /// 子 Agent 输出。
        output: AgentOutput,
    },
    /// 后台子 Agent 已创建。
    Created {
        /// AgentInstance id。
        instance_id: i64,
        /// 子 Agent Topic id。
        topic_id: i64,
        /// AgentInstance 当前状态。
        status: AgentInstanceStatus,
    },
    /// 创建子 Agent 需要用户审批。
    ApprovalRequired {
        /// 需要审批的原因。
        reason: String,
    },
}

/// 等待后台 Agent 完成。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AwaitAgentRequest {
    /// 要等待的 AgentInstance id。
    pub instance_id: i64,
    /// 等待超时时间，单位毫秒。
    pub timeout_ms: Option<u64>,
}

/// await_agent 的响应。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AwaitAgentResponse {
    /// AgentInstance id。
    pub instance_id: i64,
    /// AgentInstance 当前状态。
    pub status: AgentInstanceStatus,
    /// AgentInstance 输出。
    pub output: Option<AgentOutput>,
}

/// 读取 Agent 结果与可选 Artifact。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReadAgentResultRequest {
    /// 要读取的 AgentInstance id。
    pub instance_id: i64,
    /// 是否一并读取关联 artifact。
    pub include_artifacts: bool,
}

/// read_agent_result 的响应。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReadAgentResultResponse {
    /// AgentInstance 输出。
    pub output: Option<AgentOutput>,
    /// 关联 artifact。
    pub artifacts: Vec<AgentArtifact>,
}
