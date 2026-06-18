use serde::{Deserialize, Serialize};

use super::context::ContextPolicy;
use super::definition::AgentMode;
use super::policy::OutputContract;

/// 一次 Agent 运行实例。每个实例绑定一个可视化 Topic，并记录生命周期状态。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentInstance {
    /// AgentInstance 主键。
    pub id: i64,
    /// 本次运行所属 root Topic id。
    pub root_topic_id: i64,
    /// 当前 AgentInstance 运行所在 Topic id。
    pub topic_id: i64,
    /// 当前实例使用的 AgentDefinition id。
    pub agent_id: i64,
    /// 当前实例来源的 TopicAgentBinding id。
    pub binding_id: Option<i64>,
    /// 父 AgentInstance id。
    pub parent_instance_id: Option<i64>,
    /// 根任务 id。
    pub root_task_id: i64,
    /// 当前任务 id。
    pub task_id: i64,
    /// 当前实例运行模式。
    pub mode: AgentMode,
    /// 当前实例生命周期状态。
    pub status: AgentInstanceStatus,
    /// 当前实例在 Agent 调度树中的深度。
    pub depth: i32,
    /// 当前实例输入。
    pub input: AgentInput,
    /// 当前实例输出。
    pub output: Option<AgentOutput>,
    /// 当前实例失败信息。
    pub error: Option<AgentErrorInfo>,
    /// 创建时间戳。
    pub created_at: i64,
    /// 更新时间戳。
    pub updated_at: i64,
    /// 完成时间戳。
    pub finished_at: Option<i64>,
}

/// AgentInstance 生命周期状态。WaitingApproval 用于后台 Agent 等待用户审批 MCP。
#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, strum::EnumString, strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentInstanceStatus {
    /// 已创建。
    Created,
    /// 已进入队列。
    Queued,
    /// 正在运行。
    Running,
    /// 正在等待用户审批。
    WaitingApproval,
    /// 正在等待子 Agent。
    WaitingChild,
    /// 已完成。
    Completed,
    /// 已失败。
    Failed,
    /// 已取消。
    Cancelled,
    /// 已超时。
    TimedOut,
}

/// 创建 AgentInstance 时的任务输入。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentInput {
    /// 分配给 Agent 的任务描述。
    pub task: String,
    /// 原始用户消息。
    pub user_message: Option<String>,
    /// 任务变量。
    pub variables: serde_json::Value,
    /// 本次运行覆盖的上下文策略。
    pub context_policy: Option<ContextPolicy>,
    /// 本次运行覆盖的输出约束。
    pub output_contract: Option<OutputContract>,
    /// 本次运行显式指定的模型 id。
    pub model_id: Option<i64>,
}

/// AgentInstance 完成后的摘要输出。详细结果通过 Artifact 保存。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentOutput {
    /// 结果摘要。
    pub summary: String,
    /// 可选正文内容。
    pub content: Option<String>,
    /// 相关 artifact id 列表。
    pub artifact_ids: Vec<i64>,
    /// 本次运行是否成功。
    pub success: bool,
    /// 可选置信度。
    pub confidence: Option<f32>,
}

/// AgentInstance 失败时保存的结构化错误。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentErrorInfo {
    /// 错误代码。
    pub code: String,
    /// 错误消息。
    pub message: String,
    /// 是否可重试。
    pub retryable: bool,
    /// 错误详情。
    pub detail: serde_json::Value,
}
