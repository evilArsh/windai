use serde::{Deserialize, Serialize};

/// Agent 调度权限边界。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PermissionPolicy {
    /// 当前 Agent 是否允许创建其它 Agent。
    pub can_spawn_agents: bool,
    /// 当前 Agent 是否允许创建同步子 Agent。
    pub can_spawn_sync: bool,
    /// 当前 Agent 是否允许创建后台子 Agent。
    pub can_spawn_background: bool,
    /// 当前 Agent 是否允许创建团队 Agent。
    pub can_spawn_team: bool,
    /// 当前 Agent 是否允许创建 fork Agent。
    pub can_spawn_fork: bool,
    /// 当前 Agent 创建的子 Agent 是否允许继续创建子 Agent。
    pub can_spawn_recursive: bool,
    /// Agent 调度树的最大深度。
    pub max_spawn_depth: u32,
}

impl Default for PermissionPolicy {
    fn default() -> Self {
        Self {
            can_spawn_agents: false,
            can_spawn_sync: false,
            can_spawn_background: false,
            can_spawn_team: false,
            can_spawn_fork: false,
            can_spawn_recursive: false,
            max_spawn_depth: 0,
        }
    }
}

/// Agent 运行资源限制。
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct RuntimeLimits {
    /// 单个 Agent 实例最大运行时间，单位毫秒。
    pub max_runtime_ms: Option<u64>,
    /// 单个 Agent 实例最大 LLM 调用次数。
    pub max_llm_calls: Option<u32>,
    /// 单个 Agent 实例最大工具调用次数。
    pub max_tool_calls: Option<u32>,
    /// 单个 Agent 实例最多可创建的子 Agent 数量。
    pub max_child_agents: Option<u32>,
    /// 单个 Agent 实例最多可并行运行的子 Agent 数量。
    pub max_parallel_child_agents: Option<u32>,
    /// 单次运行允许的最大输入 token 数。
    pub max_input_tokens: Option<u32>,
    /// 单次运行允许的最大输出 token 数。
    pub max_output_tokens: Option<u32>,
    /// 单个 Agent 实例允许写入的 artifact 总大小上限。
    pub max_artifact_bytes: Option<u64>,
}

/// Agent 输出约束。用于提示模型和校验输出。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OutputContract {
    /// 期望的输出格式。
    pub format: OutputFormat,
    /// 当输出为 JSON 时使用的 JSON Schema。
    pub json_schema: Option<serde_json::Value>,
    /// Markdown 或文本输出中必须包含的章节名。
    pub required_sections: Vec<String>,
    /// 是否要求本次执行生成 artifact。
    pub artifact_required: bool,
    /// 是否要求本次执行生成摘要。
    pub summary_required: bool,
}

/// Agent 输出格式。
#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, strum::EnumString, strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum OutputFormat {
    /// 普通纯文本输出。
    PlainText,
    /// Markdown 输出。
    Markdown,
    /// JSON 输出。
    Json,
    /// Patch 或 diff 输出。
    Patch,
    /// 混合文本与 artifact 的输出。
    Mixed,
}

/// PolicyEngine 校验时需要的运行时上下文。
pub struct PolicyContext {
    /// 当前任务所属的 root Topic id。
    pub root_topic_id: i64,
    /// 当前正在运行的 Topic id。
    pub current_topic_id: i64,
    /// 当前 TopicAgentBinding id。
    pub current_binding_id: Option<i64>,
    /// 当前 AgentInstance id。
    pub current_agent_instance_id: Option<i64>,
    /// 当前 AgentDefinition id。
    pub current_agent_id: Option<i64>,
    /// 当前 Agent 调度深度。
    pub current_depth: u32,
    /// 当前已创建的子 Agent 数量。
    pub active_child_count: u32,
    /// 当前正在并行运行的子 Agent 数量。
    pub active_parallel_child_count: u32,
}

/// PolicyEngine 的决策结果。
pub enum PolicyDecision {
    /// 允许执行。
    Allow,
    /// 拒绝执行并返回原因。
    Deny {
        /// 拒绝执行的原因。
        reason: String,
    },
    /// 需要用户审批并返回原因。
    RequireApproval {
        /// 需要审批的原因。
        reason: String,
    },
}
