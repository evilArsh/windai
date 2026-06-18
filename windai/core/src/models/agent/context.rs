use serde::{Deserialize, Serialize};

/// 子 Agent 创建时的上下文继承策略。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContextPolicy {
    /// 从父 Agent 或 root Topic 继承上下文的模式。
    pub inherit_mode: ContextInheritMode,
    /// 是否包含父 Agent 上下文摘要。
    pub include_parent_summary: bool,
    /// 是否包含最近的消息片段。
    pub include_recent_messages: bool,
    /// 允许继承的最近消息数量上限。
    pub recent_message_limit: u32,
    /// 显式注入上下文的 artifact id 列表。
    pub include_artifact_ids: Vec<i64>,
    /// 构建上下文时是否执行敏感信息脱敏。
    pub redact_secrets: bool,
    /// 当前 Agent 上下文最多包含的消息数量，None 表示交给默认策略。
    pub max_context_messages: Option<u32>,
}

/// 上下文继承模式。
#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, strum::EnumString, strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ContextInheritMode {
    /// 不继承父上下文。
    None,
    /// 只传递当前任务描述。
    TaskOnly,
    /// 继承父上下文摘要。
    Summary,
    /// 只继承显式选择的 artifact。
    SelectedArtifacts,
    /// 继承父 Agent 的完整上下文快照，主要保留给 fork 模式。
    FullParentSnapshot,
}

impl Default for ContextPolicy {
    fn default() -> Self {
        Self {
            inherit_mode: ContextInheritMode::Summary,
            include_parent_summary: true,
            include_recent_messages: true,
            recent_message_limit: 20,
            include_artifact_ids: Vec::new(),
            redact_secrets: true,
            max_context_messages: None,
        }
    }
}
