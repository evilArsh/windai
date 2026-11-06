use serde::{Deserialize, Serialize};

use crate::agent::tree::Domain;

#[derive(Debug, Clone, PartialEq)]
pub struct LlmDomain;

impl Domain for LlmDomain {
    type Spec = LlmSpec;
    type Suspend = Suspended;
    type Resume = WaitAnswer;
    type Output = LlmOutcome;
    type Error = LlmFailure;
}

/// 一次 agent 运行的输入任务单
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmSpec {
    /// agent 任务实例
    pub instance_id: i64,
    /// 续跑锚定的消息
    pub message_id: Option<i64>,
    /// 待消费的 agent_pending 行
    pub pending_id: Option<i64>,
    /// 直接使用的 turn
    pub turn_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Suspended {
    pub external_wait_id: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WaitAnswer {
    /// 外部等待标识
    pub external_wait_id: i64,
    /// tool call 调用审批判据
    pub decisions: Vec<CallDecision>,
}

/// 对一次调用的判定
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CallDecision {
    /// 调用标识
    pub call_id: String,
    /// 是否放行
    pub allow: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LlmOutcome {
    pub message_id: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LlmFailure {
    pub message_id: Option<i64>,
    pub error: String,
}
