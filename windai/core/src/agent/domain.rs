use super::tree::Domain;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
pub struct AgentDomain;

impl Domain for AgentDomain {
    type Spec = AgentSpec;
    type Suspend = SuspendedWait;
    type Resume = WaitAnswer;
    type Output = AgentOutcome;
    type Error = AgentFailure;
}

/// 一次 agent 运行的输入任务单
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSpec {
    /// 运行所属的任务实例
    pub instance_id: i64,
    /// 续跑锚定的消息
    pub message_id: Option<i64>,
    /// 待消费的 agent_pending 行
    pub pending_id: Option<i64>,
    /// 直接使用的 turn
    pub turn_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr, strum::EnumString, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum WaitKind {
    Approval,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuspendedWait {
    pub instance_id: i64,
    pub kind: String,
    pub message_id: i64,
    pub handle: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

pub fn pending_call_ids(payload: &serde_json::Value) -> Vec<String> {
    payload
        .get("pending_call_ids")
        .and_then(serde_json::Value::as_array)
        .map(|ids| {
            ids.iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WaitAnswer {
    pub handle: String,
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
pub struct AgentOutcome {
    pub message_id: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentFailure {
    pub message_id: Option<i64>,
    pub error: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wait_round_trips_through_json() {
        let wait = SuspendedWait {
            instance_id: 7,
            kind: "approval".into(),
            message_id: 11,
            handle: "h-1".into(),
            payload: Some(serde_json::json!({ "pending_call_ids": ["c-2"] })),
        };
        let text = serde_json::to_string(&wait).expect("serialize");
        let back: SuspendedWait = serde_json::from_str(&text).expect("deserialize");
        assert_eq!(back, wait, "round trip preserves the original value");
    }

    #[test]
    fn a_payload_with_an_undeclared_field_is_rejected() {
        let stray = r#"{"message_id": 5, "handle": "h-1"}"#;
        assert!(
            serde_json::from_str::<AgentOutcome>(stray).is_err(),
            "an output payload with an undeclared field must be rejected, not silently truncated"
        );

        let stray = r#"{"message_id": 5, "error": "boom", "kind": "approval"}"#;
        assert!(
            serde_json::from_str::<AgentFailure>(stray).is_err(),
            "a failure payload with an undeclared field must be rejected, not silently truncated"
        );

        let stray = r#"{"instance_id": 7, "kind": "approval", "message_id": 11, "handle": "h-1", "stray": 1}"#;
        assert!(
            serde_json::from_str::<SuspendedWait>(stray).is_err(),
            "a suspended payload with an undeclared field must be rejected, not silently truncated"
        );
    }

    #[test]
    fn an_answer_is_keyed_by_call_identifier() {
        let answer = WaitAnswer {
            handle: "h-1".into(),
            decisions: vec![
                CallDecision {
                    call_id: "a".into(),
                    allow: true,
                },
                CallDecision {
                    call_id: "b".into(),
                    allow: false,
                },
            ],
        };
        let text = serde_json::to_string(&answer).expect("serialize");
        let back: WaitAnswer = serde_json::from_str(&text).expect("deserialize");
        assert_eq!(back, answer);
    }

    #[test]
    fn the_domain_bounds_hold() {
        fn assert_domain<D: Domain>() {}
        assert_domain::<AgentDomain>();
    }
}
