use super::domain::{AgentDomain, AgentFailure, AgentOutcome, SuspendedWait};
use super::tree::phase::{CancelOrigin, Phase};
use crate::models::AgentPhase;

#[derive(Debug, thiserror::Error)]
pub enum PhaseCodecError {
    #[error("unknown phase name: {0}")]
    UnknownPhase(String),
    #[error("phase {0} requires a payload")]
    MissingPayload(String),
    #[error("phase {0} does not take a payload")]
    UnexpectedPayload(String),
    #[error("payload for phase {name} could not be decoded: {detail}")]
    BadPayload { name: String, detail: String },
    #[error("payload for phase {name} could not be encoded: {detail}")]
    Unencodable { name: String, detail: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredPhase {
    pub name: String,
    pub payload: Option<serde_json::Value>,
}

pub fn encode(phase: &Phase<AgentDomain>) -> Result<StoredPhase, PhaseCodecError> {
    let (kind, payload) = match phase {
        Phase::Running => (AgentPhase::Running, None),
        Phase::Suspended(wait) => (
            AgentPhase::Suspended,
            Some(payload_of(AgentPhase::Suspended, wait)?),
        ),
        Phase::WaitingChildren => (AgentPhase::WaitingChildren, None),
        Phase::Cancelling(origin) => (
            AgentPhase::Cancelling,
            Some(payload_of(AgentPhase::Cancelling, origin)?),
        ),
        Phase::Done(outcome) => (
            AgentPhase::Done,
            Some(payload_of(AgentPhase::Done, outcome)?),
        ),
        Phase::Failed(failure) => (
            AgentPhase::Failed,
            Some(payload_of(AgentPhase::Failed, failure)?),
        ),
        Phase::Cancelled(origin) => (
            AgentPhase::Cancelled,
            Some(payload_of(AgentPhase::Cancelled, origin)?),
        ),
        Phase::Aborted => (AgentPhase::Aborted, None),
    };
    Ok(StoredPhase {
        name: kind.as_ref().to_string(),
        payload,
    })
}

fn payload_of<T: serde::Serialize>(
    phase: AgentPhase,
    value: &T,
) -> Result<serde_json::Value, PhaseCodecError> {
    serde_json::to_value(value).map_err(|error| PhaseCodecError::Unencodable {
        name: phase.as_ref().to_string(),
        detail: error.to_string(),
    })
}

pub fn decode(stored: &StoredPhase) -> Result<Phase<AgentDomain>, PhaseCodecError> {
    let kind: AgentPhase = stored
        .name
        .parse()
        .map_err(|_| PhaseCodecError::UnknownPhase(stored.name.clone()))?;

    if !kind.takes_payload() && stored.payload.is_some() {
        return Err(PhaseCodecError::UnexpectedPayload(stored.name.clone()));
    }

    match kind {
        AgentPhase::Running => Ok(Phase::Running),
        AgentPhase::WaitingChildren => Ok(Phase::WaitingChildren),
        AgentPhase::Aborted => Ok(Phase::Aborted),
        AgentPhase::Suspended => decode_payload::<SuspendedWait>(stored).map(Phase::Suspended),
        AgentPhase::Cancelling => decode_payload::<CancelOrigin>(stored).map(Phase::Cancelling),
        AgentPhase::Done => decode_payload::<AgentOutcome>(stored).map(Phase::Done),
        AgentPhase::Failed => decode_payload::<AgentFailure>(stored).map(Phase::Failed),
        AgentPhase::Cancelled => decode_payload::<CancelOrigin>(stored).map(Phase::Cancelled),
    }
}

fn decode_payload<T>(stored: &StoredPhase) -> Result<T, PhaseCodecError>
where
    T: serde::de::DeserializeOwned,
{
    let value = stored
        .payload
        .clone()
        .ok_or_else(|| PhaseCodecError::MissingPayload(stored.name.clone()))?;
    serde_json::from_value(value).map_err(|error| PhaseCodecError::BadPayload {
        name: stored.name.clone(),
        detail: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// 各用例复用的挂起载荷
    fn wait() -> SuspendedWait {
        SuspendedWait {
            instance_id: 7,
            kind: "approval".into(),
            message_id: 11,
            handle: "h-1".into(),
            payload: None,
        }
    }

    /// `Phase` 各取值的样本，取消来源的两种取值各出现一次
    fn every_phase() -> Vec<Phase<AgentDomain>> {
        vec![
            Phase::Running,
            Phase::Suspended(wait()),
            Phase::WaitingChildren,
            Phase::Cancelling(CancelOrigin::External),
            Phase::Cancelling(CancelOrigin::Ancestor(3)),
            Phase::Done(AgentOutcome { message_id: 5 }),
            Phase::Failed(AgentFailure {
                message_id: Some(5),
                error: "boom".into(),
            }),
            Phase::Cancelled(CancelOrigin::External),
            Phase::Cancelled(CancelOrigin::Ancestor(3)),
            Phase::Aborted,
        ]
    }

    #[test]
    fn every_phase_survives_a_round_trip() {
        for phase in every_phase() {
            let stored = encode(&phase).expect("encode");
            let back = decode(&stored).expect("decode");
            assert_eq!(
                back, phase,
                "round trip preserves the Phase and its payload exactly"
            );
        }
    }

    #[test]
    fn there_are_exactly_eight_phase_names() {
        let names: BTreeSet<String> = every_phase()
            .iter()
            .map(|phase| encode(phase).expect("encode").name)
            .collect();
        assert_eq!(
            names,
            BTreeSet::from([
                "aborted".to_string(),
                "cancelled".to_string(),
                "cancelling".to_string(),
                "done".to_string(),
                "failed".to_string(),
                "running".to_string(),
                "suspended".to_string(),
                "waiting_children".to_string(),
            ]),
            "there are exactly eight Phases; the stored Phase names are a persistence contract"
        );
    }

    #[test]
    fn a_cancellation_keeps_its_origin() {
        let stored = encode(&Phase::Cancelled(CancelOrigin::Ancestor(42))).expect("encode");
        let back = decode(&stored).expect("decode");
        assert!(
            matches!(back, Phase::Cancelled(CancelOrigin::Ancestor(42))),
            "cancellation by an ancestor must be distinguishable from external cancellation, got {back:?}"
        );
    }

    #[test]
    fn a_suspension_keeps_its_handle() {
        let stored = encode(&Phase::Suspended(wait())).expect("encode");
        let back = decode(&stored).expect("decode");
        match back {
            Phase::Suspended(payload) => assert_eq!(payload.handle, "h-1"),
            other => panic!("expected a suspended phase, got {other:?}"),
        }
    }

    #[test]
    fn a_payload_free_phase_reports_no_payload() {
        for phase in [
            Phase::<AgentDomain>::Running,
            Phase::WaitingChildren,
            Phase::Aborted,
        ] {
            assert!(
                encode(&phase).expect("encode").payload.is_none(),
                "{phase:?} must not carry a payload"
            );
        }
    }

    #[test]
    fn an_unknown_phase_name_is_rejected() {
        let stored = StoredPhase {
            name: "zombie".into(),
            payload: None,
        };
        assert!(
            matches!(decode(&stored), Err(PhaseCodecError::UnknownPhase(name)) if name == "zombie"),
            "an unknown Phase name must be rejected, not guessed to the nearest value"
        );
    }

    #[test]
    fn a_phase_that_needs_a_payload_rejects_its_absence() {
        for name in ["suspended", "done", "failed", "cancelled", "cancelling"] {
            let stored = StoredPhase {
                name: name.into(),
                payload: None,
            };
            assert!(
                matches!(
                    decode(&stored),
                    Err(PhaseCodecError::MissingPayload(missing)) if missing == name
                ),
                "{name} must report an error when its payload is missing, otherwise a semantically wrong Phase is constructed"
            );
        }
    }

    #[test]
    fn a_phase_that_takes_no_payload_rejects_a_stray_one() {
        for name in ["running", "waiting_children", "aborted"] {
            let stored = StoredPhase {
                name: name.into(),
                payload: Some(serde_json::json!({ "unexpected": true })),
            };
            assert!(
                matches!(
                    decode(&stored),
                    Err(PhaseCodecError::UnexpectedPayload(stray)) if stray == name
                ),
                "a payload-free Phase receiving a payload indicates data corruption"
            );
        }
    }

    #[test]
    fn a_mismatched_payload_is_rejected() {
        let suspended = serde_json::to_value(wait()).expect("serialize");
        let cases = [
            // 完成 Phase 取到挂起 Phase 的载荷，多出的字段必须被拒绝而不是被截断
            ("done", suspended.clone()),
            // 取消 Phase 是标记枚举，对象载荷无法解析
            ("cancelled", suspended),
            // 完成 Phase 缺少自己必需的字段
            ("done", serde_json::json!({ "unexpected": true })),
        ];
        for (name, payload) in cases {
            let stored = StoredPhase {
                name: name.into(),
                payload: Some(payload),
            };
            assert!(
                matches!(
                    decode(&stored),
                    Err(PhaseCodecError::BadPayload { name: reported, .. }) if reported == name
                ),
                "a payload shape that does not match the discriminator must be rejected: {stored:?}"
            );
        }
    }
}
