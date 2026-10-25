use super::domain::AgentDomain;
use super::events::{AgentError, AgentStatus, CORE_AGENT_ERROR, CORE_AGENT_STATUS};
use super::phase_codec::{StoredPhase, encode};
use super::tree::phase::Terminal;
use super::tree::{NodeId, Phase, TreeEvent};

#[derive(Debug, Clone, PartialEq)]
pub struct Translated {
    pub name: &'static str,
    pub payload: serde_json::Value,
}

pub fn translate(event: &TreeEvent<AgentDomain>) -> Vec<Translated> {
    match event {
        TreeEvent::NodeCreated { .. } => Vec::new(),
        TreeEvent::Sealed => Vec::new(),
        TreeEvent::PhaseChanged { node, to } => status_of(*node, to).into_iter().collect(),
        TreeEvent::Terminated { node, terminal } => {
            let mut out: Vec<Translated> = status_of(*node, &terminal_phase(terminal))
                .into_iter()
                .collect();
            match terminal {
                Terminal::Done(_) | Terminal::Cancelled(_) => {}
                Terminal::Failed(failure) => {
                    if let Some(item) = error(*node, failure.message_id, &failure.error) {
                        out.push(item);
                    }
                }
                Terminal::Aborted => {
                    if let Some(item) = error(*node, None, "instance aborted") {
                        out.push(item);
                    }
                }
            }
            out
        }
    }
}

fn status_of(node: NodeId, phase: &Phase<AgentDomain>) -> Option<Translated> {
    let stored = match encode(phase) {
        Ok(stored) => stored,
        Err(error) => {
            log::error!("[agent] cannot encode the phase of {node}: {error}");
            return None;
        }
    };
    status(node, &stored)
}

fn status(node: NodeId, stored: &StoredPhase) -> Option<Translated> {
    Some(Translated {
        name: CORE_AGENT_STATUS.name,
        payload: payload_of(
            CORE_AGENT_STATUS.name,
            &AgentStatus {
                instance_id: node,
                phase: stored.name.clone(),
                phase_payload: stored.payload.clone(),
            },
        )?,
    })
}

fn error(node: NodeId, message_id: Option<i64>, text: &str) -> Option<Translated> {
    Some(Translated {
        name: CORE_AGENT_ERROR.name,
        payload: payload_of(
            CORE_AGENT_ERROR.name,
            &AgentError {
                instance_id: node,
                message_id,
                error: text.to_string(),
            },
        )?,
    })
}

fn terminal_phase(terminal: &Terminal<AgentDomain>) -> Phase<AgentDomain> {
    match terminal {
        Terminal::Done(output) => Phase::Done(output.clone()),
        Terminal::Failed(failure) => Phase::Failed(failure.clone()),
        Terminal::Cancelled(origin) => Phase::Cancelled(*origin),
        Terminal::Aborted => Phase::Aborted,
    }
}

fn payload_of<T: serde::Serialize>(name: &str, value: &T) -> Option<serde_json::Value> {
    match serde_json::to_value(value) {
        Ok(value) => Some(value),
        Err(error) => {
            log::error!("[agent] cannot encode the {name} payload: {error}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::domain::{AgentFailure, AgentOutcome, SuspendedWait};
    use crate::agent::tree::Phase;
    use crate::agent::tree::phase::CancelOrigin;
    use crate::agent::tree::phase::Terminal;

    /// 取出翻译结果的事件名序列
    fn names(items: &[Translated]) -> Vec<&str> {
        items.iter().map(|t| t.name).collect()
    }

    #[test]
    fn a_non_terminal_phase_change_becomes_a_status_event() {
        let event = TreeEvent::<AgentDomain>::PhaseChanged {
            node: 1,
            to: Box::new(Phase::Running),
        };
        let translated = translate(&event);
        assert_eq!(names(&translated), vec!["core/agent.status"]);
        assert_eq!(translated[0].payload["phase"], serde_json::json!("running"));
        assert!(
            translated[0].payload.get("phase_payload").is_none(),
            "a payload-free Phase writes no empty key"
        );
    }

    #[test]
    fn finishing_produces_both_a_status_and_a_terminal_fact() {
        let event = TreeEvent::<AgentDomain>::Terminated {
            node: 1,
            terminal: Terminal::Done(AgentOutcome { message_id: 5 }),
        };
        let translated = translate(&event);

        let status = translated
            .iter()
            .find(|t| t.name == "core/agent.status")
            .expect("find the status event");
        assert_eq!(
            status.payload["phase"],
            serde_json::json!("done"),
            "a Phase event must report the terminal Phase"
        );
        assert_eq!(
            status.payload["phase_payload"]["message_id"],
            serde_json::json!(5),
            "a Phase event must carry the terminal payload"
        );
        assert!(
            translated.iter().all(|t| t.name != "core/agent.error"),
            "completion is not failure, so no error event may be produced"
        );
    }

    #[test]
    fn a_suspension_yields_a_status_and_leaves_the_wait_event_to_the_initiator() {
        let event = TreeEvent::<AgentDomain>::PhaseChanged {
            node: 1,
            to: Box::new(Phase::Suspended(SuspendedWait {
                instance_id: 1,
                kind: "approval".into(),
                message_id: 11,
                handle: "h-1".into(),
                payload: None,
            })),
        };
        let translated = translate(&event);
        assert_eq!(
            names(&translated),
            vec!["core/agent.status"],
            "the pending call list must be read from blocks on the message; the translation exit has no storage and cannot produce this event"
        );
        assert_eq!(
            translated[0].payload["phase"],
            serde_json::json!("suspended")
        );
        assert_eq!(
            translated[0].payload["phase_payload"]["handle"],
            serde_json::json!("h-1"),
            "the Phase payload must carry the suspension handle unchanged"
        );
    }

    #[test]
    fn a_failure_reports_its_error_text() {
        let event = TreeEvent::<AgentDomain>::Terminated {
            node: 1,
            terminal: Terminal::Failed(AgentFailure {
                message_id: Some(5),
                error: "boom".into(),
            }),
        };
        let translated = translate(&event);
        let error = translated
            .iter()
            .find(|t| t.name == "core/agent.error")
            .expect("find the error event");
        assert_eq!(error.payload["error"], serde_json::json!("boom"));
        assert_eq!(error.payload["message_id"], serde_json::json!(5));
    }

    #[test]
    fn a_cancellation_reports_status_but_no_error() {
        let event = TreeEvent::<AgentDomain>::Terminated {
            node: 1,
            terminal: Terminal::Cancelled(CancelOrigin::External),
        };
        let translated = translate(&event);
        assert_eq!(names(&translated), vec!["core/agent.status"]);
        assert_eq!(
            translated[0].payload["phase"],
            serde_json::json!("cancelled")
        );
    }

    #[test]
    fn an_abort_reports_itself_rather_than_a_generic_failure() {
        let event = TreeEvent::<AgentDomain>::Terminated {
            node: 1,
            terminal: Terminal::Aborted,
        };
        let translated = translate(&event);
        let error = translated
            .iter()
            .find(|t| t.name == "core/agent.error")
            .expect("find the abort event");
        assert_eq!(
            error.payload["error"],
            serde_json::json!("instance aborted"),
            "an abort must be distinguishable from a generic failure; collapsing both into one failed public enum would lose information"
        );
    }

    #[test]
    fn a_created_node_produces_no_event_of_its_own() {
        let event = TreeEvent::<AgentDomain>::NodeCreated {
            node: 1,
            parent: None,
        };
        assert!(
            translate(&event).is_empty(),
            "instance creation produces its event directly on the creation path"
        );
    }

    #[test]
    fn a_sealed_tree_produces_no_event_of_its_own() {
        let event = TreeEvent::<AgentDomain>::Sealed;
        assert!(
            translate(&event).is_empty(),
            "no separate event is emitted when the whole tree stops changing"
        );
    }
}
