use super::domain::AgentDomain;
use super::tree::phase::Phase;
use wind_wire::tool::FunctionCallOutput;

pub fn spawn_response(
    call_id: &str,
    instance_id: i64,
    phase: &Phase<AgentDomain>,
    reply: &str,
) -> FunctionCallOutput {
    let content = match phase {
        Phase::Done(_) => serde_json::Value::String(reply.to_string()),
        Phase::Failed(_) | Phase::Cancelled(_) | Phase::Aborted => {
            serde_json::json!({ "error": "child task did not complete" })
        }
        Phase::Running | Phase::WaitingChildren | Phase::Cancelling(_) => serde_json::json!({
            "instance_id": instance_id,
            "phase": "running",
        }),
        Phase::Suspended(wait) => serde_json::json!({
            "instance_id": instance_id,
            "phase": "suspended",
            "phase_payload": wait,
        }),
    };
    FunctionCallOutput {
        id: call_id.to_string(),
        content,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::domain::{AgentFailure, AgentOutcome, SuspendedWait};
    use crate::agent::events::AgentStatus;
    use crate::agent::tree::phase::CancelOrigin;

    /// 一次外部等待的挂起标记
    fn wait() -> SuspendedWait {
        SuspendedWait {
            instance_id: 7,
            kind: "approval".into(),
            message_id: 11,
            handle: "h-1".into(),
            payload: None,
        }
    }

    #[test]
    fn a_finished_child_returns_its_reply() {
        let phase = Phase::Done(AgentOutcome { message_id: 5 });
        let output = spawn_response("c-1", 7, &phase, "child conclusion");
        assert_eq!(
            output.id, "c-1",
            "the response must be attached to its call"
        );
        assert_eq!(output.content, serde_json::json!("child conclusion"));
    }

    #[test]
    fn a_child_that_never_completes_reports_an_error() {
        let phases = [
            Phase::Failed(AgentFailure {
                message_id: Some(5),
                error: "boom".into(),
            }),
            Phase::Cancelled(CancelOrigin::External),
            Phase::Aborted,
        ];
        for phase in phases {
            let output = spawn_response("c-1", 7, &phase, "a reply that must not be used");
            assert_eq!(
                output.content,
                serde_json::json!({ "error": "child task did not complete" }),
                "{phase:?} must fall into the error branch; a terminated child must not be reported as started"
            );
        }
    }

    #[test]
    fn a_child_still_on_its_way_reports_that_it_started() {
        let phases = [
            Phase::Running,
            Phase::WaitingChildren,
            Phase::Cancelling(CancelOrigin::External),
        ];
        for phase in phases {
            let output = spawn_response("c-1", 7, &phase, "a reply not yet arrived");
            assert_eq!(
                output.content,
                serde_json::json!({ "instance_id": 7, "phase": "running" }),
                "{phase:?} has not produced a result yet, so it can only report started"
            );
        }
    }

    #[test]
    fn a_waiting_child_reports_the_handle_it_wait_is_on() {
        let phase = Phase::Suspended(wait());
        let output = spawn_response("c-1", 7, &phase, "a reply not yet arrived");
        assert_eq!(output.content["instance_id"], serde_json::json!(7));
        assert_eq!(output.content["phase"], serde_json::json!("suspended"));
        assert_eq!(
            output.content["phase_payload"]["handle"],
            serde_json::json!("h-1"),
            "the caller must know which handle it is waiting on, not just a 'started' reply"
        );
    }

    #[test]
    fn the_report_carries_the_shape_of_the_status_event() {
        for phase in [
            Phase::Running,
            Phase::Suspended(wait()),
            Phase::WaitingChildren,
            Phase::Cancelling(CancelOrigin::External),
        ] {
            let output = spawn_response("c-1", 7, &phase, "");
            let status: AgentStatus =
                serde_json::from_value(output.content).expect("match the status event payload");
            assert_eq!(
                status.instance_id, 7,
                "{phase:?} must carry the instance identifier"
            );
        }
    }
}
