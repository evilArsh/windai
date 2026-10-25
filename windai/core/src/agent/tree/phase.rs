use super::domain::Domain;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum CancelOrigin {
    External,
    Ancestor(i64),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Terminal<D: Domain> {
    Done(D::Output),
    Failed(D::Error),
    Cancelled(CancelOrigin),
    Aborted,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Phase<D: Domain> {
    Running,
    Suspended(D::Suspend),
    WaitingChildren,
    Cancelling(CancelOrigin),
    Done(D::Output),
    Failed(D::Error),
    Cancelled(CancelOrigin),
    Aborted,
}

impl<D: Domain> Phase<D> {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Phase::Done(_) | Phase::Failed(_) | Phase::Cancelled(_) | Phase::Aborted
        )
    }

    pub fn is_resting(&self) -> bool {
        self.is_terminal() || matches!(self, Phase::Suspended(_) | Phase::WaitingChildren)
    }

    pub fn terminal(&self) -> Option<Terminal<D>> {
        match self {
            Phase::Done(output) => Some(Terminal::Done(output.clone())),
            Phase::Failed(error) => Some(Terminal::Failed(error.clone())),
            Phase::Cancelled(origin) => Some(Terminal::Cancelled(*origin)),
            Phase::Aborted => Some(Terminal::Aborted),
            _ => None,
        }
    }

    pub fn has_live_job(&self) -> bool {
        matches!(self, Phase::Running | Phase::Cancelling(_))
    }

    pub fn awaiting_external_input(&self) -> Option<&D::Suspend> {
        match self {
            Phase::Suspended(wait) => Some(wait),
            Phase::Running
            | Phase::WaitingChildren
            | Phase::Cancelling(_)
            | Phase::Done(_)
            | Phase::Failed(_)
            | Phase::Cancelled(_)
            | Phase::Aborted => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tree::domain::test_support::{
        TestDomain, TestError, TestOutput, TestSuspend,
    };

    #[test]
    fn resting_and_terminal_are_distinct() {
        let suspended: Phase<TestDomain> = Phase::Suspended(TestSuspend::Ask);
        assert!(!suspended.is_terminal(), "suspended is not terminal");
        assert!(suspended.is_resting(), "suspended is a resting phase");

        let cancelling: Phase<TestDomain> = Phase::Cancelling(CancelOrigin::External);
        assert!(
            !cancelling.is_terminal(),
            "Cancelling is an intermediate phase"
        );
        assert!(
            !cancelling.is_resting(),
            "Cancelling is the phase that is cancelling"
        );

        let done: Phase<TestDomain> = Phase::Done(TestOutput::Ok);
        assert!(done.is_terminal());
        assert!(done.is_resting());

        let aborted: Phase<TestDomain> = Phase::Aborted;
        assert!(aborted.is_terminal(), "Aborted is terminal");
        assert!(aborted.is_resting(), "Aborted is a resting phase");
        assert!(!aborted.has_live_job(), "Aborted has no running worker");

        assert!(Phase::<TestDomain>::WaitingChildren.is_resting());
        assert!(!Phase::<TestDomain>::Running.is_resting());
    }

    #[test]
    fn terminal_view_carries_the_payload() {
        let done: Phase<TestDomain> = Phase::Done(TestOutput::Ok);
        assert!(matches!(
            done.terminal(),
            Some(Terminal::Done(TestOutput::Ok))
        ));

        let cancelled: Phase<TestDomain> = Phase::Cancelled(CancelOrigin::Ancestor(7));
        assert!(matches!(
            cancelled.terminal(),
            Some(Terminal::Cancelled(CancelOrigin::Ancestor(7)))
        ));

        let failed: Phase<TestDomain> = Phase::Failed(TestError::Boom);
        assert!(matches!(
            failed.terminal(),
            Some(Terminal::Failed(TestError::Boom))
        ));

        let aborted: Phase<TestDomain> = Phase::Aborted;
        assert!(
            matches!(aborted.terminal(), Some(Terminal::Aborted)),
            "Aborted carries no payload"
        );
        assert!(Phase::<TestDomain>::Running.terminal().is_none());
    }

    #[test]
    fn only_running_and_cancelling_carry_a_live_job() {
        assert!(Phase::<TestDomain>::Running.has_live_job());
        assert!(Phase::<TestDomain>::Cancelling(CancelOrigin::External).has_live_job());
        assert!(!Phase::<TestDomain>::WaitingChildren.has_live_job());
        assert!(!Phase::<TestDomain>::Done(TestOutput::Ok).has_live_job());
    }

    #[test]
    fn only_the_suspended_phase_awaits_external_input() {
        assert!(
            Phase::<TestDomain>::Suspended(TestSuspend::Ask)
                .awaiting_external_input()
                .is_some()
        );
        for phase in [
            Phase::<TestDomain>::Running,
            Phase::WaitingChildren,
            Phase::Cancelling(CancelOrigin::External),
            Phase::Done(TestOutput::Ok),
            Phase::Failed(TestError::Boom),
            Phase::Cancelled(CancelOrigin::External),
            Phase::Aborted,
        ] {
            assert!(
                phase.awaiting_external_input().is_none(),
                "{phase:?} must not be treated as awaiting external input"
            );
        }
    }
}
