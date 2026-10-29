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
