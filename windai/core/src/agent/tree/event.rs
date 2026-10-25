use super::{JobId, NodeId, domain::Domain, phase::CancelOrigin, view::TreeEvent};
use crate::agent::behavior::{Input, SpawnRequest};

#[derive(Debug)]
pub enum Event<D: Domain> {
    Spawn {
        id: NodeId,
        parent: Option<NodeId>,
        spec: D::Spec,
        detached: bool,
    },
    Enqueue {
        node: NodeId,
        spec: D::Spec,
    },
    Deliver {
        node: NodeId,
    },
    Resume {
        node: NodeId,
        expect: D::Suspend,
        input: D::Resume,
    },
    CancelRequest {
        root: NodeId,
        origin: CancelOrigin,
    },
    Suspended {
        node: NodeId,
        job: JobId,
        payload: D::Suspend,
    },
    Spawned {
        node: NodeId,
        job: JobId,
        spawn: Vec<SpawnRequest<D>>,
    },
    Completed {
        node: NodeId,
        job: JobId,
        output: D::Output,
    },
    Failed {
        node: NodeId,
        job: JobId,
        error: D::Error,
    },
    Stopped {
        node: NodeId,
        job: JobId,
    },
    Aborted {
        node: NodeId,
        job: JobId,
    },
    Recovered {
        node: NodeId,
    },
}

impl<D: Domain> Event<D> {
    pub fn node(&self) -> Option<NodeId> {
        match self {
            Event::Spawn { parent, .. } => *parent,
            Event::Enqueue { node, .. }
            | Event::Deliver { node }
            | Event::Resume { node, .. }
            | Event::CancelRequest { root: node, .. }
            | Event::Suspended { node, .. }
            | Event::Spawned { node, .. }
            | Event::Completed { node, .. }
            | Event::Failed { node, .. }
            | Event::Stopped { node, .. }
            | Event::Aborted { node, .. }
            | Event::Recovered { node } => Some(*node),
        }
    }

    pub fn reopens(&self) -> bool {
        matches!(self, Event::Spawn { .. } | Event::Enqueue { .. })
    }

    pub fn bypasses_entry_absorption(&self) -> bool {
        matches!(
            self,
            Event::CancelRequest { .. } | Event::Recovered { .. } | Event::Deliver { .. }
        )
    }
}

#[derive(Debug)]
pub enum Effect<D: Domain> {
    Launch {
        node: NodeId,
        job: JobId,
        input: Input<D>,
    },
    Stop {
        node: NodeId,
        job: JobId,
    },
    Persist {
        node: NodeId,
    },
    Publish(TreeEvent<D>),
}
