use super::{NodeId, domain::Domain, phase::Phase};

#[derive(Debug, Clone)]
pub enum TreeEvent<D: Domain> {
    NodeCreated {
        node: NodeId,
        parent: Option<NodeId>,
    },
    PhaseChanged {
        node: NodeId,
        to: Phase<D>,
    },
    Sealed {
        root: NodeId,
    },
}
