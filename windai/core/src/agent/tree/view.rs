use super::{NodeId, domain::Domain, phase::Phase, phase::Terminal};

#[derive(Debug, Clone)]
pub enum TreeEvent<D: Domain> {
    NodeCreated {
        node: NodeId,
        parent: Option<NodeId>,
    },
    PhaseChanged {
        node: NodeId,
        to: Box<Phase<D>>,
    },
    Terminated {
        node: NodeId,
        terminal: Terminal<D>,
    },
    Sealed,
}
