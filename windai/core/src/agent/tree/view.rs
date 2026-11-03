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
    Sealed {
        node: NodeId,
    },
}
impl<D: Domain> TreeEvent<D> {
    pub fn node_id(&self) -> NodeId {
        match self {
            TreeEvent::NodeCreated { node, .. } => *node,
            TreeEvent::PhaseChanged { node, .. } => *node,
            TreeEvent::Terminated { node, .. } => *node,
            TreeEvent::Sealed { node } => *node,
        }
    }
}
