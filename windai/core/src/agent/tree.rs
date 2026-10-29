pub mod cancel;
pub mod domain;
pub mod event;
pub mod phase;
pub mod reduce;
pub mod view;
pub use domain::Domain;
pub use phase::{Phase, Terminal};

use std::collections::{BTreeMap, VecDeque};
use std::mem;

/// instance id 映射成 node id
pub type NodeId = i64;
pub type JobId = u64;

#[derive(Debug, Clone)]
pub struct Node<D: Domain> {
    pub id: NodeId,
    pub parent: Option<NodeId>,
    pub children: Vec<NodeId>,
    pub phase: Phase<D>,
    pub job: Option<JobId>,
    pub detached: bool,
    pub pending: VecDeque<D::Spec>,
    pub delivered: bool,
}

#[derive(Debug)]
pub struct Tree<D: Domain> {
    nodes: BTreeMap<NodeId, Node<D>>,
    root: Option<NodeId>,
    job_seq: JobId,
}

impl<D: Domain> Default for Tree<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: Domain> Tree<D> {
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            root: None,
            job_seq: 0,
        }
    }

    pub fn node(&self, id: NodeId) -> Option<&Node<D>> {
        self.nodes.get(&id)
    }

    pub fn phase(&self, id: NodeId) -> Option<&Phase<D>> {
        self.nodes.get(&id).map(|node| &node.phase)
    }

    pub fn job_of(&self, id: NodeId) -> Option<JobId> {
        self.nodes.get(&id).and_then(|node| node.job)
    }

    pub fn root(&self) -> Option<NodeId> {
        self.root
    }

    /// 树所有节点
    pub fn nodes(&self) -> impl Iterator<Item = &Node<D>> {
        self.nodes.values()
    }

    pub fn all_node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    pub fn is_quiescent(&self) -> bool {
        self.nodes
            .values()
            .all(|node| node.phase.is_terminal() && node.pending.is_empty() && node.delivered)
    }

    pub fn in_flight_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|node| node.phase.has_live_job())
            .map(|node| node.id)
            .collect()
    }

    pub fn undelivered_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|node| !node.delivered)
            .map(|node| node.id)
            .collect()
    }

    pub fn all_children_terminal(&self, id: NodeId) -> bool {
        let barrier = self.barrier_children(id);
        !barrier.is_empty()
            && barrier
                .iter()
                .all(|child| self.phase(*child).is_some_and(Phase::is_terminal))
    }

    pub fn barrier_children(&self, id: NodeId) -> Vec<NodeId> {
        let Some(node) = self.nodes.get(&id) else {
            return Vec::new();
        };
        node.children
            .iter()
            .copied()
            .filter(|child| !self.is_detached(*child))
            .collect()
    }

    pub fn is_detached(&self, id: NodeId) -> bool {
        self.nodes.get(&id).is_some_and(|node| node.detached)
    }

    pub fn subtree_post_order(&self, root: NodeId) -> Vec<NodeId> {
        let mut out = Vec::new();
        let mut stack = vec![(root, false)];
        while let Some((id, expanded)) = stack.pop() {
            if expanded {
                out.push(id);
                continue;
            }
            stack.push((id, true));
            if let Some(node) = self.nodes.get(&id) {
                for child in node.children.iter().rev() {
                    stack.push((*child, false));
                }
            }
        }
        out
    }

    pub(crate) fn insert_node(
        &mut self,
        id: NodeId,
        parent: Option<NodeId>,
        phase: Phase<D>,
        detached: bool,
    ) -> bool {
        if self.nodes.contains_key(&id) {
            log::warn!("[tree] duplicate node id ignored: {id}");
            return false;
        }
        if let Some(parent_id) = parent
            && !self.nodes.contains_key(&parent_id)
        {
            log::warn!("[tree] unknown parent ignored: {parent_id}");
            return false;
        }
        self.nodes.insert(
            id,
            Node {
                id,
                parent,
                children: Vec::new(),
                phase,
                job: None,
                detached,
                pending: VecDeque::new(),
                delivered: true,
            },
        );
        match parent {
            Some(parent_id) => {
                if let Some(node) = self.nodes.get_mut(&parent_id) {
                    node.children.push(id);
                }
            }
            None => {
                if self.root.is_none() {
                    self.root = Some(id);
                }
            }
        }
        true
    }

    pub(crate) fn restore_node(
        &mut self,
        id: NodeId,
        parent: Option<NodeId>,
        children: Vec<NodeId>,
        phase: Phase<D>,
        detached: bool,
        pending: Vec<D::Spec>,
    ) {
        self.nodes.insert(
            id,
            Node {
                id,
                parent,
                children,
                phase,
                job: None,
                detached,
                pending: pending.into(),
                delivered: true,
            },
        );
        if parent.is_none() && self.root.is_none() {
            self.root = Some(id);
        }
    }

    pub(crate) fn set_phase(&mut self, id: NodeId, phase: Phase<D>) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.phase = phase;
        }
    }

    pub(crate) fn set_job(&mut self, id: NodeId, job: Option<JobId>) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.job = job;
        }
    }

    pub(crate) fn next_job_id(&mut self) -> JobId {
        self.job_seq += 1;
        self.job_seq
    }

    pub(crate) fn push_pending(&mut self, id: NodeId, spec: D::Spec) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.pending.push_back(spec);
        }
    }

    pub(crate) fn take_pending(&mut self, id: NodeId) -> Vec<D::Spec> {
        self.nodes
            .get_mut(&id)
            .map_or_else(Vec::new, |node| mem::take(&mut node.pending).into())
    }

    pub(crate) fn clear_pending(&mut self, id: NodeId) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.pending.clear();
        }
    }

    pub(crate) fn set_delivered(&mut self, id: NodeId, delivered: bool) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.delivered = delivered;
        }
    }

    #[cfg(test)]
    pub(crate) fn set_delivered_for_test(&mut self, id: NodeId, delivered: bool) {
        self.set_delivered(id, delivered);
    }

    #[cfg(test)]
    pub(crate) fn insert_for_test(&mut self, id: NodeId, parent: Option<NodeId>) -> bool {
        self.insert_node(id, parent, Phase::Running, false)
    }

    #[cfg(test)]
    pub(crate) fn insert_for_test_detached(&mut self, id: NodeId, parent: Option<NodeId>) -> bool {
        self.insert_node(id, parent, Phase::Running, true)
    }

    #[cfg(test)]
    pub(crate) fn set_phase_for_test(&mut self, id: NodeId, phase: Phase<D>) {
        self.set_phase(id, phase);
    }
}
