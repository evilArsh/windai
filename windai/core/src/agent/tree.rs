pub mod cancel;
pub mod domain;
pub mod event;
pub mod phase;
pub mod reduce;
pub mod view;
pub use domain::Domain;
pub use event::{Effect, Event};
pub use phase::{CancelOrigin, Phase, Terminal};
pub use view::TreeEvent;

use std::collections::BTreeMap;

pub type NodeId = i64;
pub type JobId = u64;

/// 树上的一个节点
#[derive(Debug, Clone)]
pub struct Node<D: Domain> {
    pub id: NodeId,
    pub parent: Option<NodeId>,
    pub children: Vec<NodeId>,
    pub phase: Phase<D>,
    /// 当前在跑的 worker id
    pub job: Option<JobId>,
    /// 不进入父节点的等待屏障
    pub detached: bool,
    /// 待消费输入
    pub pending: Option<D::Spec>,
    /// 产出是否已被领域投递
    ///
    /// 只对 detached 的终态节点有约束力，其它节点恒为 true
    pub delivered: bool,
}

/// 任务树，全部状态的唯一持有者
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

    /// 节点生命周期状态
    pub fn phase(&self, id: NodeId) -> Option<&Phase<D>> {
        self.nodes.get(&id).map(|node| &node.phase)
    }

    pub fn job_of(&self, id: NodeId) -> Option<JobId> {
        self.nodes.get(&id).and_then(|node| node.job)
    }

    pub fn root(&self) -> Option<NodeId> {
        self.root
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node<D>> {
        self.nodes.values()
    }

    /// 全部节点 id，升序
    pub fn all_node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    /// 所有节点都处于终态
    pub fn is_quiescent(&self) -> bool {
        self.nodes
            .values()
            .all(|node| node.phase.is_terminal() && node.pending.is_none() && node.delivered)
    }

    /// 在途节点
    pub fn in_flight_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|node| node.phase.has_live_job())
            .map(|node| node.id)
            .collect()
    }

    /// 未投递的节点
    pub fn undelivered_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|node| !node.delivered)
            .map(|node| node.id)
            .collect()
    }

    /// 屏障子节点非空且全部处于终态
    ///
    /// detached 子节点不参与父节点的等待屏障
    pub fn all_children_terminal(&self, id: NodeId) -> bool {
        let barrier = self.barrier_children(id);
        !barrier.is_empty()
            && barrier
                .iter()
                .all(|child| self.phase(*child).is_some_and(Phase::is_terminal))
    }

    /// 进入父节点等待屏障的子节点
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

    /// 节点是否不进入父节点的等待屏障
    pub fn is_detached(&self, id: NodeId) -> bool {
        self.nodes.get(&id).is_some_and(|node| node.detached)
    }

    /// 后序子树遍历
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

    /// 插入一个节点，id 由领域提供
    ///
    /// id 已存在或父节点不存在时不做任何改动并返回 false
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
                pending: None,
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

    /// 用持久化数据重建一个节点
    ///
    /// 重建出来的节点不背投递义务，`delivered` 一律为 true
    pub(crate) fn restore_node(
        &mut self,
        id: NodeId,
        parent: Option<NodeId>,
        children: Vec<NodeId>,
        phase: Phase<D>,
        detached: bool,
        pending: Option<D::Spec>,
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
                pending,
                delivered: true,
            },
        );
        if parent.is_none() && self.root.is_none() {
            self.root = Some(id);
        }
    }

    /// 设置节点生命周期状态
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

    pub(crate) fn set_pending(&mut self, id: NodeId, spec: Option<D::Spec>) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.pending = spec;
        }
    }

    /// 取走待消费输入
    pub(crate) fn take_pending(&mut self, id: NodeId) -> Option<D::Spec> {
        self.nodes.get_mut(&id).and_then(|node| node.pending.take())
    }

    pub(crate) fn set_delivered(&mut self, id: NodeId, delivered: bool) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.delivered = delivered;
        }
    }

    #[cfg(test)]
    pub(crate) fn set_pending_for_test(&mut self, id: NodeId, spec: Option<D::Spec>) {
        self.set_pending(id, spec);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tree::domain::test_support::{
        TestDomain, TestError, TestOutput, TestSpec, TestSuspend,
    };

    #[test]
    fn children_keep_insertion_order() {
        let mut tree = Tree::<TestDomain>::new();
        assert!(tree.insert_for_test(1, None));
        assert!(tree.insert_for_test(2, Some(1)));
        assert!(tree.insert_for_test(3, Some(1)));

        assert_eq!(tree.node(1).expect("root").children, vec![2, 3]);
        assert_eq!(tree.node(2).expect("a").parent, Some(1));
        assert_eq!(tree.root(), Some(1));
    }

    #[test]
    fn a_duplicate_node_id_is_ignored() {
        let mut tree = Tree::<TestDomain>::new();
        assert!(tree.insert_for_test(1, None));
        assert!(
            !tree.insert_for_test(1, None),
            "id 由领域提供，重复必须被拒绝"
        );
        assert_eq!(tree.all_node_ids(), vec![1]);
    }

    #[test]
    fn an_unknown_parent_is_rejected() {
        let mut tree = Tree::<TestDomain>::new();
        assert!(
            !tree.insert_for_test(1, Some(99)),
            "未知父节点必须被拒绝，否则会留下挂不上任何 children 的悬空节点"
        );
        assert_eq!(tree.all_node_ids(), Vec::<NodeId>::new());
        assert_eq!(tree.root(), None);
    }

    #[test]
    fn the_root_is_the_first_parentless_node_in_both_paths() {
        let mut tree = Tree::<TestDomain>::new();
        assert!(tree.insert_for_test(1, None));
        assert!(tree.insert_for_test(2, None));
        assert_eq!(tree.root(), Some(1), "插入路径认第一个无父节点");

        let mut restored = Tree::<TestDomain>::new();
        restored.restore_node(7, None, Vec::new(), Phase::Running, false, None);
        restored.restore_node(8, None, Vec::new(), Phase::Running, false, None);
        assert_eq!(restored.root(), Some(7), "恢复路径与插入路径必须一致");
    }

    #[test]
    fn post_order_puts_descendants_before_ancestors() {
        let mut tree = Tree::<TestDomain>::new();
        tree.insert_for_test(1, None);
        tree.insert_for_test(2, Some(1));
        tree.insert_for_test(3, Some(1));
        tree.insert_for_test(4, Some(2));

        assert_eq!(tree.subtree_post_order(1), vec![4, 2, 3, 1]);
        assert_eq!(tree.subtree_post_order(2), vec![4, 2]);
    }

    #[test]
    fn quiescent_only_when_every_node_is_terminal() {
        let mut tree = Tree::<TestDomain>::new();
        tree.insert_for_test(1, None);
        tree.insert_for_test(2, Some(1));
        assert!(!tree.is_quiescent());

        tree.set_phase_for_test(1, Phase::Done(TestOutput::Ok));
        assert!(!tree.is_quiescent(), "还有子节点在跑");

        tree.set_phase_for_test(2, Phase::Failed(TestError::Boom));
        assert!(tree.is_quiescent());
    }

    #[test]
    fn detached_children_do_not_enter_the_barrier() {
        let mut tree = Tree::<TestDomain>::new();
        tree.insert_for_test(1, None);
        tree.insert_for_test(2, Some(1));
        tree.insert_for_test_detached(3, Some(1));
        assert!(tree.is_detached(3));
        assert!(!tree.is_detached(2));

        assert_eq!(tree.barrier_children(1), vec![2], "detached 不参与屏障");
        tree.set_phase_for_test(2, Phase::Done(TestOutput::Ok));
        assert!(tree.all_children_terminal(1), "屏障只看非 detached 子节点");

        // detached 子节点仍然计入整棵树的收敛
        tree.set_phase_for_test(1, Phase::Done(TestOutput::Ok));
        assert!(!tree.is_quiescent(), "detached 未结束，整棵树不安静");
        tree.set_phase_for_test(3, Phase::Done(TestOutput::Ok));
        assert!(tree.is_quiescent());
    }

    #[test]
    fn restore_node_keeps_the_snapshot_shape_and_the_first_root() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(5, None, vec![6], Phase::Running, false, None);
        tree.restore_node(
            6,
            Some(5),
            Vec::new(),
            Phase::Suspended(TestSuspend::Ask),
            true,
            None,
        );
        tree.restore_node(
            9,
            None,
            Vec::new(),
            Phase::Done(TestOutput::Ok),
            false,
            None,
        );

        assert_eq!(tree.node(5).expect("node").children, vec![6]);
        assert_eq!(tree.node(6).expect("node").parent, Some(5));
        assert!(tree.is_detached(6), "detached 必须随快照重建");
        assert!(!tree.is_detached(5));
        assert_eq!(tree.job_of(5), None, "重建的节点不得带上在跑的 worker");
        assert!(matches!(
            tree.phase(6),
            Some(Phase::Suspended(TestSuspend::Ask))
        ));
        assert_eq!(tree.all_node_ids(), vec![5, 6, 9]);
        assert_eq!(tree.root(), Some(5), "根只认第一个无父节点");
    }

    #[test]
    fn all_children_terminal_requires_a_non_empty_barrier() {
        let mut tree = Tree::<TestDomain>::new();
        tree.insert_for_test(1, None);
        tree.insert_for_test(2, Some(1));
        tree.insert_for_test_detached(3, Some(1));

        assert!(!tree.all_children_terminal(1));
        tree.set_phase_for_test(2, Phase::Done(TestOutput::Ok));
        assert!(tree.all_children_terminal(1));
        assert!(!tree.all_children_terminal(3), "无子节点不算满足");

        // 只有 detached 子节点时屏障为空
        let mut only_detached = Tree::<TestDomain>::new();
        only_detached.insert_for_test(10, None);
        only_detached.insert_for_test_detached(11, Some(10));
        assert!(only_detached.barrier_children(10).is_empty());
        assert!(!only_detached.all_children_terminal(10));
    }

    #[test]
    fn undelivered_nodes_lists_only_the_nodes_that_owe_a_delivery() {
        let mut tree = Tree::<TestDomain>::new();
        tree.insert_for_test(1, None);
        tree.insert_for_test_detached(2, Some(1));
        tree.insert_for_test_detached(3, Some(1));

        assert_eq!(
            tree.undelivered_nodes(),
            Vec::<NodeId>::new(),
            "新登记的节点不背义务"
        );

        tree.set_phase_for_test(2, Phase::Done(TestOutput::Ok));
        tree.set_phase_for_test(3, Phase::Done(TestOutput::Ok));
        tree.set_delivered_for_test(3, false);

        assert_eq!(
            tree.undelivered_nodes(),
            vec![3],
            "只有义务未释放的节点进列表，与终态无关"
        );

        tree.set_delivered_for_test(3, true);
        assert_eq!(tree.undelivered_nodes(), Vec::<NodeId>::new());
    }

    #[test]
    fn a_restored_node_keeps_its_pending_and_owes_no_delivery() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(
            1,
            None,
            Vec::new(),
            Phase::Done(TestOutput::Ok),
            true,
            Some(TestSpec::Noop),
        );

        assert_eq!(
            tree.node(1).expect("node").pending,
            Some(TestSpec::Noop),
            "待消费输入必须随快照重建"
        );
        assert!(
            tree.node(1).expect("node").delivered,
            "投递义务不跨重启，否则恢复出的树永不安静"
        );
        assert!(!tree.is_quiescent(), "有待消费输入就不是安静");

        let mut owed = Tree::<TestDomain>::new();
        owed.restore_node(1, None, Vec::new(), Phase::Done(TestOutput::Ok), true, None);
        assert!(
            owed.node(1).expect("node").delivered,
            "重建出来的终态 detached 节点同样不带义务"
        );
        assert!(owed.is_quiescent());
    }
}
