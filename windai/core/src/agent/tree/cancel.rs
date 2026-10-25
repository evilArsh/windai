use super::{
    JobId, NodeId, Tree,
    domain::Domain,
    event::Effect,
    phase::{CancelOrigin, Phase, Terminal},
    view::TreeEvent,
};

impl<D: Domain> Tree<D> {
    pub(super) fn reduce_cancel_request(
        &mut self,
        root: NodeId,
        origin: CancelOrigin,
    ) -> Vec<Effect<D>> {
        let mut effects = Vec::new();
        for id in self.subtree_post_order(root) {
            let node_origin = if id == root {
                origin
            } else {
                CancelOrigin::Ancestor(root)
            };
            self.clear_pending(id);
            match self.phase(id) {
                Some(Phase::Suspended(_) | Phase::WaitingChildren) => {
                    self.set_job(id, None);
                    self.set_phase(id, Phase::Cancelled(node_origin));
                    self.owe_delivery(id);
                    effects.push(Effect::Persist { node: id });
                    effects.push(Effect::Publish(TreeEvent::Terminated {
                        node: id,
                        terminal: Terminal::Cancelled(node_origin),
                    }));
                    if id == root {
                        effects.extend(self.reenter_parent_if_ready(id));
                    }
                }
                Some(Phase::Running | Phase::Cancelling(_)) => {
                    self.set_phase(id, Phase::Cancelling(node_origin));
                    effects.push(Effect::Persist { node: id });
                    effects.push(Effect::Publish(TreeEvent::PhaseChanged {
                        node: id,
                        to: Box::new(Phase::Cancelling(node_origin)),
                    }));
                    if let Some(job) = self.job_of(id) {
                        effects.push(Effect::Stop { node: id, job });
                    }
                }
                _ => {}
            }
        }
        effects
    }

    pub(super) fn reduce_stopped(&mut self, node: NodeId, job: JobId) -> Vec<Effect<D>> {
        if self.job_of(node) != Some(job) {
            return Vec::new();
        }
        let Some(Phase::Cancelling(origin)) = self.phase(node) else {
            return Vec::new();
        };
        let origin = *origin;
        self.set_job(node, None);
        self.set_phase(node, Phase::Cancelled(origin));
        self.owe_delivery(node);
        let mut effects = vec![
            Effect::Persist { node },
            Effect::Publish(TreeEvent::Terminated {
                node,
                terminal: Terminal::Cancelled(origin),
            }),
        ];
        effects.extend(self.reenter_parent_if_ready(node));
        effects
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::behavior::{Input, SpawnRequest};
    use crate::agent::tree::domain::test_support::{TestDomain, TestOutput, TestSpec, TestSuspend};
    use crate::agent::tree::event::{Effect, Event};
    use crate::agent::tree::phase::{CancelOrigin, Phase};
    use crate::agent::tree::view::TreeEvent;

    /// 建 1 -> 2 -> 3 三层链，三个节点均在跑
    fn build_chain(tree: &mut Tree<TestDomain>) -> (NodeId, NodeId, NodeId) {
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        let root_job = tree.node(1).and_then(|node| node.job).expect("job");
        tree.reduce(Event::Spawned {
            node: 1,
            job: root_job,
            spawn: vec![SpawnRequest {
                id: 2,
                spec: TestSpec::Noop,
                detached: true,
            }],
        });
        let a_job = tree.node(2).and_then(|node| node.job).expect("job");
        tree.reduce(Event::Spawned {
            node: 2,
            job: a_job,
            spawn: vec![SpawnRequest {
                id: 3,
                spec: TestSpec::Noop,
                detached: true,
            }],
        });
        (1, 2, 3)
    }

    fn stop_order(effects: &[Effect<TestDomain>]) -> Vec<NodeId> {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::Stop { node, .. } => Some(*node),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn cancel_walks_the_subtree_children_first() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, a, a1) = build_chain(&mut tree);
        let effects = tree.reduce(Event::CancelRequest {
            root,
            origin: CancelOrigin::External,
        });
        assert_eq!(
            stop_order(&effects),
            vec![a1, a, root],
            "must be post-order, children before parents"
        );
        for id in [root, a, a1] {
            assert!(
                effects
                    .iter()
                    .any(|e| matches!(e, Effect::Persist { node } if *node == id)),
                "every node that enters Cancelling must be persisted"
            );
            assert!(
                effects.iter().any(|e| matches!(
                    e,
                    Effect::Publish(TreeEvent::PhaseChanged { node, to })
                        if *node == id && matches!(**to, Phase::Cancelling(_))
                )),
                "every node that enters Cancelling must announce its Phase transition"
            );
        }
    }

    #[test]
    fn a_suspended_node_is_terminalised_without_confirmation() {
        let mut tree = Tree::<TestDomain>::new();
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        let root = tree.root().expect("root");
        let job = tree.node(root).expect("root").job.expect("job");
        tree.reduce(Event::Suspended {
            node: root,
            job,
            payload: TestSuspend::Ask,
        });

        let effects = tree.reduce(Event::CancelRequest {
            root,
            origin: CancelOrigin::External,
        });
        assert!(
            stop_order(&effects).is_empty(),
            "a suspended node has no worker, so no Stop is needed"
        );
        assert!(matches!(tree.phase(root), Some(Phase::Cancelled(_))));
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::Publish(TreeEvent::Terminated { .. })))
        );
    }

    #[test]
    fn only_the_live_job_can_confirm_a_stop() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, ..) = build_chain(&mut tree);
        tree.reduce(Event::CancelRequest {
            root,
            origin: CancelOrigin::External,
        });

        let live = tree.node(root).and_then(|n| n.job).expect("live job");
        assert!(
            tree.reduce(Event::Stopped {
                node: root,
                job: 999
            })
            .is_empty(),
            "a stale job's confirmation must be discarded"
        );
        assert!(matches!(tree.phase(root), Some(Phase::Cancelling(_))));

        let confirmed = tree.reduce(Event::Stopped {
            node: root,
            job: live,
        });
        assert!(
            confirmed.iter().any(|e| matches!(
                e,
                Effect::Publish(TreeEvent::Terminated {
                    node,
                    terminal: Terminal::Cancelled(CancelOrigin::External),
                }) if *node == root
            )),
            "a confirmation must announce the terminal phase and the cancel origin"
        );
        assert!(
            confirmed
                .iter()
                .any(|e| matches!(e, Effect::Persist { node } if *node == root)),
            "a confirmation must be persisted"
        );
        assert!(matches!(
            tree.phase(root),
            Some(Phase::Cancelled(CancelOrigin::External))
        ));
        assert_eq!(
            tree.job_of(root),
            None,
            "the job must be released after confirmation"
        );
    }

    #[test]
    fn a_stopped_child_delivers_cancelled_to_its_parent() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, a, a1) = build_chain(&mut tree);
        tree.reduce(Event::CancelRequest {
            root,
            origin: CancelOrigin::External,
        });

        assert!(
            matches!(tree.phase(a), Some(Phase::Cancelling(_))),
            "a is in Cancelling"
        );
        let job = tree.node(a1).and_then(|n| n.job).expect("live job");
        let effects = tree.reduce(Event::Stopped { node: a1, job });
        // a1 是 a 的独子，a 处于 Cancelling，不重入
        assert!(!effects.iter().any(|e| matches!(e, Effect::Launch { .. })));
        assert!(tree.phase(a1).is_some_and(Phase::is_terminal));
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::Publish(TreeEvent::Terminated {
                    node: n,
                    terminal: Terminal::Cancelled(CancelOrigin::Ancestor(ancestor)),
                }) if *n == a1 && *ancestor == root
            )),
            "a descendant cancelled by cascade must carry the ancestor origin"
        );
    }

    #[test]
    fn cancelling_never_flips_a_terminal_node() {
        let mut tree = Tree::<TestDomain>::new();
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        let root = tree.root().expect("root");
        let job = tree.node(root).expect("root").job.expect("job");
        tree.reduce(Event::Completed {
            node: root,
            job,
            output: TestOutput::Ok,
        });

        assert!(
            tree.reduce(Event::CancelRequest {
                root,
                origin: CancelOrigin::External
            })
            .is_empty()
        );
        assert!(matches!(tree.phase(root), Some(Phase::Done(_))));
    }

    #[test]
    fn sealed_waits_for_every_descendant_confirmation() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, a, a1) = build_chain(&mut tree);
        tree.reduce(Event::CancelRequest {
            root,
            origin: CancelOrigin::External,
        });

        let mut sealed = 0;
        // 故意乱序确认，依次为 root、a1、a
        for id in [root, a1, a] {
            let job = tree.node(id).and_then(|n| n.job).expect("live job");
            sealed += tree
                .reduce(Event::Stopped { node: id, job })
                .iter()
                .filter(|e| matches!(e, Effect::Publish(TreeEvent::Sealed)))
                .count();
        }
        assert_eq!(sealed, 0, "both detached descendants still owe a delivery");
        for id in [a1, a] {
            sealed += tree
                .reduce(Event::Deliver { node: id })
                .iter()
                .filter(|e| matches!(e, Effect::Publish(TreeEvent::Sealed)))
                .count();
        }
        assert_eq!(
            sealed, 1,
            "Sealed is sent only once, when the last confirmation and delivery have both arrived"
        );
        for id in [root, a, a1] {
            assert!(tree.phase(id).is_some_and(Phase::is_terminal));
        }
    }

    #[test]
    fn subtree_scoped_events_bypass_entry_absorption() {
        let mut tree = Tree::<TestDomain>::new();
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        let root_job = tree.node(1).and_then(|node| node.job).expect("job");
        // detached 子节点不把父节点拖进屏障，父节点得以继续跑并进入终态
        tree.reduce(Event::Spawned {
            node: 1,
            job: root_job,
            spawn: vec![SpawnRequest {
                id: 2,
                spec: TestSpec::Noop,
                detached: true,
            }],
        });
        let root_job = tree.node(1).and_then(|node| node.job).expect("job");
        tree.reduce(Event::Completed {
            node: 1,
            job: root_job,
            output: TestOutput::Ok,
        });
        assert!(matches!(tree.phase(1), Some(Phase::Done(_))));
        assert!(matches!(tree.phase(2), Some(Phase::Running)));

        let effects = tree.reduce(Event::CancelRequest {
            root: 1,
            origin: CancelOrigin::External,
        });
        assert!(
            !effects.is_empty(),
            "subtree-scoped events must not be swallowed by entry terminal absorption, otherwise running descendants can never stop"
        );
        assert_eq!(
            stop_order(&effects),
            vec![2],
            "the root is already terminal, so only the still-running child should be stopped"
        );
    }

    #[test]
    fn a_suspended_child_delivers_cancelled_to_its_waiting_parent() {
        let mut tree = Tree::<TestDomain>::new();
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        let root_job = tree.node(1).and_then(|node| node.job).expect("job");
        tree.reduce(Event::Spawned {
            node: 1,
            job: root_job,
            spawn: vec![SpawnRequest {
                id: 2,
                spec: TestSpec::Noop,
                detached: false,
            }],
        });
        assert!(matches!(tree.phase(1), Some(Phase::WaitingChildren)));
        let child_job = tree.node(2).and_then(|node| node.job).expect("job");
        tree.reduce(Event::Suspended {
            node: 2,
            job: child_job,
            payload: TestSuspend::Ask,
        });

        let effects = tree.reduce(Event::CancelRequest {
            root: 2,
            origin: CancelOrigin::External,
        });
        assert!(matches!(tree.phase(2), Some(Phase::Cancelled(_))));
        let delivered = effects.iter().find_map(|e| match e {
            Effect::Launch {
                node,
                input: Input::Children(results),
                ..
            } if *node == 1 => Some(results.len()),
            _ => None,
        });
        assert_eq!(
            delivered,
            Some(1),
            "a child cancelled while suspended must still deliver its result to the waiting parent"
        );
        assert!(matches!(tree.phase(1), Some(Phase::Running)));
    }

    #[test]
    fn a_parked_subtree_is_cancelled_without_launching_anything() {
        let mut tree = Tree::<TestDomain>::new();
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        let root_job = tree.node(1).and_then(|node| node.job).expect("job");
        tree.reduce(Event::Spawned {
            node: 1,
            job: root_job,
            spawn: vec![SpawnRequest {
                id: 2,
                spec: TestSpec::Noop,
                detached: false,
            }],
        });
        let child_job = tree.node(2).and_then(|node| node.job).expect("job");
        tree.reduce(Event::Suspended {
            node: 2,
            job: child_job,
            payload: TestSuspend::Ask,
        });
        assert!(matches!(tree.phase(1), Some(Phase::WaitingChildren)));

        let effects = tree.reduce(Event::CancelRequest {
            root: 1,
            origin: CancelOrigin::External,
        });
        assert!(matches!(tree.phase(1), Some(Phase::Cancelled(_))));
        assert!(matches!(tree.phase(2), Some(Phase::Cancelled(_))));
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::Launch { .. })),
            "a parent inside the subtree must not be re-entered mid-cancellation"
        );
        for id in [1, 2] {
            assert!(
                effects
                    .iter()
                    .any(|e| matches!(e, Effect::Persist { node } if *node == id)),
                "every cancelled node must be persisted"
            );
        }
        assert!(tree.is_quiescent(), "the whole subtree must converge");
        assert_eq!(
            effects
                .iter()
                .filter(|e| matches!(e, Effect::Publish(TreeEvent::Sealed)))
                .count(),
            1,
            "Sealed is sent exactly once on convergence"
        );
    }

    #[test]
    fn a_running_node_cannot_be_confirmed_stopped() {
        let mut tree = Tree::<TestDomain>::new();
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        let job = tree.node(1).and_then(|node| node.job).expect("job");

        let effects = tree.reduce(Event::Stopped { node: 1, job });
        assert!(
            effects.is_empty(),
            "a node that was not asked to cancel does not accept a stop confirmation"
        );
        assert!(matches!(tree.phase(1), Some(Phase::Running)));
        assert_eq!(
            tree.job_of(1),
            Some(job),
            "a rejected confirmation must not clear the job"
        );
    }

    #[test]
    fn cancelling_a_subtree_drops_pending_on_every_node() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, a, a1) = build_chain(&mut tree);
        // 先让 a1 进入终态，再补上恢复出的终态与待消费输入
        let job = tree.node(a1).and_then(|n| n.job).expect("job");
        tree.reduce(Event::Completed {
            node: a1,
            job,
            output: TestOutput::Ok,
        });
        tree.push_pending(a1, TestSpec::Noop);
        tree.push_pending(a, TestSpec::Noop);
        assert!(!tree.is_quiescent(), "a node still carries pending input");

        tree.reduce(Event::CancelRequest {
            root,
            origin: CancelOrigin::External,
        });
        for id in [root, a, a1] {
            assert!(
                tree.node(id).expect("node").pending.is_empty(),
                "cancellation must drop the pending input of node {id}, including nodes that are already terminal"
            );
        }

        for id in [root, a] {
            let job = tree.node(id).and_then(|n| n.job).expect("live job");
            tree.reduce(Event::Stopped { node: id, job });
        }
        assert!(
            !tree.is_quiescent(),
            "the cancelled detached descendant still owes a delivery, so the whole tree is still not quiet"
        );

        // a1 的义务在它进入终态时登记，取消不释放；a 的义务在确认停止时登记
        for id in [a1, a] {
            tree.reduce(Event::Deliver { node: id });
        }
        assert!(
            tree.is_quiescent(),
            "the whole subtree must converge after pending input and delivery obligations are cleared"
        );
    }

    /// 建一个在跑的根节点，并让它派生一个在跑的 detached 子节点
    fn build_detached_child(tree: &mut Tree<TestDomain>) -> (NodeId, NodeId) {
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        let job = tree.node(1).and_then(|node| node.job).expect("job");
        tree.reduce(Event::Spawned {
            node: 1,
            job,
            spawn: vec![SpawnRequest {
                id: 2,
                spec: TestSpec::Noop,
                detached: true,
            }],
        });
        (1, 2)
    }

    fn sealed_count(effects: &[Effect<TestDomain>]) -> usize {
        effects
            .iter()
            .filter(|effect| matches!(effect, Effect::Publish(TreeEvent::Sealed)))
            .count()
    }

    #[test]
    fn cancelling_a_subtree_keeps_the_delivery_obligations_until_delivered() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, child) = build_detached_child(&mut tree);
        let job = tree.node(child).and_then(|n| n.job).expect("job");
        tree.reduce(Event::Completed {
            node: child,
            job,
            output: TestOutput::Ok,
        });
        assert!(
            !tree.node(child).expect("child").delivered,
            "a terminal detached child has registered an obligation"
        );

        let effects = tree.reduce(Event::CancelRequest {
            root,
            origin: CancelOrigin::External,
        });
        assert!(
            !tree.node(child).expect("child").delivered,
            "cancellation does not release delivery obligations, including for nodes that are already terminal"
        );
        assert_eq!(
            sealed_count(&effects),
            0,
            "some node is still mid-cancellation"
        );

        let job = tree.node(root).and_then(|n| n.job).expect("live job");
        let sealed = tree.reduce(Event::Stopped { node: root, job });
        assert!(
            !tree.is_quiescent(),
            "the obligation is unreleased, so the whole tree is still not quiet"
        );
        assert_eq!(sealed_count(&sealed), 0);

        let delivered = tree.reduce(Event::Deliver { node: child });
        assert!(
            tree.is_quiescent(),
            "the whole tree can only converge after the delivery arrives"
        );
        assert_eq!(sealed_count(&delivered), 1);
    }

    #[test]
    fn a_cancelled_detached_child_still_owes_a_delivery() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, child) = build_detached_child(&mut tree);
        let child_job = tree.node(child).and_then(|n| n.job).expect("job");

        tree.reduce(Event::CancelRequest {
            root,
            origin: CancelOrigin::External,
        });
        assert!(
            matches!(tree.phase(child), Some(Phase::Cancelling(_))),
            "cancellation puts a running child into Cancelling awaiting confirmation"
        );
        assert!(
            tree.node(child).expect("child").delivered,
            "an obligation is registered only on entering a terminal phase, never mid-cancellation"
        );

        let stopped = tree.reduce(Event::Stopped {
            node: child,
            job: child_job,
        });
        assert!(matches!(tree.phase(child), Some(Phase::Cancelled(_))));
        assert!(
            !tree.node(child).expect("child").delivered,
            "a cancelled background child must deliver a notification too; the obligation is registered in reduce_stopped"
        );
        assert!(!tree.is_quiescent());
        assert_eq!(sealed_count(&stopped), 0);

        let root_job = tree.node(root).and_then(|n| n.job).expect("live job");
        tree.reduce(Event::Stopped {
            node: root,
            job: root_job,
        });
        assert!(
            !tree.is_quiescent(),
            "the obligation is unreleased, so the whole tree is still not quiet"
        );

        let delivered = tree.reduce(Event::Deliver { node: child });
        assert!(tree.is_quiescent());
        assert_eq!(sealed_count(&delivered), 1);
    }

    #[test]
    fn cancelling_a_suspended_detached_child_still_owes_a_delivery() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, child) = build_detached_child(&mut tree);
        let child_job = tree.node(child).and_then(|n| n.job).expect("job");
        tree.reduce(Event::Suspended {
            node: child,
            job: child_job,
            payload: TestSuspend::Ask,
        });
        // 主节点先跑完，后台子节点单独处于挂起状态
        let root_job = tree.node(root).and_then(|n| n.job).expect("job");
        tree.reduce(Event::Completed {
            node: root,
            job: root_job,
            output: TestOutput::Ok,
        });
        assert!(
            !tree.is_quiescent(),
            "the child is suspended, so the whole tree is not quiet"
        );

        let effects = tree.reduce(Event::CancelRequest {
            root: child,
            origin: CancelOrigin::External,
        });
        assert!(matches!(tree.phase(child), Some(Phase::Cancelled(_))));
        assert!(
            !tree.node(child).expect("child").delivered,
            "cancelling a suspended background child must still deliver a notification"
        );
        assert!(
            !tree.is_quiescent(),
            "the obligation is unreleased, so the whole tree is not quiet"
        );
        assert_eq!(sealed_count(&effects), 0, "must not race the stream close");

        let delivered = tree.reduce(Event::Deliver { node: child });
        assert!(tree.is_quiescent());
        assert_eq!(sealed_count(&delivered), 1);
    }

    #[test]
    fn cancelling_a_waiting_detached_child_still_owes_a_delivery() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, child) = build_detached_child(&mut tree);
        let child_job = tree.node(child).and_then(|n| n.job).expect("job");
        tree.reduce(Event::Spawned {
            node: child,
            job: child_job,
            spawn: vec![SpawnRequest {
                id: 3,
                spec: TestSpec::Noop,
                detached: false,
            }],
        });
        assert!(matches!(tree.phase(child), Some(Phase::WaitingChildren)));
        let root_job = tree.node(root).and_then(|n| n.job).expect("job");
        tree.reduce(Event::Completed {
            node: root,
            job: root_job,
            output: TestOutput::Ok,
        });

        let effects = tree.reduce(Event::CancelRequest {
            root: child,
            origin: CancelOrigin::External,
        });
        assert!(matches!(tree.phase(child), Some(Phase::Cancelled(_))));
        assert!(
            !tree.node(child).expect("child").delivered,
            "cancelling a detached child that is waiting on a subtask must still deliver a notification"
        );
        assert!(!tree.is_quiescent());
        assert_eq!(sealed_count(&effects), 0, "must not race the stream close");

        let grandchild_job = tree.node(3).and_then(|n| n.job).expect("live job");
        tree.reduce(Event::Stopped {
            node: 3,
            job: grandchild_job,
        });
        assert!(
            !tree.is_quiescent(),
            "the obligation is unreleased, so the whole tree is still not quiet"
        );

        let delivered = tree.reduce(Event::Deliver { node: child });
        assert!(tree.is_quiescent());
        assert_eq!(sealed_count(&delivered), 1);
    }
}
