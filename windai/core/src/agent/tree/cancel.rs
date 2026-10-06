use super::{
    JobId, NodeId, Tree,
    domain::Domain,
    event::Effect,
    phase::{CancelOrigin, Phase, Terminal},
    view::TreeEvent,
};

impl<D: Domain> Tree<D> {
    /// 后序展开子树并进入取消逻辑
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
            self.set_pending(id, None);
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

    /// 确认一个在跑的节点已停止
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
            "必须后序：子先父后"
        );
        for id in [root, a, a1] {
            assert!(
                effects
                    .iter()
                    .any(|e| matches!(e, Effect::Persist { node } if *node == id)),
                "每个进入 Cancelling 的节点都必须落库"
            );
            assert!(
                effects.iter().any(|e| matches!(
                    e,
                    Effect::Publish(TreeEvent::PhaseChanged { node, to })
                        if *node == id && matches!(**to, Phase::Cancelling(_))
                )),
                "每个进入 Cancelling 的节点都必须对外宣告相位迁移"
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
            "挂起节点没有 worker，不需要 Stop"
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
            "旧 job 的确认必须丢弃"
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
            "确认必须对外宣告终态与取消来源"
        );
        assert!(
            confirmed
                .iter()
                .any(|e| matches!(e, Effect::Persist { node } if *node == root)),
            "确认必须落库"
        );
        assert!(matches!(
            tree.phase(root),
            Some(Phase::Cancelled(CancelOrigin::External))
        ));
        assert_eq!(tree.job_of(root), None, "确认后 job 必须释放");
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
            "a 处于 Cancelling"
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
            "级联取消的后代必须带上祖先来源"
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
        // 故意乱序确认：root -> a1 -> a
        for id in [root, a1, a] {
            let job = tree.node(id).and_then(|n| n.job).expect("live job");
            sealed += tree
                .reduce(Event::Stopped { node: id, job })
                .iter()
                .filter(|e| matches!(e, Effect::Publish(TreeEvent::Sealed)))
                .count();
        }
        assert_eq!(sealed, 0, "两个 detached 后代都还欠一次投递");
        for id in [a1, a] {
            sealed += tree
                .reduce(Event::Deliver { node: id })
                .iter()
                .filter(|e| matches!(e, Effect::Publish(TreeEvent::Sealed)))
                .count();
        }
        assert_eq!(sealed, 1, "Sealed 只在最后一个确认与投递都到达时发一次");
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
        // detached 子节点不把父节点拖进屏障，父节点得以继续跑并落终态
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
            "子树级事件不得被入口的终态吸收吃掉，否则在跑的后代永远停不下来"
        );
        assert_eq!(
            stop_order(&effects),
            vec![2],
            "根已终态，只该停还在跑的子节点"
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
            "挂起中被取消的子节点，结果必须交付给等待中的父节点"
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
            "子树内部的父节点不得在取消途中被重入"
        );
        for id in [1, 2] {
            assert!(
                effects
                    .iter()
                    .any(|e| matches!(e, Effect::Persist { node } if *node == id)),
                "每个被取消的节点都必须落库"
            );
        }
        assert!(tree.is_quiescent(), "整棵子树必须收敛");
        assert_eq!(
            effects
                .iter()
                .filter(|e| matches!(e, Effect::Publish(TreeEvent::Sealed)))
                .count(),
            1,
            "收敛时恰好发一次 Sealed"
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
        assert!(effects.is_empty(), "未请求取消的节点不接受停止确认");
        assert!(matches!(tree.phase(1), Some(Phase::Running)));
        assert_eq!(tree.job_of(1), Some(job), "被拒的确认不得清掉 job");
    }

    #[test]
    fn cancelling_a_subtree_drops_pending_on_every_node() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, a, a1) = build_chain(&mut tree);
        // 让 a1 先落终态，再补上恢复出来的「终态 + 待消费输入」
        let job = tree.node(a1).and_then(|n| n.job).expect("job");
        tree.reduce(Event::Completed {
            node: a1,
            job,
            output: TestOutput::Ok,
        });
        tree.set_pending_for_test(a1, Some(TestSpec::Noop));
        tree.set_pending_for_test(a, Some(TestSpec::Noop));
        assert!(!tree.is_quiescent(), "有节点带着待消费输入");

        tree.reduce(Event::CancelRequest {
            root,
            origin: CancelOrigin::External,
        });
        for id in [root, a, a1] {
            assert!(
                tree.node(id).expect("node").pending.is_none(),
                "取消必须清掉节点 {id} 的待消费输入，含已经终态的那个"
            );
        }

        for id in [root, a] {
            let job = tree.node(id).and_then(|n| n.job).expect("live job");
            tree.reduce(Event::Stopped { node: id, job });
        }
        assert!(
            !tree.is_quiescent(),
            "被取消的 detached 后代还欠一次投递，整棵树仍不安静"
        );

        // a1 的义务是它自己落终态时登记的，取消不释放；a 的是确认停止时登记的
        for id in [a1, a] {
            tree.reduce(Event::Deliver { node: id });
        }
        assert!(
            tree.is_quiescent(),
            "清掉待消费输入与投递义务后整棵子树必须能收敛"
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
            "终态 detached 子节点登记了义务"
        );

        let effects = tree.reduce(Event::CancelRequest {
            root,
            origin: CancelOrigin::External,
        });
        assert!(
            !tree.node(child).expect("child").delivered,
            "取消不释放投递义务，含已经终态的那个"
        );
        assert_eq!(sealed_count(&effects), 0, "还有节点在取消途中");

        let job = tree.node(root).and_then(|n| n.job).expect("live job");
        let sealed = tree.reduce(Event::Stopped { node: root, job });
        assert!(!tree.is_quiescent(), "义务未释放，整棵树仍不安静");
        assert_eq!(sealed_count(&sealed), 0);

        let delivered = tree.reduce(Event::Deliver { node: child });
        assert!(tree.is_quiescent(), "投递到达后整棵树才能收敛");
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
            "取消把一个在跑的子节点送进 Cancelling 等确认"
        );
        assert!(
            tree.node(child).expect("child").delivered,
            "义务只在落终态时登记，取消途中不得提前登记"
        );

        let stopped = tree.reduce(Event::Stopped {
            node: child,
            job: child_job,
        });
        assert!(matches!(tree.phase(child), Some(Phase::Cancelled(_))));
        assert!(
            !tree.node(child).expect("child").delivered,
            "被取消的后台子节点也要投递通知，义务在 reduce_stopped 里登记"
        );
        assert!(!tree.is_quiescent());
        assert_eq!(sealed_count(&stopped), 0);

        let root_job = tree.node(root).and_then(|n| n.job).expect("live job");
        tree.reduce(Event::Stopped {
            node: root,
            job: root_job,
        });
        assert!(!tree.is_quiescent(), "义务未释放，整棵树仍不安静");

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
        // 主节点先跑完，后台子节点单独挂着
        let root_job = tree.node(root).and_then(|n| n.job).expect("job");
        tree.reduce(Event::Completed {
            node: root,
            job: root_job,
            output: TestOutput::Ok,
        });
        assert!(!tree.is_quiescent(), "子节点挂起，整棵树不安静");

        let effects = tree.reduce(Event::CancelRequest {
            root: child,
            origin: CancelOrigin::External,
        });
        assert!(matches!(tree.phase(child), Some(Phase::Cancelled(_))));
        assert!(
            !tree.node(child).expect("child").delivered,
            "取消一个挂起的后台子节点照样要投递通知"
        );
        assert!(!tree.is_quiescent(), "义务未释放，整棵树不安静");
        assert_eq!(sealed_count(&effects), 0, "不得抢跑关流");

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
            "取消一个等子任务的 detached 子节点照样要投递通知"
        );
        assert!(!tree.is_quiescent());
        assert_eq!(sealed_count(&effects), 0, "不得抢跑关流");

        let grandchild_job = tree.node(3).and_then(|n| n.job).expect("live job");
        tree.reduce(Event::Stopped {
            node: 3,
            job: grandchild_job,
        });
        assert!(!tree.is_quiescent(), "义务未释放，整棵树仍不安静");

        let delivered = tree.reduce(Event::Deliver { node: child });
        assert!(tree.is_quiescent());
        assert_eq!(sealed_count(&delivered), 1);
    }
}
