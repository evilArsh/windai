use super::{
    JobId, NodeId, Tree,
    domain::Domain,
    event::{Effect, Event},
    phase::Phase,
    view::TreeEvent,
};
use crate::agent::behavior::{ChildResult, Input};

impl<D: Domain> Tree<D> {
    pub fn reduce(&mut self, event: Event<D>) -> Vec<Effect<D>> {
        let before = self.is_quiescent();
        let mut effects = self.reduce_inner(event);
        if !before && self.is_quiescent() {
            effects.push(Effect::Publish(TreeEvent::Sealed));
        }
        effects
    }

    pub(super) fn reduce_inner(&mut self, event: Event<D>) -> Vec<Effect<D>> {
        if !event.bypasses_entry_absorption()
            && let Some(node) = event.node()
            && self.phase(node).is_some_and(Phase::is_terminal)
            && !event.reopens()
        {
            return Vec::new();
        }
        match event {
            Event::Spawn {
                id,
                parent,
                spec,
                detached,
            } => self.reduce_spawn(id, parent, spec, detached),
            Event::Enqueue { node, spec } => self.reduce_enqueue(node, spec),
            Event::Deliver { node } => self.reduce_deliver(node),
            Event::Completed { node, job, output } => {
                if !self.accepts_job_report(node, job) {
                    return Vec::new();
                }
                self.finish_terminal(node, Phase::Done(output))
            }
            Event::Failed { node, job, error } => {
                if !self.accepts_job_report(node, job) {
                    return Vec::new();
                }
                self.finish_terminal(node, Phase::Failed(error))
            }
            Event::Spawned { node, job, spawn } => {
                if !self.accepts_job_report(node, job) {
                    return Vec::new();
                }
                self.set_job(node, None);
                let mut effects = Vec::new();
                for request in spawn {
                    effects.extend(self.reduce_inner(Event::Spawn {
                        id: request.id,
                        parent: Some(node),
                        spec: request.spec,
                        detached: request.detached,
                    }));
                }
                if self.barrier_children(node).is_empty() {
                    let job = self.next_job_id();
                    self.set_job(node, Some(job));
                    self.set_phase(node, Phase::Running);
                    effects.push(Effect::Persist { node });
                    effects.push(Effect::Publish(TreeEvent::PhaseChanged {
                        node,
                        to: Box::new(Phase::Running),
                    }));
                    effects.push(Effect::Launch {
                        node,
                        job,
                        input: Input::Children(Vec::new()),
                    });
                } else {
                    self.set_phase(node, Phase::WaitingChildren);
                    effects.push(Effect::Persist { node });
                    effects.push(Effect::Publish(TreeEvent::PhaseChanged {
                        node,
                        to: Box::new(Phase::WaitingChildren),
                    }));
                }
                effects
            }
            Event::Suspended { node, job, payload } => {
                if !self.accepts_job_report(node, job) {
                    return Vec::new();
                }
                self.set_job(node, None);
                self.set_phase(node, Phase::Suspended(payload.clone()));
                vec![
                    Effect::Persist { node },
                    Effect::Publish(TreeEvent::PhaseChanged {
                        node,
                        to: Box::new(Phase::Suspended(payload)),
                    }),
                ]
            }
            Event::Resume {
                node,
                expect,
                input,
            } => {
                let matched = matches!(self.phase(node), Some(Phase::Suspended(current)) if *current == expect);
                if !matched {
                    return Vec::new();
                }
                let job = self.next_job_id();
                self.set_job(node, Some(job));
                self.set_phase(node, Phase::Running);
                vec![
                    Effect::Persist { node },
                    Effect::Publish(TreeEvent::PhaseChanged {
                        node,
                        to: Box::new(Phase::Running),
                    }),
                    Effect::Launch {
                        node,
                        job,
                        input: Input::External(input),
                    },
                ]
            }
            Event::CancelRequest { root, origin } => self.reduce_cancel_request(root, origin),
            Event::Stopped { node, job } => self.reduce_stopped(node, job),
            Event::Aborted { node, job } => {
                if self.job_of(node) != Some(job) {
                    return Vec::new();
                }
                self.finish_terminal(node, Phase::Aborted)
            }
            Event::Recovered { node } => self.reduce_recovered(node),
        }
    }

    fn reduce_spawn(
        &mut self,
        id: NodeId,
        parent: Option<NodeId>,
        spec: D::Spec,
        detached: bool,
    ) -> Vec<Effect<D>> {
        if !self.insert_node(id, parent, Phase::Running, detached) {
            return Vec::new();
        }
        let job = self.next_job_id();
        self.set_job(id, Some(job));
        vec![
            Effect::Persist { node: id },
            Effect::Publish(TreeEvent::NodeCreated { node: id, parent }),
            Effect::Launch {
                node: id,
                job,
                input: Input::Fresh(spec),
            },
        ]
    }

    pub(crate) fn reduce_enqueue(&mut self, node: NodeId, spec: D::Spec) -> Vec<Effect<D>> {
        let Some(phase) = self.phase(node) else {
            log::warn!("[tree] enqueue for unknown node ignored: {node}");
            return Vec::new();
        };
        if matches!(phase, Phase::Cancelling(_)) {
            log::warn!("[tree] enqueue for a cancelling node ignored: {node}");
            return Vec::new();
        }
        let terminal = phase.is_terminal();
        self.push_pending(node, spec);
        if terminal {
            return self.drain_pending(node);
        }
        Vec::new()
    }

    pub(super) fn drain_pending(&mut self, node: NodeId) -> Vec<Effect<D>> {
        let specs = self.take_pending(node);
        if specs.is_empty() {
            return Vec::new();
        }
        let job = self.next_job_id();
        self.set_job(node, Some(job));
        self.set_phase(node, Phase::Running);
        vec![
            Effect::Persist { node },
            Effect::Publish(TreeEvent::PhaseChanged {
                node,
                to: Box::new(Phase::Running),
            }),
            Effect::Launch {
                node,
                job,
                input: Input::Idle(specs),
            },
        ]
    }

    pub(crate) fn reduce_deliver(&mut self, node: NodeId) -> Vec<Effect<D>> {
        match self.node(node) {
            None => log::warn!("[tree] deliver for unknown node ignored: {node}"),
            Some(current) if current.delivered => {}
            Some(_) => self.set_delivered(node, true),
        }
        Vec::new()
    }

    pub(super) fn owe_delivery(&mut self, node: NodeId) {
        if self.is_detached(node) {
            self.set_delivered(node, false);
        }
    }

    pub(super) fn accepts_job_report(&self, node: NodeId, job: JobId) -> bool {
        self.job_of(node) == Some(job) && matches!(self.phase(node), Some(Phase::Running))
    }

    pub(super) fn finish_terminal(&mut self, node: NodeId, phase: Phase<D>) -> Vec<Effect<D>> {
        self.set_job(node, None);
        self.set_phase(node, phase);
        let Some(terminal) = self.phase(node).and_then(Phase::terminal) else {
            return vec![Effect::Persist { node }];
        };
        self.owe_delivery(node);
        let mut effects = vec![
            Effect::Persist { node },
            Effect::Publish(TreeEvent::Terminated { node, terminal }),
        ];
        effects.extend(self.reenter_parent_if_ready(node));
        effects.extend(self.drain_pending(node));
        effects
    }

    fn reduce_recovered(&mut self, node: NodeId) -> Vec<Effect<D>> {
        let disposition = match self.phase(node) {
            Some(Phase::Cancelling(origin)) => Some(Phase::Cancelled(*origin)),
            Some(Phase::Running) if self.job_of(node).is_none() => Some(Phase::Aborted),
            _ => None,
        };
        match disposition {
            Some(phase) => {
                let effects = self.finish_terminal(node, phase);
                self.set_delivered(node, true);
                effects
            }
            None => {
                let mut effects = self.reenter_parent_if_ready(node);
                if self.phase(node).is_some_and(Phase::is_terminal) {
                    effects.extend(self.drain_pending(node));
                }
                effects
            }
        }
    }

    pub(super) fn reenter_parent_if_ready(&mut self, child: NodeId) -> Vec<Effect<D>> {
        let Some(parent) = self.node(child).and_then(|node| node.parent) else {
            return Vec::new();
        };
        if !matches!(self.phase(parent), Some(Phase::WaitingChildren)) {
            return Vec::new();
        }
        if !self.all_children_terminal(parent) {
            return Vec::new();
        }
        let results: Vec<ChildResult<D>> = self
            .barrier_children(parent)
            .into_iter()
            .filter_map(|id| {
                self.phase(id)
                    .and_then(Phase::terminal)
                    .map(|terminal| ChildResult { node: id, terminal })
            })
            .collect();
        let job = self.next_job_id();
        self.set_job(parent, Some(job));
        self.set_phase(parent, Phase::Running);
        vec![
            Effect::Persist { node: parent },
            Effect::Publish(TreeEvent::PhaseChanged {
                node: parent,
                to: Box::new(Phase::Running),
            }),
            Effect::Launch {
                node: parent,
                job,
                input: Input::Children(results),
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::behavior::{Input, SpawnRequest};
    use crate::agent::tree::domain::test_support::{
        TestDomain, TestError, TestOutput, TestResume, TestSpec, TestSuspend,
    };
    use crate::agent::tree::event::{Effect, Event};
    use crate::agent::tree::view::TreeEvent;
    use crate::agent::tree::{CancelOrigin, Terminal, Tree};
    use std::collections::VecDeque;

    /// 派生一个节点并返回 (节点 id, job)
    fn spawn(
        tree: &mut Tree<TestDomain>,
        id: NodeId,
        parent: Option<NodeId>,
        detached: bool,
    ) -> (NodeId, JobId) {
        tree.reduce(Event::Spawn {
            id,
            parent,
            spec: TestSpec::Noop,
            detached,
        });
        let job = tree.node(id).and_then(|node| node.job).expect("job");
        (id, job)
    }

    /// 让在跑的节点以 `Done` 进入终态
    fn complete(tree: &mut Tree<TestDomain>, node: NodeId) {
        let job = tree.node(node).and_then(|node| node.job).expect("job");
        tree.reduce(Event::Completed {
            node,
            job,
            output: TestOutput::Ok,
        });
    }

    fn launches(effects: &[Effect<TestDomain>]) -> Vec<NodeId> {
        effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Launch { node, .. } => Some(*node),
                _ => None,
            })
            .collect()
    }

    fn sealed_count(effects: &[Effect<TestDomain>]) -> usize {
        effects
            .iter()
            .filter(|effect| matches!(effect, Effect::Publish(TreeEvent::Sealed)))
            .count()
    }

    /// 建一个在跑的根节点
    fn tree_with_running_root() -> Tree<TestDomain> {
        let mut tree = Tree::<TestDomain>::new();
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        tree
    }

    /// 让在跑的父节点派生一个 detached 子节点，返回子节点的 job
    fn spawn_detached_child(tree: &mut Tree<TestDomain>, parent: NodeId, id: NodeId) -> JobId {
        let job = tree.job_of(parent).expect("parent job");
        tree.reduce(Event::Spawned {
            node: parent,
            job,
            spawn: vec![SpawnRequest {
                id,
                spec: TestSpec::Noop,
                detached: true,
            }],
        });
        tree.job_of(id).expect("child job")
    }

    #[test]
    fn spawn_creates_a_running_node_with_a_job() {
        let mut tree = Tree::<TestDomain>::new();
        let effects = tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        assert!(matches!(tree.phase(1), Some(Phase::Running)));
        assert!(tree.job_of(1).is_some());
        assert_eq!(tree.root(), Some(1));

        assert!(effects.iter().any(|e| matches!(e, Effect::Persist { .. })));
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::Publish(TreeEvent::NodeCreated { .. })))
        );
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::Launch {
                input: Input::Fresh(TestSpec::Noop),
                ..
            }
        )));
    }

    #[test]
    fn a_spawn_with_an_unknown_parent_produces_nothing() {
        let mut tree = Tree::<TestDomain>::new();
        let effects = tree.reduce(Event::Spawn {
            id: 2,
            parent: Some(99),
            spec: TestSpec::Noop,
            detached: false,
        });
        assert!(
            effects.is_empty(),
            "an unknown parent must produce no effects"
        );
        assert_eq!(tree.all_node_ids(), Vec::<NodeId>::new());
    }

    #[test]
    fn a_duplicate_spawn_is_ignored() {
        let mut tree = Tree::<TestDomain>::new();
        spawn(&mut tree, 1, None, false);
        let effects = tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        assert!(effects.is_empty(), "a duplicate id must be rejected");
        assert_eq!(tree.all_node_ids(), vec![1]);
    }

    #[test]
    fn terminal_nodes_absorb_every_single_node_event() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, job) = spawn(&mut tree, 1, None, false);
        tree.reduce(Event::Completed {
            node,
            job,
            output: TestOutput::Ok,
        });

        let absorbed = vec![
            Event::Stopped { node, job },
            Event::Aborted { node, job },
            Event::Suspended {
                node,
                job,
                payload: TestSuspend::Ask,
            },
        ];
        for event in absorbed {
            assert!(
                tree.reduce(event).is_empty(),
                "a terminal phase must silently absorb single-node events"
            );
        }
        assert!(matches!(tree.phase(node), Some(Phase::Done(_))));
    }

    /// 终态节点收到入队立即重开一轮，运行中的节点只把输入留在队列里
    #[test]
    fn enqueue_reopens_a_terminal_node_and_flags_a_running_one() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, job) = spawn(&mut tree, 1, None, false);
        tree.reduce(Event::Completed {
            node,
            job,
            output: TestOutput::Ok,
        });

        let effects = tree.reduce(Event::Enqueue {
            node,
            spec: TestSpec::Noop,
        });
        assert!(matches!(tree.phase(node), Some(Phase::Running)));
        assert_ne!(
            tree.job_of(node),
            Some(job),
            "a new round must obtain a new job"
        );
        assert_eq!(launches(&effects), vec![node]);

        let running_job = tree.job_of(node).expect("job");
        let busy = tree.reduce(Event::Enqueue {
            node,
            spec: TestSpec::Children(1),
        });
        assert!(
            busy.is_empty(),
            "while busy it only enqueues and does not interrupt the running round"
        );
        assert!(matches!(tree.phase(node), Some(Phase::Running)));
        assert_eq!(
            tree.job_of(node),
            Some(running_job),
            "the running round's job must not be replaced"
        );
        assert_eq!(
            tree.node(node).expect("node").pending,
            VecDeque::from(vec![TestSpec::Children(1)]),
            "later input stays in the queue"
        );
    }

    #[test]
    fn sealed_is_published_exactly_once_when_the_tree_settles() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, job) = spawn(&mut tree, 1, None, false);
        assert!(!tree.is_quiescent());

        let effects = tree.reduce(Event::Completed {
            node,
            job,
            output: TestOutput::Ok,
        });
        let sealed = effects
            .iter()
            .filter(|e| matches!(e, Effect::Publish(TreeEvent::Sealed)))
            .count();
        assert_eq!(
            sealed, 1,
            "Sealed is published exactly once when the tree settles"
        );

        assert_eq!(
            tree.job_of(node),
            None,
            "entering a terminal phase must release the job, otherwise a stale report could still be taken as valid"
        );

        let again = tree.reduce(Event::Stopped {
            node,
            job: job + 99,
        });
        assert!(
            !again
                .iter()
                .any(|e| matches!(e, Effect::Publish(TreeEvent::Sealed))),
            "Sealed must not be published again once the tree is quiet"
        );
    }

    /// 建 id=1 的根节点，并让它派生 id 从 2 开始的 n 个子节点
    fn build_fanout(tree: &mut Tree<TestDomain>, n: usize) -> (NodeId, Vec<NodeId>) {
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        let root_job = tree.node(1).and_then(|node| node.job).expect("job");
        let spawn = (0..n)
            .map(|i| SpawnRequest {
                id: 2 + i as NodeId,
                spec: TestSpec::Noop,
                detached: false,
            })
            .collect();
        tree.reduce(Event::Spawned {
            node: 1,
            job: root_job,
            spawn,
        });
        let children = tree.node(1).expect("root").children.clone();
        (1, children)
    }

    #[test]
    fn awaiting_children_creates_them_and_parks_the_parent() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, children) = build_fanout(&mut tree, 2);
        assert_eq!(children.len(), 2);
        assert!(matches!(tree.phase(root), Some(Phase::WaitingChildren)));
        assert_eq!(
            tree.job_of(root),
            None,
            "no job may remain while waiting for subtasks"
        );
        for child in children {
            assert!(matches!(tree.phase(child), Some(Phase::Running)));
            assert!(tree.job_of(child).is_some());
        }
    }

    #[test]
    fn the_last_child_to_finish_reenters_the_parent() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, children) = build_fanout(&mut tree, 2);
        let (a, b) = (children[0], children[1]);
        let (job_a, job_b) = (
            tree.node(a).expect("a").job.expect("job"),
            tree.node(b).expect("b").job.expect("job"),
        );

        let first = tree.reduce(Event::Completed {
            node: a,
            job: job_a,
            output: TestOutput::Ok,
        });
        assert!(
            !first.iter().any(|e| matches!(e, Effect::Launch { .. })),
            "children are still unfinished, so the parent must not be re-entered"
        );

        let second = tree.reduce(Event::Completed {
            node: b,
            job: job_b,
            output: TestOutput::Ok,
        });
        let relaunched = second.iter().find_map(|e| match e {
            Effect::Launch { node, input, .. } => Some((*node, input)),
            _ => None,
        });
        let (node, input) = relaunched.expect("reenter after the last child");
        assert_eq!(node, root);
        match input {
            Input::Children(results) => assert_eq!(results.len(), 2),
            _ => panic!("expected Input::Children"),
        }
        assert!(matches!(tree.phase(root), Some(Phase::Running)));
    }

    #[test]
    fn a_failing_child_still_delivers_its_result() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, children) = build_fanout(&mut tree, 1);
        let child = children[0];
        let job = tree.node(child).expect("child").job.expect("job");

        let effects = tree.reduce(Event::Failed {
            node: child,
            job,
            error: TestError::Boom,
        });
        let delivered = effects.iter().find_map(|e| match e {
            Effect::Launch {
                node,
                input: Input::Children(results),
                ..
            } if *node == root => Some(results.len()),
            _ => None,
        });
        assert_eq!(
            delivered,
            Some(1),
            "a failure must still be delivered to the parent, not swallowed silently"
        );
    }

    #[test]
    fn an_all_detached_spawn_relaunches_the_parent_immediately() {
        let mut tree = Tree::<TestDomain>::new();
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        let root_job = tree.node(1).and_then(|node| node.job).expect("job");

        let effects = tree.reduce(Event::Spawned {
            node: 1,
            job: root_job,
            spawn: vec![SpawnRequest {
                id: 2,
                spec: TestSpec::Noop,
                detached: true,
            }],
        });

        assert!(
            matches!(tree.phase(1), Some(Phase::Running)),
            "when the barrier is empty the parent is re-entered immediately and does not enter WaitingChildren"
        );
        assert!(
            matches!(tree.phase(2), Some(Phase::Running)),
            "the child starts as usual"
        );
        assert_ne!(
            tree.job_of(1),
            Some(root_job),
            "re-entry must obtain a new job"
        );
        let relaunched = effects.iter().find_map(|e| match e {
            Effect::Launch {
                node,
                input: Input::Children(results),
                ..
            } if *node == 1 => Some(results.len()),
            _ => None,
        });
        assert_eq!(
            relaunched,
            Some(0),
            "an immediate re-entry carries no barrier results"
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::Publish(TreeEvent::PhaseChanged { node, to })
                    if *node == 1 && matches!(**to, Phase::Running)
            )),
            "re-entry must be announced"
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::Persist { node } if *node == 1)),
            "re-entry must be persisted"
        );
    }

    /// 全是 detached 的派生让父节点立即重入，重入轮不消耗待消费队列
    #[test]
    fn an_all_detached_respawn_keeps_the_pending_input_for_the_next_idle() {
        let mut tree = Tree::<TestDomain>::new();
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        tree.reduce(Event::Enqueue {
            node: 1,
            spec: TestSpec::Children(1),
        });
        assert_eq!(
            tree.node(1).expect("root").pending,
            VecDeque::from(vec![TestSpec::Children(1)])
        );

        let job = tree.job_of(1).expect("job");
        let effects = tree.reduce(Event::Spawned {
            node: 1,
            job,
            spawn: vec![SpawnRequest {
                id: 2,
                spec: TestSpec::Noop,
                detached: true,
            }],
        });
        assert_eq!(
            launches(&effects),
            vec![2, 1],
            "the detached child starts first, then the parent is re-entered immediately because the barrier is empty"
        );
        assert_eq!(
            tree.node(1).expect("root").pending,
            VecDeque::from(vec![TestSpec::Children(1)]),
            "an immediate re-entry round does not consume the queue; the input is left for the next idle"
        );

        let job = tree.job_of(1).expect("job");
        let settled = tree.reduce(Event::Completed {
            node: 1,
            job,
            output: TestOutput::Ok,
        });
        let idle = settled.iter().find_map(|effect| match effect {
            Effect::Launch {
                input: Input::Idle(specs),
                ..
            } => Some(specs.len()),
            _ => None,
        });
        assert_eq!(
            idle,
            Some(1),
            "after the re-entry round ends, the idle path drains the whole queue"
        );
    }

    #[test]
    fn an_aborted_worker_terminalises_the_node_and_reenters_its_parent() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, children) = build_fanout(&mut tree, 1);
        let child = children[0];
        let job = tree.node(child).expect("child").job.expect("job");

        let effects = tree.reduce(Event::Aborted { node: child, job });
        assert!(
            matches!(tree.phase(child), Some(Phase::Aborted)),
            "a worker crash must enter Aborted"
        );
        assert_eq!(tree.job_of(child), None, "Aborted must release the job");
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::Publish(TreeEvent::Terminated {
                    node,
                    terminal: Terminal::Aborted
                }) if *node == child
            )),
            "Aborted must be announced"
        );
        let delivered = effects.iter().find_map(|e| match e {
            Effect::Launch {
                node,
                input: Input::Children(results),
                ..
            } if *node == root => Some(results.len()),
            _ => None,
        });
        assert_eq!(
            delivered,
            Some(1),
            "Aborted must also be delivered to the parent"
        );
    }

    #[test]
    fn an_aborted_worker_resolves_a_cancelling_node() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, job) = spawn(&mut tree, 1, None, false);
        tree.set_phase_for_test(node, Phase::Cancelling(CancelOrigin::External));

        let effects = tree.reduce(Event::Aborted { node, job });
        assert!(
            matches!(tree.phase(node), Some(Phase::Aborted)),
            "a worker that crashes mid-cancellation cannot produce a stop confirmation, so Aborted is the truthful conclusion"
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::Publish(TreeEvent::Terminated {
                    node: reported,
                    terminal: Terminal::Aborted
                }) if *reported == node
            )),
            "a cancelling node must also converge and be announced"
        );
    }

    #[test]
    fn a_parent_that_is_being_cancelled_is_never_reentered() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, children) = build_fanout(&mut tree, 1);
        let child = children[0];
        let job = tree.node(child).expect("child").job.expect("job");

        tree.set_phase_for_test(root, Phase::Cancelling(CancelOrigin::External));
        let effects = tree.reduce(Event::Completed {
            node: child,
            job,
            output: TestOutput::Ok,
        });
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::Launch { .. })),
            "cancellation takes precedence over re-entry"
        );
    }

    #[test]
    fn a_cancelling_node_ignores_a_late_completion() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, job) = spawn(&mut tree, 1, None, false);
        tree.set_phase_for_test(node, Phase::Cancelling(CancelOrigin::External));

        let effects = tree.reduce(Event::Completed {
            node,
            job,
            output: TestOutput::Ok,
        });
        assert!(
            effects.is_empty(),
            "a late completion report must not be accepted"
        );
        assert!(matches!(
            tree.phase(node),
            Some(Phase::Cancelling(CancelOrigin::External))
        ));
        assert_eq!(
            tree.job_of(node),
            Some(job),
            "while cancelling, the job must remain for Stopped to verify"
        );
    }

    #[test]
    fn a_cancelling_node_ignores_a_late_spawn_report() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, job) = spawn(&mut tree, 1, None, false);
        tree.set_phase_for_test(node, Phase::Cancelling(CancelOrigin::External));

        let effects = tree.reduce(Event::Spawned {
            node,
            job,
            spawn: vec![SpawnRequest {
                id: 9,
                spec: TestSpec::Noop,
                detached: false,
            }],
        });
        assert!(
            effects.is_empty(),
            "a late spawn report must not be accepted"
        );
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::Launch { .. })),
            "cancellation takes precedence over re-entry"
        );
        assert_eq!(
            tree.all_node_ids(),
            vec![node],
            "no child node may be created"
        );
        assert!(matches!(
            tree.phase(node),
            Some(Phase::Cancelling(CancelOrigin::External))
        ));
    }

    #[test]
    fn suspending_frees_the_job_and_never_seals() {
        let mut tree = Tree::<TestDomain>::new();
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        let node = tree.root().expect("root");
        let job = tree.node(node).expect("node").job.expect("job");

        let effects = tree.reduce(Event::Suspended {
            node,
            job,
            payload: TestSuspend::Ask,
        });
        assert!(matches!(
            tree.phase(node),
            Some(Phase::Suspended(TestSuspend::Ask))
        ));
        assert_eq!(
            tree.job_of(node),
            None,
            "no job may remain after suspension"
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::Publish(TreeEvent::Sealed))),
            "suspension is not terminal, so the stream must not close"
        );
    }

    #[test]
    fn resume_must_echo_the_current_suspension() {
        let mut tree = Tree::<TestDomain>::new();
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        let node = tree.root().expect("root");
        let job = tree.node(node).expect("node").job.expect("job");
        tree.reduce(Event::Suspended {
            node,
            job,
            payload: TestSuspend::Ask,
        });

        let accepted = tree.reduce(Event::Resume {
            node,
            expect: TestSuspend::Ask,
            input: TestResume::Answer,
        });
        assert!(matches!(tree.phase(node), Some(Phase::Running)));
        assert!(accepted.iter().any(|e| matches!(
            e,
            Effect::Launch {
                input: Input::External(TestResume::Answer),
                ..
            }
        )));
        let relaunched = accepted.iter().find_map(|e| match e {
            Effect::Launch { job, .. } => Some(*job),
            _ => None,
        });
        assert_eq!(
            relaunched,
            tree.job_of(node),
            "the Launch job must be registered on the node"
        );
        assert_ne!(
            tree.job_of(node),
            Some(job),
            "resume must allocate a new job"
        );

        let replay = tree.reduce(Event::Resume {
            node,
            expect: TestSuspend::Ask,
            input: TestResume::Answer,
        });
        assert!(
            replay.is_empty(),
            "duplicate or stale external input must be dropped silently"
        );
    }

    #[test]
    fn resume_with_a_stale_token_is_dropped() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, job) = spawn(&mut tree, 1, None, false);
        tree.reduce(Event::Suspended {
            node,
            job,
            payload: TestSuspend::Ask,
        });
        assert!(matches!(
            tree.phase(node),
            Some(Phase::Suspended(TestSuspend::Ask))
        ));

        let stale = tree.reduce(Event::Resume {
            node,
            expect: TestSuspend::Other,
            input: TestResume::Answer,
        });
        assert!(
            stale.is_empty(),
            "a suspension token that does not match the echo must be dropped silently"
        );
        assert!(
            matches!(tree.phase(node), Some(Phase::Suspended(TestSuspend::Ask))),
            "a rejected resume must not rewrite the suspension token"
        );
        assert_eq!(
            tree.job_of(node),
            None,
            "a rejected resume must not leave a job"
        );
    }

    #[test]
    fn resume_on_a_running_node_is_dropped() {
        let mut tree = Tree::<TestDomain>::new();
        tree.reduce(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        });
        let node = tree.root().expect("root");

        let effects = tree.reduce(Event::Resume {
            node,
            expect: TestSuspend::Ask,
            input: TestResume::Answer,
        });
        assert!(effects.is_empty());
        assert!(matches!(tree.phase(node), Some(Phase::Running)));
    }

    #[test]
    fn enqueue_wakes_a_terminal_node_and_launches_one_round() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, _) = spawn(&mut tree, 1, None, false);
        complete(&mut tree, node);

        let effects = tree.reduce(Event::Enqueue {
            node,
            spec: TestSpec::Noop,
        });
        assert!(
            matches!(tree.phase(node), Some(Phase::Running)),
            "terminal absorption must not swallow an enqueue"
        );
        assert!(
            tree.node(node).expect("node").pending.is_empty(),
            "the queue must be empty after a full drain"
        );
        assert_eq!(
            launches(&effects),
            vec![node],
            "enqueue opens exactly one round"
        );
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::Publish(TreeEvent::PhaseChanged { node: reported, to })
                if *reported == node && matches!(**to, Phase::Running)
        )));
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::Publish(TreeEvent::NodeCreated { .. }))),
            "enqueue only opens a new round, it does not create a new node"
        );
    }

    #[test]
    fn enqueue_on_a_running_node_only_sets_the_flag() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, _) = spawn(&mut tree, 1, None, false);

        let effects = tree.reduce(Event::Enqueue {
            node,
            spec: TestSpec::Noop,
        });
        assert!(
            effects.is_empty(),
            "while busy it only enqueues and produces no effects"
        );
        assert!(matches!(tree.phase(node), Some(Phase::Running)));
        assert_eq!(
            tree.node(node).expect("node").pending,
            VecDeque::from(vec![TestSpec::Noop])
        );
    }

    #[test]
    fn two_enqueues_produce_one_launch() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, _) = spawn(&mut tree, 1, None, false);
        complete(&mut tree, node);

        let first = tree.reduce(Event::Enqueue {
            node,
            spec: TestSpec::Noop,
        });
        let second = tree.reduce(Event::Enqueue {
            node,
            spec: TestSpec::Noop,
        });
        let all: Vec<NodeId> = launches(&first)
            .into_iter()
            .chain(launches(&second))
            .collect();
        assert_eq!(
            all,
            vec![node],
            "two consecutive enqueues open only one round"
        );
        assert_eq!(
            tree.node(node).expect("node").pending,
            VecDeque::from(vec![TestSpec::Noop]),
            "later input stays in the queue and opens the next round once this one ends"
        );
    }

    #[test]
    fn a_tree_with_pending_is_not_quiescent() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, _) = spawn(&mut tree, 1, None, false);
        complete(&mut tree, node);
        assert!(tree.is_quiescent());

        // 恢复出的终态与待消费输入，实际由 `recover` 重建
        tree.push_pending(node, TestSpec::Noop);
        assert!(!tree.is_quiescent(), "having pending input means not quiet");

        let drained = tree.reduce(Event::Enqueue {
            node,
            spec: TestSpec::Noop,
        });
        assert!(
            !drained
                .iter()
                .any(|e| matches!(e, Effect::Publish(TreeEvent::Sealed))),
            "a drain opens a new round, so the stream must not close early"
        );
        assert!(
            !tree.is_quiescent(),
            "a new round is running, so it is still not quiet"
        );

        let job = tree.job_of(node).expect("job");
        let sealed = tree.reduce(Event::Completed {
            node,
            job,
            output: TestOutput::Ok,
        });
        assert!(
            tree.is_quiescent(),
            "it is quiet only once this round finishes"
        );
        assert_eq!(
            sealed
                .iter()
                .filter(|e| matches!(e, Effect::Publish(TreeEvent::Sealed)))
                .count(),
            1
        );
    }

    #[test]
    fn a_pending_input_opens_a_round_when_the_node_finishes() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, _) = spawn(&mut tree, 1, None, false);
        tree.reduce(Event::Enqueue {
            node,
            spec: TestSpec::Noop,
        });

        let job = tree.job_of(node).expect("job");
        let effects = tree.reduce(Event::Completed {
            node,
            job,
            output: TestOutput::Ok,
        });
        assert!(
            matches!(tree.phase(node), Some(Phase::Running)),
            "while input is waiting, open another round"
        );
        assert!(tree.node(node).expect("node").pending.is_empty());
        let input = effects.iter().find_map(|e| match e {
            Effect::Launch {
                node: reported,
                input: Input::Idle(specs),
                ..
            } if *reported == node => Some(specs.clone()),
            _ => None,
        });
        assert_eq!(
            input,
            Some(vec![TestSpec::Noop]),
            "the new round uses exactly the pending input drained from the queue"
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::Publish(TreeEvent::Terminated { node: reported, .. }) if *reported == node
            )),
            "this round's terminal phase is announced as usual"
        );
    }

    #[test]
    fn a_child_drains_only_after_its_parent_is_reentered() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, children) = build_fanout(&mut tree, 1);
        let child = children[0];
        assert!(matches!(tree.phase(root), Some(Phase::WaitingChildren)));

        tree.reduce(Event::Enqueue {
            node: child,
            spec: TestSpec::Noop,
        });

        let job = tree.job_of(child).expect("job");
        let effects = tree.reduce(Event::Completed {
            node: child,
            job,
            output: TestOutput::Ok,
        });
        assert_eq!(
            launches(&effects),
            vec![root, child],
            "the parent is re-entered first; the child drains only after the parent turns Running"
        );
    }

    /// `Children` 重入不消费队列，停泊期间到达的输入留给下一次空闲
    #[test]
    fn a_children_round_keeps_the_pending_input_for_the_next_idle() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, children) = build_fanout(&mut tree, 1);
        let child = children[0];
        // 父节点停泊期间到达的输入只入队，不打断等待
        tree.reduce(Event::Enqueue {
            node: root,
            spec: TestSpec::Children(1),
        });
        assert_eq!(
            tree.node(root).expect("root").pending,
            VecDeque::from(vec![TestSpec::Children(1)])
        );

        let job = tree.job_of(child).expect("job");
        let reentered = tree.reduce(Event::Completed {
            node: child,
            job,
            output: TestOutput::Ok,
        });
        assert_eq!(
            launches(&reentered),
            vec![root],
            "all-terminal children must re-enter the parent"
        );
        assert_eq!(
            tree.node(root).expect("root").pending,
            VecDeque::from(vec![TestSpec::Children(1)]),
            "a Children re-entry does not consume the queue"
        );

        let job = tree.job_of(root).expect("job");
        let settled = tree.reduce(Event::Completed {
            node: root,
            job,
            output: TestOutput::Ok,
        });
        let idle = settled.iter().find_map(|e| match e {
            Effect::Launch {
                node: reported,
                input: Input::Idle(specs),
                ..
            } if *reported == root => Some(specs.len()),
            _ => None,
        });
        assert_eq!(
            idle,
            Some(1),
            "after this round ends the idle path drains the whole queue"
        );
        assert!(
            tree.node(root).expect("root").pending.is_empty(),
            "the queue must be empty after a full drain"
        );

        let job = tree.job_of(root).expect("job");
        tree.reduce(Event::Completed {
            node: root,
            job,
            output: TestOutput::Ok,
        });
        assert!(tree.is_quiescent());
    }

    #[test]
    fn enqueue_reopens_a_cancelled_node() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(
            1,
            None,
            Vec::new(),
            Phase::Cancelled(CancelOrigin::External),
            false,
            Vec::new(),
        );

        let effects = tree.reduce(Event::Enqueue {
            node: 1,
            spec: TestSpec::Noop,
        });
        assert!(
            matches!(tree.phase(1), Some(Phase::Running)),
            "new input arriving after cancellation must be able to open a new round"
        );
        assert_eq!(launches(&effects), vec![1]);
        assert!(!tree.is_quiescent(), "a new round is running");
    }

    #[test]
    fn enqueue_reopens_an_aborted_node() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(1, None, Vec::new(), Phase::Aborted, false, Vec::new());

        let effects = tree.reduce(Event::Enqueue {
            node: 1,
            spec: TestSpec::Noop,
        });
        assert!(
            matches!(tree.phase(1), Some(Phase::Running)),
            "new input arriving after a crash reaches a terminal phase must be able to open a new round"
        );
        assert_eq!(launches(&effects), vec![1]);
        assert!(!tree.is_quiescent(), "a new round is running");
    }

    #[test]
    fn enqueue_on_a_cancelling_node_is_ignored() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, job) = spawn(&mut tree, 1, None, false);
        tree.set_phase_for_test(node, Phase::Cancelling(CancelOrigin::External));

        let effects = tree.reduce(Event::Enqueue {
            node,
            spec: TestSpec::Noop,
        });
        assert!(effects.is_empty());
        assert!(
            tree.node(node).expect("node").pending.is_empty(),
            "a cancelling node must not register pending input"
        );
        assert!(!tree.is_quiescent(), "the cancelling node is still running");

        tree.reduce(Event::Stopped { node, job });
        assert!(
            tree.is_quiescent(),
            "a rejected enqueue must not stall the tree. Once the node reaches a terminal phase the whole tree must be quiet"
        );
    }

    #[test]
    fn enqueue_for_an_unknown_node_is_ignored() {
        let mut tree = Tree::<TestDomain>::new();
        let effects = tree.reduce(Event::Enqueue {
            node: 7,
            spec: TestSpec::Noop,
        });
        assert!(effects.is_empty());
        assert_eq!(tree.all_node_ids(), Vec::<NodeId>::new());
    }

    #[test]
    fn a_recovered_node_drains_its_pending() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(
            1,
            None,
            Vec::new(),
            Phase::Done(TestOutput::Ok),
            false,
            Vec::new(),
        );
        // 恢复出的 Done 与待消费输入，实际由 `recover` 重建
        tree.push_pending(1, TestSpec::Noop);

        let effects = tree.reduce(Event::Recovered { node: 1 });
        assert!(
            matches!(tree.phase(1), Some(Phase::Running)),
            "pending input produced by recovery must drain, otherwise that topic is never quiet"
        );
        assert_eq!(launches(&effects), vec![1]);

        let job = tree.job_of(1).expect("job");
        tree.reduce(Event::Completed {
            node: 1,
            job,
            output: TestOutput::Ok,
        });
        assert!(
            tree.is_quiescent(),
            "the tree must converge once this drained round completes"
        );
    }

    /// 恢复路径先重入父节点，带待消费输入的子节点才出队
    #[test]
    fn a_recovered_child_drains_only_after_its_parent_is_reentered() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(1, None, vec![2], Phase::WaitingChildren, false, Vec::new());
        tree.restore_node(
            2,
            Some(1),
            Vec::new(),
            Phase::Done(TestOutput::Ok),
            false,
            vec![TestSpec::Children(1)],
        );

        let effects = tree.reduce(Event::Recovered { node: 2 });
        assert_eq!(
            launches(&effects),
            vec![1, 2],
            "the parent is re-entered first; the child with pending input drains only afterwards"
        );
        assert!(matches!(tree.phase(1), Some(Phase::Running)));
    }

    #[test]
    fn a_recovered_cancelled_node_stays_quiet() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(
            1,
            None,
            Vec::new(),
            Phase::Cancelled(CancelOrigin::External),
            false,
            Vec::new(),
        );

        let effects = tree.reduce(Event::Recovered { node: 1 });
        assert!(
            effects.is_empty(),
            "a cancelled instance must not spawn pending input"
        );
        assert!(tree.is_quiescent());
    }

    /// 恢复出的在途 detached 节点收敛后不得背交付义务
    ///
    /// 崩溃期间没人交付，watcher 又在本轮恢复之后才订阅，没人会补发 `Deliver`
    #[test]
    fn a_recovered_in_flight_detached_node_owes_no_delivery() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(1, None, Vec::new(), Phase::Running, true, Vec::new());

        let effects = tree.reduce(Event::Recovered { node: 1 });
        assert!(matches!(tree.phase(1), Some(Phase::Aborted)));
        assert!(
            tree.node(1).expect("node").delivered,
            "a terminal phase produced by recovery convergence must not create a delivery obligation, otherwise that topic's stream hangs forever"
        );
        assert!(tree.is_quiescent());
        assert_eq!(sealed_count(&effects), 1);
    }

    /// 恢复出的终态 detached 节点同样不背交付义务
    #[test]
    fn a_recovered_terminal_detached_node_owes_no_delivery() {
        let mut done = Tree::<TestDomain>::new();
        done.restore_node(
            1,
            None,
            Vec::new(),
            Phase::Done(TestOutput::Ok),
            true,
            Vec::new(),
        );
        let done_effects = done.reduce(Event::Recovered { node: 1 });
        assert!(done.node(1).expect("node").delivered);
        assert!(done.is_quiescent());
        assert_eq!(
            sealed_count(&done_effects),
            0,
            "it is already quiet at recovery time; the hub re-emits Sealed on the recovery branch"
        );

        let mut cancelling = Tree::<TestDomain>::new();
        cancelling.restore_node(
            1,
            None,
            Vec::new(),
            Phase::Cancelling(CancelOrigin::External),
            true,
            Vec::new(),
        );
        let cancel_effects = cancelling.reduce(Event::Recovered { node: 1 });
        assert!(matches!(
            cancelling.phase(1),
            Some(Phase::Cancelled(CancelOrigin::External))
        ));
        assert!(
            cancelling.node(1).expect("node").delivered,
            "a Cancelled produced by convergence must likewise not create a delivery obligation"
        );
        assert!(cancelling.is_quiescent());
        assert_eq!(sealed_count(&cancel_effects), 1);
    }

    /// 恢复出的 `Cancelling` 节点带待消费输入时同样出队
    ///
    /// 领域侧不给取消的实例派生待消费输入，本用例覆盖即使派生出来也不会被卡住的情形
    #[test]
    fn a_recovered_cancelling_node_drains_its_pending() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(
            1,
            None,
            Vec::new(),
            Phase::Cancelling(CancelOrigin::External),
            false,
            vec![TestSpec::Noop],
        );

        let effects = tree.reduce(Event::Recovered { node: 1 });
        assert_eq!(
            launches(&effects),
            vec![1],
            "pending input produced by recovery must drain"
        );
        assert!(tree.node(1).expect("node").pending.is_empty());
        assert!(matches!(tree.phase(1), Some(Phase::Running)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::Publish(TreeEvent::Terminated {
                node,
                terminal: Terminal::Cancelled(CancelOrigin::External),
            }) if *node == 1
        )));

        let job = tree.job_of(1).expect("job");
        let sealed = tree.reduce(Event::Completed {
            node: 1,
            job,
            output: TestOutput::Ok,
        });
        assert!(
            tree.is_quiescent(),
            "the tree must converge once this drained round completes"
        );
        assert_eq!(sealed_count(&sealed), 1);
    }

    #[test]
    fn a_terminal_detached_child_keeps_the_tree_unquiet_until_delivered() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, _) = spawn(&mut tree, 1, None, false);
        let child_job = spawn_detached_child(&mut tree, root, 2);

        let child_effects = tree.reduce(Event::Completed {
            node: 2,
            job: child_job,
            output: TestOutput::Ok,
        });
        assert!(tree.phase(2).is_some_and(Phase::is_terminal));
        assert!(
            !tree.is_quiescent(),
            "the background output has not returned to the parent yet, so the whole tree is not quiet"
        );
        assert_eq!(
            sealed_count(&child_effects),
            0,
            "must not race the stream close"
        );

        let parent_job = tree.job_of(root).expect("parent job");
        let parent_effects = tree.reduce(Event::Completed {
            node: root,
            job: parent_job,
            output: TestOutput::Ok,
        });
        assert!(
            !tree.is_quiescent(),
            "the parent finished, but the obligation remains"
        );
        assert_eq!(sealed_count(&parent_effects), 0);

        let delivered = tree.reduce(Event::Deliver { node: 2 });
        assert!(tree.node(2).expect("child").delivered);
        assert!(tree.is_quiescent());
        assert_eq!(
            sealed_count(&delivered),
            1,
            "the stream closes only when the obligation is released"
        );
    }

    #[test]
    fn deliver_is_not_eaten_by_entry_absorption() {
        assert_eq!(Event::<TestDomain>::Deliver { node: 1 }.node(), Some(1));
        assert!(
            Event::<TestDomain>::Deliver { node: 1 }.bypasses_entry_absorption(),
            "Deliver targets an already terminal node, so it must bypass entry absorption"
        );
        assert!(
            !Event::<TestDomain>::Deliver { node: 1 }.reopens(),
            "releasing an obligation must not reactivate the node"
        );

        let mut tree = Tree::<TestDomain>::new();
        let (node, _) = spawn(&mut tree, 1, None, false);
        complete(&mut tree, node);
        tree.set_delivered_for_test(node, false);
        assert!(
            !tree.is_quiescent(),
            "registering an obligation makes the tree not quiet"
        );

        let effects = tree.reduce(Event::Deliver { node });
        assert!(
            tree.node(node).expect("node").delivered,
            "terminal absorption must not swallow Deliver"
        );
        assert!(tree.is_quiescent());
        assert_eq!(sealed_count(&effects), 1);
    }

    #[test]
    fn deliver_on_a_node_without_an_obligation_is_ignored() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, job) = spawn(&mut tree, 1, None, false);

        let running = tree.reduce(Event::Deliver { node });
        assert!(
            running.is_empty(),
            "no effects may be produced when there is no obligation to release"
        );
        assert!(matches!(tree.phase(node), Some(Phase::Running)));
        assert_eq!(
            tree.job_of(node),
            Some(job),
            "a running node's bookkeeping must not be rewritten"
        );

        assert!(
            tree.reduce(Event::Deliver { node: 42 }).is_empty(),
            "an unknown node must be ignored silently"
        );

        complete(&mut tree, node);
        assert!(
            tree.reduce(Event::Deliver { node }).is_empty(),
            "a non-detached terminal node has no obligation, so a repeated release must be silent"
        );
        assert!(tree.is_quiescent());
    }

    #[test]
    fn two_inputs_arriving_while_running_both_wait_for_the_next_idle() {
        let mut tree = tree_with_running_root();
        tree.reduce(Event::Enqueue {
            node: 1,
            spec: TestSpec::Noop,
        });
        tree.reduce(Event::Enqueue {
            node: 1,
            spec: TestSpec::Suspend,
        });
        assert_eq!(tree.node(1).expect("node").pending.len(), 2);

        let effects = tree.reduce(Event::Completed {
            node: 1,
            job: 1,
            output: TestOutput::Ok,
        });
        let idle = effects.iter().find_map(|effect| match effect {
            Effect::Launch {
                input: Input::Idle(specs),
                ..
            } => Some(specs),
            _ => None,
        });
        assert_eq!(
            idle.expect("open a round when idle").len(),
            2,
            "the whole queue is drained at once"
        );
    }

    /// 恢复出的挂起节点不消费队列。答复回来之前不得抢跑一轮。
    #[test]
    fn a_recovered_suspended_node_keeps_its_pending_until_resumed() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(
            1,
            None,
            Vec::new(),
            Phase::Suspended(TestSuspend::Ask),
            false,
            vec![TestSpec::Noop],
        );

        let effects = tree.reduce(Event::Recovered { node: 1 });
        assert!(
            matches!(tree.phase(1), Some(Phase::Suspended(TestSuspend::Ask))),
            "recovery must not rewrite the suspended Phase"
        );
        assert!(
            launches(&effects).is_empty(),
            "recovery must not open a round for a suspended node"
        );
        assert_eq!(
            tree.node(1).expect("node").pending.len(),
            1,
            "pending input is left for the next idle after recovery"
        );

        let resumed = tree.reduce(Event::Resume {
            node: 1,
            expect: TestSuspend::Ask,
            input: TestResume::Answer,
        });
        assert!(
            resumed.iter().any(|effect| matches!(
                effect,
                Effect::Launch {
                    input: Input::External(_),
                    ..
                }
            )),
            "it resumes running only once the answer arrives"
        );
    }

    /// 恢复后，等待子节点的父节点不得与仍在跑的子树并行开轮
    #[test]
    fn a_recovered_waiting_parent_does_not_race_its_running_children() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(
            1,
            None,
            vec![2],
            Phase::WaitingChildren,
            false,
            vec![TestSpec::Noop],
        );
        tree.restore_node(2, Some(1), Vec::new(), Phase::Running, false, Vec::new());

        let effects = tree.reduce(Event::Recovered { node: 1 });
        assert!(
            matches!(tree.phase(1), Some(Phase::WaitingChildren)),
            "a child has not converged yet, so the parent must stay in the barrier"
        );
        assert!(
            launches(&effects).is_empty(),
            "a parent waiting for children must not open a round first"
        );
        assert_eq!(tree.node(1).expect("node").pending.len(), 1);

        let child = tree.reduce(Event::Recovered { node: 2 });
        let input = child.iter().find_map(|effect| match effect {
            Effect::Launch { node, input, .. } if *node == 1 => Some(input),
            _ => None,
        });
        assert!(
            matches!(input, Some(Input::Children(_))),
            "the parent is re-entered with Children only after the child converges"
        );
        assert_eq!(
            tree.node(1).expect("node").pending.len(),
            1,
            "a Children re-entry does not consume the queue"
        );
    }

    /// 恢复出的已答复等待仍能续行，续行结束后再消费排队的输入
    #[test]
    fn a_recovered_answered_wait_runs_its_queued_input_after_resuming() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(
            1,
            None,
            Vec::new(),
            Phase::Suspended(TestSuspend::Ask),
            false,
            vec![TestSpec::Noop],
        );
        tree.reduce(Event::Recovered { node: 1 });

        tree.reduce(Event::Resume {
            node: 1,
            expect: TestSuspend::Ask,
            input: TestResume::Answer,
        });
        let job = tree.job_of(1).expect("read the job after resume");
        let effects = tree.reduce(Event::Completed {
            node: 1,
            job,
            output: TestOutput::Ok,
        });
        let idle = effects.iter().find_map(|effect| match effect {
            Effect::Launch {
                node: 1,
                input: Input::Idle(specs),
                ..
            } => Some(specs.len()),
            _ => None,
        });
        assert_eq!(
            idle,
            Some(1),
            "after the resumed round ends, the queued input must open the next round"
        );
    }

    #[test]
    fn a_children_reentry_does_not_consume_the_pending_queue() {
        let mut tree = tree_with_running_root();
        // 先让根派生一个进入屏障的子节点，根停泊在 WaitingChildren
        let root_job = tree.job_of(1).expect("job");
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

        tree.reduce(Event::Enqueue {
            node: 1,
            spec: TestSpec::Noop,
        });
        assert_eq!(tree.node(1).expect("node").pending.len(), 1);

        // 子节点终态触发 Children 重入
        let child_job = tree.job_of(2).expect("job");
        let effects = tree.reduce(Event::Completed {
            node: 2,
            job: child_job,
            output: TestOutput::Ok,
        });
        let input = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::Launch { node, input, .. } if *node == 1 => Some(input),
                _ => None,
            })
            .expect("reenter after the child ends");
        assert!(
            matches!(input, Input::Children(_)),
            "a re-entry round is not Idle, so it must not consume the queue"
        );
        assert_eq!(
            tree.node(1).expect("node").pending.len(),
            1,
            "the earlier pending input is still there"
        );
    }
}
