use super::{
    JobId, NodeId, Tree,
    domain::Domain,
    event::{Effect, Event},
    phase::Phase,
    view::TreeEvent,
};
use crate::agent::behavior::{ChildResult, Input};

impl<D: Domain> Tree<D> {
    /// 归约事件并产出副作用
    pub fn reduce(&mut self, event: Event<D>) -> Vec<Effect<D>> {
        let before = self.is_quiescent();
        let mut effects = self.reduce_inner(event);
        if !before && self.is_quiescent() {
            effects.push(Effect::Publish(TreeEvent::Sealed));
        }
        effects
    }

    pub(super) fn reduce_inner(&mut self, event: Event<D>) -> Vec<Effect<D>> {
        // 非可绕过的事件，并且节点已终态，并且不是让终态节点重新活跃
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
            Event::Progress { node, job, chunk } => self.reduce_progress(node, job, chunk),
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
                    // 全是 detached：父节点立即重入
                    self.set_pending(node, None);
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
                // 回显比对：外部输入必须匹配当前挂起标记
                let matched = matches!(self.phase(node), Some(Phase::Suspended(current)) if *current == expect);
                if !matched {
                    return Vec::new();
                }
                self.set_pending(node, None);
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
        self.set_pending(id, None);
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

    /// 登记待消费输入：节点空闲则立刻出队，忙则只置位等它自然落终态
    ///
    /// 终态一律重开，取消中的节点忽略
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
        self.set_pending(node, Some(spec));
        if terminal {
            return self.drain_pending(node);
        }
        Vec::new()
    }

    /// 取走待消费输入并开新的一轮
    pub(super) fn drain_pending(&mut self, node: NodeId) -> Vec<Effect<D>> {
        let Some(spec) = self.take_pending(node) else {
            return Vec::new();
        };
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
                input: Input::Fresh(spec),
            },
        ]
    }

    /// 释放一个节点的投递义务
    pub(crate) fn reduce_deliver(&mut self, node: NodeId) -> Vec<Effect<D>> {
        match self.node(node) {
            None => log::warn!("[tree] deliver for unknown node ignored: {node}"),
            Some(current) if current.delivered => {}
            Some(_) => self.set_delivered(node, true),
        }
        Vec::new()
    }

    /// 给 detached 节点登记待投递
    pub(super) fn owe_delivery(&mut self, node: NodeId) {
        if self.is_detached(node) {
            self.set_delivered(node, false);
        }
    }

    /// 是否接受 job 回报，只在节点仍处于 Running 时被接受
    pub(super) fn accepts_job_report(&self, node: NodeId, job: JobId) -> bool {
        self.job_of(node) == Some(job) && matches!(self.phase(node), Some(Phase::Running))
    }

    /// 只有 Running 节点才能下发进度
    fn reduce_progress(&mut self, node: NodeId, job: JobId, chunk: D::Progress) -> Vec<Effect<D>> {
        if !self.accepts_job_report(node, job) {
            return Vec::new();
        }
        vec![Effect::Publish(TreeEvent::Progress { node, chunk })]
    }

    /// 落终态并下发终态事件
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

    /// 给一个重建出来的节点补上收敛，并重跑一次重入判定
    ///
    /// Cancelling -> Cancelled
    /// 
    /// Running    -> Aborted
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
                effects.extend(self.drain_pending(node));
                effects
            }
        }
    }

    /// 子节点全部结束后重入父节点，父节点不在 WaitingChildren 则跳过
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
        // 只交付进入屏障的子节点的结果
        let results: Vec<ChildResult<D>> = self
            .barrier_children(parent)
            .into_iter()
            .filter_map(|id| {
                self.phase(id)
                    .and_then(Phase::terminal)
                    .map(|terminal| ChildResult { node: id, terminal })
            })
            .collect();
        self.set_pending(parent, None);
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
        TestDomain, TestError, TestOutput, TestProgress, TestResume, TestSpec, TestSuspend,
    };
    use crate::agent::tree::event::{Effect, Event};
    use crate::agent::tree::view::TreeEvent;
    use crate::agent::tree::{CancelOrigin, Terminal, Tree};

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

    /// 让在跑的节点以 Done 落终态
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
        assert!(effects.is_empty(), "未知父节点不得产出任何副作用");
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
        assert!(effects.is_empty(), "重复 id 必须被拒绝");
        assert_eq!(tree.all_node_ids(), vec![1]);
    }

    #[test]
    fn progress_is_published_only_for_the_live_running_job() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, job) = spawn(&mut tree, 1, None, false);

        let effects = tree.reduce(Event::Progress {
            node,
            job,
            chunk: TestProgress::Tick,
        });
        assert!(matches!(
            effects.as_slice(),
            [Effect::Publish(TreeEvent::Progress { .. })]
        ));

        let stale = tree.reduce(Event::Progress {
            node,
            job: job + 99,
            chunk: TestProgress::Tick,
        });
        assert!(stale.is_empty(), "旧 job 的进度必须丢弃");

        // 取消中的节点仍持有有效 job，但进度不得再漏出去
        tree.set_phase_for_test(node, Phase::Cancelling(CancelOrigin::External));
        assert_eq!(tree.job_of(node), Some(job), "Cancelling 阶段 job 尚未失效");
        let during_cancel = tree.reduce(Event::Progress {
            node,
            job,
            chunk: TestProgress::Tick,
        });
        assert!(during_cancel.is_empty(), "取消后不得再下发任何分片");
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
            Event::Progress {
                node,
                job,
                chunk: TestProgress::Tick,
            },
            Event::Stopped { node, job },
            Event::Aborted { node, job },
            Event::Suspended {
                node,
                job,
                payload: TestSuspend::Ask,
            },
        ];
        for event in absorbed {
            assert!(tree.reduce(event).is_empty(), "终态必须静默吸收单节点事件");
        }
        assert!(matches!(tree.phase(node), Some(Phase::Done(_))));
    }

    /// 终态节点收到入队就地重开一轮，运行中的节点只把输入留在触发位上
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
        assert_ne!(tree.job_of(node), Some(job), "新一轮必须拿到新的 job");
        assert_eq!(launches(&effects), vec![node]);

        let running_job = tree.job_of(node).expect("job");
        let busy = tree.reduce(Event::Enqueue {
            node,
            spec: TestSpec::Children(1),
        });
        assert!(busy.is_empty(), "忙时只置位，不打断在跑的那一轮");
        assert!(matches!(tree.phase(node), Some(Phase::Running)));
        assert_eq!(
            tree.job_of(node),
            Some(running_job),
            "在跑的那一轮不得被换掉"
        );
        assert_eq!(
            tree.node(node).expect("node").pending,
            Some(TestSpec::Children(1)),
            "后到的输入留在触发位上"
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
        assert_eq!(sealed, 1, "收敛时恰好发一次 Sealed");

        assert_eq!(
            tree.job_of(node),
            None,
            "落终态必须释放 job，否则过期的回报还有可能被认成有效"
        );

        let again = tree.reduce(Event::Stopped {
            node,
            job: job + 99,
        });
        assert!(
            !again
                .iter()
                .any(|e| matches!(e, Effect::Publish(TreeEvent::Sealed))),
            "已安静后不得重复发 Sealed"
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
        assert_eq!(tree.job_of(root), None, "等待子任务时不得残留 job");
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
            "还有子节点未结束，不得重入父节点"
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
        let (node, input) = relaunched.expect("最后一个子节点结束必须重入父节点");
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
        assert_eq!(delivered, Some(1), "失败也必须交付给父节点，不能静默");
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
            "屏障为空时父节点立即重入，不进入 WaitingChildren"
        );
        assert!(
            matches!(tree.phase(2), Some(Phase::Running)),
            "子节点照常启动"
        );
        assert_ne!(tree.job_of(1), Some(root_job), "重入必须换新 job");
        let relaunched = effects.iter().find_map(|e| match e {
            Effect::Launch {
                node,
                input: Input::Children(results),
                ..
            } if *node == 1 => Some(results.len()),
            _ => None,
        });
        assert_eq!(relaunched, Some(0), "立即重入不带任何屏障结果");
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::Publish(TreeEvent::PhaseChanged { node, to })
                    if *node == 1 && matches!(**to, Phase::Running)
            )),
            "重入必须对外宣告"
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::Persist { node } if *node == 1)),
            "重入必须落库"
        );
    }

    /// 全是 detached 的派生让父节点立即重入，那一轮同样清掉待消费输入
    #[test]
    fn an_all_detached_respawn_clears_the_pending_input_it_consumed() {
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
            Some(TestSpec::Children(1))
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
            "先启动 detached 子节点，屏障为空时父节点立即重入"
        );
        assert_eq!(
            tree.node(1).expect("root").pending,
            None,
            "立即重入的那一轮全量重载，待消费输入不得留下再开一轮"
        );

        let job = tree.job_of(1).expect("job");
        let settled = tree.reduce(Event::Completed {
            node: 1,
            job,
            output: TestOutput::Ok,
        });
        assert_eq!(
            launches(&settled),
            Vec::<NodeId>::new(),
            "那条输入已在重入轮被模型看过，不得再开一轮"
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
            "worker 崩了必须落 Aborted"
        );
        assert_eq!(tree.job_of(child), None, "Aborted 必须释放 job");
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::Publish(TreeEvent::Terminated {
                    node,
                    terminal: Terminal::Aborted
                }) if *node == child
            )),
            "Aborted 必须对外宣告"
        );
        let delivered = effects.iter().find_map(|e| match e {
            Effect::Launch {
                node,
                input: Input::Children(results),
                ..
            } if *node == root => Some(results.len()),
            _ => None,
        });
        assert_eq!(delivered, Some(1), "Aborted 也必须交付给父节点");
    }

    #[test]
    fn an_aborted_worker_resolves_a_cancelling_node() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, job) = spawn(&mut tree, 1, None, false);
        tree.set_phase_for_test(node, Phase::Cancelling(CancelOrigin::External));

        let effects = tree.reduce(Event::Aborted { node, job });
        assert!(
            matches!(tree.phase(node), Some(Phase::Aborted)),
            "取消途中崩掉的 worker 交不出停止确认，Aborted 才是如实的结论"
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::Publish(TreeEvent::Terminated {
                    node: reported,
                    terminal: Terminal::Aborted
                }) if *reported == node
            )),
            "取消中的节点也必须收敛并宣告"
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
            "取消优先于重入"
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
        assert!(effects.is_empty(), "迟到的完成回报不得被接受");
        assert!(matches!(
            tree.phase(node),
            Some(Phase::Cancelling(CancelOrigin::External))
        ));
        assert_eq!(
            tree.job_of(node),
            Some(job),
            "取消中 job 必须留待 Stopped 核对"
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
        assert!(effects.is_empty(), "迟到的派生回报不得被接受");
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::Launch { .. })),
            "取消优先于重入"
        );
        assert_eq!(tree.all_node_ids(), vec![node], "不得建出任何子节点");
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
        assert_eq!(tree.job_of(node), None, "挂起后不得残留 job");
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::Publish(TreeEvent::Sealed))),
            "挂起不是终态，不得关流"
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
            "Launch 的 job 必须登记在节点上"
        );
        assert_ne!(tree.job_of(node), Some(job), "恢复必须分配新的 job");

        let replay = tree.reduce(Event::Resume {
            node,
            expect: TestSuspend::Ask,
            input: TestResume::Answer,
        });
        assert!(replay.is_empty(), "重复或过期的外部输入必须静默丢弃");
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
        assert!(stale.is_empty(), "回显不匹配的挂起标记必须静默丢弃");
        assert!(
            matches!(tree.phase(node), Some(Phase::Suspended(TestSuspend::Ask))),
            "被拒的恢复不得改写挂起标记"
        );
        assert_eq!(tree.job_of(node), None, "被拒的恢复不得留下 job");
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
            "终态吸收不得吃掉入队"
        );
        assert_eq!(
            tree.node(node).expect("node").pending,
            None,
            "被 Launch 的一轮必然全量重载，待消费输入必须清掉"
        );
        assert_eq!(launches(&effects), vec![node], "入队恰好开一轮");
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::Publish(TreeEvent::PhaseChanged { node: reported, to })
                if *reported == node && matches!(**to, Phase::Running)
        )));
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::Publish(TreeEvent::NodeCreated { .. }))),
            "入队只是开新的一轮，不开新节点"
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
        assert!(effects.is_empty(), "忙时只置位，不产出任何副作用");
        assert!(matches!(tree.phase(node), Some(Phase::Running)));
        assert_eq!(tree.node(node).expect("node").pending, Some(TestSpec::Noop));
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
        assert_eq!(all, vec![node], "连发两次入队只开一轮");
        assert_eq!(
            tree.node(node).expect("node").pending,
            Some(TestSpec::Noop),
            "后到的那次留在触发位上，等这一轮结束再开一轮"
        );
    }

    #[test]
    fn a_tree_with_pending_is_not_quiescent() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, _) = spawn(&mut tree, 1, None, false);
        complete(&mut tree, node);
        assert!(tree.is_quiescent());

        // 恢复出来的「终态 + 待消费输入」，实际由 recover 重建
        tree.set_pending_for_test(node, Some(TestSpec::Noop));
        assert!(!tree.is_quiescent(), "有待消费输入就不是安静");

        let drained = tree.reduce(Event::Enqueue {
            node,
            spec: TestSpec::Noop,
        });
        assert!(
            !drained
                .iter()
                .any(|e| matches!(e, Effect::Publish(TreeEvent::Sealed))),
            "出队开的是新一轮，不得顺手关流"
        );
        assert!(!tree.is_quiescent(), "新一轮在跑，仍不安静");

        let job = tree.job_of(node).expect("job");
        let sealed = tree.reduce(Event::Completed {
            node,
            job,
            output: TestOutput::Ok,
        });
        assert!(tree.is_quiescent(), "这一轮跑完才算安静");
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
            "有输入在等就接着开一轮"
        );
        assert_eq!(tree.node(node).expect("node").pending, None);
        let input = effects.iter().find_map(|e| match e {
            Effect::Launch {
                node: reported,
                input: Input::Fresh(spec),
                ..
            } if *reported == node => Some(spec.clone()),
            _ => None,
        });
        assert_eq!(
            input,
            Some(TestSpec::Noop),
            "新一轮用的就是待消费的那条输入"
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::Publish(TreeEvent::Terminated { node: reported, .. }) if *reported == node
            )),
            "这一轮的终态照常宣告"
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
            "先重入父节点，父节点转 Running 之后子节点才出队"
        );
    }

    /// 重入的那一轮全量重载上下文，停泊期间到达的输入必须一并清掉
    #[test]
    fn a_children_round_clears_the_pending_input_it_consumed() {
        let mut tree = Tree::<TestDomain>::new();
        let (root, children) = build_fanout(&mut tree, 1);
        let child = children[0];
        // 父节点停泊期间到达的输入：只置位，不打断等待
        tree.reduce(Event::Enqueue {
            node: root,
            spec: TestSpec::Children(1),
        });
        assert_eq!(
            tree.node(root).expect("root").pending,
            Some(TestSpec::Children(1))
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
            "子节点全终态必须重入父节点"
        );
        assert_eq!(
            tree.node(root).expect("root").pending,
            None,
            "重入轮已全量重载，待消费输入不得留下再开一轮"
        );

        let job = tree.job_of(root).expect("job");
        let settled = tree.reduce(Event::Completed {
            node: root,
            job,
            output: TestOutput::Ok,
        });
        assert_eq!(
            launches(&settled),
            Vec::<NodeId>::new(),
            "那条输入已在重入轮被模型看过，不得再开一轮"
        );
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
            None,
        );

        let effects = tree.reduce(Event::Enqueue {
            node: 1,
            spec: TestSpec::Noop,
        });
        assert!(
            matches!(tree.phase(1), Some(Phase::Running)),
            "取消之后新来的输入必须能重新开轮"
        );
        assert_eq!(launches(&effects), vec![1]);
        assert!(!tree.is_quiescent(), "新一轮在跑");
    }

    #[test]
    fn enqueue_reopens_an_aborted_node() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(1, None, Vec::new(), Phase::Aborted, false, None);

        let effects = tree.reduce(Event::Enqueue {
            node: 1,
            spec: TestSpec::Noop,
        });
        assert!(
            matches!(tree.phase(1), Some(Phase::Running)),
            "崩溃收尾之后新来的输入必须能重新开轮"
        );
        assert_eq!(launches(&effects), vec![1]);
        assert!(!tree.is_quiescent(), "新一轮在跑");
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
            tree.node(node).expect("node").pending.is_none(),
            "正在取消的节点不得登记待消费输入"
        );
        assert!(!tree.is_quiescent(), "取消中的节点还在跑");

        tree.reduce(Event::Stopped { node, job });
        assert!(
            tree.is_quiescent(),
            "被拒的入队不得钉住树：节点落终态后整棵树必须安静"
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
            None,
        );
        // 恢复出来的「Done + 待消费输入」，实际由 recover 重建
        tree.set_pending_for_test(1, Some(TestSpec::Noop));

        let effects = tree.reduce(Event::Recovered { node: 1 });
        assert!(
            matches!(tree.phase(1), Some(Phase::Running)),
            "恢复出的待消费输入必须出队，否则该话题永不安静"
        );
        assert_eq!(launches(&effects), vec![1]);

        let job = tree.job_of(1).expect("job");
        tree.reduce(Event::Completed {
            node: 1,
            job,
            output: TestOutput::Ok,
        });
        assert!(tree.is_quiescent(), "出队后的这一轮跑完必须能收敛");
    }

    /// 恢复路径与 `finish_terminal` 同序：先重入父节点，带待消费输入的子节点才出队
    #[test]
    fn a_recovered_child_drains_only_after_its_parent_is_reentered() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(1, None, vec![2], Phase::WaitingChildren, false, None);
        tree.restore_node(
            2,
            Some(1),
            Vec::new(),
            Phase::Done(TestOutput::Ok),
            false,
            Some(TestSpec::Children(1)),
        );

        let effects = tree.reduce(Event::Recovered { node: 2 });
        assert_eq!(
            launches(&effects),
            vec![1, 2],
            "先重入父节点，带待消费输入的子节点才出队"
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
            None,
        );

        let effects = tree.reduce(Event::Recovered { node: 1 });
        assert!(effects.is_empty(), "取消的实例不得派生待消费输入");
        assert!(tree.is_quiescent());
    }

    /// 恢复出的在途 detached 节点收敛后不得背投递义务
    ///
    /// 崩溃期间没人投递，watcher 又在本轮恢复之后才订阅，没人会补发 `Deliver`
    #[test]
    fn a_recovered_in_flight_detached_node_owes_no_delivery() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(1, None, Vec::new(), Phase::Running, true, None);

        let effects = tree.reduce(Event::Recovered { node: 1 });
        assert!(matches!(tree.phase(1), Some(Phase::Aborted)));
        assert!(
            tree.node(1).expect("node").delivered,
            "恢复收敛出的终态不得产生投递义务，否则该话题的流永久挂着"
        );
        assert!(tree.is_quiescent());
        assert_eq!(sealed_count(&effects), 1);
    }

    /// 恢复出的终态 detached 节点同样不背投递义务
    #[test]
    fn a_recovered_terminal_detached_node_owes_no_delivery() {
        let mut done = Tree::<TestDomain>::new();
        done.restore_node(1, None, Vec::new(), Phase::Done(TestOutput::Ok), true, None);
        let done_effects = done.reduce(Event::Recovered { node: 1 });
        assert!(done.node(1).expect("node").delivered);
        assert!(done.is_quiescent());
        assert_eq!(
            sealed_count(&done_effects),
            0,
            "恢复时本就安静，Sealed 由 hub 在恢复分支补发"
        );

        let mut cancelling = Tree::<TestDomain>::new();
        cancelling.restore_node(
            1,
            None,
            Vec::new(),
            Phase::Cancelling(CancelOrigin::External),
            true,
            None,
        );
        let cancel_effects = cancelling.reduce(Event::Recovered { node: 1 });
        assert!(matches!(
            cancelling.phase(1),
            Some(Phase::Cancelled(CancelOrigin::External))
        ));
        assert!(
            cancelling.node(1).expect("node").delivered,
            "收敛出的 Cancelled 也不得产生投递义务"
        );
        assert!(cancelling.is_quiescent());
        assert_eq!(sealed_count(&cancel_effects), 1);
    }

    /// 恢复出的 `Cancelling` 节点带待消费输入时同样出队
    ///
    /// `Cancelling` 收敛成 `Cancelled` 走 `finish_terminal`，出队紧随其后；
    /// 领域侧不给取消的实例派生待消费输入，这里盖住的是「万一派生出来也不会挂着」
    #[test]
    fn a_recovered_cancelling_node_drains_its_pending() {
        let mut tree = Tree::<TestDomain>::new();
        tree.restore_node(
            1,
            None,
            Vec::new(),
            Phase::Cancelling(CancelOrigin::External),
            false,
            Some(TestSpec::Noop),
        );

        let effects = tree.reduce(Event::Recovered { node: 1 });
        assert_eq!(launches(&effects), vec![1], "恢复出的待消费输入必须出队");
        assert!(tree.node(1).expect("node").pending.is_none());
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
        assert!(tree.is_quiescent(), "出队后的这一轮跑完必须能收敛");
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
            "后台产出还没回到父节点，整棵树不算安静"
        );
        assert_eq!(sealed_count(&child_effects), 0, "不得抢跑关流");

        let parent_job = tree.job_of(root).expect("parent job");
        let parent_effects = tree.reduce(Event::Completed {
            node: root,
            job: parent_job,
            output: TestOutput::Ok,
        });
        assert!(!tree.is_quiescent(), "父节点跑完，义务仍在");
        assert_eq!(sealed_count(&parent_effects), 0);

        let delivered = tree.reduce(Event::Deliver { node: 2 });
        assert!(tree.node(2).expect("child").delivered);
        assert!(tree.is_quiescent());
        assert_eq!(sealed_count(&delivered), 1, "释放义务的那一刻才关流");
    }

    #[test]
    fn deliver_is_not_eaten_by_entry_absorption() {
        assert_eq!(Event::<TestDomain>::Deliver { node: 1 }.node(), Some(1));
        assert!(
            Event::<TestDomain>::Deliver { node: 1 }.bypasses_entry_absorption(),
            "Deliver 作用于已终态节点，必须绕过入口吸收"
        );
        assert!(
            !Event::<TestDomain>::Deliver { node: 1 }.reopens(),
            "释放义务不得让节点重新活跃"
        );

        let mut tree = Tree::<TestDomain>::new();
        let (node, _) = spawn(&mut tree, 1, None, false);
        complete(&mut tree, node);
        tree.set_delivered_for_test(node, false);
        assert!(!tree.is_quiescent(), "登记了义务就不安静");

        let effects = tree.reduce(Event::Deliver { node });
        assert!(
            tree.node(node).expect("node").delivered,
            "终态吸收不得吃掉 Deliver"
        );
        assert!(tree.is_quiescent());
        assert_eq!(sealed_count(&effects), 1);
    }

    #[test]
    fn deliver_on_a_node_without_an_obligation_is_ignored() {
        let mut tree = Tree::<TestDomain>::new();
        let (node, job) = spawn(&mut tree, 1, None, false);

        let running = tree.reduce(Event::Deliver { node });
        assert!(running.is_empty(), "无义务可释放时不得产出任何副作用");
        assert!(matches!(tree.phase(node), Some(Phase::Running)));
        assert_eq!(tree.job_of(node), Some(job), "在跑的节点不得被记账改写");

        assert!(
            tree.reduce(Event::Deliver { node: 42 }).is_empty(),
            "未知节点必须静默忽略"
        );

        complete(&mut tree, node);
        assert!(
            tree.reduce(Event::Deliver { node }).is_empty(),
            "非 detached 的终态节点没有义务，重复释放必须静默"
        );
        assert!(tree.is_quiescent());
    }
}
