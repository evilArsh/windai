use super::behavior::Behavior;
use super::jobs::{JobRequest, Jobs, REPORT_CAPACITY};
use super::tree::event::{Effect, Event};
use super::tree::phase::Phase;
use super::tree::view::TreeEvent;
use super::tree::{Domain, NodeId, Tree};
use crate::error::{CoreError, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// 对外事件总线的默认容量
pub const BUS_CAPACITY: usize = 1024;

/// 节点状态的持久化适配
#[async_trait::async_trait]
pub trait Persist<D: Domain>: Send + Sync {
    /// 保存一个节点的当前状态
    async fn save(&self, node: NodeId, phase: &Phase<D>) -> Result<()>;
}

/// hub 的启动参数
pub struct HubOptions<D: Domain> {
    pub behavior: Arc<dyn Behavior<D>>,
    pub persist: Arc<dyn Persist<D>>,
    /// worker 不响应取消时的强制回收宽限期
    pub grace: Duration,
    /// 对外事件总线的容量；接收方跟不上时按它跳帧
    pub bus_capacity: usize,
}

/// 节点的只读快照
#[derive(Debug, Clone)]
pub struct NodeSnapshot<D: Domain> {
    pub id: NodeId,
    pub parent: Option<NodeId>,
    pub children: Vec<NodeId>,
    pub phase: Phase<D>,
    /// 领域侧由 AgentMode 推出
    pub detached: bool,
    /// 恢复出来的待消费输入
    pub pending: Option<D::Spec>,
}

enum Command<D: Domain> {
    Event(Event<D>),
    Recover(Vec<NodeSnapshot<D>>, oneshot::Sender<()>),
    InFlight(oneshot::Sender<Vec<NodeId>>),
    Undelivered(oneshot::Sender<Vec<NodeId>>),
    Snapshot(oneshot::Sender<Vec<NodeSnapshot<D>>>),
    Shutdown,
}

/// hub 的对外句柄
#[derive(Clone)]
pub struct HubHandle<D: Domain> {
    tx: mpsc::Sender<Command<D>>,
    bus: broadcast::Sender<TreeEvent<D>>,
    cancel: CancellationToken,
}

impl<D: Domain> HubHandle<D> {
    /// hub 是否已停机
    pub fn is_stopped(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// 投递一个事件
    pub async fn send(&self, event: Event<D>) -> Result<()> {
        self.tx
            .send(Command::Event(event))
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))
    }

    /// 订阅对外事件流
    pub fn subscribe(&self) -> broadcast::Receiver<TreeEvent<D>> {
        self.bus.subscribe()
    }

    /// 读取全部节点的当前状态
    pub async fn snapshot(&self) -> Result<Vec<NodeSnapshot<D>>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Snapshot(reply))
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?;
        rx.await.map_err(|err| CoreError::Internal(err.to_string()))
    }

    /// 用持久化快照重建树，并收敛其中的在途节点
    ///
    /// 前置条件：针对一个**空 hub** 调用一次。它按快照覆盖式重建，不合并既有节点，
    /// 也不记忆「已宣告 Sealed」，重复调用会重复广播
    pub async fn recover(&self, nodes: Vec<NodeSnapshot<D>>) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Recover(nodes, reply))
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?;
        rx.await.map_err(|err| CoreError::Internal(err.to_string()))
    }

    /// 读取在途相（Running / Cancelling）的节点
    ///
    /// 运行中它同时包含已请求取消、还在等 `Stopped` 的节点
    pub async fn in_flight_nodes(&self) -> Result<Vec<NodeId>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::InFlight(reply))
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?;
        rx.await.map_err(|err| CoreError::Internal(err.to_string()))
    }

    /// 停止 hub
    pub async fn shutdown(&self) -> Result<()> {
        self.cancel.cancel();
        self.tx
            .send(Command::Shutdown)
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))
    }

    /// 读取投递义务尚未释放的节点
    ///
    /// 事件流被跳帧后，调用方靠它重扫出没人投递的节点
    pub async fn undelivered_nodes(&self) -> Result<Vec<NodeId>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Undelivered(reply))
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?;
        rx.await.map_err(|err| CoreError::Internal(err.to_string()))
    }
}

/// 一次循环的唤醒来源
enum Wake<D: Domain> {
    Event(Event<D>),
    Command(Command<D>),
    Exit,
}

struct Hub<D: Domain> {
    tree: Tree<D>,
    jobs: Jobs<D>,
    persist: Arc<dyn Persist<D>>,
    bus: broadcast::Sender<TreeEvent<D>>,
    inbox: mpsc::Receiver<Command<D>>,
    reports: mpsc::Receiver<Event<D>>,
    cancel: CancellationToken,
}

impl<D: Domain> Hub<D> {
    async fn run(mut self) {
        loop {
            let wake = {
                tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => Wake::Exit,
                    report = self.reports.recv() => match report {
                        Some(event) => Wake::Event(event),
                        None => Wake::Exit,
                    },
                    command = self.inbox.recv() => match command {
                        Some(command) => Wake::Command(command),
                        None => Wake::Exit,
                    },
                }
            };
            match wake {
                Wake::Event(event) => self.apply(event).await,
                Wake::Command(Command::Event(event)) => self.apply(event).await,
                Wake::Command(Command::Recover(nodes, reply)) => {
                    for node in nodes.iter() {
                        self.tree.restore_node(
                            node.id,
                            node.parent,
                            node.children.clone(),
                            node.phase.clone(),
                            node.detached,
                            node.pending.clone(),
                        );
                    }
                    // 恢复 = 收敛判定 + 重入判定 + 崩溃策略，全部走归约器，
                    // 让 Persist / Terminated / Launch / Sealed 都落在正常路径上
                    let quiet_before = self.tree.is_quiescent();
                    for id in self.tree.all_node_ids() {
                        self.apply(Event::Recovered { node: id }).await;
                    }
                    // 本已安静的树不会再经历「由不安静变安静」，Sealed 在这里补发
                    if quiet_before {
                        let _ = self.bus.send(TreeEvent::Sealed);
                    }
                    let _ = reply.send(());
                }
                Wake::Command(Command::InFlight(reply)) => {
                    let _ = reply.send(self.tree.in_flight_nodes());
                }
                Wake::Command(Command::Undelivered(reply)) => {
                    let _ = reply.send(self.tree.undelivered_nodes());
                }
                Wake::Command(Command::Snapshot(reply)) => {
                    let _ = reply.send(self.snapshot());
                }
                Wake::Command(Command::Shutdown) | Wake::Exit => break,
            }
        }
        self.jobs.shutdown();
    }

    fn snapshot(&self) -> Vec<NodeSnapshot<D>> {
        self.tree
            .nodes()
            .map(|node| NodeSnapshot {
                id: node.id,
                parent: node.parent,
                children: node.children.clone(),
                phase: node.phase.clone(),
                detached: node.detached,
                pending: node.pending.clone(),
            })
            .collect()
    }

    /// 归约一个事件，按 reduce 的产出顺序依次执行副作用
    async fn apply(&mut self, event: Event<D>) {
        for effect in self.tree.reduce(event) {
            self.execute(effect).await;
        }
    }

    async fn execute(&mut self, effect: Effect<D>) {
        match effect {
            Effect::Publish(event) => {
                let _ = self.bus.send(event);
            }
            Effect::Persist { node } => {
                if let Some(phase) = self.tree.phase(node).cloned()
                    && let Err(err) = self.persist.save(node, &phase).await
                {
                    // 重试 / 降级 / 停机由适配层与部署决定，hub 的底线是不能静默
                    log::error!("[hub] persist failed for node {node}: {err}");
                }
            }
            Effect::Launch { node, job, input } => {
                self.jobs.launch(JobRequest { node, job, input });
            }
            Effect::Stop { node, job } => {
                self.jobs.stop(node, job);
            }
        }
    }
}

/// 启动一个 hub
pub fn spawn<D: Domain>(options: HubOptions<D>) -> HubHandle<D> {
    let (tx, rx) = mpsc::channel(256);
    let (bus, _) = broadcast::channel(options.bus_capacity);
    let cancel = CancellationToken::new();
    let (report_tx, report_rx) = mpsc::channel(REPORT_CAPACITY);

    let jobs = Jobs::new(options.behavior, report_tx, options.grace);
    let hub = Hub {
        tree: Tree::new(),
        jobs,
        persist: options.persist,
        bus: bus.clone(),
        inbox: rx,
        reports: report_rx,
        cancel: cancel.clone(),
    };

    tokio::spawn(hub.run());

    HubHandle { tx, bus, cancel }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::behavior::{Behavior, Ctx, Input, Outcome, SpawnRequest};
    use crate::agent::tree::domain::test_support::{
        TestDomain, TestError, TestOutput, TestProgress, TestSpec, TestSuspend,
    };
    use crate::agent::tree::event::Event;
    use crate::agent::tree::phase::{CancelOrigin, Phase, Terminal};
    use crate::agent::tree::view::TreeEvent;
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, Ordering};

    /// 测试用的自增节点 id，模拟领域侧的 id 分配
    static NEXT_ID: AtomicI64 = AtomicI64::new(100);
    fn next_id() -> NodeId {
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    }

    /// 按 Spec 分派的脚本化行为
    struct ScriptedBehavior;

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for ScriptedBehavior {
        async fn run(
            &self,
            ctx: Ctx<'_, TestDomain>,
            input: Input<TestDomain>,
        ) -> Outcome<TestDomain> {
            ctx.emit(TestProgress::Tick);
            match input {
                Input::Fresh(TestSpec::Noop) => Outcome::Completed(TestOutput::Ok),
                Input::Fresh(TestSpec::Children(n)) => Outcome::Spawn {
                    spawn: (0..n)
                        .map(|_| SpawnRequest {
                            id: next_id(),
                            // 子节点挂起，父节点才会稳定停在 WaitingChildren，
                            // 取消请求才能在没有任何 worker 在跑时确定性落地
                            spec: TestSpec::Suspend,
                            detached: false,
                        })
                        .collect(),
                },
                Input::Fresh(TestSpec::Suspend) => Outcome::Suspend(TestSuspend::Ask),
                Input::Fresh(TestSpec::Fail) => Outcome::Failed(TestError::Boom),
                Input::Children(_) | Input::External(_) => Outcome::Completed(TestOutput::Ok),
            }
        }
    }

    struct NoopPersist;

    #[async_trait::async_trait]
    impl Persist<TestDomain> for NoopPersist {
        async fn save(&self, _node: NodeId, _phase: &Phase<TestDomain>) -> Result<()> {
            Ok(())
        }
    }

    /// 一直等到取消信号才返回的 worker
    struct BlockingBehavior;

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for BlockingBehavior {
        async fn run(
            &self,
            ctx: Ctx<'_, TestDomain>,
            _input: Input<TestDomain>,
        ) -> Outcome<TestDomain> {
            ctx.cancel.cancelled().await;
            Outcome::Completed(TestOutput::Ok)
        }
    }

    /// 永远落库失败的适配器
    struct FailingPersist;

    #[async_trait::async_trait]
    impl Persist<TestDomain> for FailingPersist {
        async fn save(&self, node: NodeId, _phase: &Phase<TestDomain>) -> Result<()> {
            Err(CoreError::Internal(format!("store down for {node}")))
        }
    }

    #[tokio::test]
    async fn a_persist_failure_reports_without_stopping_the_tree() {
        let hub = spawn(HubOptions {
            behavior: Arc::new(ScriptedBehavior),
            persist: Arc::new(FailingPersist),
            grace: Duration::from_millis(50),
            bus_capacity: BUS_CAPACITY,
        });
        let mut rx = hub.subscribe();
        hub.send(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        })
        .await
        .expect("send");

        let seen = wait_until(&mut rx, |e| matches!(e, TreeEvent::Sealed)).await;
        assert!(
            seen.iter()
                .any(|e| matches!(e, TreeEvent::Terminated { node: 1, .. })),
            "落库失败必须被上报，但不得中断节点的迁移"
        );
    }

    /// 把每次保存的状态记下来供断言
    struct RecordingPersist {
        recorded: Arc<Mutex<Vec<(NodeId, Phase<TestDomain>)>>>,
    }

    #[async_trait::async_trait]
    impl Persist<TestDomain> for RecordingPersist {
        async fn save(&self, node: NodeId, phase: &Phase<TestDomain>) -> Result<()> {
            if let Ok(mut recorded) = self.recorded.lock() {
                recorded.push((node, phase.clone()));
            }
            Ok(())
        }
    }

    /// 把落库与广播按先后记进同一条日志
    struct OrderedPersist {
        log: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl Persist<TestDomain> for OrderedPersist {
        async fn save(&self, _node: NodeId, phase: &Phase<TestDomain>) -> Result<()> {
            // 让出一次，订阅方的观察点才有可能落在落库之前
            tokio::task::yield_now().await;
            let mark = match phase {
                Phase::Running => "saved running",
                Phase::Suspended(_) => "saved suspended",
                _ => "saved other",
            };
            if let Ok(mut log) = self.log.lock() {
                log.push(mark);
            }
            Ok(())
        }
    }

    fn hub() -> HubHandle<TestDomain> {
        spawn(HubOptions {
            behavior: Arc::new(ScriptedBehavior),
            persist: Arc::new(NoopPersist),
            grace: Duration::from_millis(50),
            bus_capacity: BUS_CAPACITY,
        })
    }

    async fn wait_until<F>(
        rx: &mut broadcast::Receiver<TreeEvent<TestDomain>>,
        mut done: F,
    ) -> Vec<TreeEvent<TestDomain>>
    where
        F: FnMut(&TreeEvent<TestDomain>) -> bool,
    {
        let mut seen = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = rx.recv().await.expect("event");
                let finished = done(&event);
                seen.push(event);
                if finished {
                    return;
                }
            }
        })
        .await
        .expect("timed out");
        seen
    }

    #[tokio::test]
    async fn a_single_node_runs_to_completion_before_sealing() {
        let hub = hub();
        let mut rx = hub.subscribe();
        hub.send(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        })
        .await
        .expect("send");

        let seen = wait_until(&mut rx, |e| matches!(e, TreeEvent::Sealed)).await;
        let terminated = seen
            .iter()
            .position(|e| matches!(e, TreeEvent::Terminated { .. }))
            .expect("必须收到终态事件");
        let sealed = seen
            .iter()
            .position(|e| matches!(e, TreeEvent::Sealed))
            .expect("seal");
        assert!(terminated < sealed, "终态事件必须先于关流");
    }

    #[tokio::test]
    async fn progress_reaches_the_bus() {
        let hub = hub();
        let mut rx = hub.subscribe();
        hub.send(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        })
        .await
        .expect("send");

        let seen = wait_until(
            &mut rx,
            |e| matches!(e, TreeEvent::Progress { node, .. } if *node == 1),
        )
        .await;
        assert!(!seen.is_empty());
    }

    #[tokio::test]
    async fn a_suspended_node_is_visible_in_the_snapshot() {
        let hub = hub();
        let mut rx = hub.subscribe();
        hub.send(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Suspend,
            detached: false,
        })
        .await
        .expect("send");
        wait_until(&mut rx, |e| matches!(e, TreeEvent::PhaseChanged { to, .. } if matches!(**to, Phase::Suspended(_)))).await;

        let snapshot = hub.snapshot().await.expect("snapshot");
        assert_eq!(snapshot.len(), 1);
        assert!(matches!(
            snapshot[0].phase,
            Phase::Suspended(TestSuspend::Ask)
        ));
    }

    #[tokio::test]
    async fn cancelling_a_parent_cascades_to_the_whole_subtree() {
        let hub = hub();
        let mut rx = hub.subscribe();
        hub.send(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Children(3),
            detached: false,
        })
        .await
        .expect("send");

        let seen = wait_until(&mut rx, |e| {
            matches!(e, TreeEvent::PhaseChanged { to, .. } if matches!(**to, Phase::WaitingChildren))
        })
        .await;
        let root = seen
            .iter()
            .find_map(|e| match e {
                TreeEvent::NodeCreated { node, parent: None } => Some(*node),
                _ => None,
            })
            .expect("root");

        let mut rx2 = hub.subscribe();
        hub.send(Event::CancelRequest {
            root,
            origin: CancelOrigin::External,
        })
        .await
        .expect("send");

        let seen = wait_until(&mut rx2, |e| matches!(e, TreeEvent::Sealed)).await;
        let cancelled: Vec<NodeId> = seen
            .iter()
            .filter_map(|e| match e {
                TreeEvent::Terminated { node, terminal } => {
                    matches!(terminal, Terminal::Cancelled(_)).then_some(*node)
                }
                _ => None,
            })
            .collect();
        assert!(cancelled.contains(&root), "根节点必须被取消");
        assert_eq!(cancelled.len(), 4, "整棵子树都要被取消: {cancelled:?}");
    }

    #[tokio::test]
    async fn a_stop_effect_reaches_the_worker() {
        let hub = spawn(HubOptions {
            behavior: Arc::new(BlockingBehavior),
            persist: Arc::new(NoopPersist),
            grace: Duration::from_millis(50),
            bus_capacity: BUS_CAPACITY,
        });
        let mut rx = hub.subscribe();
        hub.send(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        })
        .await
        .expect("send");
        wait_until(&mut rx, |e| {
            matches!(e, TreeEvent::NodeCreated { node: 1, .. })
        })
        .await;

        let snapshot = hub.snapshot().await.expect("snapshot");
        assert!(
            snapshot
                .iter()
                .any(|node| node.id == 1 && matches!(node.phase, Phase::Running)),
            "取消前 worker 必须在跑，否则测不到 Stop"
        );

        let mut rx2 = hub.subscribe();
        hub.send(Event::CancelRequest {
            root: 1,
            origin: CancelOrigin::External,
        })
        .await
        .expect("send");

        let seen = wait_until(&mut rx2, |e| matches!(e, TreeEvent::Sealed)).await;
        assert!(
            seen.iter().any(|e| matches!(
                e,
                TreeEvent::Terminated { node: 1, terminal }
                    if matches!(terminal, Terminal::Cancelled(_))
            )),
            "在跑的节点必须在 worker 确认停止后落 Cancelled"
        );
    }

    #[tokio::test]
    async fn persist_is_dispatched_for_the_running_and_done_phases() {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let hub = spawn(HubOptions {
            behavior: Arc::new(ScriptedBehavior),
            persist: Arc::new(RecordingPersist {
                recorded: recorded.clone(),
            }),
            grace: Duration::from_millis(50),
            bus_capacity: BUS_CAPACITY,
        });
        let mut rx = hub.subscribe();
        hub.send(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        })
        .await
        .expect("send");
        wait_until(&mut rx, |e| matches!(e, TreeEvent::Sealed)).await;

        let recorded = recorded.lock().expect("lock");
        assert!(
            recorded
                .iter()
                .any(|(node, phase)| *node == 1 && matches!(phase, Phase::Running)),
            "启动必须落 Running"
        );
        assert!(
            recorded
                .iter()
                .any(|(node, phase)| *node == 1 && matches!(phase, Phase::Done(TestOutput::Ok))),
            "完成必须落 Done"
        );
    }

    #[tokio::test]
    async fn a_suspension_is_durable_before_it_is_published() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let hub = spawn(HubOptions {
            behavior: Arc::new(ScriptedBehavior),
            persist: Arc::new(OrderedPersist { log: log.clone() }),
            grace: Duration::from_millis(50),
            bus_capacity: BUS_CAPACITY,
        });
        let mut rx = hub.subscribe();
        let watcher = {
            let log = log.clone();
            tokio::spawn(async move {
                while let Ok(event) = rx.recv().await {
                    if matches!(event, TreeEvent::PhaseChanged { to, .. } if matches!(*to, Phase::Suspended(_)))
                    {
                        if let Ok(mut log) = log.lock() {
                            log.push("published suspended");
                        }
                        break;
                    }
                }
            })
        };

        hub.send(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Suspend,
            detached: false,
        })
        .await
        .expect("send");

        tokio::time::timeout(Duration::from_secs(5), watcher)
            .await
            .expect("timed out")
            .expect("join");

        let log = log.lock().expect("lock");
        let saved = log
            .iter()
            .position(|mark| *mark == "saved suspended")
            .expect("挂起相必须先落库");
        let published = log
            .iter()
            .position(|mark| *mark == "published suspended")
            .expect("挂起相必须广播");
        assert!(saved < published, "落库必须先于广播: {log:?}");
    }

    #[tokio::test]
    async fn recovery_keeps_resting_phases_and_converges_in_flight_ones() {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let hub = spawn(HubOptions {
            behavior: Arc::new(ScriptedBehavior),
            persist: Arc::new(RecordingPersist {
                recorded: recorded.clone(),
            }),
            grace: Duration::from_millis(50),
            bus_capacity: BUS_CAPACITY,
        });
        let mut rx = hub.subscribe();
        hub.recover(vec![
            NodeSnapshot {
                id: 1,
                parent: None,
                children: vec![2, 3],
                phase: Phase::Running,
                detached: false,
                pending: None,
            },
            NodeSnapshot {
                id: 2,
                parent: Some(1),
                children: vec![],
                phase: Phase::Suspended(TestSuspend::Ask),
                detached: false,
                pending: None,
            },
            NodeSnapshot {
                id: 3,
                parent: Some(1),
                children: vec![],
                phase: Phase::Cancelling(CancelOrigin::Ancestor(1)),
                detached: false,
                pending: None,
            },
        ])
        .await
        .expect("recover");

        let snapshot = hub.snapshot().await.expect("snapshot");
        let node_of = |id: NodeId| snapshot.iter().find(|n| n.id == id).expect("node");
        let phase_of = |id: NodeId| node_of(id).phase.clone();

        assert_eq!(node_of(1).children, vec![2, 3], "子节点必须随快照重建");
        assert_eq!(node_of(2).parent, Some(1), "父指针必须随快照重建");
        assert_eq!(node_of(3).parent, Some(1));
        assert!(!node_of(1).detached);

        assert!(
            matches!(phase_of(2), Phase::Suspended(TestSuspend::Ask)),
            "静息相必须无损保留"
        );
        assert!(
            matches!(phase_of(3), Phase::Cancelled(CancelOrigin::Ancestor(1))),
            "Cancelling 依赖一个永不到来的确认，必须收敛为 Cancelled 并保留原始来源"
        );
        assert!(
            matches!(phase_of(1), Phase::Aborted),
            "重建出的 Running 节点没有 worker，必须收敛为 Aborted"
        );
        assert!(
            hub.in_flight_nodes().await.expect("in flight").is_empty(),
            "恢复后不得残留等不到回报的在途节点"
        );

        let seen = wait_until(&mut rx, |e| {
            matches!(e, TreeEvent::Terminated { node: 3, .. })
        })
        .await;
        assert!(
            seen.iter().any(|e| matches!(
                e,
                TreeEvent::Terminated { node: 3, terminal }
                    if matches!(terminal, Terminal::Cancelled(CancelOrigin::Ancestor(1)))
            )),
            "收敛必须广播带原始来源的终态事件"
        );

        let recorded = recorded.lock().expect("lock");
        assert!(
            recorded.iter().any(|(node, phase)| *node == 3
                && matches!(phase, Phase::Cancelled(CancelOrigin::Ancestor(1)))),
            "收敛必须落库，否则持久化侧仍显示 Cancelling"
        );
    }

    #[tokio::test]
    async fn a_recovered_quiet_tree_publishes_sealed() {
        let hub = hub();
        let mut rx = hub.subscribe();
        hub.recover(vec![NodeSnapshot {
            id: 7,
            parent: None,
            children: vec![],
            phase: Phase::Done(TestOutput::Ok),
            detached: false,
            pending: None,
        }])
        .await
        .expect("recover");

        let seen = wait_until(&mut rx, |e| matches!(e, TreeEvent::Sealed)).await;
        assert!(
            seen.iter().any(|e| matches!(e, TreeEvent::Sealed)),
            "安静的恢复树必须自行宣告 Sealed，否则等待方永远等不到"
        );
    }

    /// 一跑就崩的 worker
    struct PanicBehavior;

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for PanicBehavior {
        async fn run(
            &self,
            _ctx: Ctx<'_, TestDomain>,
            _input: Input<TestDomain>,
        ) -> Outcome<TestDomain> {
            panic!("boom")
        }
    }

    #[tokio::test]
    async fn a_panicking_worker_terminates_its_node_as_aborted_and_seals() {
        let hub = spawn(HubOptions {
            behavior: Arc::new(PanicBehavior),
            persist: Arc::new(NoopPersist),
            grace: Duration::from_millis(50),
            bus_capacity: BUS_CAPACITY,
        });
        let mut rx = hub.subscribe();
        hub.send(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        })
        .await
        .expect("send");

        let seen = wait_until(&mut rx, |e| matches!(e, TreeEvent::Sealed)).await;
        assert!(
            seen.iter().any(|e| matches!(
                e,
                TreeEvent::Terminated {
                    node: 1,
                    terminal: Terminal::Aborted
                }
            )),
            "worker 崩了必须让节点落 Aborted，而不是把树卡在 Running"
        );
        let snapshot = hub.snapshot().await.expect("snapshot");
        assert!(snapshot[0].phase.is_terminal(), "节点必须落终态");
    }

    #[tokio::test]
    async fn recovery_reenters_a_parent_whose_children_are_all_terminal() {
        let hub = hub();
        let mut rx = hub.subscribe();
        hub.recover(vec![
            NodeSnapshot {
                id: 1,
                parent: None,
                children: vec![2],
                phase: Phase::WaitingChildren,
                detached: false,
                pending: None,
            },
            NodeSnapshot {
                id: 2,
                parent: Some(1),
                children: vec![],
                phase: Phase::Done(TestOutput::Ok),
                detached: false,
                pending: None,
            },
        ])
        .await
        .expect("recover");

        let seen = wait_until(&mut rx, |e| {
            matches!(e, TreeEvent::PhaseChanged { node: 1, to } if matches!(**to, Phase::Running))
        })
        .await;
        assert!(
            seen.iter()
                .any(|e| matches!(e, TreeEvent::PhaseChanged { node: 1, .. })),
            "子节点已全部终态的父节点必须被重入，否则永久停靠"
        );

        wait_until(&mut rx, |e| matches!(e, TreeEvent::Sealed)).await;
        let snapshot = hub.snapshot().await.expect("snapshot");
        assert!(
            snapshot.iter().all(|node| node.phase.is_terminal()),
            "重入后的树必须能收敛"
        );
    }

    #[tokio::test]
    async fn recovery_disposes_of_a_restored_in_flight_node() {
        let hub = hub();
        let mut rx = hub.subscribe();
        hub.recover(vec![NodeSnapshot {
            id: 1,
            parent: None,
            children: vec![],
            phase: Phase::Running,
            detached: false,
            pending: None,
        }])
        .await
        .expect("recover");

        let seen = wait_until(&mut rx, |e| matches!(e, TreeEvent::Sealed)).await;
        assert!(
            seen.iter().any(|e| matches!(
                e,
                TreeEvent::Terminated {
                    node: 1,
                    terminal: Terminal::Aborted
                }
            )),
            "重建出的 Running 节点没有 worker 可确认，必须能走到终态"
        );
    }

    #[tokio::test]
    async fn recovery_disposes_of_a_detached_in_flight_node_without_owing_a_delivery() {
        let hub = hub();
        let mut rx = hub.subscribe();
        hub.recover(vec![NodeSnapshot {
            id: 1,
            parent: None,
            children: vec![],
            phase: Phase::Running,
            detached: true,
            pending: None,
        }])
        .await
        .expect("recover");

        let seen = wait_until(&mut rx, |e| matches!(e, TreeEvent::Sealed)).await;
        assert!(
            seen.iter().any(|e| matches!(
                e,
                TreeEvent::Terminated {
                    node: 1,
                    terminal: Terminal::Aborted
                }
            )),
            "重建出的 detached 在途节点同样没有 worker 可确认"
        );
        assert!(
            seen.iter().any(|e| matches!(e, TreeEvent::Sealed)),
            "恢复收敛出的终态不得留下没人能释放的投递义务，否则该话题的流永久挂着"
        );
    }

    #[tokio::test]
    async fn dropping_every_handle_ends_the_hub() {
        let hub = hub();
        let mut rx = hub.subscribe();
        drop(hub);

        let outcome = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("最后一个句柄放手后 hub 必须退出");
        assert!(
            matches!(outcome, Err(broadcast::error::RecvError::Closed)),
            "hub 退出时事件流必须关闭"
        );
    }

    /// 事实驱动程序：根据输入决定派生、挂起还是完成
    struct TreeBehavior;

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for TreeBehavior {
        async fn run(
            &self,
            _ctx: Ctx<'_, TestDomain>,
            input: Input<TestDomain>,
        ) -> Outcome<TestDomain> {
            match input {
                Input::Fresh(TestSpec::Children(n)) => Outcome::Spawn {
                    spawn: (0..n)
                        .map(|_| SpawnRequest {
                            id: next_id(),
                            // 子节点挂起，父节点才会稳定停在 WaitingChildren
                            spec: TestSpec::Suspend,
                            detached: false,
                        })
                        .collect(),
                },
                Input::Children(_) => Outcome::Completed(TestOutput::Ok),
                Input::Fresh(TestSpec::Noop) => Outcome::Completed(TestOutput::Ok),
                Input::Fresh(TestSpec::Suspend) => Outcome::Suspend(TestSuspend::Ask),
                Input::Fresh(TestSpec::Fail) => Outcome::Failed(TestError::Boom),
                Input::External(_) => Outcome::Completed(TestOutput::Ok),
            }
        }
    }

    #[tokio::test]
    async fn cancelling_a_parent_with_children_loses_no_events_and_seals_once() {
        let hub = spawn(HubOptions {
            behavior: Arc::new(TreeBehavior),
            persist: Arc::new(NoopPersist),
            grace: Duration::from_millis(50),
            bus_capacity: BUS_CAPACITY,
        });
        let mut rx = hub.subscribe();
        // root + 3 个子节点，共 4 个节点
        hub.send(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Children(3),
            detached: false,
        })
        .await
        .expect("send");

        let seen = wait_until(&mut rx, |e| {
            matches!(e, TreeEvent::PhaseChanged { to, .. } if matches!(**to, Phase::WaitingChildren))
        })
        .await;
        let root = seen
            .iter()
            .find_map(|e| match e {
                TreeEvent::NodeCreated { node, parent: None } => Some(*node),
                _ => None,
            })
            .expect("root");

        let snapshot = hub.snapshot().await.expect("snapshot");
        assert_eq!(snapshot.len(), 4, "根 + 3 个子节点");

        let mut rx2 = hub.subscribe();
        hub.send(Event::CancelRequest {
            root,
            origin: CancelOrigin::External,
        })
        .await
        .expect("send");

        let mut seen = wait_until(&mut rx2, |e| matches!(e, TreeEvent::Sealed)).await;
        // 第一个 Sealed 之后再排空一次：逐节点多发的 Sealed 会落在这次排空里
        let _ = hub.snapshot().await.expect("snapshot");
        while let Ok(event) = rx2.try_recv() {
            seen.push(event);
        }

        let sealed = seen
            .iter()
            .filter(|e| matches!(e, TreeEvent::Sealed))
            .count();
        assert_eq!(sealed, 1, "Sealed 只能发一次");

        let cancelled: BTreeSet<NodeId> = seen
            .iter()
            .filter_map(|e| match e {
                TreeEvent::Terminated { node, terminal }
                    if matches!(terminal, Terminal::Cancelled(_)) =>
                {
                    Some(*node)
                }
                _ => None,
            })
            .collect();
        let expected: BTreeSet<NodeId> = snapshot.iter().map(|n| n.id).collect();
        assert_eq!(cancelled, expected, "每个节点都必须收到取消终态，一个不丢");
    }

    #[tokio::test]
    async fn a_cancelled_tree_stays_cancelled_after_a_stray_report() {
        let hub = hub();
        let mut rx = hub.subscribe();
        hub.send(Event::Spawn {
            id: 1,
            parent: None,
            spec: TestSpec::Noop,
            detached: false,
        })
        .await
        .expect("send");
        let seen = wait_until(&mut rx, |e| matches!(e, TreeEvent::Sealed)).await;
        let node = seen
            .iter()
            .find_map(|e| match e {
                TreeEvent::NodeCreated { node, .. } => Some(*node),
                _ => None,
            })
            .expect("node");

        // 终态之后到达的杂乱回报不得改变任何状态
        hub.send(Event::Stopped { node, job: 1 })
            .await
            .expect("send");
        hub.send(Event::Progress {
            node,
            job: 1,
            chunk: TestProgress::Tick,
        })
        .await
        .expect("send");

        let snapshot = hub.snapshot().await.expect("snapshot");
        assert!(snapshot[0].phase.is_terminal());
        // snapshot 是屏障：此前入队的回报都已被处理，此后缓冲里不应再有任何事件
        assert!(
            matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)),
            "终态之后的杂乱回报不得产出任何对外事件"
        );
    }

    #[tokio::test]
    async fn recovery_clears_every_delivery_obligation() {
        let hub = hub();
        let mut rx = hub.subscribe();
        hub.recover(vec![NodeSnapshot {
            id: 1,
            parent: None,
            children: vec![],
            phase: Phase::Done(TestOutput::Ok),
            detached: true,
            pending: None,
        }])
        .await
        .expect("recover");

        let seen = wait_until(&mut rx, |e| matches!(e, TreeEvent::Sealed)).await;
        assert!(
            seen.iter().any(|e| matches!(e, TreeEvent::Sealed)),
            "恢复出的终态 detached 节点不得带着投递义务，否则该话题的流永不关"
        );
        assert!(
            hub.in_flight_nodes().await.expect("in flight").is_empty(),
            "恢复后不得残留等不到回报的在途节点"
        );
        assert!(
            hub.undelivered_nodes()
                .await
                .expect("undelivered")
                .is_empty(),
            "恢复后不得残留投递义务"
        );
    }

    #[tokio::test]
    async fn a_hub_reports_itself_stopped_only_after_shutdown() {
        let hub = hub();
        assert!(!hub.is_stopped(), "刚起来的 hub 不是停止态");
        hub.shutdown().await.expect("shutdown");
        assert!(hub.is_stopped(), "shutdown 之后必须能观察到停止");
    }
}
