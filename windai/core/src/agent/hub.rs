use super::behavior::Behavior;
use super::domain::{AgentDomain, AgentSpec};
use super::events::{
    AgentCancel, AgentCreate, AgentNodeView, AgentSpawn, AgentTerminate, AgentTree,
    CORE_AGENT_CANCEL, CORE_AGENT_CREATE, CORE_AGENT_SPAWN, CORE_AGENT_TERMINATE, directory,
};
use super::jobs::{JobRequest, Jobs, REPORT_CAPACITY};
use super::phase_codec::encode;
use super::translate::Translated;
use super::tree::event::{Effect, Event};
use super::tree::phase::{CancelOrigin, Phase};
use super::tree::view::TreeEvent;
use super::tree::{Domain, NodeId, Tree};
use crate::error::{CoreError, Result};
use crate::models::AgentMode;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use wind_event::context::DispatchCx;
use wind_event::def::{Directory, EventDef, EventMeta};
use wind_event::dispatch::Dispatcher;
use wind_event::envelope::{Origin, Scope, Source};

pub const BUS_CAPACITY: usize = 1024;

/// 派发翻译出的观测事件时，一次顶层操作的预算
const OBSERVE_BUDGET: Duration = Duration::from_millis(50);

pub type Translator<D> = fn(&TreeEvent<D>) -> Vec<Translated>;

#[async_trait::async_trait]
pub trait Persist<D: Domain>: Send + Sync {
    async fn save(&self, node: NodeId, phase: &Phase<D>) -> Result<()>;
}

pub struct HubOptions<D: Domain> {
    pub behavior: Arc<dyn Behavior<D>>,
    pub persist: Arc<dyn Persist<D>>,
    pub grace: Duration,
    pub bus_capacity: usize,
    pub dispatcher: Arc<Dispatcher>,
    pub topic_id: i64,
    pub translator: Translator<D>,
}

/// 树节点快照
#[derive(Debug, Clone)]
pub struct NodeSnapshot<D: Domain> {
    pub id: NodeId,
    pub parent: Option<NodeId>,
    pub children: Vec<NodeId>,
    pub phase: Phase<D>,
    pub detached: bool,
    pub pending: Vec<D::Spec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitOutcome {
    Queued,
    Launched,
    Dropped,
}

enum Command<D: Domain> {
    Event(Event<D>),
    Spawn {
        id: NodeId,
        parent: Option<NodeId>,
        spec: D::Spec,
        detached: bool,
        reply: oneshot::Sender<bool>,
    },
    Submit {
        node: NodeId,
        spec: D::Spec,
        reply: oneshot::Sender<SubmitOutcome>,
    },
    Recover(Vec<NodeSnapshot<D>>, oneshot::Sender<()>),
    InFlight(oneshot::Sender<Vec<NodeId>>),
    Undelivered(oneshot::Sender<Vec<NodeId>>),
    Snapshot(oneshot::Sender<Vec<NodeSnapshot<D>>>),
    Shutdown,
}

/// Agent 对外 hub 句柄
#[derive(Clone)]
pub struct HubHandle<D: Domain> {
    tx: mpsc::Sender<Command<D>>,
    bus: broadcast::Sender<TreeEvent<D>>,
    cancel: CancellationToken,
    dispatcher: Arc<Dispatcher>,
}

impl<D: Domain> HubHandle<D> {
    pub fn is_stopped(&self) -> bool {
        self.cancel.is_cancelled()
    }

    pub(crate) async fn send(&self, event: Event<D>) -> Result<()> {
        self.tx
            .send(Command::Event(event))
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TreeEvent<D>> {
        self.bus.subscribe()
    }

    pub async fn wait_stopped(&self) {
        self.cancel.cancelled().await;
    }

    pub async fn deliver(&self, node: NodeId) -> Result<()> {
        self.send(Event::Deliver { node }).await
    }

    /// 当前节点状态树快照
    pub async fn snapshot(&self) -> Result<Vec<NodeSnapshot<D>>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Snapshot(reply))
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?;
        rx.await.map_err(|err| CoreError::Internal(err.to_string()))
    }

    pub async fn recover(&self, nodes: Vec<NodeSnapshot<D>>) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Recover(nodes, reply))
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?;
        rx.await.map_err(|err| CoreError::Internal(err.to_string()))
    }

    pub async fn in_flight_nodes(&self) -> Result<Vec<NodeId>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::InFlight(reply))
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?;
        rx.await.map_err(|err| CoreError::Internal(err.to_string()))
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.cancel.cancel();
        self.tx
            .send(Command::Shutdown)
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))
    }

    pub async fn undelivered_nodes(&self) -> Result<Vec<NodeId>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Undelivered(reply))
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?;
        rx.await.map_err(|err| CoreError::Internal(err.to_string()))
    }

    async fn intercept<P>(&self, def: &EventDef<P>, cx: &DispatchCx, payload: P) -> P
    where
        P: Serialize + DeserializeOwned + Send + Sync,
    {
        self.dispatcher.decide(def, cx, payload).await
    }

    async fn notify<P>(&self, cx: &DispatchCx, def: &EventDef<P>, payload: P)
    where
        P: Serialize + Send + Sync,
    {
        self.dispatcher.notify(def, cx, payload).await;
    }
}

impl HubHandle<AgentDomain> {
    pub async fn intercept_create(&self, cx: &DispatchCx, payload: AgentCreate) -> AgentCreate {
        self.intercept(&CORE_AGENT_CREATE, cx, payload).await
    }

    pub async fn spawn_at(
        &self,
        cx: &DispatchCx,
        id: NodeId,
        payload: &AgentCreate,
    ) -> Result<NodeId> {
        let spec = AgentSpec {
            instance_id: id,
            message_id: None,
            pending_id: None,
            turn_id: None,
        };
        self.spawn_at_with_spec(cx, id, payload, spec).await
    }

    /// 开始一个任务
    pub async fn spawn_at_with_spec(
        &self,
        cx: &DispatchCx,
        id: NodeId,
        payload: &AgentCreate,
        spec: AgentSpec,
    ) -> Result<NodeId> {
        let (reply, inserted) = oneshot::channel();
        self.tx
            .send(Command::Spawn {
                id,
                parent: payload.parent_id,
                spec,
                detached: payload.mode.as_str() == AgentMode::Background.as_ref(),
                reply,
            })
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?;

        if !inserted
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?
        {
            return Err(CoreError::Internal(format!(
                "the tree rejected the new node {id}: its id is taken or its parent is unknown"
            )));
        }

        self.notify(
            cx,
            &CORE_AGENT_SPAWN,
            AgentSpawn {
                instance_id: id,
                parent_id: payload.parent_id,
                mode: payload.mode.clone(),
            },
        )
        .await;
        Ok(id)
    }

    /// 将任务加入 pending 队列
    pub async fn submit(&self, node: NodeId, spec: AgentSpec) -> Result<SubmitOutcome> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Submit { node, spec, reply })
            .await
            .map_err(|err| CoreError::Internal(err.to_string()))?;
        rx.await.map_err(|err| CoreError::Internal(err.to_string()))
    }

    pub async fn terminate(
        &self,
        cx: &DispatchCx,
        instance_id: NodeId,
        reason: &str,
    ) -> Result<()> {
        let payload = self
            .intercept(
                &CORE_AGENT_TERMINATE,
                cx,
                AgentTerminate {
                    instance_id,
                    reason: reason.to_string(),
                },
            )
            .await;
        self.request_cancel(payload.instance_id, CancelOrigin::External)
            .await
    }

    pub async fn cancel(
        &self,
        cx: &DispatchCx,
        instance_id: NodeId,
        origin: CancelOrigin,
    ) -> Result<()> {
        let payload = self
            .intercept(
                &CORE_AGENT_CANCEL,
                cx,
                AgentCancel {
                    instance_id,
                    origin: serde_json::to_value(origin)
                        .map_err(|err| CoreError::Bus(err.to_string()))?,
                },
            )
            .await;
        let origin: CancelOrigin = serde_json::from_value(payload.origin)
            .map_err(|err| CoreError::Bus(err.to_string()))?;
        self.request_cancel(payload.instance_id, origin).await
    }

    async fn request_cancel(&self, instance_id: NodeId, origin: CancelOrigin) -> Result<()> {
        self.send(Event::CancelRequest {
            root: instance_id,
            origin,
        })
        .await
    }

    pub async fn tree_snapshot(&self) -> Result<AgentTree> {
        let nodes = self.snapshot().await?;
        Ok(AgentTree {
            nodes: nodes.iter().filter_map(node_view).collect(),
        })
    }
}

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
    dispatcher: Arc<Dispatcher>,
    topic_id: i64,
    translator: Translator<D>,
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
                Wake::Event(event) => {
                    self.apply(event).await;
                }
                Wake::Command(Command::Event(event)) => {
                    self.apply(event).await;
                }
                Wake::Command(Command::Spawn {
                    id,
                    parent,
                    spec,
                    detached,
                    reply,
                }) => {
                    let created = self
                        .apply(Event::Spawn {
                            id,
                            parent,
                            spec,
                            detached,
                        })
                        .await;
                    let _ = reply.send(created);
                }
                Wake::Command(Command::Submit { node, spec, reply }) => {
                    let outcome = self.submit(node, spec).await;
                    let _ = reply.send(outcome);
                }
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
                    let quiet_before = self.tree.is_quiescent();
                    for id in self.tree.all_node_ids() {
                        self.apply(Event::Recovered { node: id }).await;
                    }
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

    async fn submit(&mut self, node: NodeId, spec: D::Spec) -> SubmitOutcome {
        let (accepted, idle) = match self.tree.node(node) {
            None => {
                log::warn!("[tree] submit for an unknown node dropped: {node}");
                (false, false)
            }
            Some(current) if matches!(current.phase, Phase::Cancelling(_)) => {
                log::warn!("[tree] submit for a cancelling node dropped: {node}");
                (false, false)
            }
            Some(current) => (
                true,
                current.phase.is_terminal() && current.pending.is_empty(),
            ),
        };
        if !accepted {
            return SubmitOutcome::Dropped;
        }
        self.apply(Event::Enqueue { node, spec }).await;
        if idle {
            SubmitOutcome::Launched
        } else {
            SubmitOutcome::Queued
        }
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
                pending: node.pending.iter().cloned().collect(),
            })
            .collect()
    }

    async fn apply(&mut self, event: Event<D>) -> bool {
        let mut created = false;
        for effect in self.tree.reduce(event) {
            if matches!(effect, Effect::Publish(TreeEvent::NodeCreated { .. })) {
                created = true;
            }
            self.execute(effect).await;
        }
        created
    }

    async fn observe(&self, event: &TreeEvent<D>) {
        for item in (self.translator)(event) {
            let Some(def) = observe_def(item.name) else {
                log::error!("[hub] translated event {} has no definition", item.name);
                continue;
            };
            self.dispatcher
                .notify(&def, &self.observe_cx(), item.payload)
                .await;
        }
    }

    fn observe_cx(&self) -> DispatchCx {
        DispatchCx::root(
            Origin::Internal,
            Scope::Topic {
                topic_id: self.topic_id,
            },
            Source::core("agent"),
            OBSERVE_BUDGET,
        )
    }

    async fn execute(&mut self, effect: Effect<D>) {
        match effect {
            Effect::Publish(event) => {
                self.observe(&event).await;
                let _ = self.bus.send(event);
            }
            Effect::Persist { node } => {
                if let Some(phase) = self.tree.phase(node).cloned()
                    && let Err(err) = self.persist.save(node, &phase).await
                {
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
        dispatcher: Arc::clone(&options.dispatcher),
        topic_id: options.topic_id,
        translator: options.translator,
    };

    tokio::spawn(hub.run());

    HubHandle {
        tx,
        bus,
        cancel,
        dispatcher: options.dispatcher,
    }
}

fn catalog() -> &'static Directory {
    static CATALOG: OnceLock<Directory> = OnceLock::new();
    CATALOG.get_or_init(directory)
}

fn observe_def(name: &str) -> Option<EventDef<serde_json::Value>> {
    let meta = catalog().get(name)?;
    meta.is_observe().then(|| erased(meta))
}

fn node_view(node: &NodeSnapshot<AgentDomain>) -> Option<AgentNodeView> {
    let stored = match encode(&node.phase) {
        Ok(stored) => stored,
        Err(error) => {
            log::error!("[hub] cannot encode the phase of {}: {error}", node.id);
            return None;
        }
    };
    Some(AgentNodeView {
        instance_id: node.id,
        parent_id: node.parent,
        detached: node.detached,
        phase: stored.name,
        phase_payload: stored.payload,
    })
}

fn erased(meta: &EventMeta) -> EventDef<serde_json::Value> {
    EventDef::new(meta.name, meta.shape, meta.log, meta.mount, meta.schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::behavior::{Behavior, Ctx, Input, Outcome, SpawnRequest};
    use crate::agent::domain::{AgentFailure, AgentOutcome, SuspendedWait};
    use crate::agent::translate::translate;
    use crate::agent::tree::domain::test_support::{
        TestDomain, TestError, TestOutput, TestSpec, TestSuspend,
    };
    use crate::agent::tree::event::Event;
    use crate::agent::tree::phase::{CancelOrigin, Phase, Terminal};
    use crate::agent::tree::view::TreeEvent;
    use std::collections::BTreeSet;
    use std::result::Result as StdResult;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, Ordering};
    use wind_event::def::Directory;
    use wind_event::dispatch::Limits;
    use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
    use wind_event::subscription::{EnabledSet, Subscription};

    /// 测试用的自增节点 id，模拟领域侧的 id 分配
    static NEXT_ID: AtomicI64 = AtomicI64::new(100);
    fn next_id() -> NodeId {
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    }

    /// 测试域不产生对外观测
    fn no_events<D: Domain>(_event: &TreeEvent<D>) -> Vec<Translated> {
        Vec::new()
    }

    /// 全部 mod 都不启用
    struct NoEnablement;

    impl ModEnablement for NoEnablement {
        fn enabled(&self, _scope: &Scope) -> EnabledSet {
            EnabledSet::none()
        }
    }

    /// 不会被调用的宿主
    struct NoHost;

    #[async_trait::async_trait]
    impl ModHost for NoHost {
        async fn call(
            &self,
            _sub: &Subscription,
            _payload: serde_json::Value,
            _cx: CallCx,
        ) -> StdResult<Option<serde_json::Value>, HostError> {
            Err(HostError::Internal("the test domain emits nothing".into()))
        }
    }

    /// 一组测试用的启动参数，树事件不外发
    fn options(
        behavior: Arc<dyn Behavior<TestDomain>>,
        persist: Arc<dyn Persist<TestDomain>>,
    ) -> HubOptions<TestDomain> {
        HubOptions {
            behavior,
            persist,
            grace: Duration::from_millis(50),
            bus_capacity: BUS_CAPACITY,
            dispatcher: Arc::new(Dispatcher::new(
                Directory::default(),
                Arc::new(NoEnablement),
                Arc::new(NoHost),
                Limits::default(),
            )),
            topic_id: 1,
            translator: no_events::<TestDomain>,
        }
    }

    /// 按 Spec 分派的脚本化行为
    struct ScriptedBehavior;

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for ScriptedBehavior {
        async fn run(&self, _ctx: Ctx, input: Input<TestDomain>) -> Outcome<TestDomain> {
            match input {
                Input::Fresh(TestSpec::Noop) => Outcome::Completed(TestOutput::Ok),
                Input::Fresh(TestSpec::Children(n)) => Outcome::Spawn {
                    spawn: (0..n)
                        .map(|_| SpawnRequest {
                            id: next_id(),
                            // 子节点挂起，父节点停在 WaitingChildren
                            spec: TestSpec::Suspend,
                            detached: false,
                        })
                        .collect(),
                },
                Input::Fresh(TestSpec::Suspend) => Outcome::Suspend(TestSuspend::Ask),
                Input::Fresh(TestSpec::Fail) => Outcome::Failed(TestError::Boom),
                Input::Children(_) | Input::External(_) | Input::Idle(_) => {
                    Outcome::Completed(TestOutput::Ok)
                }
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
        async fn run(&self, ctx: Ctx, _input: Input<TestDomain>) -> Outcome<TestDomain> {
            ctx.cancel.cancelled().await;
            Outcome::Completed(TestOutput::Ok)
        }
    }

    /// 永远保存失败的适配器
    struct FailingPersist;

    #[async_trait::async_trait]
    impl Persist<TestDomain> for FailingPersist {
        async fn save(&self, node: NodeId, _phase: &Phase<TestDomain>) -> Result<()> {
            Err(CoreError::Internal(format!("store down for {node}")))
        }
    }

    #[tokio::test]
    async fn a_persist_failure_reports_without_stopping_the_tree() {
        let hub = spawn(options(
            Arc::new(ScriptedBehavior),
            Arc::new(FailingPersist),
        ));
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
            "a persist failure must be reported without interrupting the node's transition"
        );
    }

    /// 一次保存下来的节点与 Phase
    type SavedState = (NodeId, Phase<TestDomain>);

    /// 把每次保存的状态记下来供断言
    struct RecordingPersist {
        /// 已保存的节点与 Phase，按保存顺序
        recorded: Arc<Mutex<Vec<SavedState>>>,
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

    /// 把保存与广播按先后记进同一条日志
    struct OrderedPersist {
        /// 保存与广播的先后标记
        log: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl Persist<TestDomain> for OrderedPersist {
        async fn save(&self, _node: NodeId, phase: &Phase<TestDomain>) -> Result<()> {
            // 让出一次执行权，订阅方的观察点才有可能落在保存之前
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
        spawn(options(Arc::new(ScriptedBehavior), Arc::new(NoopPersist)))
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
            .expect("receive terminal event");
        let sealed = seen
            .iter()
            .position(|e| matches!(e, TreeEvent::Sealed))
            .expect("seal");
        assert!(
            terminated < sealed,
            "the terminal event must precede the stream close"
        );
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
        assert!(cancelled.contains(&root), "the root node must be cancelled");
        assert_eq!(
            cancelled.len(),
            4,
            "the whole subtree must be cancelled: {cancelled:?}"
        );
    }

    #[tokio::test]
    async fn a_stop_effect_reaches_the_worker() {
        let hub = spawn(options(Arc::new(BlockingBehavior), Arc::new(NoopPersist)));
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
            "the worker must be running before cancellation, otherwise Stop is not exercised"
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
            "a running node must land on Cancelled after the worker confirms it stopped"
        );
    }

    #[tokio::test]
    async fn persist_is_dispatched_for_the_running_and_done_phases() {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let hub = spawn(options(
            Arc::new(ScriptedBehavior),
            Arc::new(RecordingPersist {
                recorded: recorded.clone(),
            }),
        ));
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
            "startup must persist Running"
        );
        assert!(
            recorded
                .iter()
                .any(|(node, phase)| *node == 1 && matches!(phase, Phase::Done(TestOutput::Ok))),
            "completion must persist Done"
        );
    }

    #[tokio::test]
    async fn a_suspension_is_durable_before_it_is_published() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let hub = spawn(options(
            Arc::new(ScriptedBehavior),
            Arc::new(OrderedPersist { log: log.clone() }),
        ));
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
            .expect("persist the suspended phase");
        let published = log
            .iter()
            .position(|mark| *mark == "published suspended")
            .expect("publish the suspended phase");
        assert!(
            saved < published,
            "persistence must precede publication: {log:?}"
        );
    }

    #[tokio::test]
    async fn recovery_keeps_resting_phases_and_converges_in_flight_ones() {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let hub = spawn(options(
            Arc::new(ScriptedBehavior),
            Arc::new(RecordingPersist {
                recorded: recorded.clone(),
            }),
        ));
        let mut rx = hub.subscribe();
        hub.recover(vec![
            NodeSnapshot {
                id: 1,
                parent: None,
                children: vec![2, 3],
                phase: Phase::Running,
                detached: false,
                pending: Vec::new(),
            },
            NodeSnapshot {
                id: 2,
                parent: Some(1),
                children: vec![],
                phase: Phase::Suspended(TestSuspend::Ask),
                detached: false,
                pending: Vec::new(),
            },
            NodeSnapshot {
                id: 3,
                parent: Some(1),
                children: vec![],
                phase: Phase::Cancelling(CancelOrigin::Ancestor(1)),
                detached: false,
                pending: Vec::new(),
            },
        ])
        .await
        .expect("recover");

        let snapshot = hub.snapshot().await.expect("snapshot");
        let node_of = |id: NodeId| snapshot.iter().find(|n| n.id == id).expect("node");
        let phase_of = |id: NodeId| node_of(id).phase.clone();

        assert_eq!(
            node_of(1).children,
            vec![2, 3],
            "children must be rebuilt from the snapshot"
        );
        assert_eq!(
            node_of(2).parent,
            Some(1),
            "the parent pointer must be rebuilt from the snapshot"
        );
        assert_eq!(node_of(3).parent, Some(1));
        assert!(!node_of(1).detached);

        assert!(
            matches!(phase_of(2), Phase::Suspended(TestSuspend::Ask)),
            "a resting phase must be preserved losslessly"
        );
        assert!(
            matches!(phase_of(3), Phase::Cancelled(CancelOrigin::Ancestor(1))),
            "Cancelling depends on a confirmation that never arrives; it must converge to Cancelled and preserve the original origin"
        );
        assert!(
            matches!(phase_of(1), Phase::Aborted),
            "a rebuilt Running node has no worker, so it must converge to Aborted"
        );
        assert!(
            hub.in_flight_nodes().await.expect("in flight").is_empty(),
            "recovery must not leave in-flight nodes that can never receive a report"
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
            "convergence must broadcast a terminal event carrying the original origin"
        );

        let recorded = recorded.lock().expect("lock");
        assert!(
            recorded.iter().any(|(node, phase)| *node == 3
                && matches!(phase, Phase::Cancelled(CancelOrigin::Ancestor(1)))),
            "convergence must be written back, otherwise persistence still shows Cancelling"
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
            pending: Vec::new(),
        }])
        .await
        .expect("recover");

        let seen = wait_until(&mut rx, |e| matches!(e, TreeEvent::Sealed)).await;
        assert!(
            seen.iter().any(|e| matches!(e, TreeEvent::Sealed)),
            "a quiet recovered tree must announce Sealed itself, otherwise waiters never see it"
        );
    }

    /// 一跑就崩的 worker
    struct PanicBehavior;

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for PanicBehavior {
        async fn run(&self, _ctx: Ctx, _input: Input<TestDomain>) -> Outcome<TestDomain> {
            panic!("boom")
        }
    }

    #[tokio::test]
    async fn a_panicking_worker_terminates_its_node_as_aborted_and_seals() {
        let hub = spawn(options(Arc::new(PanicBehavior), Arc::new(NoopPersist)));
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
            "a panicking worker must land the node on Aborted instead of leaving the tree stuck in Running"
        );
        let snapshot = hub.snapshot().await.expect("snapshot");
        assert!(
            snapshot[0].phase.is_terminal(),
            "the node must reach a terminal phase"
        );
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
                pending: Vec::new(),
            },
            NodeSnapshot {
                id: 2,
                parent: Some(1),
                children: vec![],
                phase: Phase::Done(TestOutput::Ok),
                detached: false,
                pending: Vec::new(),
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
            "a parent whose children are all terminal must be re-entered, otherwise it parks forever"
        );

        wait_until(&mut rx, |e| matches!(e, TreeEvent::Sealed)).await;
        let snapshot = hub.snapshot().await.expect("snapshot");
        assert!(
            snapshot.iter().all(|node| node.phase.is_terminal()),
            "the tree must converge after re-entry"
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
            pending: Vec::new(),
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
            "a rebuilt Running node has no worker to confirm it, so it must still reach a terminal phase"
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
            pending: Vec::new(),
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
            "a rebuilt detached in-flight node also has no worker to confirm it"
        );
        assert!(
            seen.iter().any(|e| matches!(e, TreeEvent::Sealed)),
            "a terminal phase produced by recovery convergence must not leave a delivery obligation nobody can release, otherwise that topic's stream hangs forever"
        );
    }

    #[tokio::test]
    async fn dropping_every_handle_ends_the_hub() {
        let hub = hub();
        let mut rx = hub.subscribe();
        drop(hub);

        let outcome = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("stop hub after last handle drop");
        assert!(
            matches!(outcome, Err(broadcast::error::RecvError::Closed)),
            "the event stream must close when the hub exits"
        );
    }

    /// 按输入决定派生、挂起还是完成的脚本化行为
    struct TreeBehavior;

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for TreeBehavior {
        async fn run(&self, _ctx: Ctx, input: Input<TestDomain>) -> Outcome<TestDomain> {
            match input {
                Input::Fresh(TestSpec::Children(n)) => Outcome::Spawn {
                    spawn: (0..n)
                        .map(|_| SpawnRequest {
                            id: next_id(),
                            // 子节点挂起，父节点停在 WaitingChildren
                            spec: TestSpec::Suspend,
                            detached: false,
                        })
                        .collect(),
                },
                Input::Children(_) => Outcome::Completed(TestOutput::Ok),
                Input::Fresh(TestSpec::Noop) => Outcome::Completed(TestOutput::Ok),
                Input::Fresh(TestSpec::Suspend) => Outcome::Suspend(TestSuspend::Ask),
                Input::Fresh(TestSpec::Fail) => Outcome::Failed(TestError::Boom),
                Input::External(_) | Input::Idle(_) => Outcome::Completed(TestOutput::Ok),
            }
        }
    }

    #[tokio::test]
    async fn cancelling_a_parent_with_children_loses_no_events_and_seals_once() {
        let hub = spawn(options(Arc::new(TreeBehavior), Arc::new(NoopPersist)));
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
        assert_eq!(snapshot.len(), 4, "root plus 3 children");

        let mut rx2 = hub.subscribe();
        hub.send(Event::CancelRequest {
            root,
            origin: CancelOrigin::External,
        })
        .await
        .expect("send");

        let mut seen = wait_until(&mut rx2, |e| matches!(e, TreeEvent::Sealed)).await;
        // 首次 Sealed 之后排空一次，取到其余节点发出的 Sealed
        let _ = hub.snapshot().await.expect("snapshot");
        while let Ok(event) = rx2.try_recv() {
            seen.push(event);
        }

        let sealed = seen
            .iter()
            .filter(|e| matches!(e, TreeEvent::Sealed))
            .count();
        assert_eq!(sealed, 1, "Sealed must be sent only once");

        let cancelled: BTreeSet<NodeId> = seen
            .iter()
            .filter_map(|e| match e {
                TreeEvent::Terminated {
                    node,
                    terminal: Terminal::Cancelled(_),
                } => Some(*node),
                _ => None,
            })
            .collect();
        let expected: BTreeSet<NodeId> = snapshot.iter().map(|n| n.id).collect();
        assert_eq!(
            cancelled, expected,
            "every node must receive a cancellation terminal, none may be lost"
        );
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

        let snapshot = hub.snapshot().await.expect("snapshot");
        assert!(snapshot[0].phase.is_terminal());
        // `snapshot` 是屏障。此前入队的回报都已处理，此后缓冲里不应再有任何事件。
        assert!(
            matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)),
            "a stray report after the terminal phase must not produce any outward event"
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
            pending: Vec::new(),
        }])
        .await
        .expect("recover");

        let seen = wait_until(&mut rx, |e| matches!(e, TreeEvent::Sealed)).await;
        assert!(
            seen.iter().any(|e| matches!(e, TreeEvent::Sealed)),
            "a recovered terminal detached node must not carry a delivery obligation, otherwise that topic's stream never closes"
        );
        assert!(
            hub.in_flight_nodes().await.expect("in flight").is_empty(),
            "recovery must not leave in-flight nodes that can never receive a report"
        );
        assert!(
            hub.undelivered_nodes()
                .await
                .expect("undelivered")
                .is_empty(),
            "recovery must not leave any delivery obligation"
        );
    }

    #[tokio::test]
    async fn a_hub_reports_itself_stopped_only_after_shutdown() {
        let hub = hub();
        assert!(!hub.is_stopped(), "a freshly started hub is not stopped");
        hub.shutdown().await.expect("shutdown");
        assert!(
            hub.is_stopped(),
            "stopping must be observable after shutdown"
        );
    }

    /// 翻译可能产出的每一条事件都必须能派发出去
    #[test]
    fn every_translated_event_can_be_dispatched() {
        let events = vec![
            TreeEvent::<AgentDomain>::NodeCreated {
                node: 1,
                parent: None,
            },
            TreeEvent::<AgentDomain>::PhaseChanged {
                node: 1,
                to: Box::new(Phase::Running),
            },
            TreeEvent::<AgentDomain>::PhaseChanged {
                node: 1,
                to: Box::new(Phase::Suspended(SuspendedWait {
                    instance_id: 1,
                    kind: "approval".into(),
                    message_id: 1,
                    handle: "h-1".into(),
                    payload: None,
                })),
            },
            TreeEvent::<AgentDomain>::Terminated {
                node: 1,
                terminal: Terminal::Done(AgentOutcome { message_id: 1 }),
            },
            TreeEvent::<AgentDomain>::Terminated {
                node: 1,
                terminal: Terminal::Failed(AgentFailure {
                    message_id: None,
                    error: "boom".into(),
                }),
            },
            TreeEvent::<AgentDomain>::Terminated {
                node: 1,
                terminal: Terminal::Cancelled(CancelOrigin::External),
            },
            TreeEvent::<AgentDomain>::Terminated {
                node: 1,
                terminal: Terminal::Aborted,
            },
            TreeEvent::<AgentDomain>::Sealed,
        ];

        for event in events {
            for item in translate(&event) {
                assert!(
                    observe_def(item.name).is_some(),
                    "{} must be dispatchable by name",
                    item.name
                );
            }
        }
    }

    /// 派发与否只由目录里的形态决定，不存在另立的名单
    #[test]
    fn the_dispatch_table_follows_the_catalog() {
        let dir = directory();
        for name in dir.names() {
            let meta = dir.get(name).expect("find the name in the catalog");
            assert_eq!(
                observe_def(name).is_some(),
                meta.is_observe(),
                "{name} dispatchability must match its shape in the catalog"
            );
        }
        assert!(
            observe_def("core/not.a.real.event").is_none(),
            "names outside the catalog are never dispatchable"
        );
    }
}
