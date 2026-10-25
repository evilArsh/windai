//! 验证 hub 入口的拦截与翻译出口的派发。

use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use wind_core::CoreError;
use wind_core::agent::behavior::{Behavior, Ctx, Input, Outcome};
use wind_core::agent::domain::{AgentDomain, AgentOutcome};
use wind_core::agent::events::{AgentCreate, CORE_AGENT_CREATE, directory};
use wind_core::agent::hub::{BUS_CAPACITY, HubHandle, HubOptions, NodeSnapshot, Persist, spawn};
use wind_core::agent::translate::translate;
use wind_core::agent::tree::phase::{CancelOrigin, Phase};
use wind_core::error::Result as CoreResult;
use wind_event::context::DispatchCx;
use wind_event::def::Shape;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::subscription::{EnabledSet, Phase as SubPhase, Subscription};

/// 等待观测到达的时限。超过即判定观测丢失
const PATIENCE: Duration = Duration::from_secs(5);

/// 一律放行的宿主
struct Allowed;

#[async_trait::async_trait]
impl ModHost for Allowed {
    async fn call(
        &self,
        _sub: &Subscription,
        _payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        Ok(None)
    }
}

/// 记录每次派发的事件名与载荷
#[derive(Default)]
struct Recorder {
    /// 按到达顺序记录的派发
    seen: Mutex<Vec<(&'static str, serde_json::Value)>>,
}

impl Recorder {
    /// 某一事件名下的载荷
    fn payloads_of(&self, event: &str) -> Vec<serde_json::Value> {
        self.seen
            .lock()
            .map(|seen| {
                seen.iter()
                    .filter(|(name, _)| *name == event)
                    .map(|(_, payload)| payload.clone())
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl ModHost for Recorder {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        if let Ok(mut seen) = self.seen.lock() {
            seen.push((sub.event, payload));
        }
        Ok(None)
    }
}

/// 全部订阅者都启用
struct AllEnabled;

impl ModEnablement for AllEnabled {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::all()
    }
}

/// 组装一条守卫订阅
fn sub(event: &'static str) -> Subscription {
    Subscription {
        mod_name: "guard".into(),
        mod_version: 1,
        event,
        phase: Some(SubPhase::Guard),
        handler: "onEvent".into(),
        timeout_ms: 50,
        registration_seq: 1,
    }
}

/// 组装一个按给定订阅表派发的事件总线
fn bus(host: Arc<dyn ModHost>, subs: Vec<Subscription>) -> Arc<Dispatcher> {
    let dispatcher = Dispatcher::new(directory(), Arc::new(AllEnabled), host, Limits::default());
    dispatcher.rebuild_index(subs);
    Arc::new(dispatcher)
}

/// 顶层操作的 `DispatchCx`
fn cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id: 1 },
        Source::core("agent"),
        Duration::from_millis(50),
    )
}

/// 每次运行都立即完成的行为
struct CompletingBehavior;

#[async_trait::async_trait]
impl Behavior<AgentDomain> for CompletingBehavior {
    async fn run(&self, _ctx: Ctx, _input: Input<AgentDomain>) -> Outcome<AgentDomain> {
        Outcome::Completed(AgentOutcome { message_id: 1 })
    }
}

/// 等待取消信号后才返回的行为
struct BlockingBehavior;

#[async_trait::async_trait]
impl Behavior<AgentDomain> for BlockingBehavior {
    async fn run(&self, ctx: Ctx, _input: Input<AgentDomain>) -> Outcome<AgentDomain> {
        ctx.cancel.cancelled().await;
        Outcome::Completed(AgentOutcome { message_id: 1 })
    }
}

/// 不写入任何状态的 `Persist` 实现
struct NoopPersist;

#[async_trait::async_trait]
impl Persist<AgentDomain> for NoopPersist {
    async fn save(&self, _node: i64, _phase: &Phase<AgentDomain>) -> CoreResult<()> {
        Ok(())
    }
}

/// 由测试侧分配节点 id 的 hub
///
/// 节点 id 由实例行分配。测试不建实例行，因此按树上已有的最大 id 递增
struct Harness {
    /// 被包装的 hub
    hub: HubHandle<AgentDomain>,
}

impl Harness {
    /// 分两段创建实例。先经过入口拦截，再用分配到的 id 插入节点
    async fn create(
        &self,
        cx: &DispatchCx,
        parent: Option<i64>,
        mode: &str,
        task: Option<&str>,
    ) -> CoreResult<i64> {
        let payload = self
            .hub
            .intercept_create(
                cx,
                AgentCreate {
                    topic_id: 1,
                    parent_id: parent,
                    mode: mode.to_string(),
                    task: task.map(str::to_string),
                },
            )
            .await;
        let id = self
            .hub
            .snapshot()
            .await?
            .iter()
            .map(|node| node.id)
            .max()
            .unwrap_or(0)
            + 1;
        self.hub.spawn_at(cx, id, &payload).await
    }
}

impl Deref for Harness {
    type Target = HubHandle<AgentDomain>;

    fn deref(&self) -> &Self::Target {
        &self.hub
    }
}

/// 构造运行给定行为、接入给定总线的 hub
fn hub_with(behavior: Arc<dyn Behavior<AgentDomain>>, bus: Arc<Dispatcher>) -> Harness {
    Harness {
        hub: spawn(HubOptions {
            behavior,
            persist: Arc::new(NoopPersist),
            grace: Duration::from_millis(50),
            bus_capacity: BUS_CAPACITY,
            topic_id: 1,
            dispatcher: bus.clone(),
            translator: translate,
        }),
    }
}

/// 轮询快照直到条件成立
async fn wait_for<F>(hub: &HubHandle<AgentDomain>, mut done: F) -> Vec<NodeSnapshot<AgentDomain>>
where
    F: FnMut(&[NodeSnapshot<AgentDomain>]) -> bool,
{
    timeout(PATIENCE, async {
        loop {
            let nodes = hub.snapshot().await.expect("query");
            if done(&nodes) {
                return nodes;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("see condition within deadline")
}

/// 轮询记录器直到某一事件名下的载荷满足条件
async fn wait_for_recorded<F>(
    recorder: &Recorder,
    event: &str,
    mut done: F,
) -> Vec<serde_json::Value>
where
    F: FnMut(&[serde_json::Value]) -> bool,
{
    timeout(PATIENCE, async {
        loop {
            let payloads = recorder.payloads_of(event);
            if done(&payloads) {
                return payloads;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("receive observation within deadline")
}

#[test]
fn the_entry_events_are_intercepts() {
    assert_eq!(CORE_AGENT_CREATE.shape, Shape::Intercept);
}

#[tokio::test]
async fn an_allowed_create_inserts_a_node() {
    let hub = hub_with(Arc::new(CompletingBehavior), bus(Arc::new(Allowed), vec![]));
    let id = hub
        .create(&cx(), None, "sync", Some("task"))
        .await
        .expect("succeeds");
    assert_eq!(id, 1, "the first node id allocated by the entry");
    assert_eq!(hub.snapshot().await.expect("query").len(), 1);
}

#[tokio::test]
async fn an_entry_intercept_of_ui_origin_bypasses_every_mod() {
    let hub = hub_with(
        Arc::new(CompletingBehavior),
        bus(Arc::new(Allowed), vec![sub("core/agent.create")]),
    );
    let ui = DispatchCx::root(
        Origin::Ui,
        Scope::Topic { topic_id: 1 },
        Source::core("ui"),
        Duration::from_millis(50),
    );
    let id = hub
        .create(&ui, None, "sync", None)
        .await
        .expect("bypass intercept for ui source");
    assert_eq!(id, 1);
}

#[tokio::test]
async fn a_created_instance_is_announced_on_the_bus() {
    let recorder = Arc::new(Recorder::default());
    let hub = hub_with(
        Arc::new(CompletingBehavior),
        bus(recorder.clone(), vec![sub("core/agent.spawn")]),
    );
    let id = hub
        .create(&cx(), None, "sync", Some("task"))
        .await
        .expect("succeeds");

    let payloads = wait_for_recorded(&recorder, "core/agent.spawn", |seen| !seen.is_empty()).await;
    assert_eq!(payloads[0]["instance_id"], serde_json::json!(id));
    assert_eq!(payloads[0]["mode"], serde_json::json!("sync"));
}

#[tokio::test]
async fn a_rejected_insertion_announces_nothing() {
    let recorder = Arc::new(Recorder::default());
    let hub = hub_with(
        Arc::new(CompletingBehavior),
        bus(recorder.clone(), vec![sub("core/agent.spawn")]),
    );
    let result = hub.create(&cx(), Some(999), "sync", None).await;
    // 先等待一段时间。若确实发出宣告，这段时间足以让宣告到达记录器
    sleep(Duration::from_millis(30)).await;
    assert!(
        recorder.payloads_of("core/agent.spawn").is_empty(),
        "an instance that never materialized must not be announced, otherwise the event stream would contain a nonexistent instance"
    );
    assert!(
        matches!(result, Err(CoreError::Internal(_))),
        "creating under a nonexistent parent must fail"
    );
    assert_eq!(
        hub.snapshot().await.expect("query").len(),
        0,
        "no node may be added to the tree"
    );
}

#[tokio::test]
async fn a_background_child_enters_no_wait_barrier() {
    let hub = hub_with(Arc::new(CompletingBehavior), bus(Arc::new(Allowed), vec![]));
    let root = hub
        .create(&cx(), None, "sync", None)
        .await
        .expect("create succeeds");
    let background = hub
        .create(&cx(), Some(root), "background", None)
        .await
        .expect("create succeeds");
    let sync = hub
        .create(&cx(), Some(root), "sync", None)
        .await
        .expect("create succeeds");

    let nodes = hub.snapshot().await.expect("query");
    let detached_of = |id: i64| {
        nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("expected node {id} in tree"))
            .detached
    };
    assert!(
        detached_of(background),
        "a background-mode child does not enter the parent's wait barrier, otherwise the parent would wait for it forever"
    );
    assert!(
        !detached_of(sync),
        "a sync-mode child must enter the wait barrier"
    );
    assert!(
        !detached_of(root),
        "the root node has no parent to wait for"
    );
}

#[tokio::test]
async fn a_create_after_recovery_does_not_collide_with_a_restored_node() {
    let hub = hub_with(Arc::new(CompletingBehavior), bus(Arc::new(Allowed), vec![]));
    hub.recover(vec![
        NodeSnapshot {
            id: 1,
            parent: None,
            children: vec![2],
            phase: Phase::Done(AgentOutcome { message_id: 1 }),
            detached: false,
            pending: Vec::new(),
        },
        NodeSnapshot {
            id: 2,
            parent: Some(1),
            children: vec![],
            phase: Phase::Done(AgentOutcome { message_id: 2 }),
            detached: false,
            pending: Vec::new(),
        },
    ])
    .await
    .expect("recover");

    let node = hub
        .create(&cx(), None, "sync", None)
        .await
        .expect("create after recovery");
    assert!(
        node > 2,
        "a new node id must exceed the largest recovered node id, actual {node}"
    );
    assert!(
        hub.snapshot()
            .await
            .expect("query")
            .iter()
            .any(|created| created.id == node),
        "the created node must be in the tree"
    );
}

#[tokio::test]
async fn an_allowed_terminate_lands_the_node_cancelled() {
    let hub = hub_with(Arc::new(BlockingBehavior), bus(Arc::new(Allowed), vec![]));
    let root = hub
        .create(&cx(), None, "sync", None)
        .await
        .expect("create succeeds");
    wait_for(&hub, |nodes| {
        nodes.len() == 1 && matches!(nodes[0].phase, Phase::Running)
    })
    .await;

    hub.terminate(&cx(), root, "retry")
        .await
        .expect("terminate the node");

    let nodes = wait_for(&hub, |nodes| {
        nodes.iter().all(|node| node.phase.is_terminal())
    })
    .await;
    assert!(
        matches!(nodes[0].phase, Phase::Cancelled(CancelOrigin::External)),
        "an external terminate must land as a cancellation with an external origin"
    );
}
