//! 本模块验收 agent 事件目录、`Phase` 编解码与取消路径。

use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use wind_core::agent::behavior::{Behavior, Ctx, Input, Outcome, SpawnRequest};
use wind_core::agent::domain::{AgentDomain, AgentFailure, AgentOutcome, AgentSpec, SuspendedWait};
use wind_core::agent::events::{
    AgentCreate, AgentNodeView, CORE_AGENT_CANCEL, CORE_AGENT_CREATE, CORE_AGENT_ERROR,
    CORE_AGENT_SPAWN, CORE_AGENT_STATUS, CORE_AGENT_TERMINATE, CORE_AGENT_TREE,
    CORE_APPROVAL_REQUEST, CORE_APPROVAL_RESOLVED, directory,
};
use wind_core::agent::hub::{BUS_CAPACITY, HubHandle, HubOptions, NodeSnapshot, Persist, spawn};
use wind_core::agent::phase_codec::{PhaseCodecError, StoredPhase, decode, encode};
use wind_core::agent::translate::translate;
use wind_core::agent::tree::phase::{CancelOrigin, Phase};
use wind_core::error::Result as CoreResult;
use wind_event::context::DispatchCx;
use wind_event::def::Mount;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::subscription::{EnabledSet, Phase as SubPhase, Subscription};

/// 等待观测到达的时限。超过即判定观测丢失
const PATIENCE: Duration = Duration::from_secs(5);

/// 事件目录恰好包含九个事件
///
/// 目录按名收集，重名条目会被静默覆盖。名字表与条数须同时核对
#[test]
fn the_agent_family_declares_nine_events() {
    let catalog = directory();
    let mut names: Vec<&str> = catalog.names().collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "core/agent.cancel",
            "core/agent.create",
            "core/agent.error",
            "core/agent.spawn",
            "core/agent.status",
            "core/agent.terminate",
            "core/agent.tree",
            "core/approval.request",
            "core/approval.resolved",
        ],
        "the names of the nine events are a public contract"
    );
    assert_eq!(
        catalog.len(),
        9,
        "duplicate names are silently merged by the catalog"
    );
}

/// 三条入口事件为拦截，挂载在用例层
#[test]
fn the_three_entries_intercept_at_the_use_case_layer() {
    for def in [
        CORE_AGENT_CREATE.meta(),
        CORE_AGENT_TERMINATE.meta(),
        CORE_AGENT_CANCEL.meta(),
    ] {
        assert!(def.is_intercept(), "{} must be interceptable", def.name);
        assert_eq!(
            def.mount,
            Mount::UseCase,
            "{} mounts at the use-case layer",
            def.name
        );
    }
}

/// 事后上报的事件一律为观测，挂载在翻译出口
#[test]
fn everything_reported_after_the_fact_observes_at_the_translation_layer() {
    for def in [
        CORE_AGENT_SPAWN.meta(),
        CORE_AGENT_STATUS.meta(),
        CORE_AGENT_ERROR.meta(),
        CORE_AGENT_TREE.meta(),
    ] {
        assert!(def.is_observe(), "{} is observe-only", def.name);
        assert_eq!(
            def.mount,
            Mount::TranslationLayer,
            "{} mounts at the translation layer",
            def.name
        );
    }
    // 两条等待事件同样为观测，挂载在子系统内部
    for def in [CORE_APPROVAL_REQUEST.meta(), CORE_APPROVAL_RESOLVED.meta()] {
        assert!(def.is_observe(), "{} is observe-only", def.name);
        assert_eq!(
            def.mount,
            Mount::Subsystem,
            "{} mounts inside the subsystem",
            def.name
        );
    }
}

/// 两条等待事件都是观测，没有任何 Mod 能否决用户的答复
#[test]
fn no_mod_can_veto_a_user_answer() {
    assert!(CORE_APPROVAL_REQUEST.meta().is_observe());
    assert!(CORE_APPROVAL_RESOLVED.meta().is_observe());
}

/// 八个 `Phase` 连同各自的存储判别式与载荷
///
/// 三个无载荷 `Phase` 各有独立的判别式，按名字逐一断言才能区分
fn every_phase() -> Vec<(Phase<AgentDomain>, &'static str, Option<serde_json::Value>)> {
    vec![
        (Phase::Running, "running", None),
        (
            Phase::Suspended(SuspendedWait {
                instance_id: 7,
                kind: "approval".into(),
                message_id: 11,
                handle: "h-1".into(),
                payload: None,
            }),
            "suspended",
            Some(serde_json::json!({
                "instance_id": 7,
                "kind": "approval",
                "message_id": 11,
                "handle": "h-1",
            })),
        ),
        (Phase::WaitingChildren, "waiting_children", None),
        (
            Phase::Cancelling(CancelOrigin::Ancestor(3)),
            "cancelling",
            Some(serde_json::json!({ "Ancestor": 3 })),
        ),
        (
            Phase::Done(AgentOutcome { message_id: 5 }),
            "done",
            Some(serde_json::json!({ "message_id": 5 })),
        ),
        (
            Phase::Failed(AgentFailure {
                message_id: Some(5),
                error: "boom".into(),
            }),
            "failed",
            Some(serde_json::json!({ "message_id": 5, "error": "boom" })),
        ),
        (
            Phase::Cancelled(CancelOrigin::External),
            "cancelled",
            Some(serde_json::json!("External")),
        ),
        (Phase::Aborted, "aborted", None),
    ]
}

/// 八个 `Phase` 逐个往返编解码，取消来源在载荷中保持不变
#[test]
fn every_phase_round_trips_with_its_payload() {
    for (phase, name, payload) in every_phase() {
        let stored = encode(&phase).expect("encode");
        assert_eq!(
            stored.name, name,
            "{phase:?} has this discriminant when written to the database"
        );
        assert_eq!(
            stored.payload, payload,
            "{name} has this payload when written to the database"
        );
        let back = decode(&stored).expect("decode");
        assert_eq!(back, phase, "{name} must round-trip verbatim");
    }
}

/// 四种损坏记录各自返回一种错误，不会静默还原成近似的 `Phase`
#[test]
fn corrupted_phase_records_are_rejected() {
    let unknown = StoredPhase {
        name: "zombie".into(),
        payload: None,
    };
    assert!(
        matches!(decode(&unknown), Err(PhaseCodecError::UnknownPhase(name)) if name == "zombie"),
        "an unknown Phase name must raise an error"
    );

    let missing = StoredPhase {
        name: "suspended".into(),
        payload: None,
    };
    assert!(
        matches!(decode(&missing), Err(PhaseCodecError::MissingPayload(name)) if name == "suspended"),
        "a payload-carrying Phase missing its payload must raise an error"
    );

    let stray = StoredPhase {
        name: "running".into(),
        payload: Some(serde_json::json!({})),
    };
    assert!(
        matches!(decode(&stray), Err(PhaseCodecError::UnexpectedPayload(name)) if name == "running"),
        "a payload-free Phase with an unexpected payload must raise an error"
    );

    let mismatched = StoredPhase {
        name: "done".into(),
        payload: Some(serde_json::json!({ "nope": 1 })),
    };
    assert!(
        matches!(decode(&mismatched), Err(PhaseCodecError::BadPayload { name, .. }) if name == "done"),
        "a payload whose shape does not match the discriminant must raise an error"
    );
}

/// 结构快照的节点经 JSON 往返后仍保留 `Phase` 与载荷
///
/// 对外的有损枚举无法还原取消来源与挂起原因
#[test]
fn a_snapshot_node_keeps_the_phase_through_json() {
    for (phase, name, payload) in every_phase() {
        let view = AgentNodeView {
            instance_id: 1,
            parent_id: None,
            detached: false,
            phase: name.to_string(),
            phase_payload: payload,
        };
        let text = serde_json::to_string(&view).expect("serialize");
        let back: AgentNodeView = serde_json::from_str(&text).expect("deserialize");
        let restored = decode(&StoredPhase {
            name: back.phase,
            payload: back.phase_payload,
        })
        .expect("decode phase from snapshot");
        assert_eq!(restored, phase, "{name} must be restorable from a snapshot");
    }
}

/// 记录每次派发的事件名与载荷
struct Watcher {
    /// 按到达顺序记录的派发
    seen: Mutex<Vec<(&'static str, serde_json::Value)>>,
}

impl Watcher {
    /// 构造一个记录器
    fn new() -> Self {
        Self {
            seen: Mutex::new(Vec::new()),
        }
    }

    /// 某一事件名下的载荷
    fn payloads_of(&self, event: &str) -> Vec<serde_json::Value> {
        self.seen
            .lock()
            .expect("read dispatch record")
            .iter()
            .filter(|(name, _)| *name == event)
            .map(|(_, payload)| payload.clone())
            .collect()
    }
}

#[async_trait::async_trait]
impl ModHost for Watcher {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        if let Ok(mut seen) = self.seen.lock() {
            match payload {
                serde_json::Value::Array(values) => {
                    for value in values {
                        seen.push((sub.event, value));
                    }
                }
                value => seen.push((sub.event, value)),
            }
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

/// 组装一条 Guard 阶段的订阅
fn sub(event: &'static str) -> Subscription {
    Subscription {
        mod_name: "watcher".into(),
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

/// 每次运行都以失败结束的行为
struct FailingBehavior;

#[async_trait::async_trait]
impl Behavior<AgentDomain> for FailingBehavior {
    async fn run(&self, _ctx: Ctx, _input: Input<AgentDomain>) -> Outcome<AgentDomain> {
        Outcome::Failed(AgentFailure {
            message_id: Some(7),
            error: "boom".into(),
        })
    }
}

/// 按输入类型返回不同结果的行为
///
/// 入口创建的根派生出三个子节点。子节点挂起等待外部答复，根因此停在等待子节点的状态
struct ScriptedBehavior;

#[async_trait::async_trait]
impl Behavior<AgentDomain> for ScriptedBehavior {
    async fn run(&self, _ctx: Ctx, input: Input<AgentDomain>) -> Outcome<AgentDomain> {
        match input {
            Input::Fresh(spec) if spec.message_id.is_none() => Outcome::Spawn {
                spawn: (1..=3)
                    .map(|index| SpawnRequest {
                        id: spec.instance_id * 10 + index,
                        spec: AgentSpec {
                            instance_id: spec.instance_id * 10 + index,
                            message_id: Some(1),
                            pending_id: None,
                            turn_id: None,
                        },
                        // 最后一个子节点不进入父节点的屏障，使快照中的 `detached` 同时出现 true 与 false
                        detached: index == 3,
                    })
                    .collect(),
            },
            Input::Fresh(spec) => Outcome::Suspend(SuspendedWait {
                instance_id: spec.instance_id,
                kind: "approval".into(),
                message_id: 1,
                handle: format!("h-{}", spec.instance_id),
                payload: None,
            }),
            Input::Children(_) | Input::External(_) | Input::Idle(_) => {
                Outcome::Completed(AgentOutcome { message_id: 1 })
            }
        }
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
async fn wait_for_recorded<F>(watcher: &Watcher, event: &str, mut done: F) -> Vec<serde_json::Value>
where
    F: FnMut(&[serde_json::Value]) -> bool,
{
    timeout(PATIENCE, async {
        loop {
            let payloads = watcher.payloads_of(event);
            if done(&payloads) {
                return payloads;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("receive observation within deadline")
}

/// 逐个节点比对结构快照与树的实际 Phase
async fn assert_snapshot_matches(hub: &HubHandle<AgentDomain>) {
    let nodes = hub.snapshot().await.expect("query");
    let tree = hub.tree_snapshot().await.expect("query");
    assert_eq!(tree.nodes.len(), nodes.len(), "one view per node");
    for node in &nodes {
        let view = tree
            .nodes
            .iter()
            .find(|view| view.instance_id == node.id)
            .expect("find every node in snapshot");
        assert_eq!(
            view.parent_id, node.parent,
            "the spawn origin must be preserved verbatim"
        );
        assert_eq!(
            view.detached, node.detached,
            "whether the node enters the barrier must be preserved verbatim"
        );
        let restored = decode(&StoredPhase {
            name: view.phase.clone(),
            payload: view.phase_payload.clone(),
        })
        .expect("decode stored discriminant");
        assert_eq!(
            restored, node.phase,
            "the Phase of node {} must be lossless",
            node.id
        );
    }
}

/// `phase` 为 `cancelled` 的实例标识，按到达顺序
fn cancelled_instances(payloads: &[serde_json::Value]) -> Vec<i64> {
    payloads
        .iter()
        .filter(|payload| payload["phase"] == serde_json::json!("cancelled"))
        .filter_map(|payload| payload["instance_id"].as_i64())
        .collect()
}

/// 失败经翻译出口成为一条错误事件
#[tokio::test]
async fn a_failed_instance_reaches_the_bus_as_an_error_event() {
    let watcher = Arc::new(Watcher::new());
    let hub = hub_with(
        Arc::new(FailingBehavior),
        bus(
            watcher.clone(),
            vec![sub(CORE_AGENT_ERROR.name), sub(CORE_AGENT_STATUS.name)],
        ),
    );
    let id = hub
        .create(&cx(), None, "sync", None)
        .await
        .expect("create succeeds");

    let errors = wait_for_recorded(&watcher, CORE_AGENT_ERROR.name, |seen| !seen.is_empty()).await;
    assert_eq!(errors[0]["instance_id"], serde_json::json!(id));
    assert_eq!(errors[0]["message_id"], serde_json::json!(7));
    assert_eq!(errors[0]["error"], serde_json::json!("boom"));

    let statuses = wait_for_recorded(&watcher, CORE_AGENT_STATUS.name, |seen| {
        seen.iter()
            .any(|payload| payload["phase"] == serde_json::json!("failed"))
    })
    .await;
    let failed = statuses
        .iter()
        .find(|payload| payload["phase"] == serde_json::json!("failed"))
        .expect("report phase with terminal state");
    assert_eq!(failed["instance_id"], serde_json::json!(id));
}

/// 结构快照把每个节点的 `Phase` 映射为判别式与载荷，映射本身没有损耗
#[tokio::test]
async fn a_tree_snapshot_maps_every_node_phase_without_loss() {
    let hub = hub_with(
        Arc::new(ScriptedBehavior),
        bus(Arc::new(Watcher::new()), vec![]),
    );
    let root = hub
        .create(&cx(), None, "sync", None)
        .await
        .expect("create succeeds");
    wait_for(&hub, |nodes| {
        nodes.len() == 4 && matches!(nodes[0].phase, Phase::WaitingChildren)
    })
    .await;
    assert_snapshot_matches(&hub).await;
    assert!(
        hub.snapshot()
            .await
            .expect("query")
            .iter()
            .any(|node| node.detached),
        "the use-case tree must contain a node outside the barrier, otherwise there is no way to tell whether detached travels with the snapshot"
    );

    hub.cancel(&cx(), root, CancelOrigin::External)
        .await
        .expect("cancel the node");
    wait_for(&hub, |nodes| {
        nodes.iter().all(|node| node.phase.is_terminal())
    })
    .await;
    assert_snapshot_matches(&hub).await;
}

/// 级联取消只拦截一次，每个被取消的节点都有 `Phase` 事件
#[tokio::test]
async fn a_cascade_intercepts_the_cancel_once_and_reports_every_node() {
    let watcher = Arc::new(Watcher::new());
    let hub = hub_with(
        Arc::new(ScriptedBehavior),
        bus(
            watcher.clone(),
            vec![sub(CORE_AGENT_CANCEL.name), sub(CORE_AGENT_STATUS.name)],
        ),
    );
    let root = hub
        .create(&cx(), None, "sync", None)
        .await
        .expect("create succeeds");
    let nodes = wait_for(&hub, |nodes| {
        nodes.len() == 4 && matches!(nodes[0].phase, Phase::WaitingChildren)
    })
    .await;
    let mut expected: Vec<i64> = nodes.iter().map(|node| node.id).collect();
    expected.sort_unstable();

    hub.cancel(&cx(), root, CancelOrigin::External)
        .await
        .expect("cancel the node");

    let settled = wait_for(&hub, |nodes| {
        nodes.iter().all(|node| node.phase.is_terminal())
    })
    .await;
    assert!(
        settled
            .iter()
            .all(|node| matches!(node.phase, Phase::Cancelled(_))),
        "the entire subtree must reach the cancelled terminal state"
    );

    wait_for_recorded(&watcher, CORE_AGENT_STATUS.name, |seen| {
        cancelled_instances(seen).len() >= nodes.len()
    })
    .await;
    let mut cancelled = cancelled_instances(&watcher.payloads_of(CORE_AGENT_STATUS.name));
    cancelled.sort_unstable();
    cancelled.dedup();
    assert_eq!(
        cancelled, expected,
        "every cancelled node must reach cancelled through a Phase event"
    );
    assert_eq!(
        watcher.payloads_of(CORE_AGENT_CANCEL.name).len(),
        1,
        "the cancel request is intercepted only once at the entry; the cascade is completed by the tree itself"
    );
}
