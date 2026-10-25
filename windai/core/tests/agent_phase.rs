//! 验证实例 `Phase` 的读写。

#[path = "common/lib.rs"]
mod common;

use common::store;
use std::sync::Arc;
use std::time::Duration;
use wind_core::agent::behavior::{Behavior, Ctx, Input, Outcome};
use wind_core::agent::domain::{AgentDomain, AgentOutcome};
use wind_core::agent::events::AgentCreate;
use wind_core::agent::hub::{BUS_CAPACITY, HubHandle, HubOptions, Persist, spawn};
use wind_core::agent::store::AgentStore;
use wind_core::agent::translate::translate;
use wind_core::agent::tree::phase::Phase;
use wind_core::error::Result as CoreResult;
use wind_core::models::AgentInstanceCreate;
use wind_core::models::{AgentMode, AgentPhase, AgentRole};
use wind_core::storage::Storage;
use wind_event::context::DispatchCx;
use wind_event::def::Directory;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::subscription::{EnabledSet, Subscription};

/// 直接返回 `Completed` 的行为
struct CompletingBehavior;

#[async_trait::async_trait]
impl Behavior<AgentDomain> for CompletingBehavior {
    async fn run(&self, _ctx: Ctx, _input: Input<AgentDomain>) -> Outcome<AgentDomain> {
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

/// 不启用任何 mod
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
    ) -> Result<Option<serde_json::Value>, HostError> {
        Err(HostError::Internal("the test dispatches nothing".into()))
    }
}

/// 一条没有订阅者的事件总线
fn dispatcher() -> Arc<Dispatcher> {
    Arc::new(Dispatcher::new(
        Directory::default(),
        Arc::new(NoEnablement),
        Arc::new(NoHost),
        Limits::default(),
    ))
}

/// 绑定话题 1 的 hub
fn hub_for(_storage: &Storage) -> HubHandle<AgentDomain> {
    spawn(HubOptions {
        behavior: Arc::new(CompletingBehavior),
        persist: Arc::new(NoopPersist),
        grace: Duration::from_millis(50),
        bus_capacity: BUS_CAPACITY,
        dispatcher: dispatcher(),
        topic_id: 1,
        translator: translate,
    })
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

/// `AgentCreate` 请求的载荷
fn payload() -> AgentCreate {
    AgentCreate {
        topic_id: 1,
        parent_id: None,
        mode: AgentMode::Sync.to_string(),
        task: None,
    }
}

#[tokio::test]
async fn a_phase_survives_a_round_trip_verbatim() {
    let (_dir, store) = store().await;
    let id = store
        .agent_create(AgentInstanceCreate {
            parent_id: None,
            topic_id: 1,
            role: AgentRole::Main,
            mode: None,
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("create instance");
    let payload = r#"{"kind":"approval","handle":"h1","message_id":3}"#;
    store
        .agent_save_phase(id, AgentPhase::Suspended, Some(payload))
        .await
        .expect("store the phase");

    let loaded = store
        .agent_get(id)
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(loaded.phase, AgentPhase::Suspended);
    assert_eq!(
        loaded.phase_payload.as_deref(),
        Some(payload),
        "the payload must round-trip verbatim; the storage layer does not interpret it"
    );
}

#[tokio::test]
async fn moving_to_a_phase_without_a_payload_clears_the_previous_one() {
    let (_dir, store) = store().await;
    let id = store
        .agent_create(AgentInstanceCreate {
            parent_id: None,
            topic_id: 1,
            role: AgentRole::Main,
            mode: None,
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("create instance");
    store
        .agent_save_phase(id, AgentPhase::Suspended, Some(r#"{"handle":"h1"}"#))
        .await
        .expect("store the phase");
    store
        .agent_save_phase(id, AgentPhase::Running, None)
        .await
        .expect("store the phase");

    let loaded = store
        .agent_get(id)
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(loaded.phase, AgentPhase::Running);
    assert!(
        loaded.phase_payload.is_none(),
        "switching Phase must clear the previous Phase's payload, otherwise recovery would read a handle that does not belong to the current Phase"
    );
}

#[tokio::test]
async fn saving_a_phase_for_an_unknown_instance_reports_no_rows() {
    let (_dir, store) = store().await;
    let result = store.agent_save_phase(999, AgentPhase::Running, None).await;
    assert!(
        result.is_err(),
        "updating an unknown instance must report an error rather than silently succeed"
    );
}

#[tokio::test]
async fn instances_are_listed_by_topic() {
    let (_dir, store) = store().await;
    store
        .agent_create(AgentInstanceCreate {
            parent_id: None,
            topic_id: 1,
            role: AgentRole::Main,
            mode: None,
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("build");
    store
        .agent_create(AgentInstanceCreate {
            parent_id: Some(1),
            topic_id: 1,
            role: AgentRole::Child,
            mode: Some(AgentMode::Sync),
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("build");
    store
        .agent_create(AgentInstanceCreate {
            parent_id: None,
            topic_id: 2,
            role: AgentRole::Main,
            mode: None,
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("build");

    let listed = store.agent_list_by_topic(1).await.expect("query");
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|i| i.topic_id == 1));
}

#[tokio::test]
async fn ensure_main_is_idempotent_and_allocates_from_the_row() {
    let (_dir, storage) = store().await;
    let first = storage
        .agent_ensure_main(1, AgentMode::Sync, AgentPhase::Done)
        .await
        .expect("build");
    let second = storage
        .agent_ensure_main(1, AgentMode::Sync, AgentPhase::Done)
        .await
        .expect("build");
    assert_eq!(first, second, "a topic has exactly one main instance");
    assert_eq!(storage.agent_list_by_topic(1).await.expect("read").len(), 1);
}

#[tokio::test]
async fn a_spawned_node_uses_the_id_the_row_allocated() {
    let (_dir, storage) = store().await;
    let id = storage
        .agent_create(AgentInstanceCreate {
            parent_id: None,
            topic_id: 1,
            role: AgentRole::Main,
            mode: Some(AgentMode::Sync),
            phase: AgentPhase::Done,
            phase_payload: None,
        })
        .await
        .expect("build");
    let hub = hub_for(&storage);
    let node = hub
        .spawn_at(&cx(), id, &payload())
        .await
        .expect("spawn node at id");
    assert_eq!(node, id);
    assert_eq!(hub.snapshot().await.expect("read hub snapshot")[0].id, id);
}

#[tokio::test]
async fn a_restored_node_is_detached_only_in_background_mode() {
    let (_dir, storage) = store().await;
    let root = storage
        .agent_create(AgentInstanceCreate {
            parent_id: None,
            topic_id: 1,
            role: AgentRole::Main,
            mode: Some(AgentMode::Sync),
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("build");
    let background = storage
        .agent_create(AgentInstanceCreate {
            parent_id: Some(root),
            topic_id: 1,
            role: AgentRole::Child,
            mode: Some(AgentMode::Background),
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("build");
    let store = AgentStore::new(storage.clone(), 1, dispatcher());
    let snapshots = store.load_snapshots().await.expect("recover");
    let detached_of = |id: i64| {
        snapshots
            .iter()
            .find(|snapshot| snapshot.id == id)
            .expect("find node in snapshot")
            .detached
    };
    assert!(!detached_of(root), "sync mode does not enter detached");
    assert!(
        detached_of(background),
        "background mode must be restored as detached according to the instance row's run mode"
    );
}
