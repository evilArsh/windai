//! 验证外部等待的发起、答复与恢复。

#[path = "common/lib.rs"]
mod common;

use common::store;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use wind_core::CoreError;
use wind_core::agent::behavior::{Behavior, Ctx, Input, Outcome};
use wind_core::agent::domain::{
    AgentDomain, AgentOutcome, AgentSpec, CallDecision, SuspendedWait, WaitAnswer,
};
use wind_core::agent::events::{
    AgentCreate, CORE_APPROVAL_REQUEST, CORE_APPROVAL_RESOLVED, directory,
};
use wind_core::agent::hub::{BUS_CAPACITY, HubHandle, HubOptions, NodeSnapshot, Persist, spawn};
use wind_core::agent::store::AgentStore;
use wind_core::agent::translate::translate;
use wind_core::agent::tree::phase::Phase;
use wind_core::conversation::assembler::ProjectionInvalidations;
use wind_core::conversation::writer::BlockWriter;
use wind_core::error::Result as CoreResult;
use wind_core::feed::hub::FeedHub;
use wind_core::llm::ports::{DeclaredTool, ModRequests, Tools, TopicMods};
use wind_core::llm::tools::ToolOrigin;
use wind_core::llm::turn::{AgentRunner, DEFAULT_MAX_ROUNDS};
use wind_core::models::{
    AdapterType, AgentInstanceCreate, AgentMode, AgentPhase, AgentRole, AllowPolicy, BlockKind,
    BlockRow, MessageCreate, ModelCreate, ProviderCreate, Role, TopicCreate, TopicUpdate,
    TurnCreate, WaitOpen,
};
use wind_core::storage::Storage;
use wind_core::usecase::UseCases;
use wind_event::context::DispatchCx;
use wind_event::def::Directory;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::subscription::{EnabledSet, Phase as SubPhase, Subscription};
use wind_wire::tool::FunctionTool;

/// 等待观测到达的时限。超过即判定观测丢失
const PATIENCE: Duration = Duration::from_secs(5);

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

/// 组装一条观测订阅
fn sub(event: &'static str) -> Subscription {
    Subscription {
        mod_name: "watcher".into(),
        mod_version: 1,
        event,
        phase: None,
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

/// 分两段创建。先经过拦截，再用实例行的 id 插入节点
async fn spawn_row(
    hub: &HubHandle<AgentDomain>,
    cx: &DispatchCx,
    parent: Option<i64>,
    id: i64,
) -> CoreResult<i64> {
    let payload = hub
        .intercept_create(
            cx,
            AgentCreate {
                topic_id: 1,
                parent_id: parent,
                mode: AgentMode::Sync.to_string(),
                task: None,
            },
        )
        .await;
    hub.spawn_at(cx, id, &payload).await
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

/// 首次启动即挂起等待答复，收到答复即完成
#[derive(Default)]
struct SuspendingBehavior {
    /// 每次收到的答复
    answers: Mutex<Vec<WaitAnswer>>,
}

#[async_trait::async_trait]
impl Behavior<AgentDomain> for SuspendingBehavior {
    async fn run(&self, _ctx: Ctx, input: Input<AgentDomain>) -> Outcome<AgentDomain> {
        match input {
            Input::Fresh(spec) => Outcome::Suspend(SuspendedWait {
                instance_id: spec.instance_id,
                kind: "approval".into(),
                message_id: 1,
                handle: format!("h-{}", spec.instance_id),
                payload: None,
            }),
            Input::External(answer) => {
                if let Ok(mut answers) = self.answers.lock() {
                    answers.push(answer);
                }
                Outcome::Completed(AgentOutcome { message_id: 1 })
            }
            Input::Children(_) | Input::Idle(_) => {
                Outcome::Completed(AgentOutcome { message_id: 1 })
            }
        }
    }
}

/// 组装一个接在给定总线上的 agent 树，返回树存储与句柄
fn hub_with(
    behavior: Arc<dyn Behavior<AgentDomain>>,
    dispatcher: Arc<Dispatcher>,
    storage: Storage,
) -> (Arc<AgentStore>, HubHandle<AgentDomain>) {
    let store = Arc::new(AgentStore::new(storage, 1, dispatcher.clone()));
    let hub = spawn(HubOptions {
        behavior,
        persist: store.clone(),
        grace: Duration::from_millis(50),
        bus_capacity: BUS_CAPACITY,
        dispatcher,
        topic_id: 1,
        translator: translate,
    });
    store.attach(hub.clone());
    (store, hub)
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

/// 一条消息上已提交的工具调用块
async fn insert_tool_call(storage: &Storage, instance: i64, call_id: &str, tool: &str) -> i64 {
    let turn = storage
        .turn_create(TurnCreate {
            instance_id: instance,
            topic_id: 1,
            from_id: None,
            is_boundary: false,
            created_at: Some(0),
        })
        .await
        .expect("build turn");
    let message = storage
        .message_create(MessageCreate {
            turn_id: turn,
            role: Role::Assistant,
            model_id: None,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: Some(0),
        })
        .await
        .expect("build message");
    storage
        .block_insert(BlockRow {
            id: 1,
            message_id: message,
            seq: 0,
            kind: BlockKind::ToolCall,
            call_id: Some(call_id.into()),
            revision: 2,
            content: String::new(),
            data: Some(serde_json::json!({
                "name": tool,
                "arguments": { "city": "SF" },
            })),
            finished: true,
        })
        .await
        .expect("insert block");
    message
}

#[tokio::test]
async fn suspending_writes_the_wait_and_the_phase_together() {
    let (_dir, storage) = store().await;
    let dispatcher = bus(Arc::new(Recorder::default()), Vec::new());
    let store = AgentStore::new(storage.clone(), 1, dispatcher);
    let instance = storage
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
        .suspend(instance, "approval", 3, "h-1")
        .await
        .expect("start the wait");

    let loaded = storage
        .agent_get(instance)
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(loaded.phase, AgentPhase::Suspended);
    let stored: serde_json::Value =
        serde_json::from_str(loaded.phase_payload.as_deref().expect("read phase payload"))
            .expect("parse");
    assert_eq!(
        stored["handle"],
        serde_json::json!("h-1"),
        "the handle in the Phase payload must match the wait table's primary key, otherwise recovery cannot find the answer"
    );

    let wait = storage
        .wait_get("h-1")
        .await
        .expect("query")
        .expect("write wait row on open");
    assert_eq!(
        wait.instance_id, instance,
        "the wait row must point to the same instance"
    );
    assert_eq!(wait.kind, "approval");
    assert!(
        !wait.is_answered(),
        "a freshly started wait must not have an answer"
    );
}

#[tokio::test]
async fn a_failed_phase_write_leaves_no_wait_behind() {
    let (_dir, storage) = store().await;
    let dispatcher = bus(Arc::new(Recorder::default()), Vec::new());
    let store = AgentStore::new(storage.clone(), 1, dispatcher);

    // 实例行不存在，Phase 写不进去
    let result = store.suspend(999, "approval", 3, "h-orphan").await;
    assert!(
        result.is_err(),
        "initiating must fail when the Phase cannot be written"
    );

    assert!(
        storage.wait_get("h-orphan").await.expect("query").is_none(),
        "the Phase and the wait row share a transaction; if the former fails, no orphan row with a handle but no wait node may be left behind"
    );
}

#[tokio::test]
async fn a_recovered_instance_sees_an_already_answered_wait() {
    let (_dir, store) = store().await;
    let instance = store
        .agent_create(AgentInstanceCreate {
            parent_id: None,
            topic_id: 1,
            role: AgentRole::Main,
            mode: None,
            phase: AgentPhase::Suspended,
            phase_payload: Some("{\"handle\":\"h-1\"}".to_string()),
        })
        .await
        .expect("create instance");
    store
        .wait_open(WaitOpen {
            handle: "h-1".to_string(),
            instance_id: instance,
            kind: "approval".to_string(),
            message_id: 1,
            payload: serde_json::json!({ "pending_call_ids": [] }).clone(),
        })
        .await
        .expect("start");
    store
        .wait_answer("h-1", serde_json::json!({ "decisions": [] }))
        .await
        .expect("answer");

    let answered = store
        .wait_get("h-1")
        .await
        .expect("query")
        .expect("must exist");
    assert!(
        answered.is_answered(),
        "if recovery finds an answered wait, it must resume immediately instead of waiting idly"
    );
}

#[tokio::test]
async fn a_second_wait_on_the_same_instance_is_refused() {
    let (_dir, storage) = store().await;
    let dispatcher = bus(Arc::new(Recorder::default()), Vec::new());
    let store = AgentStore::new(storage.clone(), 1, dispatcher);
    let instance = storage
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
        .suspend(instance, "approval", 3, "h-1")
        .await
        .expect("start");
    let open = storage
        .wait_list_open_by_instance(instance)
        .await
        .expect("query");
    assert_eq!(
        open.len(),
        1,
        "before starting a second one it must be visible that the instance already has an open wait"
    );

    let second = store.suspend(instance, "approval", 3, "h-2").await;
    assert!(
        matches!(second, Err(CoreError::Denied(_))),
        "a second open wait on the same instance must be refused, otherwise it would become a permanent orphan"
    );
    assert_eq!(
        storage
            .wait_list_open_by_instance(instance)
            .await
            .expect("query")
            .len(),
        1,
        "a refused initiation must not leave a second handle behind"
    );
}

#[tokio::test]
async fn a_suspension_publishes_the_calls_pending_on_its_message() {
    let (_dir, storage) = store().await;
    let recorder = Arc::new(Recorder::default());
    let dispatcher = bus(recorder.clone(), vec![sub(CORE_APPROVAL_REQUEST.name)]);
    let store = AgentStore::new(storage.clone(), 1, dispatcher);
    let instance = storage
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
    let message = insert_tool_call(&storage, instance, "call_1", "get_weather").await;

    store
        .suspend(instance, "approval", message, "h-1")
        .await
        .expect("start the wait");

    let payloads = wait_for_recorded(&recorder, CORE_APPROVAL_REQUEST.name, |seen| {
        !seen.is_empty()
    })
    .await;
    assert_eq!(payloads[0]["handle"], serde_json::json!("h-1"));
    let calls = payloads[0]["calls"].as_array().expect("read calls array");
    assert_eq!(
        calls.len(),
        1,
        "every tool call block on the message contributes one entry"
    );
    assert_eq!(calls[0]["call_id"], serde_json::json!("call_1"));
    assert_eq!(calls[0]["tool"], serde_json::json!("get_weather"));
    assert_eq!(calls[0]["arguments"], serde_json::json!({ "city": "SF" }));
    assert_eq!(
        calls[0]["server_name"],
        serde_json::json!(""),
        "server_name has no source in this unit, so it must stay empty rather than being made up"
    );
}

#[tokio::test]
async fn answering_a_wait_resumes_the_instance() {
    let (_dir, storage) = store().await;
    let recorder = Arc::new(Recorder::default());
    let dispatcher = bus(recorder.clone(), vec![sub(CORE_APPROVAL_RESOLVED.name)]);
    let behavior = Arc::new(SuspendingBehavior::default());
    let (store, hub) = hub_with(behavior.clone(), dispatcher, storage.clone());

    // 实例行先写入存储，树的节点 id 与实例 id 按同一顺序对齐
    let instance = storage
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
    let node = spawn_row(&hub, &cx(), None, instance)
        .await
        .expect("create");
    assert_eq!(
        node, instance,
        "the tree node and the instance row must point to the same instance"
    );
    wait_for(&hub, |nodes| {
        matches!(
            nodes.first().map(|node| &node.phase),
            Some(Phase::Suspended(_))
        )
    })
    .await;

    // 树把挂起状态写入存储时，等待行随之出现
    let wait = storage
        .wait_get(&format!("h-{node}"))
        .await
        .expect("query")
        .expect("write wait row on suspend");
    assert!(!wait.is_answered());

    let decisions = vec![CallDecision {
        call_id: "call_1".into(),
        allow: true,
    }];
    store
        .answer(&cx(), &format!("h-{node}"), decisions.clone(), "user")
        .await
        .expect("answer");

    wait_for(&hub, |nodes| {
        nodes.iter().all(|node| node.phase.is_terminal())
    })
    .await;
    let answers = behavior.answers.lock().expect("lock").clone();
    assert_eq!(
        answers.len(),
        1,
        "an answer must resume the instance exactly once"
    );
    assert_eq!(answers[0].handle, format!("h-{node}"));
    assert_eq!(answers[0].decisions, decisions);

    let resolved = wait_for_recorded(&recorder, CORE_APPROVAL_RESOLVED.name, |seen| {
        !seen.is_empty()
    })
    .await;
    assert_eq!(resolved[0]["instance_id"], serde_json::json!(node));
    assert_eq!(
        resolved[0]["handle"],
        serde_json::json!(format!("h-{node}"))
    );
    assert_eq!(resolved[0]["by"], serde_json::json!("user"));
    assert_eq!(
        resolved[0]["decisions"],
        serde_json::json!([{ "call_id": "call_1", "allow": true }]),
        "the resolved event must carry the per-call decisions; an empty list would lose what the user actually approved"
    );
}

#[tokio::test]
async fn a_repeated_answer_has_no_effect_while_an_unknown_handle_is_denied() {
    let (_dir, storage) = store().await;
    let dispatcher = bus(Arc::new(Recorder::default()), Vec::new());
    let behavior = Arc::new(SuspendingBehavior::default());
    let (store, hub) = hub_with(behavior.clone(), dispatcher, storage.clone());

    let instance = storage
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
    let node = spawn_row(&hub, &cx(), None, instance)
        .await
        .expect("create");
    assert_eq!(
        node, instance,
        "the tree node and the instance row must point to the same instance"
    );
    let handle = format!("h-{node}");
    wait_for(&hub, |nodes| {
        matches!(nodes.first().map(|n| &n.phase), Some(Phase::Suspended(_)))
    })
    .await;

    store
        .answer(&cx(), &handle, Vec::new(), "user")
        .await
        .expect("answer first time");
    wait_for(&hub, |nodes| nodes.iter().all(|n| n.phase.is_terminal())).await;
    store
        .answer(&cx(), &handle, Vec::new(), "user")
        .await
        .expect("ignore duplicate answer");
    sleep(Duration::from_millis(30)).await;
    assert_eq!(
        behavior.answers.lock().expect("lock").len(),
        1,
        "a repeated answer must not wake the instance again"
    );

    let unknown = store.answer(&cx(), "nope", Vec::new(), "user").await;
    assert!(
        matches!(unknown, Err(CoreError::Denied(_))),
        "an unknown handle must raise an error, distinguishable from the no-op of a repeated answer"
    );
}

#[tokio::test]
async fn a_retried_answer_resumes_with_the_decision_recorded_earlier() {
    let (_dir, storage) = store().await;
    let dispatcher = bus(Arc::new(Recorder::default()), Vec::new());
    let behavior = Arc::new(SuspendingBehavior::default());
    let (store, hub) = hub_with(behavior.clone(), dispatcher, storage.clone());

    let instance = storage
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
    let node = spawn_row(&hub, &cx(), None, instance)
        .await
        .expect("create");
    let handle = format!("h-{node}");
    wait_for(&hub, |nodes| {
        matches!(nodes.first().map(|n| &n.phase), Some(Phase::Suspended(_)))
    })
    .await;

    // 上次投递在写入答复之后失败，节点没有恢复
    let recorded = vec![CallDecision {
        call_id: "call_1".into(),
        allow: true,
    }];
    storage
        .wait_answer(&handle, serde_json::to_value(&recorded).expect("serialize"))
        .await
        .expect("record the answer");

    // 调用方重试并带来不同的决定；恢复必须使用已记录的那一份
    let retried = vec![CallDecision {
        call_id: "call_1".into(),
        allow: false,
    }];
    store
        .answer(&cx(), &handle, retried, "user")
        .await
        .expect("retry the answer");

    wait_for(&hub, |nodes| nodes.iter().all(|n| n.phase.is_terminal())).await;
    let answers = behavior.answers.lock().expect("lock").clone();
    assert_eq!(
        answers.len(),
        1,
        "a retried answer must resume the instance instead of leaving it suspended"
    );
    assert_eq!(
        answers[0].decisions, recorded,
        "the decision stored by the first attempt is the one that takes effect"
    );
}

#[tokio::test]
async fn recovery_resumes_a_wait_that_was_answered_before_the_restart() {
    let (_dir, storage) = store().await;
    let dispatcher = bus(Arc::new(Recorder::default()), Vec::new());
    let settled = storage
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
    // 一个话题只允许一行 main，第二个实例挂在主实例名下
    let unanswered = storage
        .agent_create(AgentInstanceCreate {
            parent_id: Some(settled),
            topic_id: 1,
            role: AgentRole::Child,
            mode: None,
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("create instance");
    let writer = AgentStore::new(storage.clone(), 1, dispatcher.clone());
    writer
        .suspend(settled, "approval", 1, "h-settled")
        .await
        .expect("start the wait");
    writer
        .suspend(unanswered, "approval", 1, "h-open")
        .await
        .expect("start the wait");
    // 答复已经写入存储，实例尚未恢复就崩溃了
    let decisions = vec![CallDecision {
        call_id: "call_1".into(),
        allow: true,
    }];
    storage
        .wait_answer(
            "h-settled",
            serde_json::to_value(&decisions).expect("serialize"),
        )
        .await
        .expect("answer");

    // 重启后空树从存储中恢复。已答复的等待必须自动续行
    let behavior = Arc::new(SuspendingBehavior::default());
    let (store, hub) = hub_with(behavior.clone(), dispatcher, storage.clone());
    store.recover().await.expect("recover");

    wait_for(&hub, |nodes| {
        nodes
            .iter()
            .any(|node| node.id == settled && node.phase.is_terminal())
    })
    .await;
    let answers = behavior.answers.lock().expect("lock").clone();
    assert_eq!(
        answers.len(),
        1,
        "an answered wait must resume automatically during recovery instead of waiting idly"
    );
    assert_eq!(answers[0].handle, "h-settled");
    assert_eq!(answers[0].decisions, decisions);

    let nodes = hub.snapshot().await.expect("query");
    let still_waiting = nodes
        .iter()
        .find(|node| node.id == unanswered)
        .expect("keep instance in tree");
    assert!(
        matches!(still_waiting.phase, Phase::Suspended(_)),
        "an unanswered wait must remain suspended and must not be woken by mistake by the recovery path"
    );
}

#[tokio::test]
async fn the_tree_path_announces_the_wait_it_persists() {
    let (_dir, storage) = store().await;
    let recorder = Arc::new(Recorder::default());
    let dispatcher = bus(recorder.clone(), vec![sub(CORE_APPROVAL_REQUEST.name)]);
    let behavior = Arc::new(SuspendingBehavior::default());
    let (_store, hub) = hub_with(behavior, dispatcher, storage.clone());
    let instance = storage
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
    let node = spawn_row(&hub, &cx(), None, instance)
        .await
        .expect("create");
    assert_eq!(
        node, instance,
        "the tree node and the instance row must point to the same instance"
    );

    // 本次挂起经过树自己的写入路径，不经过发起入口
    wait_for(&hub, |nodes| {
        matches!(nodes.first().map(|n| &n.phase), Some(Phase::Suspended(_)))
    })
    .await;

    let payloads = wait_for_recorded(&recorder, CORE_APPROVAL_REQUEST.name, |seen| {
        !seen.is_empty()
    })
    .await;
    assert_eq!(
        payloads[0]["instance_id"],
        serde_json::json!(node),
        "a suspension persisted by the tree must also be announced, otherwise nobody knows there is something to answer"
    );
    assert_eq!(
        payloads[0]["handle"],
        serde_json::json!(format!("h-{node}"))
    );
    assert!(
        payloads[0]["calls"].is_array(),
        "the payload must include the call list"
    );
}

/// 只在指定实例上以给定句柄挂起，其余实例运行即完成
struct SuspendOnBehavior {
    /// 要挂起的实例
    instance: i64,
    /// 挂起用的句柄
    handle: String,
}

#[async_trait::async_trait]
impl Behavior<AgentDomain> for SuspendOnBehavior {
    async fn run(&self, _ctx: Ctx, input: Input<AgentDomain>) -> Outcome<AgentDomain> {
        match input {
            Input::Fresh(spec) if spec.instance_id == self.instance => {
                Outcome::Suspend(SuspendedWait {
                    instance_id: spec.instance_id,
                    kind: "approval".into(),
                    message_id: 1,
                    handle: self.handle.clone(),
                    payload: None,
                })
            }
            Input::Fresh(_) | Input::External(_) | Input::Children(_) | Input::Idle(_) => {
                Outcome::Completed(AgentOutcome { message_id: 1 })
            }
        }
    }
}

#[tokio::test]
async fn the_tree_path_refuses_a_handle_that_belongs_to_another_instance() {
    let (_dir, storage) = store().await;
    let dispatcher = bus(Arc::new(Recorder::default()), Vec::new());
    let behavior = Arc::new(SuspendOnBehavior {
        instance: 2,
        handle: "h-shared".into(),
    });
    let (store, hub) = hub_with(behavior, dispatcher, storage.clone());
    let holder = storage
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
    let child_row = storage
        .agent_create(AgentInstanceCreate {
            parent_id: Some(holder),
            topic_id: 1,
            role: AgentRole::Child,
            mode: None,
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("create instance");
    store
        .suspend(holder, "approval", 1, "h-shared")
        .await
        .expect("start the wait");

    let root = spawn_row(&hub, &cx(), None, holder).await.expect("create");
    assert_eq!(
        root, holder,
        "the tree node and the instance row must point to the same instance"
    );
    // 子实例的挂起使用同一个句柄，而该句柄属于主实例
    let node = spawn_row(&hub, &cx(), Some(root), child_row)
        .await
        .expect("create");
    assert_eq!(
        node, child_row,
        "the tree node and the instance row must point to the same instance"
    );
    wait_for(&hub, |nodes| {
        nodes
            .iter()
            .any(|node| node.id == child_row && matches!(node.phase, Phase::Suspended(_)))
    })
    .await;

    let open = storage
        .wait_list_open_by_instance(holder)
        .await
        .expect("query");
    assert_eq!(
        open.len(),
        1,
        "a suspension on another instance must not overwrite an existing wait row"
    );
    assert_eq!(open[0].handle, "h-shared");
    let row = storage
        .agent_get(node)
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(
        row.phase,
        AgentPhase::Running,
        "when the handle belongs to another instance, this write must fail as a whole and must not repoint the Phase"
    );
}

#[tokio::test]
async fn the_tree_path_refuses_a_second_open_wait_on_the_same_instance() {
    let (_dir, storage) = store().await;
    let dispatcher = bus(Arc::new(Recorder::default()), Vec::new());
    let behavior = Arc::new(SuspendOnBehavior {
        instance: 1,
        handle: "h-new".into(),
    });
    let (_store, hub) = hub_with(behavior, dispatcher, storage.clone());
    let instance = storage
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
    // 该实例上已经有一个未决等待
    storage
        .wait_open(WaitOpen {
            handle: "h-old".to_string(),
            instance_id: instance,
            kind: "approval".to_string(),
            message_id: 1,
            payload: serde_json::json!({ "pending_call_ids": [] }).clone(),
        })
        .await
        .expect("start the wait");

    let node = spawn_row(&hub, &cx(), None, instance)
        .await
        .expect("create");
    assert_eq!(
        node, instance,
        "the tree node and the instance row must point to the same instance"
    );
    wait_for(&hub, |nodes| {
        matches!(
            nodes.first().map(|node| &node.phase),
            Some(Phase::Suspended(_))
        )
    })
    .await;

    let open = storage
        .wait_list_open_by_instance(instance)
        .await
        .expect("query");
    assert_eq!(
        open.len(),
        1,
        "a second open wait on the same instance must be refused"
    );
    assert_eq!(open[0].handle, "h-old");
    let row = storage
        .agent_get(instance)
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(
        row.phase,
        AgentPhase::Running,
        "a refused write must not change the Phase to a suspension pointing at the second handle"
    );
}

#[tokio::test]
async fn a_failed_wait_payload_leaves_no_half_suspension() {
    let (_dir, storage) = store().await;
    let dispatcher = bus(Arc::new(Recorder::default()), Vec::new());
    let behavior = Arc::new(SuspendingBehavior::default());
    let (_store, hub) = hub_with(behavior, dispatcher, storage.clone());
    let instance = storage
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
    // 挂起所在的消息上有一条无法解析的工具调用块，待放行调用清单无法生成
    storage
        .block_insert(BlockRow {
            id: 1,
            message_id: 1,
            seq: 0,
            kind: BlockKind::ToolCall,
            call_id: None,
            revision: 1,
            content: String::new(),
            data: Some(serde_json::json!({ "name": "get_weather" })),
            finished: true,
        })
        .await
        .expect("insert block");

    let node = spawn_row(&hub, &cx(), None, instance)
        .await
        .expect("create");
    assert_eq!(
        node, instance,
        "the tree node and the instance row must point to the same instance"
    );
    wait_for(&hub, |nodes| {
        matches!(
            nodes.first().map(|node| &node.phase),
            Some(Phase::Suspended(_))
        )
    })
    .await;

    assert!(
        storage
            .wait_get(&format!("h-{node}"))
            .await
            .expect("query")
            .is_none(),
        "when the payload cannot be assembled the wait row must not be written, otherwise that suspension would never be announced"
    );
    let row = storage
        .agent_get(instance)
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(
        row.phase,
        AgentPhase::Running,
        "when the payload cannot be assembled the Phase must not be written either; a written suspension would never be answered"
    );
}

#[tokio::test]
async fn a_reused_answered_handle_is_a_loud_collision() {
    let (_dir, storage) = store().await;
    let dispatcher = bus(Arc::new(Recorder::default()), Vec::new());
    let store = AgentStore::new(storage.clone(), 1, dispatcher);
    let instance = storage
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
        .suspend(instance, "approval", 1, "h-1")
        .await
        .expect("start");
    storage
        .wait_answer("h-1", serde_json::json!([]))
        .await
        .expect("answer");
    // 答复之后实例已经续行，句柄不能再用
    storage
        .agent_save_phase(instance, AgentPhase::Running, None)
        .await
        .expect("store the phase");

    let second = store.suspend(instance, "approval", 1, "h-1").await;
    assert!(
        matches!(second, Err(CoreError::Denied(_))),
        "reusing an already answered handle must fail loudly and must not be treated as an idempotent replay"
    );
    let loaded = storage
        .agent_get(instance)
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(
        loaded.phase,
        AgentPhase::Running,
        "a refused initiation must not change the Phase back to a suspension pointing at an answered handle, otherwise the instance would never receive an answer"
    );
}

#[tokio::test]
async fn one_corrupt_answer_does_not_strand_the_other_waiting_instances() {
    let (_dir, storage) = store().await;
    let dispatcher = bus(Arc::new(Recorder::default()), Vec::new());
    let broken = storage
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
    let sound = storage
        .agent_create(AgentInstanceCreate {
            parent_id: Some(broken),
            topic_id: 1,
            role: AgentRole::Child,
            mode: None,
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("create instance");
    let writer = AgentStore::new(storage.clone(), 1, dispatcher.clone());
    writer
        .suspend(broken, "approval", 1, "a-broken")
        .await
        .expect("start");
    writer
        .suspend(sound, "approval", 1, "b-sound")
        .await
        .expect("start");
    storage
        .wait_answer("a-broken", serde_json::json!("not a list of decisions"))
        .await
        .expect("answer");
    storage
        .wait_answer(
            "b-sound",
            serde_json::to_value(vec![CallDecision {
                call_id: "call_1".into(),
                allow: true,
            }])
            .expect("serialize"),
        )
        .await
        .expect("answer");

    let behavior = Arc::new(SuspendingBehavior::default());
    let (store, hub) = hub_with(behavior.clone(), dispatcher, storage.clone());
    store
        .recover()
        .await
        .expect("recover past malformed answer");

    wait_for(&hub, |nodes| {
        nodes
            .iter()
            .any(|node| node.id == sound && node.phase.is_terminal())
    })
    .await;
    let nodes = hub.snapshot().await.expect("query");
    let stuck = nodes
        .iter()
        .find(|node| node.id == broken)
        .expect("find instance in tree");
    assert!(
        matches!(stuck.phase, Phase::Suspended(_)),
        "a corrupt answer must only skip itself; the other instances resume as usual"
    );
}

/// 首次启动需等待一个信号再挂起，供两条路径交叠的用例固定顺序
struct GatedBehavior {
    /// 放行首次挂起
    gate: Arc<Notify>,
    /// 每次收到的答复
    answers: Mutex<Vec<WaitAnswer>>,
}

#[async_trait::async_trait]
impl Behavior<AgentDomain> for GatedBehavior {
    async fn run(&self, _ctx: Ctx, input: Input<AgentDomain>) -> Outcome<AgentDomain> {
        match input {
            Input::Fresh(spec) => {
                self.gate.notified().await;
                Outcome::Suspend(SuspendedWait {
                    instance_id: spec.instance_id,
                    kind: "approval".into(),
                    message_id: 1,
                    handle: format!("h-{}", spec.instance_id),
                    payload: None,
                })
            }
            Input::External(answer) => {
                if let Ok(mut answers) = self.answers.lock() {
                    answers.push(answer);
                }
                Outcome::Completed(AgentOutcome { message_id: 1 })
            }
            Input::Children(_) | Input::Idle(_) => {
                Outcome::Completed(AgentOutcome { message_id: 1 })
            }
        }
    }
}

#[tokio::test]
async fn one_suspension_is_announced_once_across_both_paths() {
    let (_dir, storage) = store().await;
    let recorder = Arc::new(Recorder::default());
    let dispatcher = bus(recorder.clone(), vec![sub(CORE_APPROVAL_REQUEST.name)]);
    let gate = Arc::new(Notify::new());
    let behavior = Arc::new(GatedBehavior {
        gate: gate.clone(),
        answers: Mutex::new(Vec::new()),
    });
    let (store, hub) = hub_with(behavior, dispatcher, storage.clone());
    let instance = storage
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
    let node = spawn_row(&hub, &cx(), None, instance)
        .await
        .expect("create");
    assert_eq!(
        node, instance,
        "the tree node and the instance row must point to the same instance"
    );
    let handle = format!("h-{node}");

    // 发起入口先写入并先上报，树随后写入同一条挂起。树此时应保持沉默
    store
        .suspend(instance, "approval", 1, &handle)
        .await
        .expect("start");
    gate.notify_one();

    wait_for(&hub, |nodes| {
        matches!(nodes.first().map(|n| &n.phase), Some(Phase::Suspended(_)))
    })
    .await;
    // 让树的路径完成写入与宣告，再计数一次
    sleep(Duration::from_millis(50)).await;
    assert_eq!(
        recorder.payloads_of(CORE_APPROVAL_REQUEST.name).len(),
        1,
        "the same suspension reported once by each of the two paths would show subscribers two pending waits"
    );
}

// 实际运行 `AgentRunner` 的夹具。模型往返、放行与挂起都经过链上的对应路径

/// 一次应答，包含实体与声明长度
struct RunnerReply {
    /// 写回去的字节
    body: String,
    /// 头部声明的长度
    declared: usize,
}

impl RunnerReply {
    /// 构造一段完整应答
    fn whole(body: String) -> Self {
        let declared = body.len();
        Self { body, declared }
    }
}

/// 流结束的哨兵行
const DONE_LINE: &str = "data: [DONE]\n\n";

/// 一片增量拼成的数据行
fn runner_delta_line(delta: serde_json::Value) -> String {
    let chunk = serde_json::json!({
        "id": "x",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "deepseek-chat",
        "choices": [{ "index": 0, "delta": delta }],
    });
    format!("data: {chunk}\n\n")
}

/// 构造一段文本增量的 SSE
fn runner_text_reply(text: &str) -> RunnerReply {
    RunnerReply::whole(format!(
        "{}{DONE_LINE}",
        runner_delta_line(serde_json::json!({ "role": "assistant", "content": text }))
    ))
}

/// 构造一批工具调用增量的 SSE，一条调用占一片
fn runner_calls_reply(calls: &[(&str, &str, &str)]) -> RunnerReply {
    let body: String = calls
        .iter()
        .map(|(id, name, arguments)| {
            runner_delta_line(serde_json::json!({
                "role": "assistant",
                "tool_calls": [{
                    "index": 0,
                    "id": id,
                    "type": "function",
                    "function": { "name": name, "arguments": arguments },
                }],
            }))
        })
        .collect();
    RunnerReply::whole(format!("{body}{DONE_LINE}"))
}

/// 一个按请求次数给出不同响应体的最小 HTTP 应答器
#[derive(Clone)]
struct RunnerServer {
    /// 收到的请求次数
    requests: Arc<AtomicUsize>,
    /// 监听的基地址
    base_url: String,
}

impl RunnerServer {
    /// 服务的基地址
    fn base_url(&self) -> String {
        self.base_url.clone()
    }

    /// 收到过几次请求
    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    /// 启动服务并逐个处理连接
    async fn serve(replies: Vec<RunnerReply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
        let base_url = format!(
            "http://{}",
            listener.local_addr().expect("has a local address")
        );
        let requests = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&requests);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let index = counted.fetch_add(1, Ordering::SeqCst);
                let reply = replies.get(index).or_else(|| replies.last());
                let _ = runner_read_request(&mut socket).await;
                if let Some(reply) = reply {
                    let _ = runner_respond(&mut socket, &reply.body, reply.declared).await;
                }
                let _ = socket.shutdown().await;
            }
        });
        Self { requests, base_url }
    }
}

/// 把一次请求读完，头部与实体都读掉
async fn runner_read_request(socket: &mut TcpStream) -> Option<String> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(end) = runner_head_end(&buffer) {
            let head = String::from_utf8_lossy(&buffer[..end]).into_owned();
            let wanted = end + runner_body_len(&head);
            if buffer.len() >= wanted {
                return Some(String::from_utf8_lossy(&buffer[end..wanted]).into_owned());
            }
        }
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

/// 头部结束的位置，即空行之后的偏移
fn runner_head_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|at| at + 4)
}

/// 头部里声明的请求体长度
fn runner_body_len(head: &str) -> usize {
    head.lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .find_map(|(_, value)| value.trim().parse().ok())
        .unwrap_or(0)
}

/// 写回一次应答。先写头部再写实体，随后关闭连接
async fn runner_respond(socket: &mut TcpStream, body: &str, declared: usize) -> io::Result<()> {
    let head =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {declared}\r\nConnection: close\r\n\r\n");
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    socket.flush().await
}

/// 在 `core/llm.toolcall` 上点名挂起给定调用、其余一律放行的宿主
#[derive(Default)]
struct SuspendingHost {
    /// 要挂起的调用标识
    suspended: Vec<String>,
    /// 要从这一批里删掉的调用标识
    dropped: Vec<String>,
    /// 挂起调用改写后的参数
    arguments: Option<serde_json::Value>,
}

impl SuspendingHost {
    /// 按点名清单构造一个宿主
    fn new(suspended: &[String]) -> Self {
        Self {
            suspended: suspended.to_vec(),
            dropped: Vec::new(),
            arguments: None,
        }
    }

    /// 挂起给定的调用，并删掉另一批调用
    fn dropping(suspended: &[&str], dropped: &[&str]) -> Self {
        Self {
            suspended: suspended.iter().map(|id| (*id).to_string()).collect(),
            dropped: dropped.iter().map(|id| (*id).to_string()).collect(),
            arguments: None,
        }
    }

    /// 挂起给定的调用，并改写它们的参数
    fn rewriting(suspended: &[&str], arguments: serde_json::Value) -> Self {
        Self {
            suspended: suspended.iter().map(|id| (*id).to_string()).collect(),
            dropped: Vec::new(),
            arguments: Some(arguments),
        }
    }
}

#[async_trait::async_trait]
impl ModHost for SuspendingHost {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        if sub.event != "core/llm.toolcall" {
            return Ok(None);
        }
        let suspended: Vec<String> = payload["calls"]
            .as_array()
            .map(|calls| {
                calls
                    .iter()
                    .filter_map(|call| call["id"].as_str())
                    .filter(|id| self.suspended.iter().any(|suspended| suspended == id))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        if suspended.is_empty() && self.dropped.is_empty() {
            return Ok(None);
        }
        let mut rewritten = payload;
        rewritten["suspended"] = serde_json::json!(suspended);
        if let Some(calls) = rewritten["calls"].as_array_mut() {
            calls.retain(|call| {
                !self
                    .dropped
                    .iter()
                    .any(|id| call["id"].as_str() == Some(id))
            });
            if let Some(arguments) = &self.arguments {
                let text = serde_json::to_string(arguments).expect("serialize the rewrite");
                for call in calls.iter_mut() {
                    if call["id"]
                        .as_str()
                        .is_some_and(|id| self.suspended.iter().any(|suspended| suspended == id))
                    {
                        call["arguments"] = serde_json::json!(text);
                    }
                }
            }
        }
        Ok(Some(rewritten))
    }
}

/// 按工具名声明工具、并记录每次调用的 `McpTools` 实现
struct RunnerMcp {
    /// 本服务里可用的工具
    declared: Vec<DeclaredTool>,
    /// 被调用过的调用标识
    called: Mutex<Vec<String>>,
}

impl RunnerMcp {
    /// 按 (服务名, 工具名) 构造一份声明
    fn declaring(tools: &[(&'static str, &'static str)]) -> Self {
        let declared = tools
            .iter()
            .map(|(server, tool)| DeclaredTool {
                origin: ToolOrigin::Mcp {
                    server: (*server).to_string(),
                    tool: (*tool).to_string(),
                },
                declaration: Tools::Function(FunctionTool {
                    name: format!("{server}__{tool}"),
                    description: None,
                    parameters: None,
                    strict: None,
                }),
            })
            .collect();
        Self {
            declared,
            called: Mutex::new(Vec::new()),
        }
    }

    /// 被调用过的调用标识
    fn called(&self) -> Vec<String> {
        self.called.lock().expect("lock").clone()
    }
}

#[async_trait::async_trait]
impl wind_core::llm::ports::McpTools for RunnerMcp {
    async fn declarations(
        &self,
        _cx: &DispatchCx,
        _topic_id: i64,
    ) -> Result<Vec<DeclaredTool>, CoreError> {
        Ok(self.declared.clone())
    }

    async fn call(
        &self,
        _cx: &DispatchCx,
        call_id: &str,
        _server: &str,
        tool: &str,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        self.called.lock().expect("lock").push(call_id.to_string());
        Ok(serde_json::json!({ "tool": tool, "arguments": arguments }))
    }
}

/// 不声明任何片段与工具的 `TopicMods`
struct RunnerMods;

#[async_trait::async_trait]
impl TopicMods for RunnerMods {
    async fn prompt_fragments(&self, _topic_id: i64) -> Result<Vec<String>, CoreError> {
        Ok(Vec::new())
    }

    async fn agent_tools(&self, _topic_id: i64) -> Result<Vec<DeclaredTool>, CoreError> {
        Ok(Vec::new())
    }

    async fn version_of(&self, _mod_name: &str) -> Result<i64, CoreError> {
        Ok(1)
    }
}

/// 不会被调用的 `ModRequests` 实现
struct RunnerRequests;

#[async_trait::async_trait]
impl ModRequests for RunnerRequests {
    async fn call(
        &self,
        _scope: &Scope,
        _mod_name: &str,
        _version: i64,
        _request: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        Ok(input)
    }
}

/// 三条链共用的目录。含 agent 的挂起观测、模型往返与会话的块事件
fn runner_directory() -> Directory {
    directory()
        .merge(wind_core::llm::directory())
        .merge(wind_core::conversation::directory())
}

/// 只对 `core/llm.toolcall` 建一条拦截订阅。只有存在订阅者时派发才会经过宿主
fn runner_subscriptions() -> Vec<Subscription> {
    vec![Subscription {
        mod_name: "watcher".into(),
        mod_version: 1,
        event: "core/llm.toolcall",
        phase: Some(SubPhase::Guard),
        handler: "onEvent".into(),
        timeout_ms: 1000,
        registration_seq: 0,
    }]
}

/// 一个运行真实 `AgentRunner` 的夹具
struct LiveRig {
    /// 临时目录须被持有。一被丢弃，库文件随之删除
    _dir: Arc<TempDir>,
    /// 存储，用来读等待行与工具结果
    storage: Storage,
    /// 被驱动的 `AgentRunner`
    runner: AgentRunner,
    /// 模型应答的来源
    server: RunnerServer,
    /// 在本轮里挂起指定调用的宿主
    host: Arc<SuspendingHost>,
    /// 树存储，用来把挂起写成等待行
    store: Arc<AgentStore>,
    /// MCP 侧，用来确认某次调用是否执行
    mcp: Arc<RunnerMcp>,
    /// 本轮的实例标识
    instance_id: i64,
    /// 本轮的话题
    topic_id: i64,
}

/// 一次挂起的对外视图
struct WaitView {
    /// 等待行上的待放行调用标识
    calls: Vec<String>,
    /// 等待句柄
    handle: String,
}

impl LiveRig {
    /// 在一个已建好的存储上装配运行者
    async fn over(
        dir: Arc<TempDir>,
        storage: Storage,
        server: RunnerServer,
        host: Arc<SuspendingHost>,
        mcp: Arc<RunnerMcp>,
        instance_id: i64,
        topic_id: i64,
    ) -> Self {
        let bus = Arc::new(Dispatcher::new(
            runner_directory(),
            Arc::new(AllEnabled),
            host.clone(),
            Limits::default(),
        ));
        bus.rebuild_index(runner_subscriptions());
        let hub = FeedHub::open(storage.clone(), None)
            .await
            .expect("open hub");
        let runner = AgentRunner::new(
            BlockWriter::new(
                hub,
                bus.clone(),
                storage.clone(),
                ProjectionInvalidations::default(),
            ),
            UseCases::new(storage.clone(), bus.clone()),
            bus.clone(),
            Arc::new(RunnerMods),
            Some(mcp.clone()),
            Arc::new(RunnerRequests),
            storage.clone(),
            DEFAULT_MAX_ROUNDS,
        );
        let store = Arc::new(AgentStore::new(storage.clone(), topic_id, bus));
        Self {
            _dir: dir,
            storage,
            runner,
            server,
            host,
            store,
            mcp,
            instance_id,
            topic_id,
        }
    }

    /// 建一套夹具。含临时库、真实总线、假接口与按次数应答的服务
    async fn build(
        replies: Vec<RunnerReply>,
        tools: &[(&'static str, &'static str)],
        suspended: &[&str],
    ) -> Self {
        let suspended: Vec<String> = suspended.iter().map(|id| (*id).to_string()).collect();
        Self::build_with_host(replies, tools, Arc::new(SuspendingHost::new(&suspended))).await
    }

    /// 用给定的宿主建同一套夹具
    async fn build_with_host(
        replies: Vec<RunnerReply>,
        tools: &[(&'static str, &'static str)],
        host: Arc<SuspendingHost>,
    ) -> Self {
        let (dir, storage) = store().await;
        let dir = Arc::new(dir);
        let server = RunnerServer::serve(replies).await;
        let provider = storage
            .provider_create(ProviderCreate {
                name: "deepseek".into(),
                base_url: server.base_url(),
                ..Default::default()
            })
            .await
            .expect("create provider");
        let model = storage
            .model_create(ModelCreate::new(
                provider,
                "deepseek-chat".into(),
                AdapterType::OpenAICompletion,
            ))
            .await
            .expect("build model");
        let topic_id = storage
            .topic_create(TopicCreate {
                label: "test".into(),
                ..Default::default()
            })
            .await
            .expect("create topic");
        sqlx::query("UPDATE topics SET model_id = ? WHERE id = ?")
            .bind(model)
            .bind(topic_id)
            .execute(storage.pool())
            .await
            .expect("bind the model");
        // 空名单让两条调用都必须经过 `core/llm.toolcall`，挂起才有位置发生
        storage
            .topic_update(
                topic_id,
                TopicUpdate {
                    tool_approval_policy: Some(AllowPolicy { allow: Vec::new() }),
                    ..Default::default()
                },
            )
            .await
            .expect("update allow list");
        let instance_id = storage
            .agent_create(AgentInstanceCreate {
                parent_id: None,
                topic_id,
                role: AgentRole::Main,
                mode: None,
                phase: AgentPhase::Running,
                phase_payload: None,
            })
            .await
            .expect("create instance");
        let mcp = Arc::new(RunnerMcp::declaring(tools));
        Self::over(dir, storage, server, host, mcp, instance_id, topic_id).await
    }

    /// 模拟一次重启。在同一份存储上另起一个运行者
    async fn restart(&self) -> Self {
        Self::over(
            Arc::clone(&self._dir),
            self.storage.clone(),
            self.server.clone(),
            self.host.clone(),
            self.mcp.clone(),
            self.instance_id,
            self.topic_id,
        )
        .await
    }

    /// 执行一轮，挂起时把等待行与 `Phase` 一并写入存储
    async fn run_one_round(&self) -> Result<Outcome<AgentDomain>, String> {
        let ctx = Ctx::new(self.instance_id, 1, CancellationToken::new());
        let input = Input::Fresh(AgentSpec {
            instance_id: self.instance_id,
            message_id: None,
            pending_id: None,
            turn_id: None,
        });
        let outcome = match self.runner.run(ctx, input).await {
            Outcome::Failed(failure) => return Err(failure.error),
            outcome => outcome,
        };
        if let Outcome::Suspend(wait) = &outcome {
            self.store
                .save(self.instance_id, &Phase::Suspended(wait.clone()))
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(outcome)
    }

    /// 当前未决等待行
    async fn open_wait(&self) -> WaitView {
        let wait = self
            .storage
            .wait_list_open_by_instance(self.instance_id)
            .await
            .expect("query")
            .pop()
            .expect("write wait row on suspend");
        let calls = wait
            .payload
            .get("pending_call_ids")
            .and_then(serde_json::Value::as_array)
            .map(|ids| {
                ids.iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        WaitView {
            calls,
            handle: wait.handle,
        }
    }

    /// 答复当前等待并让实例续行
    async fn answer(&self, call_id: &str, allow: bool) -> Result<Outcome<AgentDomain>, String> {
        let wait = self.open_wait().await;
        let decisions = vec![CallDecision {
            call_id: call_id.to_string(),
            allow,
        }];
        self.storage
            .wait_answer(
                &wait.handle,
                serde_json::to_value(&decisions).map_err(|error| error.to_string())?,
            )
            .await
            .map_err(|error| error.to_string())?;
        let ctx = Ctx::new(self.instance_id, 1, CancellationToken::new());
        let input = Input::External(WaitAnswer {
            handle: wait.handle,
            decisions,
        });
        match self.runner.run(ctx, input).await {
            Outcome::Failed(failure) => Err(failure.error),
            outcome => Ok(outcome),
        }
    }

    /// 某条调用最后一次写入存储的工具结果
    async fn last_result_of(&self, call_id: &str) -> serde_json::Value {
        let turns = self
            .storage
            .turn_list_by_instance(self.instance_id)
            .await
            .expect("read this instance's turn");
        let turn_ids: Vec<i64> = turns.iter().map(|turn| turn.id).collect();
        let messages = self
            .storage
            .message_list_by_turns(&turn_ids)
            .await
            .expect("read messages of turn");
        let message_ids: Vec<i64> = messages.iter().map(|message| message.id).collect();
        self.storage
            .block_list_by_messages(&message_ids)
            .await
            .expect("read the committed block")
            .into_iter()
            .filter(|block| block.kind == BlockKind::ToolResult)
            .rfind(|block| block.call_id.as_deref() == Some(call_id))
            .and_then(|block| block.data)
            .expect("find tool result block")
    }

    /// 本话题的实例数
    async fn instance_count(&self) -> usize {
        self.storage
            .agent_list_by_topic(self.topic_id)
            .await
            .expect("read instance")
            .len()
    }

    /// 模型被请求了几次
    fn rounds(&self) -> usize {
        self.server.requests()
    }
}

/// 一条调用放行、另一条被点名为挂起的夹具
async fn rig_suspending(suspended: &str) -> LiveRig {
    LiveRig::build(
        vec![
            runner_calls_reply(&[
                ("c-1", "fs__read", r#"{"path":"/tmp/a"}"#),
                ("c-2", "fs__write", r#"{"path":"/tmp/b"}"#),
            ]),
            runner_text_reply("ok"),
        ],
        &[("fs", "read"), ("fs", "write")],
        &[suspended],
    )
    .await
}

/// 同批既有派生又有挂起的夹具
async fn rig_with_a_spawn_and_a_suspended_call() -> LiveRig {
    LiveRig::build(
        vec![
            runner_calls_reply(&[
                (
                    "c-1",
                    "windai_spawn_agent",
                    r#"{"mode":"sync","task":"weather"}"#,
                ),
                ("c-2", "fs__write", r#"{"path":"/tmp/b"}"#),
            ]),
            runner_text_reply("ok"),
        ],
        &[("fs", "write")],
        &["c-2"],
    )
    .await
}

#[tokio::test]
async fn a_suspension_suspends_the_batch_and_the_wait_row_names_the_calls() {
    let rig = rig_suspending("c-2").await;
    let outcome = rig.run_one_round().await.expect("run to completion");
    assert!(matches!(outcome, Outcome::Suspend(_)));
    assert_eq!(rig.open_wait().await.calls, vec!["c-2".to_string()]);
    let called = rig.mcp.called();
    assert!(
        called.contains(&"c-1".to_string()),
        "an allowed call has already executed at suspension time; it must not be held back along with pending calls"
    );
    assert!(
        !called.contains(&"c-2".to_string()),
        "a pending call must not execute before it is answered"
    );
    assert_ne!(
        rig.last_result_of("c-1").await["refused"],
        true,
        "the result of an allowed call must be folded back at suspension time, not wait for the user's answer"
    );
}

#[tokio::test]
async fn answering_allow_runs_the_call_and_a_refusal_marks_it() {
    let rig = rig_suspending("c-2").await;
    let _ = rig.run_one_round().await.expect("run to completion");
    rig.answer("c-2", false).await.expect("answer");
    assert_eq!(rig.last_result_of("c-2").await["refused"], true);
}

#[tokio::test]
async fn answering_allow_executes_the_suspended_call() {
    let rig = rig_suspending("c-2").await;
    let _ = rig.run_one_round().await.expect("run to completion");
    rig.answer("c-2", true).await.expect("answer");
    let called = rig.mcp.called();
    assert!(
        called.contains(&"c-2".to_string()),
        "an allowed call must be executed"
    );
    assert_eq!(
        called.iter().filter(|id| *id == "c-1").count(),
        1,
        "an allowed call already executed at suspension time must not be replayed on recovery"
    );
    assert_ne!(
        rig.last_result_of("c-2").await["refused"],
        true,
        "an allowed call must not be folded into a refusal"
    );
}

#[tokio::test]
async fn the_rewritten_arguments_are_what_executes_after_the_answer() {
    let rig = LiveRig::build_with_host(
        vec![
            runner_calls_reply(&[("c-1", "fs__read", r#"{"path":"/tmp/a"}"#)]),
            runner_text_reply("ok"),
        ],
        &[("fs", "read")],
        Arc::new(SuspendingHost::rewriting(
            &["c-1"],
            serde_json::json!({ "path": "/tmp/rewritten" }),
        )),
    )
    .await;

    let outcome = rig.run_one_round().await.expect("run to completion");
    assert!(matches!(outcome, Outcome::Suspend(_)));

    rig.answer("c-1", true).await.expect("answer");
    let result = rig.last_result_of("c-1").await;
    assert_eq!(
        result["arguments"]["path"],
        serde_json::json!("/tmp/rewritten"),
        "the approved call executes the rewritten arguments, not the original ones"
    );
}

#[tokio::test]
async fn a_call_the_mod_removed_never_executes_after_the_answer() {
    let rig = LiveRig::build_with_host(
        vec![
            runner_calls_reply(&[
                ("c-1", "fs__read", r#"{"path":"/tmp/a"}"#),
                ("c-2", "fs__write", r#"{"path":"/tmp/b"}"#),
            ]),
            runner_text_reply("ok"),
        ],
        &[("fs", "read"), ("fs", "write")],
        Arc::new(SuspendingHost::dropping(&["c-2"], &["c-1"])),
    )
    .await;

    let outcome = rig.run_one_round().await.expect("run to completion");
    assert!(matches!(outcome, Outcome::Suspend(_)));
    assert_eq!(rig.open_wait().await.calls, vec!["c-2".to_string()]);

    rig.answer("c-2", true).await.expect("answer");
    let called = rig.mcp.called();
    assert!(
        called.contains(&"c-2".to_string()),
        "the approved call must run"
    );
    assert!(
        !called.contains(&"c-1".to_string()),
        "a call the mod removed must never execute, even after the answer"
    );
}

#[tokio::test]
async fn an_answer_after_a_restart_still_resumes_the_batch() {
    let rig = rig_suspending("c-2").await;
    let _ = rig.run_one_round().await.expect("run to completion");

    let restarted = rig.restart().await;
    restarted.answer("c-2", true).await.expect("answer");
    let called = restarted.mcp.called();
    assert!(
        called.contains(&"c-2".to_string()),
        "the answer and the pending call list are both in the wait row, so it can still resume after a restart"
    );
    assert_eq!(
        called.iter().filter(|id| *id == "c-1").count(),
        1,
        "the already-executed list in the payload must also survive a restart, so allowed calls are not replayed"
    );
}

#[tokio::test]
async fn a_mixed_spawn_and_pending_batch_suspends_before_anything_runs() {
    let rig = rig_with_a_spawn_and_a_suspended_call().await;
    let outcome = rig.run_one_round().await.expect("run to completion");
    assert!(
        matches!(outcome, Outcome::Suspend(_)),
        "an Outcome has exactly one shape: if the batch has a suspension, the whole batch suspends"
    );
    assert_eq!(
        rig.instance_count().await,
        1,
        "no child instance may be spawned before the answer"
    );
    assert!(
        rig.mcp.called().is_empty(),
        "no call may execute before the answer"
    );

    let outcome = rig.answer("c-2", true).await.expect("answer");
    let Outcome::Spawn { spawn } = outcome else {
        panic!("expected a spawn result after recovery");
    };
    assert_eq!(
        spawn.len(),
        1,
        "that spawn call must only take effect after the answer"
    );
    assert_eq!(
        rig.instance_count().await,
        2,
        "the child instance row is created by the spawn"
    );
    assert!(
        rig.mcp.called().contains(&"c-2".to_string()),
        "pending calls allowed in the same batch must also execute"
    );
    assert_eq!(
        rig.rounds(),
        1,
        "after the spawned instance finishes, the model must not be asked again"
    );
}

#[tokio::test]
async fn repeating_an_initiation_does_not_announce_again() {
    let (_dir, storage) = store().await;
    let recorder = Arc::new(Recorder::default());
    let dispatcher = bus(recorder.clone(), vec![sub(CORE_APPROVAL_REQUEST.name)]);
    let store = AgentStore::new(storage.clone(), 1, dispatcher);
    let instance = storage
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
        .suspend(instance, "approval", 1, "h-1")
        .await
        .expect("start");
    store
        .suspend(instance, "approval", 1, "h-1")
        .await
        .expect("re-open same handle");

    wait_for_recorded(&recorder, CORE_APPROVAL_REQUEST.name, |seen| {
        !seen.is_empty()
    })
    .await;
    sleep(Duration::from_millis(30)).await;
    assert_eq!(
        recorder.payloads_of(CORE_APPROVAL_REQUEST.name).len(),
        1,
        "the call that opened no new wait row must stay silent; a repeated announcement would show subscribers two pending waits"
    );
}
