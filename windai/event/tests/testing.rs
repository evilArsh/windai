use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wind_event::context::DispatchCx;
use wind_event::def::{Directory, EventDef, EventMeta, Mount, Shape};
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{EventSeq, Origin, Scope, Source};
use wind_event::host::{CallCx, CallMode, HostError, ModEnablement, ModHost};
use wind_event::observer::BuiltinObserver;
use wind_event::pool::{DeliverySink, DispatchNotifier, Job, Notice, ObservePool, job_from};
use wind_event::subscription::{EnabledSet, Phase, Subscription};
use wind_event::tracking::{DeliveryOutcome, DeliveryRecord, DeliveryTarget};

/// 假 host 的应答脚本
#[derive(Debug, Clone)]
pub enum Reply {
    /// 不判定
    Pass,
    /// 改写为给定的值
    Rewrite(serde_json::Value),
    /// 睡满这么久，用于触发超时
    Sleep(Duration),
    /// 处理函数抛错
    Fail(String),
    /// 宿主自身出错
    HostFailure(String),
}

/// 用 Rust 写的假 host
///
/// 克隆共享同一份脚本与调用记录，便于派发后回看 host 收到了什么
#[derive(Default, Clone)]
pub struct FakeHost {
    /// (mod 名, 处理函数名) 到应答脚本
    script: Arc<Mutex<HashMap<(String, String), Reply>>>,
    /// 按发生顺序记录的调用
    calls: Arc<Mutex<Vec<(String, String)>>>,
    /// 与 calls 同序的调用模式
    modes: Arc<Mutex<Vec<CallMode>>>,
    /// 与 calls 同序的作用域
    scopes: Arc<Mutex<Vec<Scope>>>,
    /// 与 calls 同序的载荷
    payloads: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl FakeHost {
    pub fn new() -> Self {
        Self::default()
    }

    /// 声明某个处理函数如何应答，未声明的按 Pass 处理
    pub fn on(self, mod_name: &str, handler: &str, reply: Reply) -> Self {
        self.script
            .lock()
            .expect("lock the script table")
            .insert((mod_name.into(), handler.into()), reply);
        self
    }

    pub fn calls(&self) -> Vec<(String, String)> {
        self.calls.lock().expect("lock the calls").clone()
    }

    pub fn call_modes(&self) -> Vec<CallMode> {
        self.modes.lock().expect("lock the modes").clone()
    }

    pub fn call_scopes(&self) -> Vec<Scope> {
        self.scopes.lock().expect("lock the scopes").clone()
    }

    pub fn call_payloads(&self) -> Vec<serde_json::Value> {
        self.payloads.lock().expect("lock the payloads").clone()
    }

    pub fn call_count(&self) -> usize {
        self.calls.lock().expect("lock the calls").len()
    }
}

#[async_trait::async_trait]
impl ModHost for FakeHost {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        self.calls
            .lock()
            .expect("lock the calls")
            .push((sub.mod_name.clone(), sub.handler.clone()));
        self.modes.lock().expect("lock the modes").push(cx.mode);
        self.scopes
            .lock()
            .expect("lock the scopes")
            .push(cx.scope.clone());
        self.payloads
            .lock()
            .expect("lock the payloads")
            .push(payload);

        let reply = self
            .script
            .lock()
            .expect("lock the script table")
            .get(&(sub.mod_name.clone(), sub.handler.clone()))
            .cloned()
            .unwrap_or(Reply::Pass);

        match reply {
            Reply::Pass => Ok(None),
            Reply::Rewrite(v) => Ok(Some(v)),
            Reply::Fail(msg) => Err(HostError::Handler(msg)),
            Reply::HostFailure(msg) => Err(HostError::Internal(msg)),
            Reply::Sleep(d) => {
                tokio::time::sleep(d).await;
                Ok(None)
            }
        }
    }
}

/// 假的启用集
pub struct FakeEnablement(pub EnabledSet);

impl FakeEnablement {
    pub fn all() -> Self {
        Self(EnabledSet::all())
    }

    pub fn none() -> Self {
        Self(EnabledSet::none())
    }

    pub fn only(names: &[&str]) -> Self {
        Self(EnabledSet::from_names(names.iter().map(|s| s.to_string())))
    }
}

impl ModEnablement for FakeEnablement {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        self.0.clone()
    }
}

/// 轮询等待条件成立，最多等 1 秒
///
/// observe 走后台 worker，投递完成没有同步信号，只能轮询
pub async fn wait_for(mut cond: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("timed out waiting for the condition");
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ToolCall {
    tool: String,
    args: serde_json::Value,
}

fn schema() -> serde_json::Value {
    serde_json::json!({ "type": "object" })
}

const TOOL_CALL: EventDef<ToolCall> = EventDef::new(
    "core/mcp.tool.call",
    Shape::Intercept,
    true,
    Mount::Subsystem,
    schema,
);

const COMMITTED: EventDef<serde_json::Value> = EventDef::new(
    "core/block.committed",
    Shape::Observe,
    true,
    Mount::TranslationLayer,
    schema,
);

const DELIVERED: EventDef<serde_json::Value> = EventDef::new(
    "core/dispatch.delivered",
    Shape::Observe,
    true,
    Mount::Subsystem,
    schema,
);

/// 载荷为裸 JSON 的 intercept 事件，便于比对改写前后的取值
const ECHO: EventDef<serde_json::Value> = EventDef::new(
    "core/test.echo",
    Shape::Intercept,
    true,
    Mount::Subsystem,
    schema,
);

/// 一旦被序列化就 panic，用来证明无观察者时没碰载荷
#[derive(Deserialize)]
struct NeverSerialized;

impl Serialize for NeverSerialized {
    fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        panic!("no observer, the payload must not be serialized");
    }
}

/// 载荷为 NeverSerialized 的 intercept 事件
const UNTOUCHED: EventDef<NeverSerialized> = EventDef::new(
    "core/test.untouched",
    Shape::Intercept,
    true,
    Mount::Subsystem,
    schema,
);

fn call() -> ToolCall {
    ToolCall {
        tool: "fetch".into(),
        args: serde_json::json!({ "url": "http://example.com" }),
    }
}

fn sub(phase: Option<Phase>, seq: u64, name: &str) -> Subscription {
    Subscription {
        mod_name: name.into(),
        mod_version: 1,
        event: "core/mcp.tool.call",
        phase,
        handler: "onCall".into(),
        timeout_ms: 20,
        registration_seq: seq,
    }
}

fn observe_sub(seq: u64, name: &str) -> Subscription {
    Subscription {
        mod_name: name.into(),
        mod_version: 1,
        event: "core/block.committed",
        phase: None,
        handler: "onCommitted".into(),
        timeout_ms: 100,
        registration_seq: seq,
    }
}

/// 订阅 ECHO 的 guard
fn echo_sub(seq: u64, name: &str) -> Subscription {
    Subscription {
        event: "core/test.echo",
        ..sub(Some(Phase::Guard), seq, name)
    }
}

fn cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id: 7 },
        Source::core("mcp"),
        Duration::from_millis(50),
    )
}

fn dispatcher(host: FakeHost, subs: Vec<Subscription>) -> Dispatcher {
    build(
        Directory::from_metas([TOOL_CALL.meta(), COMMITTED.meta(), DELIVERED.meta()]),
        Arc::new(FakeEnablement::all()),
        host,
        subs,
    )
}

fn build(
    directory: Directory,
    enablement: Arc<dyn ModEnablement>,
    host: FakeHost,
    subs: Vec<Subscription>,
) -> Dispatcher {
    let d = Dispatcher::new(directory, enablement, Arc::new(host), Limits::default());
    d.rebuild_index(subs);
    d
}

fn names(calls: Vec<(String, String)>) -> Vec<String> {
    calls.into_iter().map(|(m, _)| m).collect()
}

/// 直接建观察池的测试不需要超时与失败事件
struct NoNotifier;

#[async_trait::async_trait]
impl DispatchNotifier for NoNotifier {
    async fn emit(&self, _event: &'static str, _scope: Scope, _notice: Notice<'_>) {}
}

fn no_notices() -> Arc<dyn DispatchNotifier> {
    Arc::new(NoNotifier)
}

// ---------------------------------------------------------------- 折叠

#[tokio::test]
async fn an_empty_chain_passes_the_original_value_through() {
    let d = dispatcher(FakeHost::new(), vec![]);
    let verdict = d.decide(&TOOL_CALL, &cx(), call()).await;
    assert_eq!(verdict, call());
}

#[tokio::test]
async fn rewrites_accumulate_across_the_chain() {
    let host = FakeHost::new().on(
        "a",
        "onCall",
        Reply::Rewrite(serde_json::json!({ "tool": "fetch", "args": { "url": "https://safe" } })),
    );
    let d = dispatcher(
        host,
        vec![
            sub(Some(Phase::Guard), 1, "a"),
            sub(Some(Phase::Policy), 2, "b"),
        ],
    );
    let verdict = d.decide(&TOOL_CALL, &cx(), call()).await;
    assert_eq!(verdict.args["url"], "https://safe");
}

#[tokio::test]
async fn guard_runs_before_policy() {
    let host = FakeHost::new();
    let d = dispatcher(
        host.clone(),
        vec![
            sub(Some(Phase::Policy), 1, "p"),
            sub(Some(Phase::Guard), 2, "g"),
        ],
    );
    d.decide(&TOOL_CALL, &cx(), call()).await;
    assert_eq!(names(host.calls()), vec!["g", "p"]);
}

// ------------------------------------------------------------ 前置判定

#[tokio::test]
async fn a_ui_origin_bypasses_intercept() {
    let host = FakeHost::new();
    let d = dispatcher(host.clone(), vec![sub(Some(Phase::Guard), 1, "a")]);
    let mut ui = cx();
    ui.origin = Origin::Ui;
    let verdict = d.decide(&TOOL_CALL, &ui, call()).await;
    assert_eq!(verdict, call());
    assert_eq!(host.call_count(), 0);
}

#[tokio::test]
async fn a_ui_origin_does_not_bypass_observe() {
    let host = FakeHost::new();
    let d = dispatcher(host.clone(), vec![observe_sub(1, "a")]);
    let mut ui = cx();
    ui.origin = Origin::Ui;
    d.notify(&COMMITTED, &ui, serde_json::json!({})).await;
    wait_for(|| host.call_count() > 0).await;
    assert_eq!(
        host.call_count(),
        1,
        "user actions can be observed, only not intercepted"
    );
}

#[tokio::test]
async fn mods_not_enabled_in_this_scope_are_not_woken() {
    let host = FakeHost::new();
    let d = build(
        Directory::from_metas([TOOL_CALL.meta()]),
        Arc::new(FakeEnablement::only(&["enabled"])),
        host.clone(),
        vec![
            sub(Some(Phase::Guard), 1, "enabled"),
            sub(Some(Phase::Guard), 2, "disabled"),
        ],
    );
    d.decide(&TOOL_CALL, &cx(), call()).await;
    assert_eq!(names(host.calls()), vec!["enabled"]);
}

#[tokio::test]
async fn the_fast_path_does_not_consult_enablement_or_the_host() {
    /// 一旦被查询就 panic，用来证明快速路径没碰它
    struct PanickingEnablement;
    impl ModEnablement for PanickingEnablement {
        fn enabled(&self, _scope: &Scope) -> EnabledSet {
            panic!("no subscription, the enabled set must not be read");
        }
    }
    let host = FakeHost::new();
    let d = build(
        Directory::from_metas([TOOL_CALL.meta()]),
        Arc::new(PanickingEnablement),
        host.clone(),
        vec![],
    );
    let verdict = d.decide(&TOOL_CALL, &cx(), call()).await;
    assert_eq!(verdict, call());
    assert_eq!(host.call_count(), 0);
}

#[tokio::test]
async fn the_seq_advances_even_on_the_fast_path() {
    let d = dispatcher(FakeHost::new(), vec![]);
    d.decide(&TOOL_CALL, &cx(), call()).await;
    d.decide(&TOOL_CALL, &cx(), call()).await;
    assert_eq!(
        d.current_seq(),
        EventSeq(2),
        "each of the two decides takes one sequence number"
    );
}

#[tokio::test]
async fn a_wrong_shape_is_reported_and_the_payload_passes() {
    let rec = Recorder::default();
    let d = dispatcher(FakeHost::new(), vec![]);
    d.attach_observer(Arc::new(rec.clone()));

    let payload = serde_json::json!({ "a": 1 });
    assert_eq!(
        d.decide(&COMMITTED, &cx(), payload.clone()).await,
        payload,
        "a wrong shape does not change the payload"
    );
    d.notify(&TOOL_CALL, &cx(), call()).await;

    let failed = rec.payloads("core/dispatch.failed");
    assert_eq!(failed.len(), 2, "each misuse is reported once");
    assert_eq!(failed[0]["failure"], serde_json::json!("wrong_shape"));
    assert_eq!(failed[1]["failure"], serde_json::json!("wrong_shape"));
}

#[tokio::test]
async fn an_event_outside_the_directory_is_reported_and_dropped() {
    const UNKNOWN: EventDef<serde_json::Value> = EventDef::new(
        "core/no.such.event",
        Shape::Observe,
        true,
        Mount::Subsystem,
        schema,
    );
    let rec = Recorder::default();
    let d = dispatcher(FakeHost::new(), vec![]);
    d.attach_observer(Arc::new(rec.clone()));

    d.notify(&UNKNOWN, &cx(), serde_json::json!({})).await;

    let failed = rec.payloads("core/dispatch.failed");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["failure"], serde_json::json!("not_in_directory"));
}

#[tokio::test]
async fn beyond_the_depth_limit_stops_the_chain_and_reports() {
    let rec = Recorder::default();
    let d = dispatcher(FakeHost::new(), vec![sub(Some(Phase::Guard), 1, "a")]);
    d.attach_observer(Arc::new(rec.clone()));
    let mut deep = cx();
    for i in 0..9 {
        deep = deep.nested(EventSeq(i));
    }
    assert_eq!(d.decide(&TOOL_CALL, &deep, call()).await, call());

    let failed = rec.payloads("core/dispatch.failed");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["failure"], serde_json::json!("too_deep"));
}

// ---------------------------------------------------------------- 失败

#[tokio::test]
async fn a_bad_rewrite_is_reported_and_the_previous_payload_survives() {
    let safe = ToolCall {
        tool: "fetch".into(),
        args: serde_json::json!({ "url": "https://safe" }),
    };
    let host = FakeHost::new()
        .on(
            "good",
            "onCall",
            Reply::Rewrite(serde_json::to_value(&safe).expect("serialize the rewrite")),
        )
        .on(
            "bad",
            "onCall",
            Reply::Rewrite(serde_json::json!({ "unexpected": true })),
        );
    let rec = Recorder::default();
    let d = dispatcher(
        host,
        vec![
            sub(Some(Phase::Guard), 1, "good"),
            sub(Some(Phase::Guard), 2, "bad"),
        ],
    );
    d.attach_observer(Arc::new(rec.clone()));

    assert_eq!(
        d.decide(&TOOL_CALL, &cx(), call()).await,
        safe,
        "a broken rewrite leaves the previous accumulated payload in place"
    );

    let failed = rec.payloads("core/dispatch.failed");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["failure"], serde_json::json!("bad_rewrite"));
    assert_eq!(failed[0]["target"]["name"], serde_json::json!("bad"));
}

#[tokio::test]
async fn a_host_failure_skips_that_mod_but_the_chain_continues() {
    let host = FakeHost::new()
        .on("broken", "onCall", Reply::HostFailure("boom".into()))
        .on("good", "onCall", Reply::Pass);
    let d = dispatcher(
        host.clone(),
        vec![
            sub(Some(Phase::Guard), 1, "broken"),
            sub(Some(Phase::Guard), 2, "good"),
        ],
    );
    let verdict = d.decide(&TOOL_CALL, &cx(), call()).await;
    assert_eq!(verdict, call());
    assert_eq!(
        host.call_count(),
        2,
        "a host failure skips only the mod that failed"
    );
}

#[tokio::test]
async fn a_handler_error_skips_the_mod() {
    let host = FakeHost::new().on("a", "onCall", Reply::Fail("boom".into()));
    let d = dispatcher(host, vec![sub(Some(Phase::Guard), 1, "a")]);
    assert_eq!(d.decide(&TOOL_CALL, &cx(), call()).await, call());
}

#[tokio::test(start_paused = true)]
async fn a_hung_mod_is_timed_out_and_the_next_subscriber_still_runs() {
    let host = FakeHost::new()
        .on("hung", "onCall", Reply::Sleep(Duration::from_secs(60)))
        .on("next", "onCall", Reply::Pass);
    let d = dispatcher(
        host.clone(),
        vec![
            sub(Some(Phase::Guard), 1, "hung"),
            sub(Some(Phase::Guard), 2, "next"),
        ],
    );
    let verdict = d.decide(&TOOL_CALL, &cx(), call()).await;
    assert_eq!(verdict, call());
    assert_eq!(
        host.call_count(),
        2,
        "after a stuck mod, the next subscriber is still invoked"
    );
}

#[tokio::test(start_paused = true)]
async fn a_hung_mod_is_skipped_without_rejecting_the_operation() {
    let host = FakeHost::new().on("hung", "onCall", Reply::Sleep(Duration::from_secs(60)));
    let d = dispatcher(host, vec![sub(Some(Phase::Guard), 1, "hung")]);
    assert_eq!(
        d.decide(&TOOL_CALL, &cx(), call()).await,
        call(),
        "a timeout skips the subscriber; it does not reject the operation"
    );
}

// -------------------------------------------------------- observe 路径

#[tokio::test]
async fn notify_delivers_in_the_background() {
    let host = FakeHost::new();
    let d = dispatcher(host.clone(), vec![observe_sub(1, "a")]);
    d.notify(&COMMITTED, &cx(), serde_json::json!({ "blockId": 1 }))
        .await;
    wait_for(|| host.call_count() > 0).await;
    assert_eq!(host.call_modes(), vec![CallMode::Observe]);
}

#[tokio::test]
async fn intercept_handlers_are_called_in_intercept_mode() {
    let host = FakeHost::new();
    let d = dispatcher(host.clone(), vec![sub(Some(Phase::Guard), 1, "a")]);
    d.decide(&TOOL_CALL, &cx(), call()).await;
    assert_eq!(host.call_modes(), vec![CallMode::Intercept]);
}

#[tokio::test]
async fn an_observe_event_without_subscribers_wakes_nobody() {
    let host = FakeHost::new();
    let d = dispatcher(host.clone(), vec![]);
    d.notify(&COMMITTED, &cx(), serde_json::json!({})).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(host.call_count(), 0);
}

// -------------------------------------------------------------- 投递追踪

#[tokio::test]
async fn tracking_is_off_by_default() {
    let host = FakeHost::new().on(
        "a",
        "onCall",
        Reply::Rewrite(serde_json::to_value(call()).expect("serialize")),
    );
    let d = dispatcher(host, vec![sub(Some(Phase::Guard), 1, "a")]);
    assert!(!d.tracking_enabled());
    d.decide(&TOOL_CALL, &cx(), call()).await;
    assert!(d.delivery_records().is_empty());
}

#[tokio::test]
async fn turning_tracking_on_records_each_delivery() {
    let host = FakeHost::new().on(
        "a",
        "onCall",
        Reply::Rewrite(serde_json::to_value(call()).expect("serialize")),
    );
    let d = dispatcher(host, vec![sub(Some(Phase::Guard), 1, "a")]);
    d.set_tracking(true);
    d.decide(&TOOL_CALL, &cx(), call()).await;
    let records = d.delivery_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].target, DeliveryTarget::mod_("a"));
    assert_eq!(records[0].outcome, DeliveryOutcome::Rewrite);
}

#[tokio::test]
async fn a_dispatch_event_never_produces_a_delivery_record_for_itself() {
    let host = FakeHost::new();
    let d = build(
        Directory::from_metas([DELIVERED.meta()]),
        Arc::new(FakeEnablement::all()),
        host.clone(),
        vec![Subscription {
            event: "core/dispatch.delivered",
            ..observe_sub(1, "watcher")
        }],
    );
    d.set_tracking(true);
    d.notify(&DELIVERED, &cx(), serde_json::json!({})).await;
    wait_for(|| host.call_count() > 0).await;
    assert!(
        d.delivery_records().is_empty(),
        "delivery records must not themselves produce delivery records"
    );
}

#[tokio::test]
async fn observer_deliveries_are_recorded_separately() {
    let d = dispatcher(FakeHost::new(), vec![]);
    d.set_tracking(true);
    d.record_observer(
        "core/block.committed",
        EventSeq(1),
        "sse",
        DeliveryOutcome::Observed,
        3,
    );
    let records = d.delivery_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].target, DeliveryTarget::observer("sse"));
}

// ---------------------------------------------------------------- 其它

#[tokio::test]
async fn rebuild_index_replaces_the_previous_generation() {
    let host = FakeHost::new();
    let d = dispatcher(host.clone(), vec![sub(Some(Phase::Guard), 1, "old")]);
    d.rebuild_index(vec![sub(Some(Phase::Guard), 1, "new")]);
    d.decide(&TOOL_CALL, &cx(), call()).await;
    assert_eq!(names(host.calls()), vec!["new"]);
}

#[tokio::test]
async fn the_limits_are_exposed_unchanged() {
    let d = dispatcher(FakeHost::new(), vec![]);
    assert_eq!(d.limits().max_depth, Limits::default().max_depth);
}

#[tokio::test]
async fn the_directory_is_exposed_for_audit() {
    let d = dispatcher(FakeHost::new(), vec![]);
    assert!(d.directory().get("core/mcp.tool.call").is_some());
}

#[tokio::test]
async fn mod_emit_rate_is_limited_per_mod() {
    let d = dispatcher(FakeHost::new(), vec![]);
    for _ in 0..Limits::default().per_window {
        assert!(d.allow_mod_emit("m"));
    }
    assert!(!d.allow_mod_emit("m"));
    d.evict_stale_rates();
}

#[tokio::test]
async fn clearing_deliveries_empties_the_buffer() {
    let d = dispatcher(FakeHost::new(), vec![]);
    d.record_delivery(DeliveryRecord {
        event_seq: EventSeq(1),
        target: DeliveryTarget::observer("sse"),
        outcome: DeliveryOutcome::Observed,
        duration_ms: 1,
        error: None,
    });
    assert_eq!(d.delivery_records().len(), 1);
    d.clear_deliveries();
    assert!(d.delivery_records().is_empty());
}

#[tokio::test]
async fn seq_only_allocates_without_dispatching() {
    let host = FakeHost::new();
    let d = dispatcher(host.clone(), vec![sub(Some(Phase::Guard), 1, "a")]);
    assert_eq!(
        d.seq_only(&cx()).expect("allocate a sequence number"),
        EventSeq(1)
    );
    assert_eq!(host.call_count(), 0);
}

fn pool_sub(name: &str) -> Subscription {
    Subscription {
        mod_name: name.into(),
        mod_version: 1,
        event: "core/block.committed",
        phase: None,
        handler: "onCommitted".into(),
        timeout_ms: 100,
        registration_seq: 1,
    }
}

fn pool_job(name: &str, seq: u64) -> Job {
    Job {
        seq: EventSeq(seq),
        event: "core/block.committed",
        scope: Scope::Global,
        payload: serde_json::json!({ "n": seq }),
        sub: pool_sub(name),
    }
}

fn sink(seen: Vec<DeliveryRecord>) -> (DeliverySink, std::sync::Arc<Mutex<Vec<DeliveryRecord>>>) {
    let shared = std::sync::Arc::new(Mutex::new(seen));
    let handle = std::sync::Arc::clone(&shared);
    (
        std::sync::Arc::new(move |_, r| handle.lock().expect("lock the record").push(r)),
        shared,
    )
}

fn pool_cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Global,
        Source::core("agent"),
        Duration::from_millis(50),
    )
}

#[tokio::test]
async fn a_full_queue_drops_instead_of_blocking() {
    // 同步入队，worker 拿不到执行机会，因此第一格填满后全部丢弃
    let host = FakeHost::new().on("m", "onCommitted", Reply::Sleep(Duration::from_secs(60)));
    let (sink, _seen) = sink(Vec::new());
    let pool = ObservePool::with_options(
        std::sync::Arc::new(host),
        sink,
        no_notices(),
        1,
        Duration::from_millis(0),
    );
    for i in 0..100 {
        pool.enqueue(pool_job("m", i));
    }
    assert!(pool.dropped_total() > 0, "a full queue must drop and count");
}

#[tokio::test]
async fn a_single_observe_event_arrives_as_a_bare_payload() {
    let (sink, _seen) = sink(Vec::new());
    let host = FakeHost::new();
    let pool = ObservePool::with_options(
        std::sync::Arc::new(host.clone()),
        sink,
        no_notices(),
        64,
        Duration::from_millis(50),
    );
    pool.enqueue(pool_job("m", 0));
    wait_for(|| host.call_count() > 0).await;
    assert_eq!(
        host.call_payloads(),
        vec![serde_json::json!({ "n": 0 })],
        "one event in a group is delivered on its own, not wrapped in an array"
    );
}

#[tokio::test]
async fn two_observe_events_in_one_scope_arrive_as_an_array() {
    let (sink, _seen) = sink(Vec::new());
    let host = FakeHost::new();
    let pool = ObservePool::with_options(
        std::sync::Arc::new(host.clone()),
        sink,
        no_notices(),
        64,
        Duration::from_millis(50),
    );
    pool.enqueue(pool_job("m", 0));
    pool.enqueue(pool_job("m", 1));
    wait_for(|| host.call_count() > 0).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        host.call_count(),
        1,
        "two events of one scope in the window are merged into one call"
    );
    assert_eq!(
        host.call_payloads(),
        vec![serde_json::json!([{ "n": 0 }, { "n": 1 }])],
        "the merged payload keeps the enqueue order"
    );
}

#[tokio::test]
async fn two_scopes_do_not_share_a_batch() {
    let (sink, _seen) = sink(Vec::new());
    let host = FakeHost::new();
    let pool = ObservePool::with_options(
        std::sync::Arc::new(host.clone()),
        sink,
        no_notices(),
        64,
        Duration::from_millis(50),
    );
    let mut first = pool_job("m", 0);
    first.scope = Scope::Topic { topic_id: 7 };
    let mut second = pool_job("m", 1);
    second.scope = Scope::Topic { topic_id: 8 };
    pool.enqueue(first);
    pool.enqueue(second);
    wait_for(|| host.call_count() == 2).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(host.call_count(), 2, "a scope is a batch boundary");
    let mut payloads = host.call_payloads();
    payloads.sort_by_key(|payload| payload["n"].as_u64());
    assert_eq!(
        payloads,
        vec![serde_json::json!({ "n": 0 }), serde_json::json!({ "n": 1 })],
        "each scope arrives as its own bare payload"
    );
}

#[tokio::test]
async fn groups_of_different_events_keep_the_enqueue_order() {
    let (sink, _seen) = sink(Vec::new());
    let host = FakeHost::new();
    let pool = ObservePool::with_options(
        std::sync::Arc::new(host.clone()),
        sink,
        no_notices(),
        64,
        Duration::from_millis(50),
    );
    let events = [
        "core/block.open",
        "core/block.committed",
        "core/block.delta",
        "core/message.seal",
        "core/input.accepted",
        "core/agent.status",
        "core/dispatch.timeout",
        "core/dispatch.failed",
    ];
    for (i, event) in events.iter().enumerate() {
        let mut job = pool_job("m", i as u64);
        job.event = event;
        job.sub.event = event;
        pool.enqueue(job);
    }
    wait_for(|| host.call_count() == events.len()).await;
    let expected: Vec<serde_json::Value> = (0..events.len())
        .map(|i| serde_json::json!({ "n": i }))
        .collect();
    assert_eq!(
        host.call_payloads(),
        expected,
        "groups of different events must follow the enqueue order"
    );
}

#[tokio::test]
async fn the_builtin_observer_still_sees_each_event_separately() {
    let rec = Recorder::default();
    let host = FakeHost::new();
    let d = dispatcher(host.clone(), vec![observe_sub(1, "a")]);
    d.attach_observer(Arc::new(rec.clone()));
    d.notify(&COMMITTED, &cx(), serde_json::json!({ "n": 0 }))
        .await;
    d.notify(&COMMITTED, &cx(), serde_json::json!({ "n": 1 }))
        .await;
    wait_for(|| host.call_count() > 0).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        rec.payloads("core/block.committed"),
        vec![serde_json::json!({ "n": 0 }), serde_json::json!({ "n": 1 })],
        "the observer sees the events one by one"
    );
    assert_eq!(host.call_count(), 1, "the mod receives one merged call");
    assert_eq!(
        host.call_payloads(),
        vec![serde_json::json!([{ "n": 0 }, { "n": 1 }])],
        "batching only applies to mod delivery"
    );
}

#[tokio::test]
async fn observe_delivery_ignores_the_return_value() {
    let host = FakeHost::new().on(
        "m",
        "onCommitted",
        Reply::Rewrite(serde_json::json!({ "ignored": true })),
    );
    let (sink, seen) = sink(Vec::new());
    let pool = ObservePool::with_options(
        std::sync::Arc::new(host.clone()),
        sink,
        no_notices(),
        8,
        Duration::from_millis(0),
    );
    pool.enqueue(pool_job("m", 1));
    wait_for(|| host.call_count() > 0).await;
    wait_for(|| !seen.lock().expect("lock the record").is_empty()).await;
    let records = seen.lock().expect("lock the record");
    assert_eq!(
        records[0].outcome,
        DeliveryOutcome::Observed,
        "return values are always ignored"
    );
    assert_eq!(records[0].event_seq, EventSeq(1));
}

#[tokio::test]
async fn a_host_failure_is_recorded_as_an_error() {
    let host = FakeHost::new().on("m", "onCommitted", Reply::HostFailure("boom".into()));
    let (sink, seen) = sink(Vec::new());
    let pool = ObservePool::with_options(
        std::sync::Arc::new(host.clone()),
        sink,
        no_notices(),
        8,
        Duration::from_millis(0),
    );
    pool.enqueue(pool_job("m", 1));
    wait_for(|| !seen.lock().expect("lock the record").is_empty()).await;
    assert_eq!(
        seen.lock().expect("lock the record")[0].outcome,
        DeliveryOutcome::Error
    );
}

#[tokio::test]
async fn a_fresh_pool_has_dropped_nothing() {
    let (sink, _seen) = sink(Vec::new());
    let pool = ObservePool::with_options(
        std::sync::Arc::new(FakeHost::new()),
        sink,
        no_notices(),
        8,
        Duration::from_millis(0),
    );
    assert_eq!(pool.dropped_total(), 0);
}

#[test]
fn job_from_carries_the_context_scope() {
    let j = job_from(
        &pool_cx(),
        "core/block.committed",
        EventSeq(3),
        serde_json::json!({}),
        pool_sub("m"),
    );
    assert_eq!(j.seq, EventSeq(3));
    assert_eq!(j.scope, Scope::Global);
    assert_eq!(j.sub.mod_name, "m");
}

// -------------------------------------------------------------- 内建观察者

/// 记下派发给内建观察者的整份信封，供断言核对
///
/// 克隆共享同一份记录
#[derive(Default, Clone)]
struct Recorder {
    /// 按收到顺序记下的信封
    seen: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl Recorder {
    /// 记下的信封，按序
    fn events(&self) -> Vec<serde_json::Value> {
        self.seen.lock().expect("lock the record").clone()
    }

    /// 记下的事件名，按序
    fn names(&self) -> Vec<String> {
        self.events()
            .iter()
            .filter_map(|event| event["name"].as_str().map(str::to_string))
            .collect()
    }

    /// 某个事件名记下的载荷，按序
    fn payloads(&self, name: &str) -> Vec<serde_json::Value> {
        self.events()
            .iter()
            .filter(|event| event["name"] == serde_json::json!(name))
            .map(|event| event["payload"].clone())
            .collect()
    }
}

#[async_trait::async_trait]
impl BuiltinObserver for Recorder {
    async fn on_event(&self, meta: &EventMeta, event: &serde_json::Value) {
        assert_eq!(event["name"], serde_json::json!(meta.name));
        self.seen
            .lock()
            .expect("lock the record")
            .push(event.clone());
    }
}

#[tokio::test]
async fn an_observer_sees_an_event_even_with_no_mod_subscribers() {
    let rec = Recorder::default();
    let d = dispatcher(FakeHost::new(), vec![]);
    d.attach_observer(Arc::new(rec.clone()));

    d.decide(&TOOL_CALL, &cx(), call()).await;
    d.notify(&COMMITTED, &cx(), serde_json::json!({})).await;

    assert_eq!(
        rec.names(),
        vec!["core/mcp.tool.call", "core/block.committed"],
        "no mod subscribers does not mean no consumers: built-in observers are always present"
    );
}

#[tokio::test]
async fn an_observer_sees_the_value_the_chain_settled_on() {
    let rec = Recorder::default();
    let host = FakeHost::new().on(
        "guard",
        "onCall",
        Reply::Rewrite(serde_json::json!({ "a": 2 })),
    );
    let d = build(
        Directory::from_metas([ECHO.meta()]),
        Arc::new(FakeEnablement::all()),
        host,
        vec![echo_sub(1, "guard")],
    );
    d.attach_observer(Arc::new(rec.clone()));

    d.decide(&ECHO, &cx(), serde_json::json!({ "a": 1 })).await;

    assert_eq!(
        rec.payloads("core/test.echo"),
        vec![serde_json::json!({ "a": 2 })],
        "what the model sees and what the log stores must be the same"
    );
}

#[tokio::test]
async fn an_observer_sees_an_event_bypassed_by_a_ui_origin() {
    let rec = Recorder::default();
    let d = dispatcher(FakeHost::new(), vec![sub(Some(Phase::Guard), 1, "a")]);
    d.attach_observer(Arc::new(rec.clone()));

    let mut ui = cx();
    ui.origin = Origin::Ui;
    d.decide(&TOOL_CALL, &ui, call()).await;

    assert_eq!(
        rec.names(),
        vec!["core/mcp.tool.call"],
        "the bypass skips only mod delivery; the event itself is still produced"
    );
}

#[tokio::test]
async fn an_observer_sees_the_event_a_mod_rewrote() {
    let rec = Recorder::default();
    let rewritten = ToolCall {
        tool: "fetch".into(),
        args: serde_json::json!({ "url": "http://example.com/rewritten" }),
    };
    let host = FakeHost::new().on(
        "a",
        "onCall",
        Reply::Rewrite(serde_json::to_value(&rewritten).expect("serialize the rewrite")),
    );
    let d = dispatcher(host, vec![sub(Some(Phase::Guard), 1, "a")]);
    d.attach_observer(Arc::new(rec.clone()));

    d.decide(&TOOL_CALL, &cx(), call()).await;
    assert_eq!(
        rec.payloads("core/mcp.tool.call"),
        vec![serde_json::to_value(&rewritten).expect("serialize the payload")],
        "the observer sees the value the chain settled on"
    );
}

#[tokio::test(start_paused = true)]
async fn an_observer_still_sees_the_event_when_a_mod_times_out() {
    let rec = Recorder::default();
    let host = FakeHost::new().on("hung", "onCall", Reply::Sleep(Duration::from_secs(60)));
    let d = dispatcher(host, vec![sub(Some(Phase::Guard), 1, "hung")]);
    d.attach_observer(Arc::new(rec.clone()));

    assert_eq!(d.decide(&TOOL_CALL, &cx(), call()).await, call());
    assert_eq!(
        rec.names(),
        vec!["core/dispatch.timeout", "core/mcp.tool.call"],
        "the timeout is observed, and the event is still produced"
    );
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_delivery_emits_the_timeout_event() {
    let rec = Recorder::default();
    let host = FakeHost::new().on("hung", "onCall", Reply::Sleep(Duration::from_secs(60)));
    let d = dispatcher(host, vec![sub(Some(Phase::Guard), 1, "hung")]);
    d.attach_observer(Arc::new(rec.clone()));

    d.decide(&TOOL_CALL, &cx(), call()).await;

    let timeouts = rec.payloads("core/dispatch.timeout");
    assert_eq!(timeouts.len(), 1, "one timeout yields one event");
    assert_eq!(
        timeouts[0]["event"],
        serde_json::json!("core/mcp.tool.call")
    );
    assert_eq!(
        timeouts[0]["target"],
        serde_json::json!({ "kind": "mod", "name": "hung" })
    );
    let envelope = rec
        .events()
        .into_iter()
        .find(|event| event["name"] == serde_json::json!("core/dispatch.timeout"))
        .expect("find the timeout envelope");
    assert_eq!(
        envelope["cause"], timeouts[0]["event_seq"],
        "the timeout points at the event it interrupted"
    );
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_delivery_does_not_emit_a_timeout_event() {
    let rec = Recorder::default();
    let host = FakeHost::new().on("hung", "onCall", Reply::Sleep(Duration::from_secs(60)));
    let d = dispatcher(host.clone(), vec![sub(Some(Phase::Guard), 1, "hung")]);
    d.attach_observer(Arc::new(rec.clone()));
    let cx = cx();
    cx.cancel().cancel();

    let _ = d.decide(&TOOL_CALL, &cx, call()).await;

    assert!(
        rec.payloads("core/dispatch.timeout").is_empty(),
        "cancellation is not a timeout"
    );
    assert_eq!(host.call_count(), 0, "the cancelled call is never polled");
}

#[tokio::test(start_paused = true)]
async fn a_timeout_is_recorded_but_the_timeout_event_is_not() {
    let host = FakeHost::new().on("hung", "onCall", Reply::Sleep(Duration::from_secs(60)));
    let d = dispatcher(host, vec![sub(Some(Phase::Guard), 1, "hung")]);
    d.set_tracking(true);

    d.decide(&TOOL_CALL, &cx(), call()).await;

    let records = d.delivery_records();
    assert_eq!(
        records.len(),
        1,
        "a dispatch event never produces a delivery record for itself"
    );
    assert_eq!(records[0].outcome, DeliveryOutcome::Timeout);
}

#[tokio::test(start_paused = true)]
async fn an_observe_timeout_emits_the_timeout_event() {
    let rec = Recorder::default();
    let host = FakeHost::new().on("slow", "onCommitted", Reply::Sleep(Duration::from_secs(60)));
    let mut slow = observe_sub(1, "slow");
    slow.timeout_ms = 10;
    let d = dispatcher(host, vec![slow]);
    d.attach_observer(Arc::new(rec.clone()));

    d.notify(&COMMITTED, &cx(), serde_json::json!({})).await;
    wait_for(|| !rec.payloads("core/dispatch.timeout").is_empty()).await;

    let timeout = rec.payloads("core/dispatch.timeout");
    assert_eq!(
        timeout[0]["event"],
        serde_json::json!("core/block.committed")
    );
    assert_eq!(
        timeout[0]["target"],
        serde_json::json!({ "kind": "mod", "name": "slow" })
    );
}

#[tokio::test]
async fn the_sequence_can_be_seeded_so_a_restart_does_not_reuse_numbers() {
    let d = dispatcher(FakeHost::new(), vec![]);
    d.seed_seq(EventSeq(41));
    assert_eq!(d.current_seq(), EventSeq(41));
    assert_eq!(
        d.seq_only(&cx()).expect("allocate a sequence number"),
        EventSeq(42)
    );
}

#[tokio::test]
async fn the_fast_path_does_not_serialize_the_payload() {
    let d = build(
        Directory::from_metas([UNTOUCHED.meta()]),
        Arc::new(FakeEnablement::all()),
        FakeHost::new(),
        vec![],
    );
    assert!(matches!(
        d.decide(&UNTOUCHED, &cx(), NeverSerialized).await,
        NeverSerialized
    ));
}
