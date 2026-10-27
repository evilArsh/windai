use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use wind_event::{
    CallCx, CallError, Committed, Directory, DispatchCx, EnabledSet, Envelope, EventDef,
    EventOutlet, Limits, ModCaller, ModRoster, Observer, Origin, Pending, Scope, Shape, Source,
    Subscription, schema,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
struct Payload {
    text: String,
}

const OBSERVED: EventDef<Payload> =
    EventDef::new("test/observed", Shape::Observe, true, schema::<Payload>);
const INTERCEPTED: EventDef<Payload> = EventDef::new(
    "test/intercepted",
    Shape::Intercept,
    true,
    schema::<Payload>,
);

fn payload(text: &str) -> Payload {
    Payload {
        text: text.to_string(),
    }
}

fn directory() -> Directory {
    Directory::from_metas([OBSERVED.meta(), INTERCEPTED.meta()])
}

fn cx(topic_id: i64) -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id },
        Source::core("test"),
        Duration::from_millis(500),
    )
}

/// 收集所有被提交的事件
#[derive(Default, Clone)]
struct RecordingOutlet {
    seen: Arc<Mutex<Vec<(Shape, String, serde_json::Value)>>>,
}

impl EventOutlet for RecordingOutlet {
    fn submit(&self, pending: Pending) {
        self.seen.lock().expect("lock the outlet").push((
            pending.shape,
            pending.envelope.name.clone(),
            pending.envelope.payload.clone(),
        ));
    }
}

impl RecordingOutlet {
    fn count(&self) -> usize {
        self.seen.lock().expect("lock the outlet").len()
    }

    fn last(&self) -> (Shape, String, serde_json::Value) {
        self.seen
            .lock()
            .expect("lock the outlet")
            .last()
            .cloned()
            .expect("one submitted event")
    }

    fn names(&self) -> Vec<String> {
        self.seen
            .lock()
            .expect("lock the outlet")
            .iter()
            .map(|(_, name, _)| name.clone())
            .collect()
    }
}

/// 所有 mod 在岗
struct AllMods;

impl ModRoster for AllMods {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::from_names(["m".to_string()])
    }
}

enum Reply {
    Pass,
    Rewrite(serde_json::Value),
    Fail(String),
    Slow(Duration),
}

#[derive(Clone)]
struct Scripted {
    reply: Arc<Mutex<Reply>>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl Scripted {
    fn new(reply: Reply) -> Self {
        Self {
            reply: Arc::new(Mutex::new(reply)),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("lock the calls").clone()
    }
}

#[async_trait::async_trait]
impl ModCaller for Scripted {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, CallError> {
        self.calls
            .lock()
            .expect("lock the calls")
            .push(format!("{}:{}", sub.mod_name, payload));
        enum Step {
            Pass,
            Rewrite(serde_json::Value),
            Fail(String),
            Slow(Duration),
        }
        let step = {
            let reply = self.reply.lock().expect("lock the reply");
            match &*reply {
                Reply::Pass => Step::Pass,
                Reply::Rewrite(value) => Step::Rewrite(value.clone()),
                Reply::Fail(message) => Step::Fail(message.clone()),
                Reply::Slow(duration) => Step::Slow(*duration),
            }
        };
        match step {
            Step::Pass => Ok(None),
            Step::Rewrite(value) => Ok(Some(value)),
            Step::Fail(message) => Err(CallError::Handler(message)),
            Step::Slow(duration) => {
                tokio::time::sleep(duration).await;
                Ok(None)
            }
        }
    }
}

fn bus(caller: &Scripted, limits: Limits) -> (wind_event::Dispatcher, RecordingOutlet) {
    let outlet = RecordingOutlet::default();
    let bus = wind_event::Dispatcher::new(
        directory(),
        Arc::new(AllMods),
        Arc::new(caller.clone()),
        Arc::new(outlet.clone()),
        limits,
    );
    (bus, outlet)
}

fn subscription(event: &'static str, timeout_ms: u64) -> Subscription {
    Subscription {
        mod_name: "m".to_string(),
        mod_version: 1,
        event,
        handler: "onEvent".to_string(),
        timeout_ms,
        rank: 1,
    }
}

#[tokio::test]
async fn notify_submits_an_observe_event() {
    let caller = Scripted::new(Reply::Pass);
    let (bus, outlet) = bus(&caller, Limits::default());

    bus.notify(&OBSERVED, &cx(7), payload("hello"));

    let (shape, name, value) = outlet.last();
    assert_eq!(shape, Shape::Observe);
    assert_eq!(name, "test/observed");
    assert_eq!(value, serde_json::json!({ "text": "hello" }));
}

#[tokio::test]
async fn notify_does_not_call_mods_inline() {
    let caller = Scripted::new(Reply::Pass);
    let (bus, _outlet) = bus(&caller, Limits::default());
    bus.rebuild_index(vec![subscription("test/observed", 50)]);

    bus.notify(&OBSERVED, &cx(7), payload("hello"));

    assert!(
        caller.calls().is_empty(),
        "observe 投递走投递池，不在 notify 里同步调用"
    );
}

#[tokio::test]
async fn decide_returns_the_rewritten_payload() {
    let caller = Scripted::new(Reply::Rewrite(serde_json::json!({ "text": "rewritten" })));
    let (bus, outlet) = bus(&caller, Limits::default());
    bus.rebuild_index(vec![subscription("test/intercepted", 50)]);

    let decided = bus.decide(&INTERCEPTED, &cx(7), payload("original")).await;

    assert_eq!(decided, payload("rewritten"));
    let (shape, name, value) = outlet.last();
    assert_eq!(shape, Shape::Intercept);
    assert_eq!(name, "test/intercepted");
    assert_eq!(value, serde_json::json!({ "text": "rewritten" }));
}

#[tokio::test]
async fn decide_without_subscribers_submits_the_original_payload() {
    let caller = Scripted::new(Reply::Pass);
    let (bus, outlet) = bus(&caller, Limits::default());

    let decided = bus.decide(&INTERCEPTED, &cx(7), payload("original")).await;

    assert_eq!(decided, payload("original"));
    assert_eq!(outlet.count(), 1);
    assert!(caller.calls().is_empty());
}

#[tokio::test]
async fn a_failing_mod_is_reported_and_the_event_still_lands() {
    let caller = Scripted::new(Reply::Fail("boom".to_string()));
    let (bus, outlet) = bus(&caller, Limits::default());
    bus.rebuild_index(vec![subscription("test/intercepted", 50)]);

    let decided = bus.decide(&INTERCEPTED, &cx(7), payload("original")).await;

    assert_eq!(decided, payload("original"), "失败的改写不生效");
    assert_eq!(
        outlet.names(),
        vec![
            "core/dispatch.failed".to_string(),
            "test/intercepted".to_string()
        ]
    );
    let reason = outlet
        .seen
        .lock()
        .expect("lock the outlet")
        .first()
        .map(|(_, _, value)| value["failure"].clone());
    assert_eq!(reason, Some(serde_json::json!("handler_error")));
}

#[tokio::test]
async fn a_wrong_shape_is_reported_not_dispatched() {
    let caller = Scripted::new(Reply::Pass);
    let (bus, outlet) = bus(&caller, Limits::default());

    bus.notify(&INTERCEPTED, &cx(7), payload("hello"));

    assert_eq!(outlet.names(), vec!["core/dispatch.failed".to_string()]);
    let (_, _, value) = outlet.last();
    assert_eq!(value["failure"], serde_json::json!("wrong_shape"));
}

#[tokio::test]
async fn the_event_budget_reports_rather_than_dispatches() {
    let caller = Scripted::new(Reply::Pass);
    let limits = Limits {
        max_events_per_trigger: 1,
        ..Limits::default()
    };
    let (bus, outlet) = bus(&caller, limits);
    let cx = cx(7);

    bus.notify(&OBSERVED, &cx, payload("first"));
    bus.notify(&OBSERVED, &cx, payload("second"));

    assert_eq!(
        outlet.names(),
        vec![
            "test/observed".to_string(),
            "core/dispatch.failed".to_string()
        ]
    );
    let (_, _, value) = outlet.last();
    assert_eq!(value["failure"], serde_json::json!("too_many_events"));
}

#[tokio::test]
async fn committed_observe_events_reach_subscribed_mods() {
    let caller = Scripted::new(Reply::Pass);
    let (bus, _outlet) = bus(&caller, Limits::default());
    bus.rebuild_index(vec![subscription("test/observed", 50)]);

    bus.on_committed(
        &OBSERVED.meta(),
        &Committed {
            pos: 1,
            shape: Shape::Observe,
            envelope: Envelope::new(
                "test/observed",
                Scope::Topic { topic_id: 7 },
                Origin::Ui,
                Source::core("test"),
                serde_json::json!({ "text": "hello" }),
            ),
        },
    );
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert_eq!(caller.calls().len(), 1);
    assert!(caller.calls()[0].contains("hello"));
}

#[tokio::test]
async fn a_timed_out_mod_is_reported_as_a_notice() {
    let caller = Scripted::new(Reply::Slow(Duration::from_millis(500)));
    let (bus, outlet) = bus(&caller, Limits::default());
    bus.rebuild_index(vec![subscription("test/observed", 10)]);

    bus.on_committed(
        &OBSERVED.meta(),
        &Committed {
            pos: 2,
            shape: Shape::Observe,
            envelope: Envelope::new(
                "test/observed",
                Scope::Topic { topic_id: 7 },
                Origin::Ui,
                Source::core("test"),
                serde_json::json!({ "text": "slow" }),
            ),
        },
    );
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(outlet.names(), vec!["core/dispatch.timeout".to_string()]);
}

#[tokio::test]
async fn intercept_events_are_not_delivered_by_the_observer_path() {
    let caller = Scripted::new(Reply::Pass);
    let (bus, _outlet) = bus(&caller, Limits::default());
    bus.rebuild_index(vec![subscription("test/intercepted", 50)]);

    bus.on_committed(
        &INTERCEPTED.meta(),
        &Committed {
            pos: 3,
            shape: Shape::Intercept,
            envelope: Envelope::new(
                "test/intercepted",
                Scope::Topic { topic_id: 7 },
                Origin::Agent,
                Source::core("test"),
                serde_json::json!({ "text": "x" }),
            ),
        },
    );
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert!(caller.calls().is_empty(), "intercept 已在链内投递过");
}

#[tokio::test]
async fn a_cancelled_call_is_dropped_without_a_notice() {
    let caller = Scripted::new(Reply::Slow(Duration::from_millis(500)));
    let (bus, outlet) = bus(&caller, Limits::default());
    bus.rebuild_index(vec![subscription("test/intercepted", 400)]);
    let cx = cx(7);
    cx.cancel().cancel();

    let decided = bus.decide(&INTERCEPTED, &cx, payload("original")).await;

    assert_eq!(decided, payload("original"));
    assert_eq!(outlet.count(), 1, "只提交事件本身，不发通知");
    assert!(caller.calls().is_empty());
}

#[test]
fn the_directory_holds_both_test_events() {
    let dir = directory();
    assert_eq!(dir.len(), 2);
    assert!(dir.get("test/observed").expect("observed").is_observe());
    assert!(
        dir.get("test/intercepted")
            .expect("intercepted")
            .is_intercept()
    );
}

#[test]
fn cancellation_tokens_are_cloneable_across_contexts() {
    let cx = cx(1);
    let token: CancellationToken = cx.cancel().clone();
    assert!(!token.is_cancelled());
}
