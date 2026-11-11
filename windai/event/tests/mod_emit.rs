use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wind_event::{
    DispatchCx, EmitChannel, EmitError, EventOutlet, Guards, Limits, ModEmitter, ModIdentity,
    Origin, Pending, Retention, Scope, Source, SubmitError,
};
use wind_wire::event::Target;

#[derive(Default)]
struct RecordingOutlet {
    events: Mutex<Vec<Pending>>,
}

impl EventOutlet for RecordingOutlet {
    fn submit(&self, pending: Pending) -> Result<(), SubmitError> {
        self.events.lock().expect("event log").push(pending);
        Ok(())
    }
}

fn channel(name: &str, event: &str, retention: Retention) -> EmitChannel {
    EmitChannel::new(name, event, retention, Arc::new(|_: &Value| Ok(())))
}

fn emitter(outlet: Arc<RecordingOutlet>, limits: Limits) -> ModEmitter {
    ModEmitter::new(
        ModIdentity {
            name: "demo".to_string(),
            version: 3,
        },
        vec![
            channel("state", "m_demo/state", Retention::Journal),
            channel("notice", "m_demo/notice", Retention::Live),
        ],
        outlet,
        Arc::new(Guards::new(limits)),
    )
}

fn cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Internal,
        Scope::Global,
        Source::core("test"),
        Duration::from_secs(1),
    )
}

#[test]
fn journal_channels_use_the_declared_event_and_retention() {
    let outlet = Arc::new(RecordingOutlet::default());
    emitter(Arc::clone(&outlet), Limits::default())
        .emit(&cx(), "state", json!({ "n": 1 }))
        .expect("the channel is declared");
    let events = outlet.events.lock().expect("event log");
    let pending = &events[0];
    assert_eq!(pending.envelope.name, "m_demo/state");
    assert_eq!(pending.retention, Retention::Journal);
    assert_eq!(pending.target, Target::module("demo", "state"));
    assert_eq!(pending.envelope.source, Source::module("demo", 3));
    assert_eq!(pending.envelope.payload, Some(json!({ "n": 1 })));
}

#[test]
fn live_channels_use_their_own_name_and_retention() {
    let outlet = Arc::new(RecordingOutlet::default());
    emitter(Arc::clone(&outlet), Limits::default())
        .emit(&cx(), "notice", json!("hello"))
        .expect("the channel is declared");
    let events = outlet.events.lock().expect("event log");
    let pending = &events[0];
    assert_eq!(pending.envelope.name, "m_demo/notice");
    assert_eq!(pending.retention, Retention::Live);
    assert_eq!(pending.target, Target::module("demo", "notice"));
}

#[test]
fn unknown_channels_are_refused() {
    let outlet = Arc::new(RecordingOutlet::default());
    let error = emitter(outlet, Limits::default())
        .emit(&cx(), "missing", json!({}))
        .expect_err("the channel is not declared");
    assert!(matches!(error, EmitError::UnknownChannel));
}

#[test]
fn invalid_payloads_are_refused() {
    let outlet = Arc::new(RecordingOutlet::default());
    let emitter = ModEmitter::new(
        ModIdentity {
            name: "demo".to_string(),
            version: 1,
        },
        vec![EmitChannel::new(
            "state",
            "m_demo/state",
            Retention::Journal,
            Arc::new(|_: &Value| Err("bad payload".to_string())),
        )],
        outlet,
        Arc::new(Guards::new(Limits::default())),
    );
    let error = emitter
        .emit(&cx(), "state", json!({}))
        .expect_err("the validator refuses");
    assert!(matches!(error, EmitError::InvalidPayload(_)));
}

#[test]
fn the_rate_limit_is_enforced_per_channel() {
    let outlet = Arc::new(RecordingOutlet::default());
    let limits = Limits {
        per_window: 1,
        ..Limits::default()
    };
    let emitter = emitter(outlet, limits);
    emitter
        .emit(&cx(), "state", json!({}))
        .expect("the first emit is admitted");
    let error = emitter
        .emit(&cx(), "notice", json!({}))
        .expect_err("the window is exhausted");
    assert!(matches!(error, EmitError::OverLimit(_)));
}
