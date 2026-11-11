use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use wind_core::script::{self, ScriptRuntime};
use wind_event::{
    CallCx, CallError, CallMode, DispatchCx, Dispatcher, EventOutlet, Limits, ModCaller, ModRoster,
    Origin, Pending, Scope, Source, SubmitError, Subscription,
};
use wind_mod::installer::Installer;
use wind_mod::roster::Roster;
use wind_script::{ScriptCall, ScriptScope, ScriptService, ScriptsError};
use wind_storage::Storage;
use wind_wire::AppDirs;
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

struct Recorded {
    payload: Option<Value>,
    retention: wind_event::Retention,
    target: Target,
}

impl RecordingOutlet {
    fn find(&self, name: &str) -> Recorded {
        let events = self.events.lock().expect("event log");
        let pending = events
            .iter()
            .find(|pending| pending.envelope.name == name)
            .unwrap_or_else(|| panic!("{name} was not submitted"));
        Recorded {
            payload: pending.envelope.payload.clone(),
            retention: pending.retention.clone(),
            target: pending.target.clone(),
        }
    }
}

#[derive(Default)]
struct FakeService {
    calls: Mutex<Vec<(String, String, ScriptScope)>>,
}

#[async_trait::async_trait]
impl ScriptService for FakeService {
    fn compile(
        &self,
        _root: &Path,
        _handlers: &std::collections::HashSet<String>,
    ) -> Result<(), ScriptsError> {
        Ok(())
    }

    async fn call(&self, call: ScriptCall) -> Result<Option<Value>, ScriptsError> {
        self.calls.lock().expect("call log").push((
            call.mod_name.clone(),
            call.handler.clone(),
            call.scope.clone(),
        ));
        call.host.emit("state", json!({ "n": 1 }))?;
        call.host.emit("notice", json!("hi"))?;
        call.host.notify("info", "hello")?;
        Ok(None)
    }
}

struct Fixture {
    _dir: TempDir,
    dirs: AppDirs,
    storage: Storage,
}

impl Fixture {
    async fn new() -> Self {
        let dir = TempDir::new().expect("temp dir");
        let dirs = AppDirs::under(dir.path().join("app"));
        fs::create_dir_all(dirs.mods_root()).expect("mods root");
        let storage = Storage::open(&dirs.db_file()).await.expect("open database");
        Self {
            _dir: dir,
            dirs,
            storage,
        }
    }

    async fn topic(&self) -> i64 {
        self.storage
            .create_topic(None, "topic", None, None, None)
            .await
            .expect("create topic")
    }

    fn write_package(&self, manifest: &Value) -> PathBuf {
        let name = manifest["name"].as_str().expect("name");
        let version = manifest["version"].as_u64().expect("version") as u32;
        let root = self.dirs.mod_version(name, version);
        fs::create_dir_all(&root).expect("version root");
        fs::write(root.join("mod.json"), manifest.to_string()).expect("manifest");
        root
    }
}

fn manifest() -> Value {
    json!({
        "name": "demo",
        "version": 1,
        "events": {
            "emit": {
                "state": {
                    "retention": "journal",
                    "eventKey": "state",
                    "payload": { "type": "object" }
                },
                "notice": {
                    "retention": "live",
                    "payload": { "type": "string" }
                }
            }
        }
    })
}

fn subscription() -> Subscription {
    Subscription {
        mod_name: "demo".to_string(),
        mod_version: 1,
        event: "core/mod.installed",
        handler: "onEvent".to_string(),
        timeout_ms: 1_000,
    }
}

fn call_cx(scope: Scope) -> CallCx {
    CallCx {
        deadline: Instant::now() + Duration::from_secs(1),
        cancel: CancellationToken::new(),
        mode: CallMode::Observe,
        scope,
    }
}

fn topic_scope(topic_id: i64) -> Scope {
    Scope::Topic {
        topic_id,
        instance_id: 9,
    }
}

#[tokio::test]
async fn the_notify_event_is_in_the_directory() {
    let directory = script::directory();
    let meta = directory
        .get("core/mod.notify")
        .expect("notify event is in the directory");
    assert!(meta.log);
}

#[tokio::test]
async fn emit_and_notify_leave_through_the_runtime_sink() {
    let fixture = Fixture::new().await;
    fixture
        .storage
        .install_mod("demo", &manifest().to_string())
        .await
        .expect("install mod");
    let outlet = Arc::new(RecordingOutlet::default());
    let service = Arc::new(FakeService::default());
    let runtime = ScriptRuntime::new(
        fixture.storage.clone(),
        fixture.dirs.clone(),
        Arc::clone(&outlet) as Arc<dyn EventOutlet>,
        Limits::default(),
        Some(Arc::clone(&service) as Arc<dyn ScriptService>),
    );

    runtime
        .call(&subscription(), json!({}), call_cx(topic_scope(3)))
        .await
        .expect("the fake service completes");

    assert_eq!(
        *service.calls.lock().expect("call log"),
        vec![(
            "demo".to_string(),
            "onEvent".to_string(),
            ScriptScope::Topic {
                topic_id: 3,
                instance_id: 9
            }
        )]
    );

    let notify = outlet.find("core/mod.notify");
    assert_eq!(
        notify.payload,
        Some(json!({ "level": "info", "text": "hello" }))
    );
    assert_eq!(notify.retention, wind_event::Retention::Journal);
    assert_eq!(notify.target, Target::Core);

    let state = outlet.find("m_demo/state");
    assert_eq!(state.payload, Some(json!({ "n": 1 })));
    assert_eq!(state.retention, wind_event::Retention::Journal);
    assert_eq!(state.target, Target::module("demo", "state"));

    let notice = outlet.find("m_demo/notice");
    assert_eq!(notice.payload, Some(json!("hi")));
    assert_eq!(notice.retention, wind_event::Retention::Live);
    assert_eq!(notice.target, Target::module("demo", "notice"));
}

#[tokio::test]
async fn a_missing_service_is_a_runtime_error() {
    let fixture = Fixture::new().await;
    fixture
        .storage
        .install_mod("demo", &manifest().to_string())
        .await
        .expect("install mod");
    let runtime = ScriptRuntime::new(
        fixture.storage.clone(),
        fixture.dirs.clone(),
        Arc::new(RecordingOutlet::default()) as Arc<dyn EventOutlet>,
        Limits::default(),
        None,
    );
    let error = runtime
        .call(&subscription(), json!({}), call_cx(topic_scope(3)))
        .await
        .expect_err("no engine is available");
    assert!(matches!(error, CallError::Internal(_)));
}

#[tokio::test]
async fn a_stale_subscription_is_refused() {
    let fixture = Fixture::new().await;
    fixture
        .storage
        .install_mod("demo", &manifest().to_string())
        .await
        .expect("install mod");
    let runtime = ScriptRuntime::new(
        fixture.storage.clone(),
        fixture.dirs.clone(),
        Arc::new(RecordingOutlet::default()) as Arc<dyn EventOutlet>,
        Limits::default(),
        Some(Arc::new(FakeService::default()) as Arc<dyn ScriptService>),
    );
    let mut stale = subscription();
    stale.mod_version = 2;
    let error = runtime
        .call(&stale, json!({}), call_cx(topic_scope(3)))
        .await
        .expect_err("the subscription names an old version");
    assert!(matches!(error, CallError::Internal(_)));
    assert!(error.to_string().contains("stale"));
}

#[tokio::test]
async fn install_and_enable_succeed_without_a_script_service() {
    let fixture = Fixture::new().await;
    let topic_id = fixture.topic().await;
    let outlet = Arc::new(RecordingOutlet::default());
    let runtime = Arc::new(ScriptRuntime::new(
        fixture.storage.clone(),
        fixture.dirs.clone(),
        Arc::clone(&outlet) as Arc<dyn EventOutlet>,
        Limits::default(),
        None,
    ));
    let roster = Arc::new(Roster::load(&fixture.storage).await.expect("roster"));
    let directory = wind_event::directory()
        .merge(wind_mod::events::directory())
        .merge(script::directory());
    let dispatcher = Arc::new(Dispatcher::new(
        directory,
        Arc::clone(&roster) as Arc<dyn ModRoster>,
        Arc::clone(&runtime) as Arc<dyn ModCaller>,
        outlet,
        Limits::default(),
    ));
    let installer = Installer::new(
        fixture.storage.clone(),
        Arc::clone(&dispatcher),
        Arc::clone(&roster),
        fixture.dirs.clone(),
        None,
    );

    let scripted = json!({
        "name": "scripted",
        "version": 1,
        "events": {"on": {"core/mod.installed": {"handler": "onEvent"}}}
    });
    let root = fixture.write_package(&scripted);
    let cx = DispatchCx::root(
        Origin::Internal,
        Scope::Global,
        Source::core("test"),
        Duration::from_secs(1),
    );
    installer.install(&cx, &root).await.expect("install");
    installer
        .enable(&cx, topic_id, "scripted")
        .await
        .expect("enable");
    assert!(roster.enabled(&topic_scope(topic_id)).contains("scripted"));

    let mut sub = subscription();
    sub.mod_name = "scripted".to_string();
    let error = runtime
        .call(&sub, json!({}), call_cx(topic_scope(topic_id)))
        .await
        .expect_err("calling without a service fails at runtime");
    assert!(matches!(error, CallError::Internal(_)));
}
