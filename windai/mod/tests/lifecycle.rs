use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::TempDir;
use wind_event::{
    CallCx, CallError, DispatchCx, Dispatcher, EventOutlet, Limits, ModCaller, ModRoster, Origin,
    Pending, Scope, Source, SubmitError, Subscription,
};
use wind_mod::error::ModError;
use wind_mod::events;
use wind_mod::installer::Installer;
use wind_mod::resources::Resources;
use wind_mod::roster::Roster;
use wind_storage::Storage;
use wind_wire::AppDirs;
use wind_wire::CompiledScripts;
use wind_wire::ScriptService;

struct RecordingCaller {
    calls: Mutex<Vec<String>>,
}

impl Default for RecordingCaller {
    fn default() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl ModCaller for RecordingCaller {
    async fn call(
        &self,
        sub: &Subscription,
        _payload: Value,
        _cx: CallCx,
    ) -> Result<Option<Value>, CallError> {
        self.calls
            .lock()
            .expect("call log")
            .push(sub.mod_name.clone());
        Ok(None)
    }
}

struct RecordingOutlet {
    events: Mutex<Vec<(String, Option<Value>)>>,
}

impl Default for RecordingOutlet {
    fn default() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }
}

impl RecordingOutlet {
    fn names(&self) -> Vec<String> {
        self.events
            .lock()
            .expect("event log")
            .iter()
            .map(|(name, _)| name.clone())
            .collect()
    }

    fn payload(&self, name: &str) -> Option<Value> {
        self.events
            .lock()
            .expect("event log")
            .iter()
            .find(|(event, _)| event == name)
            .and_then(|(_, payload)| payload.clone())
    }
}

impl EventOutlet for RecordingOutlet {
    fn submit(&self, pending: Pending) -> Result<(), SubmitError> {
        self.events
            .lock()
            .expect("event log")
            .push((pending.envelope.name, pending.envelope.payload));
        Ok(())
    }
}

struct ClosedOutlet;

impl EventOutlet for ClosedOutlet {
    fn submit(&self, _pending: Pending) -> Result<(), SubmitError> {
        Err(SubmitError::Closed)
    }
}

#[derive(Default)]
struct TestCompiler {
    handlers: Mutex<Vec<HashSet<String>>>,
    fail: bool,
}

impl TestCompiler {
    fn failing() -> Self {
        Self {
            fail: true,
            ..Self::default()
        }
    }

    fn handlers(&self) -> Vec<HashSet<String>> {
        self.handlers.lock().expect("handler log").clone()
    }
}

#[async_trait::async_trait]
impl ScriptService for TestCompiler {
    fn compile(
        &self,
        _root: &Path,
        handlers: &HashSet<String>,
    ) -> Result<Option<CompiledScripts>, String> {
        self.handlers
            .lock()
            .expect("handler log")
            .push(handlers.clone());
        if self.fail {
            return Err("the script does not compile".to_string());
        }
        Ok(Some(scripts()))
    }

    async fn call(
        &self,
        _mod_name: &str,
        _version: i64,
        _request: &str,
        _input: Value,
    ) -> Result<Value, String> {
        todo!()
    }
}

fn scripts() -> CompiledScripts {
    CompiledScripts {
        modules: HashMap::from([("main".to_string(), vec![1, 2, 3])]),
        fingerprint: "test-engine".to_string(),
    }
}

struct Harness {
    _dir: TempDir,
    dirs: AppDirs,
    storage: Storage,
    bus: Arc<Dispatcher>,
    roster: Arc<Roster>,
    caller: Arc<RecordingCaller>,
    outlet: Arc<RecordingOutlet>,
}

impl Harness {
    async fn new() -> Self {
        Self::with_outlet(Arc::new(RecordingOutlet::default())).await
    }

    async fn with_outlet(outlet: Arc<RecordingOutlet>) -> Self {
        let dir = TempDir::new().expect("temp dir");
        let dirs = AppDirs::under(dir.path().join("app"));
        fs::create_dir_all(dirs.mod_root("mods")).expect("create mods root");
        let storage = Storage::open(&dirs.db_file()).await.expect("open database");
        let roster = Arc::new(Roster::load(&storage).await.expect("load roster"));
        let caller = Arc::new(RecordingCaller::default());
        let bus = Arc::new(Dispatcher::new(
            events::directory(),
            Arc::clone(&roster) as Arc<dyn ModRoster>,
            Arc::clone(&caller) as Arc<dyn ModCaller>,
            outlet.clone() as Arc<dyn EventOutlet>,
            Limits::default(),
        ));
        Self {
            _dir: dir,
            dirs,
            storage,
            bus,
            roster,
            caller,
            outlet,
        }
    }

    fn installer(&self, compiler: Option<Arc<dyn ScriptService>>) -> Installer {
        Installer::new(
            self.storage.clone(),
            Arc::clone(&self.bus),
            Arc::clone(&self.roster),
            self.dirs.clone(),
            compiler,
        )
    }

    fn cx(&self) -> DispatchCx {
        DispatchCx::root(
            Origin::Internal,
            Scope::Global,
            Source::core("test"),
            Duration::from_secs(5),
        )
    }

    async fn topic(&self, label: &str) -> i64 {
        self.storage
            .create_topic(None, label, None, None, None)
            .await
            .expect("create topic")
    }
}

fn topic_scope(topic_id: i64) -> Scope {
    Scope::Topic {
        topic_id,
        instance_id: 1,
    }
}

fn write_package(dirs: &AppDirs, manifest: &Value, files: &[(&str, &str)]) -> PathBuf {
    let name = manifest["name"].as_str().expect("name");
    let version = manifest["version"].as_u64().expect("version") as u32;
    let root = dirs.mod_version(name, version);
    fs::create_dir_all(&root).expect("create version root");
    fs::write(root.join("mod.json"), manifest.to_string()).expect("write manifest");
    for (relative, content) in files {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create resource parent");
        }
        fs::write(path, content).expect("write resource");
    }
    root
}

fn resource_manifest(name: &str, version: u32) -> Value {
    json!({
        "name": name,
        "version": version,
        "mcpServers": {"tool": {"command": "./bin/tool"}}
    })
}

fn resource_files<'a>(prompt: &'a str) -> Vec<(&'a str, &'a str)> {
    vec![
        ("prompts/01.md", prompt),
        ("bin/tool", ""),
        ("ui/index.js", "export {};"),
    ]
}

#[tokio::test]
async fn resource_mod_lifecycle_and_restart_recovery() {
    let harness = Harness::new().await;
    let topic_id = harness.topic("topic").await;
    let resources = Resources::new(harness.storage.clone(), harness.dirs.clone());
    let installer = harness.installer(None);

    let first = resource_manifest("math", 1);
    let first_root = write_package(&harness.dirs, &first, &resource_files("one"));
    let id = installer
        .install(&harness.cx(), &first_root)
        .await
        .expect("install");
    assert_eq!(
        installer.get("math").await.expect("get").map(|row| row.id),
        Some(id)
    );
    assert_eq!(installer.list().await.expect("list").len(), 1);

    let change = installer
        .enable(&harness.cx(), topic_id, "math")
        .await
        .expect("enable");
    assert_eq!(change.affected_topics, vec![topic_id]);
    assert!(
        harness
            .roster
            .enabled(&topic_scope(topic_id))
            .contains("math")
    );
    let listed = resources.topic(topic_id).await.expect("resources");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].version, 1);
    assert_eq!(listed[0].prompts, vec!["one"]);
    assert!(listed[0].ui.is_some());
    assert!(listed[0].mcp.contains_key("tool"));

    let second = resource_manifest("math", 2);
    let second_root = write_package(&harness.dirs, &second, &resource_files("two"));
    let change = installer
        .update(&harness.cx(), &second_root)
        .await
        .expect("update");
    assert_eq!(change.affected_topics, vec![topic_id]);
    let listed = resources.topic(topic_id).await.expect("resources");
    assert_eq!(listed[0].version, 2);
    assert_eq!(listed[0].prompts, vec!["two"]);
    assert_eq!(
        installer
            .get("math")
            .await
            .expect("get")
            .expect("installed")
            .version,
        2
    );

    let change = installer
        .disable(&harness.cx(), topic_id, "math")
        .await
        .expect("disable");
    assert_eq!(change.affected_topics, vec![topic_id]);
    assert!(
        !harness
            .roster
            .enabled(&topic_scope(topic_id))
            .contains("math")
    );
    assert!(
        resources
            .topic(topic_id)
            .await
            .expect("resources")
            .is_empty()
    );

    installer
        .enable(&harness.cx(), topic_id, "math")
        .await
        .expect("enable again");

    let reopened = Storage::open(&harness.dirs.db_file())
        .await
        .expect("reopen database");
    let roster = Arc::new(Roster::load(&reopened).await.expect("load roster"));
    let bus = Arc::new(Dispatcher::new(
        events::directory(),
        Arc::clone(&roster) as Arc<dyn ModRoster>,
        Arc::clone(&harness.caller) as Arc<dyn ModCaller>,
        Arc::clone(&harness.outlet) as Arc<dyn EventOutlet>,
        Limits::default(),
    ));
    let restored = Installer::new(
        reopened.clone(),
        bus,
        Arc::clone(&roster),
        harness.dirs.clone(),
        None,
    );
    let unavailable = restored.restore().await.expect("restore");
    assert!(unavailable.is_empty());
    assert!(roster.enabled(&topic_scope(topic_id)).contains("math"));
    let restored_resources = Resources::new(reopened, harness.dirs.clone());
    let listed = restored_resources
        .topic(topic_id)
        .await
        .expect("restored resources");
    assert_eq!(listed[0].version, 2);
    assert_eq!(listed[0].prompts, vec!["two"]);

    let change = restored
        .delete(&harness.cx(), "math")
        .await
        .expect("delete");
    assert_eq!(change.affected_topics, vec![topic_id]);
    assert!(restored.get("math").await.expect("get").is_none());
    assert!(first_root.is_dir(), "delete keeps version one files");
    assert!(second_root.is_dir(), "delete keeps version two files");
    assert!(!roster.enabled(&topic_scope(topic_id)).contains("math"));
    assert!(
        restored_resources
            .topic(topic_id)
            .await
            .expect("resources")
            .is_empty()
    );
}

#[tokio::test]
async fn missing_compiler_rejects_script_publish() {
    let harness = Harness::new().await;
    let installer = harness.installer(None);
    let manifest = json!({
        "name": "scripted",
        "version": 1,
        "requests": {"run": {"handler": "run"}}
    });
    let root = write_package(&harness.dirs, &manifest, &[]);
    let error = installer
        .install(&harness.cx(), &root)
        .await
        .expect_err("scripts need a compiler");
    assert!(matches!(error, ModError::ScriptsUnavailable { .. }));
    assert!(
        harness
            .storage
            .mod_by_name("scripted")
            .await
            .expect("query")
            .is_none()
    );

    let resource = json!({"name": "resource", "version": 1});
    let root = write_package(&harness.dirs, &resource, &[]);
    installer
        .install(&harness.cx(), &root)
        .await
        .expect("a resource-only mod installs without a compiler");
}

#[tokio::test]
async fn compiler_receives_deduplicated_handlers_and_failures_do_not_commit() {
    let harness = Harness::new().await;
    let compiler = Arc::new(TestCompiler::default());
    let installer = harness.installer(Some(compiler.clone()));
    let manifest = json!({
        "name": "math",
        "version": 1,
        "requests": {
            "one": {"handler": "run"},
            "two": {"handler": "run"}
        }
    });
    let root = write_package(&harness.dirs, &manifest, &[]);
    installer
        .install(&harness.cx(), &root)
        .await
        .expect("install");
    assert_eq!(
        compiler.handlers(),
        vec![HashSet::from(["run".to_string()])],
        "handlers are passed once per install"
    );

    let failing = Arc::new(TestCompiler::failing());
    let installer = harness.installer(Some(failing));
    let manifest = json!({
        "name": "bad",
        "version": 1,
        "requests": {"run": {"handler": "run"}}
    });
    let root = write_package(&harness.dirs, &manifest, &[]);
    let error = installer
        .install(&harness.cx(), &root)
        .await
        .expect_err("the compiler refuses");
    assert!(matches!(error, ModError::Rejected { .. }));
    assert!(
        harness
            .storage
            .mod_by_name("bad")
            .await
            .expect("query")
            .is_none()
    );
}

#[tokio::test]
async fn invalid_event_rejects_publish() {
    let harness = Harness::new().await;
    let compiler = Arc::new(TestCompiler::default());
    let installer = harness.installer(Some(compiler.clone()));
    let manifest = json!({
        "name": "bad",
        "version": 1,
        "events": {"on": {"core/missing": {"handler": "run"}}}
    });
    let root = write_package(&harness.dirs, &manifest, &[]);
    let error = installer
        .install(&harness.cx(), &root)
        .await
        .expect_err("the event is not in the directory");
    assert!(matches!(error, ModError::ManifestUnknownEvent { .. }));
    assert!(compiler.handlers().is_empty(), "nothing is compiled");
    assert!(
        harness
            .storage
            .mod_by_name("bad")
            .await
            .expect("query")
            .is_none()
    );
}

#[tokio::test]
async fn restore_reports_unavailable_scripts() {
    let harness = Harness::new().await;
    let manifest = json!({
        "name": "scripted",
        "version": 1,
        "events": {"on": {"core/mod.installed": {"handler": "run"}}}
    });
    let id = harness
        .storage
        .install_mod("scripted", &manifest.to_string(), Some(&scripts()))
        .await
        .expect("seed a script mod");
    let topic_id = harness.topic("topic").await;
    harness
        .storage
        .enable_mod(topic_id, id)
        .await
        .expect("enable");

    let installer = harness.installer(None);
    let unavailable = installer.restore().await.expect("restore");
    assert_eq!(unavailable.len(), 1);
    assert_eq!(unavailable[0].name, "scripted");
    assert_eq!(unavailable[0].version, 1);
    assert!(!unavailable[0].reason.is_empty());
    assert!(
        harness
            .roster
            .enabled(&topic_scope(topic_id))
            .contains("scripted")
    );

    let envelope = wind_wire::event::Envelope::new(
        events::CORE_MOD_INSTALLED.name,
        topic_scope(topic_id),
        Origin::Internal,
        Source::core("test"),
        None,
        wind_wire::event::Shape::Observe,
    );
    let feed = wind_wire::event::FeedEvent {
        key: wind_wire::event::EventKey::ZERO,
        envelope,
        target: wind_wire::event::Target::Core,
    };
    wind_event::Observer::on_event(&*harness.bus, &feed);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        harness.caller.calls.lock().expect("call log").is_empty(),
        "an unavailable script mod is not subscribed"
    );
}

#[tokio::test]
async fn failed_prepare_preserves_roster() {
    let harness = Harness::new().await;
    let compiler = Arc::new(TestCompiler::default());
    let installer = harness.installer(Some(compiler));
    let topic_id = harness.topic("topic").await;
    let resources = Resources::new(harness.storage.clone(), harness.dirs.clone());

    let manifest = resource_manifest("math", 1);
    let root = write_package(&harness.dirs, &manifest, &resource_files("one"));
    installer
        .install(&harness.cx(), &root)
        .await
        .expect("install");
    installer
        .enable(&harness.cx(), topic_id, "math")
        .await
        .expect("enable");

    let broken = json!({
        "name": "broken",
        "version": 1,
        "events": {"on": {"core/missing": {"handler": "run"}}}
    });
    harness
        .storage
        .install_mod("broken", &broken.to_string(), Some(&scripts()))
        .await
        .expect("seed a broken mod");

    let second = resource_manifest("math", 2);
    let second_root = write_package(&harness.dirs, &second, &resource_files("two"));
    let error = installer
        .update(&harness.cx(), &second_root)
        .await
        .expect_err("the stored manifests cannot be prepared");
    assert!(matches!(error, ModError::ManifestUnknownEvent { .. }));

    assert_eq!(
        harness
            .storage
            .mod_by_name("math")
            .await
            .expect("query")
            .expect("installed")
            .version,
        1,
        "the failed update rolls back"
    );
    assert!(
        harness
            .roster
            .enabled(&topic_scope(topic_id))
            .contains("math")
    );
    let listed = resources.topic(topic_id).await.expect("resources");
    assert_eq!(listed[0].version, 1);
    assert_eq!(listed[0].prompts, vec!["one"]);
    assert!(matches!(
        installer.restore().await,
        Err(ModError::ManifestUnknownEvent { .. })
    ));
}

#[tokio::test]
async fn concurrent_updates_do_not_go_backwards() {
    let harness = Harness::new().await;
    let installer = harness.installer(None);
    let topic_id = harness.topic("topic").await;
    let first = resource_manifest("math", 1);
    let root = write_package(&harness.dirs, &first, &resource_files("one"));
    installer
        .install(&harness.cx(), &root)
        .await
        .expect("install");
    installer
        .enable(&harness.cx(), topic_id, "math")
        .await
        .expect("enable");

    let second = resource_manifest("math", 2);
    let second_root = write_package(&harness.dirs, &second, &resource_files("two"));
    let left_cx = harness.cx();
    let right_cx = harness.cx();
    let left = installer.update(&left_cx, &second_root);
    let right = installer.update(&right_cx, &second_root);
    let (left, right) = tokio::join!(left, right);
    let successes = usize::from(left.is_ok()) + usize::from(right.is_ok());
    assert_eq!(successes, 1, "exactly one update wins");
    let failure = left.err().or_else(|| right.err()).expect("one failure");
    assert!(matches!(failure, ModError::Mismatch { .. }));

    let current = harness
        .storage
        .mod_by_name("math")
        .await
        .expect("query")
        .expect("installed");
    assert_eq!(current.version, 2);
    let resources = Resources::new(harness.storage.clone(), harness.dirs.clone());
    assert_eq!(
        resources.topic(topic_id).await.expect("resources")[0].version,
        2
    );
}

#[tokio::test]
async fn completion_events_follow_commit() {
    let harness = Harness::new().await;
    let topic_id = harness.topic("topic").await;
    let installer = harness.installer(None);

    let first = resource_manifest("math", 1);
    let root = write_package(&harness.dirs, &first, &resource_files("one"));
    installer
        .install(&harness.cx(), &root)
        .await
        .expect("install");
    installer
        .enable(&harness.cx(), topic_id, "math")
        .await
        .expect("enable");
    let second = resource_manifest("math", 2);
    let root = write_package(&harness.dirs, &second, &resource_files("two"));
    installer
        .update(&harness.cx(), &root)
        .await
        .expect("update");
    installer
        .delete(&harness.cx(), "math")
        .await
        .expect("delete");

    assert_eq!(
        harness.outlet.names(),
        vec![
            "core/mod.installed".to_string(),
            "core/mod.updated".to_string(),
            "core/mod.deleted".to_string(),
        ]
    );
    let installed = harness
        .outlet
        .payload("core/mod.installed")
        .expect("installed payload");
    assert_eq!(installed["name"], "math");
    assert_eq!(installed["version"], 1);
    assert!(installed["ui_entry"].as_str().is_some());
    let updated = harness
        .outlet
        .payload("core/mod.updated")
        .expect("updated payload");
    assert_eq!(updated["from_version"], 1);
    assert_eq!(updated["to_version"], 2);
    let deleted = harness
        .outlet
        .payload("core/mod.deleted")
        .expect("deleted payload");
    assert_eq!(deleted["version"], 2);
}

#[tokio::test]
async fn closed_outlet_does_not_revert_the_commit() {
    let harness = Harness::with_outlet(Arc::new(RecordingOutlet::default())).await;
    let topic_id = harness.topic("topic").await;
    let roster = Arc::new(Roster::load(&harness.storage).await.expect("load roster"));
    let caller = Arc::new(RecordingCaller::default());
    let bus = Arc::new(Dispatcher::new(
        events::directory(),
        Arc::clone(&roster) as Arc<dyn ModRoster>,
        caller as Arc<dyn ModCaller>,
        Arc::new(ClosedOutlet) as Arc<dyn EventOutlet>,
        Limits::default(),
    ));
    let installer = Installer::new(
        harness.storage.clone(),
        bus,
        Arc::clone(&roster),
        harness.dirs.clone(),
        None,
    );

    let manifest = resource_manifest("math", 1);
    let root = write_package(&harness.dirs, &manifest, &resource_files("one"));
    installer
        .install(&harness.cx(), &root)
        .await
        .expect("a closed outlet does not fail the install");
    installer
        .enable(&harness.cx(), topic_id, "math")
        .await
        .expect("enable");
    assert!(installer.get("math").await.expect("get").is_some());
    assert!(roster.enabled(&topic_scope(topic_id)).contains("math"));
    let resources = Resources::new(harness.storage.clone(), harness.dirs.clone());
    assert_eq!(resources.topic(topic_id).await.expect("resources").len(), 1);
}

#[tokio::test]
async fn delete_keeps_files_and_other_topics_keep_their_mods() {
    let harness = Harness::new().await;
    let installer = harness.installer(None);
    let first_topic = harness.topic("first").await;
    let second_topic = harness.topic("second").await;
    let manifest = resource_manifest("math", 1);
    let root = write_package(&harness.dirs, &manifest, &resource_files("one"));
    let resources = Resources::new(harness.storage.clone(), harness.dirs.clone());
    installer
        .install(&harness.cx(), &root)
        .await
        .expect("install");
    installer
        .enable(&harness.cx(), first_topic, "math")
        .await
        .expect("enable first");
    installer
        .enable(&harness.cx(), second_topic, "math")
        .await
        .expect("enable second");

    installer
        .disable(&harness.cx(), first_topic, "math")
        .await
        .expect("disable first");
    assert!(
        resources
            .topic(first_topic)
            .await
            .expect("first resources")
            .is_empty()
    );
    assert_eq!(
        resources
            .topic(second_topic)
            .await
            .expect("second resources")
            .len(),
        1,
        "another topic keeps its enablement"
    );

    let change = installer
        .delete(&harness.cx(), "math")
        .await
        .expect("delete");
    assert_eq!(
        change.affected_topics,
        vec![second_topic],
        "only topics that still enable the mod are affected"
    );
    assert!(root.is_dir(), "delete keeps the version files");
    assert!(
        resources
            .topic(second_topic)
            .await
            .expect("second resources")
            .is_empty()
    );
    assert!(
        !harness
            .roster
            .enabled(&topic_scope(second_topic))
            .contains("math")
    );
}
