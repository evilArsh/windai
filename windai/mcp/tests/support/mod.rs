//! 集成测试共用的总线替身与快速构造

#![allow(dead_code)]

use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use wind_event::{
    CallCx, CallError, Dispatcher, EnabledSet, EventOutlet, Limits, ModCaller, ModRoster, Pending,
    Scope, SubmitError, Subscription,
};
use wind_mcp::events::{CORE_MCP_LAUNCH, CORE_MCP_REPLACED};
use wind_mcp::{BoundTool, CallOutcome, Catalog, McpOptions, RegistryHandle};
use wind_wire::mcp::{McpConfig, McpDefinition};

const RECORDING_MOD: &str = "recording";

pub fn bus() -> Arc<Dispatcher> {
    Arc::new(Dispatcher::new(
        wind_event::Directory::default(),
        Arc::new(EmptyRoster),
        Arc::new(NoCaller),
        Arc::new(NoopOutlet),
        Limits::default(),
    ))
}

pub fn options() -> McpOptions {
    McpOptions::new(test_server_directory())
}

pub fn definition(name: &str) -> McpDefinition {
    McpDefinition {
        name: name.to_owned(),
        description: None,
        config: McpConfig::Stdio {
            command: env!("CARGO_BIN_EXE_mcp_test_server").to_owned(),
            args: Vec::new(),
            env: BTreeMap::new(),
            cwd: test_server_directory(),
        },
    }
}

pub fn with_env(mut definition: McpDefinition, key: &str, value: &str) -> McpDefinition {
    let env = match &mut definition.config {
        McpConfig::Stdio { env, .. } => env,
        McpConfig::StreamableHttp { .. } => panic!("the test definition must use stdio"),
    };
    env.insert(key.to_owned(), value.to_owned());
    definition
}

pub fn with_envs<'a>(
    mut definition: McpDefinition,
    envs: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> McpDefinition {
    let env = match &mut definition.config {
        McpConfig::Stdio { env, .. } => env,
        McpConfig::StreamableHttp { .. } => panic!("the test definition must use stdio"),
    };
    for (key, value) in envs {
        env.insert(key.to_owned(), value.to_owned());
    }
    definition
}

pub fn stderr_definition(name: &str, line: &str) -> McpDefinition {
    McpDefinition {
        name: name.to_owned(),
        description: None,
        config: McpConfig::Stdio {
            command: env!("CARGO_BIN_EXE_mcp_exec_helper").to_owned(),
            args: vec!["stderr".to_owned(), line.to_owned()],
            env: BTreeMap::new(),
            cwd: test_server_directory(),
        },
    }
}

pub async fn wait_started(handle: &RegistryHandle, name: &str) {
    let started = Instant::now();
    loop {
        let catalog = handle.list_tools().await;
        if catalog.iter().any(|bound| bound.server == name) {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the service {name} must start"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn wait_gone(handle: &RegistryHandle, name: &str) {
    let started = Instant::now();
    loop {
        let catalog = handle.list_tools().await;
        if !catalog.iter().any(|bound| bound.server == name) {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the service {name} must be removed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn find_tool(handle: &RegistryHandle, server: &str, tool: &str) -> BoundTool {
    let started = Instant::now();
    loop {
        let catalog = handle.list_tools().await;
        if let Some(bound) = catalog
            .iter()
            .find(|bound| bound.server == server && bound.tool.name == tool)
        {
            return bound.clone();
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the service {server} must expose the tool {tool}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn call_tool(handle: &RegistryHandle, tool: &BoundTool, args: Value) -> CallOutcome {
    handle
        .call(
            &tool.server,
            &tool.tool.name,
            args,
            CancellationToken::new(),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
}

pub async fn tool_names(handle: &RegistryHandle) -> Vec<String> {
    let catalog: Catalog = handle.list_tools().await;
    catalog
        .iter()
        .map(|bound| bound.tool.name.clone())
        .collect()
}

pub fn test_server_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_mcp_test_server"))
        .parent()
        .expect("the test server binary must have a parent")
        .to_path_buf()
}

pub fn temporary_directory() -> PathBuf {
    static DIRECTORIES: OnceLock<Mutex<Vec<TempDir>>> = OnceLock::new();
    let directory = TempDir::new().expect("create a temporary directory");
    let path = directory.path().to_path_buf();
    DIRECTORIES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("lock the temporary directories")
        .push(directory);
    path
}

/// 记录 notify 并允许改写 decide 载荷的事件总线
#[derive(Clone)]
pub struct RecordingBus {
    inner: Arc<RecordingInner>,
    dispatcher: Arc<Dispatcher>,
}

type Rewrite = Box<dyn Fn(Value) -> Value + Send + Sync>;

#[derive(Default)]
struct RecordingInner {
    rewrites: Mutex<HashMap<String, Rewrite>>,
    events: Mutex<Vec<(String, Option<Value>)>>,
}

impl Default for RecordingBus {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordingBus {
    pub fn new() -> Self {
        let inner = Arc::new(RecordingInner::default());
        let dispatcher = Dispatcher::new(
            wind_mcp::events::directory(),
            Arc::new(RecordingRoster),
            Arc::new(RecordingCaller {
                inner: Arc::clone(&inner),
            }),
            Arc::new(RecordingOutlet {
                inner: Arc::clone(&inner),
            }),
            Limits::default(),
        );
        dispatcher.rebuild_index(vec![
            recording_subscription(CORE_MCP_LAUNCH.name),
            recording_subscription(CORE_MCP_REPLACED.name),
        ]);
        Self {
            inner,
            dispatcher: Arc::new(dispatcher),
        }
    }

    pub fn dispatcher(&self) -> Arc<Dispatcher> {
        Arc::clone(&self.dispatcher)
    }

    pub fn rewrite(&self, event: &str, rewrite: impl Fn(Value) -> Value + Send + Sync + 'static) {
        self.inner
            .rewrites
            .lock()
            .expect("lock the rewrites")
            .insert(event.to_owned(), Box::new(rewrite));
    }

    pub fn seen(&self, event: &str) -> bool {
        self.inner
            .events
            .lock()
            .expect("lock the recorded events")
            .iter()
            .any(|(name, _)| name == event)
    }

    pub fn payloads(&self, event: &str) -> Vec<Value> {
        self.inner
            .events
            .lock()
            .expect("lock the recorded events")
            .iter()
            .filter(|(name, _)| name == event)
            .filter_map(|(_, payload)| payload.clone())
            .collect()
    }

    pub async fn poll_seen(&self, event: &str) {
        self.poll_until_seen(event, |_| true).await;
    }

    pub async fn poll_until_seen(
        &self,
        event: &str,
        accept: impl Fn(&Value) -> bool + Send + Sync,
    ) {
        let started = Instant::now();
        loop {
            if self.payloads(event).iter().any(&accept) {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "the event {event} must be published"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn recording_subscription(event: &'static str) -> Subscription {
    Subscription {
        mod_name: RECORDING_MOD.to_owned(),
        mod_version: 1,
        event,
        handler: "rewrite".to_owned(),
        timeout_ms: 1000,
    }
}

struct RecordingRoster;

impl ModRoster for RecordingRoster {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::from_names([RECORDING_MOD.to_owned()])
    }
}

struct RecordingCaller {
    inner: Arc<RecordingInner>,
}

#[async_trait::async_trait]
impl ModCaller for RecordingCaller {
    async fn call(
        &self,
        sub: &Subscription,
        payload: Value,
        _cx: CallCx,
    ) -> std::result::Result<Option<Value>, CallError> {
        let rewrites = self.inner.rewrites.lock().expect("lock the rewrites");
        match rewrites.get(sub.event) {
            Some(rewrite) => Ok(Some(rewrite(payload))),
            None => Ok(None),
        }
    }
}

struct RecordingOutlet {
    inner: Arc<RecordingInner>,
}

impl EventOutlet for RecordingOutlet {
    fn submit(&self, pending: Pending) -> std::result::Result<(), SubmitError> {
        self.inner
            .events
            .lock()
            .expect("lock the recorded events")
            .push((pending.envelope.name, pending.envelope.payload));
        Ok(())
    }
}

struct NoopOutlet;

impl EventOutlet for NoopOutlet {
    fn submit(&self, _pending: Pending) -> std::result::Result<(), SubmitError> {
        Ok(())
    }
}

struct EmptyRoster;

impl ModRoster for EmptyRoster {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::none()
    }
}

struct NoCaller;

#[async_trait::async_trait]
impl ModCaller for NoCaller {
    async fn call(
        &self,
        _sub: &Subscription,
        _payload: serde_json::Value,
        _cx: CallCx,
    ) -> std::result::Result<Option<serde_json::Value>, CallError> {
        Err(CallError::Internal(
            "mod calls are not supported".to_owned(),
        ))
    }
}
