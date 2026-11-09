//! 集成测试共用的连接与 actor 替身

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::oneshot;
use wind_event::{
    CallCx, CallError, Dispatcher, EnabledSet, EventOutlet, Limits, ModCaller, ModRoster, Pending,
    Scope, SubmitError, Subscription,
};
use wind_mcp::connector::{Connection, ToolDescriptor};
use wind_mcp::error::{McpError, Result as McpResult};
use wind_mcp::events::{CORE_MCP_LAUNCH, CORE_MCP_REPLACED};
use wind_mcp::options::McpOptions;
use wind_mcp::registry::{BoxFuture, LaunchRequest, Launcher, RegistryHandle, Started};
use wind_mcp::roots::Roots;
use wind_mcp::runner::plan;
use wind_mcp::spec::{ServiceSpec, StdioSpec, Transport};

const RECORDING_MOD: &str = "recording";

pub async fn open_test_server() -> Arc<Connection> {
    open_test_server_with(McpOptions::default()).await
}

pub async fn open_test_server_with(options: McpOptions) -> Arc<Connection> {
    let command = env!("CARGO_BIN_EXE_mcp_test_server").to_owned();
    let cwd = temporary_directory();
    let spec = ServiceSpec {
        name: "test".to_owned(),
        description: None,
        transport: Transport::Stdio(StdioSpec {
            command,
            args: Vec::new(),
            env: BTreeMap::new(),
            cwd,
        }),
    };
    let roots = Roots::new(test_server_directory());
    let Transport::Stdio(stdio) = &spec.transport else {
        panic!("the test server must use the stdio transport");
    };
    let plan = plan(stdio, &roots).expect("the test server must plan");
    Connection::open(&spec, &plan, &options)
        .await
        .expect("the test server must connect")
}

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
    McpOptions::default()
}

pub fn roots() -> Roots {
    Roots::new(test_server_directory())
}

pub fn spec(name: &str) -> ServiceSpec {
    service_spec(name, BTreeMap::new())
}

pub fn stderr_spec(name: &str, line: &str) -> ServiceSpec {
    ServiceSpec {
        name: name.to_owned(),
        description: None,
        transport: Transport::Stdio(StdioSpec {
            command: env!("CARGO_BIN_EXE_mcp_exec_helper").to_owned(),
            args: vec!["stderr".to_owned(), line.to_owned()],
            env: BTreeMap::new(),
            cwd: test_server_directory(),
        }),
    }
}

pub fn spec_with_env(name: &str, key: &str, value: &str) -> ServiceSpec {
    let mut env = BTreeMap::new();
    env.insert(key.to_owned(), value.to_owned());
    service_spec(name, env)
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
            "the service must start"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn wait_generation(handle: &RegistryHandle, name: &str, expected: u64) {
    let started = Instant::now();
    loop {
        if handle.entry_generation(name).await == Some(expected) {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the entry must reach generation {expected}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn wait_gone(handle: &RegistryHandle, name: &str) {
    let started = Instant::now();
    loop {
        if handle.entry_generation(name).await.is_none() {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the entry must be removed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn entry_generation(handle: &RegistryHandle, name: &str) -> u64 {
    handle
        .entry_generation(name)
        .await
        .expect("the entry must exist")
}

pub async fn tool_names(handle: &RegistryHandle) -> Vec<String> {
    let catalog = handle.list_tools().await;
    catalog
        .iter()
        .map(|bound| bound.tool.name.clone())
        .collect()
}

#[derive(Clone, Default)]
pub struct ScriptedLauncher {
    inner: Arc<Scripted>,
}

#[derive(Default)]
struct Scripted {
    waits: Mutex<HashMap<(String, u64), ScriptWait>>,
    tools: Mutex<HashMap<String, Vec<String>>>,
    generation_tools: Mutex<HashMap<(String, u64), Vec<String>>>,
    panics: Mutex<HashSet<(String, u64)>>,
}

enum ScriptWait {
    Waiting(oneshot::Sender<McpResult<Started>>),
    Ready(McpResult<Started>),
}

impl ScriptedLauncher {
    pub fn serve_tools(&self, name: &str, tools: &[&str]) {
        self.inner
            .tools
            .lock()
            .expect("lock the scripted tools")
            .insert(
                name.to_owned(),
                tools.iter().map(|tool| (*tool).to_owned()).collect(),
            );
    }

    pub fn serve_tools_for(&self, name: &str, generation: u64, tools: &[&str]) {
        self.inner
            .generation_tools
            .lock()
            .expect("lock the generation tools")
            .insert(
                (name.to_owned(), generation),
                tools.iter().map(|tool| (*tool).to_owned()).collect(),
            );
    }

    pub fn panic_on_start(&self, name: &str, generation: u64) {
        self.inner
            .panics
            .lock()
            .expect("lock the panicking starts")
            .insert((name.to_owned(), generation));
    }

    pub fn complete(&self, name: &str, generation: u64) {
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();
        tokio::spawn(async move {
            let started = Started {
                connection: open_test_server().await,
                tools: inner.tools_for(&name, generation),
                server_task: None,
            };
            inner.deliver(&name, generation, Ok(started));
        });
    }

    pub fn fail(&self, name: &str, generation: u64, message: &str) {
        self.inner
            .deliver(name, generation, Err(McpError::Service(message.to_owned())));
    }
}

impl Scripted {
    fn tools_for(&self, name: &str, generation: u64) -> Vec<ToolDescriptor> {
        let names = self
            .generation_tools
            .lock()
            .expect("lock the generation tools")
            .get(&(name.to_owned(), generation))
            .cloned()
            .or_else(|| {
                self.tools
                    .lock()
                    .expect("lock the scripted tools")
                    .get(name)
                    .cloned()
            })
            .unwrap_or_else(|| vec!["echo".to_owned()]);
        descriptors(names)
    }

    fn tool_descriptors(&self, name: &str) -> Vec<ToolDescriptor> {
        let names = self
            .tools
            .lock()
            .expect("lock the scripted tools")
            .get(name)
            .cloned()
            .unwrap_or_else(|| vec!["echo".to_owned()]);
        descriptors(names)
    }

    fn deliver(&self, name: &str, generation: u64, result: McpResult<Started>) {
        let key = (name.to_owned(), generation);
        let mut pending = Some(result);
        let waiting = {
            let mut waits = self.waits.lock().expect("lock the scripted waits");
            match waits.remove(&key) {
                Some(ScriptWait::Waiting(sender)) => Some(sender),
                Some(ScriptWait::Ready(previous)) => {
                    waits.insert(key, ScriptWait::Ready(previous));
                    None
                }
                None => {
                    waits.insert(
                        key,
                        ScriptWait::Ready(
                            pending.take().expect("the delivered result must be fresh"),
                        ),
                    );
                    None
                }
            }
        };
        if let Some(sender) = waiting {
            let result = pending.take().expect("the delivered result must be fresh");
            if let Err(result) = sender.send(result)
                && let Ok(started) = result
            {
                discard(started);
            }
        }
        if let Some(result) = pending
            && let Ok(started) = result
        {
            discard(started);
        }
    }
}

fn descriptors(names: Vec<String>) -> Vec<ToolDescriptor> {
    names
        .into_iter()
        .map(|name| ToolDescriptor {
            name,
            description: None,
            input_schema: json!({"type": "object"}),
        })
        .collect()
}

impl Launcher for ScriptedLauncher {
    fn start<'a>(
        &'a self,
        request: &'a LaunchRequest,
        _roots: &'a Roots,
        _bus: &'a Dispatcher,
        generation: u64,
    ) -> BoxFuture<'a, McpResult<Started>> {
        Box::pin(async move {
            let key = (request.name(), generation);
            if self
                .inner
                .panics
                .lock()
                .expect("lock the panicking starts")
                .contains(&key)
            {
                panic!(
                    "the scripted launcher panicked for {} generation {generation}",
                    key.0
                );
            }
            let (sender, receiver) = oneshot::channel();
            let ready = {
                let mut waits = self.inner.waits.lock().expect("lock the scripted waits");
                match waits.remove(&key) {
                    Some(ScriptWait::Ready(result)) => Some(result),
                    Some(ScriptWait::Waiting(_)) => {
                        panic!(
                            "the scripted launcher started {} generation {generation} twice",
                            key.0
                        );
                    }
                    None => {
                        waits.insert(key, ScriptWait::Waiting(sender));
                        None
                    }
                }
            };
            match ready {
                Some(result) => result,
                None => receiver.await.unwrap_or_else(|_| {
                    Err(McpError::Service(
                        "the scripted launch was dropped".to_owned(),
                    ))
                }),
            }
        })
    }

    fn list_tools<'a>(
        &'a self,
        name: &'a str,
        _connection: &'a Arc<Connection>,
    ) -> BoxFuture<'a, McpResult<Vec<ToolDescriptor>>> {
        Box::pin(async move { Ok(self.inner.tool_descriptors(name)) })
    }
}

fn discard(started: Started) {
    tokio::spawn(async move {
        started.connection.close().await;
        if let Some(task) = started.server_task {
            task.abort();
        }
    });
}

fn service_spec(name: &str, env: BTreeMap<String, String>) -> ServiceSpec {
    ServiceSpec {
        name: name.to_owned(),
        description: None,
        transport: Transport::Stdio(StdioSpec {
            command: env!("CARGO_BIN_EXE_mcp_test_server").to_owned(),
            args: Vec::new(),
            env,
            cwd: test_server_directory(),
        }),
    }
}

fn test_server_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_mcp_test_server"))
        .parent()
        .expect("the test server binary must have a parent")
        .to_path_buf()
}

fn temporary_directory() -> PathBuf {
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

#[derive(Default)]
struct RecordingInner {
    rewrites: Mutex<HashMap<String, Box<dyn Fn(Value) -> Value + Send + Sync>>>,
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
