//! 真 JS mod 拦住一次工具调用

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use wind_core::agent;
use wind_core::conversation;
use wind_core::db::Db;
use wind_core::feed::frame::{Baseline, Frame};
use wind_core::feed::hub::FeedHub;
use wind_core::feed::subscribe::Subscribe;
use wind_core::llm;
use wind_core::models::{
    AgentInstanceCreate, AgentPhase, AgentRole, BlockKind, BlockRow, MessageCreate, Role,
    TurnCreate,
};
use wind_core::storage::Storage;
use wind_core::usecase;
use wind_event::context::DispatchCx;
use wind_event::def::EventMeta;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{EventSeq, Origin, Scope, Source};
use wind_event::host::{CallCx, CallMode, HostError, ModHost};
use wind_event::observer::BuiltinObserver;
use wind_event::subscription::{Phase, Subscription};
use wind_mcp::events as mcp_events;
use wind_mod::assembly::rebuild_subscriptions;
use wind_mod::dirs::AppDirs;
use wind_mod::enablement::Enablement;
use wind_mod::events as mod_events;
use wind_mod::feed::ModFeed;
use wind_mod::installer::{Installer, NoServers};
use wind_mod::manifest::{DEFAULT_SUB_TIMEOUT_MS, Manifest, parse as parse_manifest};
use wind_script::assembly::{DeferredHost, ScriptCompiler, ScriptRuntime};
use wind_script::bridge;
use wind_script::bytecode::{compile_scripts, engine_fingerprint};
use wind_script::circuit::Breaker;
use wind_script::deployment::{DeployError, Deployment, NotDeployed};
use wind_script::facade::{FacadeError, HostApi, HttpRequest, TranscriptEntry};
use wind_script::request::RequestError;
use wind_script::runtime::{CallHost, HostFactory, ScriptHost, ScriptHostOptions};
use wind_script::threads::ThreadPool;

/// 一个写好的包，目录由它自己持有
struct Package {
    /// 安装根，随结构体一起活到用例结束
    root: tempfile::TempDir,
    /// 该版本的清单
    manifest: wind_mod::manifest::Manifest,
}

/// 写一个只含宿主侧脚本的包
fn package(source: &str, manifest_raw: &str) -> Package {
    let root = tempfile::tempdir().expect("create temp dir");
    let scripts = root.path().join("scripts");
    std::fs::create_dir_all(&scripts).expect("create dir");
    std::fs::write(scripts.join("index.js"), source).expect("write the script");
    let manifest = parse_manifest(manifest_raw, &wind_script::events::directory())
        .expect("the manifest is valid");
    Package { root, manifest }
}

/// 状态的两半
///
/// 写下的值先进 `pending`，`finish` 时才写进 `stored`
#[derive(Default)]
struct State {
    /// 已经写回的值
    stored: HashMap<String, serde_json::Value>,
    /// 本次调用写下、等 `finish` 的值
    pending: HashMap<String, serde_json::Value>,
}

/// 一次调用期间的宿主替身
///
/// 状态走一个内存表，出网一律取回一段固定文本，其余调用记在一张清单里
#[derive(Default)]
struct FakeHost {
    /// 对外状态的两半
    state: Mutex<State>,
    /// `finish` 交出去的、发生变化的键，真实实现据它广播
    flushed: Mutex<Vec<(String, serde_json::Value)>>,
    /// 记录被触达过的方法
    seen: Mutex<Vec<String>>,
    /// 出网要睡多久
    http_delay: Option<Duration>,
    /// `finish` 是否一律报错
    finish_fails: bool,
}

impl FakeHost {
    /// 已经写回的键
    fn stored(&self, key: &str) -> Option<serde_json::Value> {
        self.state
            .lock()
            .expect("lock the state")
            .stored
            .get(key)
            .cloned()
    }

    /// `finish` 交出去的、发生变化的键
    fn flushed(&self) -> Vec<(String, serde_json::Value)> {
        self.flushed.lock().expect("lock the state").clone()
    }
}

impl HostApi for FakeHost {
    fn state_read(&self, key: &str) -> Option<serde_json::Value> {
        let state = self.state.lock().expect("lock the state");
        state
            .stored
            .get(key)
            .or_else(|| state.pending.get(key))
            .cloned()
    }
    fn state_write(&self, key: &str, value: serde_json::Value) -> Result<(), FacadeError> {
        self.state
            .lock()
            .expect("lock the state")
            .pending
            .insert(key.to_string(), value);
        Ok(())
    }
    fn state_delete(&self, key: &str) {
        let mut state = self.state.lock().expect("lock the state");
        state.stored.remove(key);
        state.pending.remove(key);
    }
    fn state_keys(&self) -> Vec<String> {
        let state = self.state.lock().expect("lock the state");
        let mut keys: Vec<String> = state
            .stored
            .keys()
            .chain(state.pending.keys())
            .cloned()
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }
    fn storage_read(&self, key: &str) -> Option<serde_json::Value> {
        self.state_read(key)
    }
    fn storage_write(&self, key: &str, value: serde_json::Value) {
        self.state_write(key, value).expect("lock the state");
    }
    fn storage_delete(&self, key: &str) {
        self.state_delete(key);
    }
    fn storage_keys(&self) -> Vec<String> {
        self.state_keys()
    }
    fn http_fetch(&self, _init: HttpRequest) -> Result<Vec<u8>, FacadeError> {
        self.seen
            .lock()
            .expect("lock the state")
            .push("http".into());
        if let Some(delay) = self.http_delay {
            // ctx 调用阻塞在这条线程上，中断处理此时不生效
            std::thread::sleep(delay);
        }
        Ok(b"blocked".to_vec())
    }
    fn http_fetch_many(&self, requests: Vec<HttpRequest>) -> Vec<Result<Vec<u8>, FacadeError>> {
        requests
            .into_iter()
            .map(|request| self.http_fetch(request))
            .collect()
    }
    fn agent_prompt(&self, _scope: &Scope, text: &str) -> Result<(), FacadeError> {
        self.seen
            .lock()
            .expect("lock the state")
            .push(format!("prompt:{text}"));
        Ok(())
    }
    fn agent_draft(&self, _scope: &Scope, text: &str) -> Result<(), FacadeError> {
        self.seen
            .lock()
            .expect("lock the state")
            .push(format!("draft:{text}"));
        Ok(())
    }
    fn agent_spawn(&self, _scope: &Scope, mode: &str, task: &str) -> Result<(), FacadeError> {
        self.seen
            .lock()
            .expect("lock the state")
            .push(format!("spawn:{mode}:{task}"));
        Ok(())
    }
    fn agent_transcript(&self, _scope: &Scope, limit: Option<u32>) -> Vec<TranscriptEntry> {
        vec![TranscriptEntry {
            message_id: 1,
            role: "assistant".to_string(),
            // 未给出条数时由宿主自己定缺省值，替身只把取得的值交出来
            text: format!("limit={}", limit.unwrap_or(0)),
        }]
    }
    fn fs_dir(&self, name: &str) -> PathBuf {
        PathBuf::from(name)
    }
    fn fs_read(&self, _name: &str, _path: &str) -> Result<String, FacadeError> {
        Ok(String::new())
    }
    fn fs_write(&self, _name: &str, _path: &str, _data: &str) -> Result<(), FacadeError> {
        Ok(())
    }
    fn fs_list(&self, _name: &str, _path: &str) -> Result<Vec<String>, FacadeError> {
        Ok(Vec::new())
    }
    fn fs_remove(&self, _name: &str, _path: &str) -> Result<(), FacadeError> {
        Ok(())
    }
    fn emit(
        &self,
        _scope: &Scope,
        name: &str,
        _payload: serde_json::Value,
    ) -> Result<(), FacadeError> {
        self.seen
            .lock()
            .expect("lock the state")
            .push(format!("emit:{name}"));
        Ok(())
    }
    fn notify(&self, _scope: &Scope, level: &str, text: &str) -> Result<(), FacadeError> {
        self.seen
            .lock()
            .expect("lock the state")
            .push(format!("notify:{level}:{text}"));
        Ok(())
    }
    fn log(&self, level: &str, message: &str) {
        self.seen
            .lock()
            .expect("lock the state")
            .push(format!("log:{level}:{message}"));
    }
}

#[async_trait::async_trait]
impl CallHost for FakeHost {
    /// 替身不按期限约束自己的阻塞调用，因此重设只记一笔
    fn rebase(&self, _deadline: tokio::time::Instant) {}

    async fn finish(&self) -> Result<(), DeployError> {
        // 真实实现会在这里写回状态并广播。替身把本次调用写下的值写进 `stored`，
        // 并留下「变化了哪些键」供用例查看
        let written: Vec<(String, serde_json::Value)> = {
            let mut state = self.state.lock().expect("lock the state");
            let written: Vec<(String, serde_json::Value)> = state.pending.drain().collect();
            for (key, value) in &written {
                state.stored.insert(key.clone(), value.clone());
            }
            written
        };
        self.flushed.lock().expect("lock the state").extend(written);
        if self.finish_fails {
            return Err(DeployError::NotDeployed(NotDeployed {
                name: "demo".to_string(),
                version: 1,
            }));
        }
        Ok(())
    }
}

/// 一个只交出 `Deployment` 的工厂
struct OneDeployment {
    /// 安装根，由 Package 持有
    root: PathBuf,
    /// 该版本的清单
    manifest: wind_mod::manifest::Manifest,
    /// 该版本编译好的字节码，为空时入口改读源码
    compiled: BTreeMap<String, Vec<u8>>,
    /// 出网要睡多久，透给每一次开出来的宿主
    http_delay: Option<Duration>,
    /// 每次打开宿主时建的替身，用例留一份句柄查它记下的调用
    hosts: Arc<Mutex<Vec<Arc<FakeHost>>>>,
    /// 每一次 open 取得的作用域，按先后顺序
    scopes: Arc<Mutex<Vec<Scope>>>,
    /// 解释器线程的池，用例要按 `busy()` 观察占用
    pool: Arc<ThreadPool>,
    /// 连续超时的熔断，用例要查它是否已经拦下某个 mod
    breaker: Arc<Breaker>,
    /// 开出来的宿主 `finish` 是否一律报错
    finish_fails: bool,
}

impl OneDeployment {
    /// 按一份包建工厂
    fn of(package: &Package) -> Self {
        Self {
            root: package.root.path().to_path_buf(),
            manifest: package.manifest.clone(),
            compiled: BTreeMap::new(),
            http_delay: None,
            hosts: Arc::new(Mutex::new(Vec::new())),
            scopes: Arc::new(Mutex::new(Vec::new())),
            pool: Arc::new(ThreadPool::new(2)),
            breaker: Arc::new(Breaker::new(3)),
            finish_fails: false,
        }
    }

    /// 交出一份安装期编译好的字节码
    fn with_compiled(mut self, compiled: BTreeMap<String, Vec<u8>>) -> Self {
        self.compiled = compiled;
        self
    }

    /// 让出网睡一段时间，用来观察一次调用在宿主函数里卡住时的处置
    fn with_http_delay(mut self, delay: Duration) -> Self {
        self.http_delay = Some(delay);
        self
    }

    /// 换一个并发上限更小的池，用来观察池满
    fn with_pool(mut self, threads: usize) -> Self {
        self.pool = Arc::new(ThreadPool::new(threads));
        self
    }

    /// 让每一次写回都报错，用来观察写回失败时的处置
    fn with_failing_flush(mut self) -> Self {
        self.finish_fails = true;
        self
    }

    /// 包成宿主要的形式
    fn into_script_host(self) -> ScriptHost {
        let pool = Arc::clone(&self.pool);
        let breaker = Arc::clone(&self.breaker);
        ScriptHost::new(ScriptHostOptions {
            factory: Arc::new(self),
            pool,
            breaker,
        })
    }
}

#[async_trait::async_trait]
impl HostFactory for OneDeployment {
    async fn open(
        &self,
        name: &str,
        version: i64,
        scope: &Scope,
        _cx: &CallCx,
    ) -> Result<(Deployment, Box<dyn CallHost>), DeployError> {
        let host = Arc::new(FakeHost {
            http_delay: self.http_delay,
            finish_fails: self.finish_fails,
            ..FakeHost::default()
        });
        self.hosts
            .lock()
            .expect("lock the state")
            .push(host.clone());
        self.scopes
            .lock()
            .expect("lock the state")
            .push(scope.clone());
        let deployment = Deployment {
            name: name.to_string(),
            version,
            root: self.root.clone(),
            manifest: self.manifest.clone(),
            compiled: self.compiled.clone(),
        };
        Ok((deployment, Box::new(SharedHost(host))))
    }
}

/// 一个打不开 `Deployment` 的工厂
struct NoDeployment;

#[async_trait::async_trait]
impl HostFactory for NoDeployment {
    async fn open(
        &self,
        name: &str,
        version: i64,
        _scope: &Scope,
        _cx: &CallCx,
    ) -> Result<(Deployment, Box<dyn CallHost>), DeployError> {
        Err(DeployError::NotDeployed(NotDeployed {
            name: name.to_string(),
            version,
        }))
    }
}

/// 让 `Arc<FakeHost>` 充当一份 `CallHost`
///
/// 用例要留一份句柄查它记下的调用
struct SharedHost(Arc<FakeHost>);

impl HostApi for SharedHost {
    fn state_read(&self, key: &str) -> Option<serde_json::Value> {
        self.0.state_read(key)
    }
    fn state_write(&self, key: &str, value: serde_json::Value) -> Result<(), FacadeError> {
        self.0.state_write(key, value)
    }
    fn state_delete(&self, key: &str) {
        self.0.state_delete(key);
    }
    fn state_keys(&self) -> Vec<String> {
        self.0.state_keys()
    }
    fn storage_read(&self, key: &str) -> Option<serde_json::Value> {
        self.0.storage_read(key)
    }
    fn storage_write(&self, key: &str, value: serde_json::Value) {
        self.0.storage_write(key, value);
    }
    fn storage_delete(&self, key: &str) {
        self.0.storage_delete(key);
    }
    fn storage_keys(&self) -> Vec<String> {
        self.0.storage_keys()
    }
    fn http_fetch(&self, init: HttpRequest) -> Result<Vec<u8>, FacadeError> {
        self.0.http_fetch(init)
    }
    fn http_fetch_many(&self, requests: Vec<HttpRequest>) -> Vec<Result<Vec<u8>, FacadeError>> {
        self.0.http_fetch_many(requests)
    }
    fn agent_prompt(&self, scope: &Scope, text: &str) -> Result<(), FacadeError> {
        self.0.agent_prompt(scope, text)
    }
    fn agent_draft(&self, scope: &Scope, text: &str) -> Result<(), FacadeError> {
        self.0.agent_draft(scope, text)
    }
    fn agent_spawn(&self, scope: &Scope, mode: &str, task: &str) -> Result<(), FacadeError> {
        self.0.agent_spawn(scope, mode, task)
    }
    fn agent_transcript(&self, scope: &Scope, limit: Option<u32>) -> Vec<TranscriptEntry> {
        self.0.agent_transcript(scope, limit)
    }
    fn fs_dir(&self, name: &str) -> PathBuf {
        self.0.fs_dir(name)
    }
    fn fs_read(&self, name: &str, path: &str) -> Result<String, FacadeError> {
        self.0.fs_read(name, path)
    }
    fn fs_write(&self, name: &str, path: &str, data: &str) -> Result<(), FacadeError> {
        self.0.fs_write(name, path, data)
    }
    fn fs_list(&self, name: &str, path: &str) -> Result<Vec<String>, FacadeError> {
        self.0.fs_list(name, path)
    }
    fn fs_remove(&self, name: &str, path: &str) -> Result<(), FacadeError> {
        self.0.fs_remove(name, path)
    }
    fn emit(
        &self,
        scope: &Scope,
        name: &str,
        payload: serde_json::Value,
    ) -> Result<(), FacadeError> {
        self.0.emit(scope, name, payload)
    }
    fn notify(&self, scope: &Scope, level: &str, text: &str) -> Result<(), FacadeError> {
        self.0.notify(scope, level, text)
    }
    fn log(&self, level: &str, message: &str) {
        self.0.log(level, message);
    }
}

#[async_trait::async_trait]
impl CallHost for SharedHost {
    /// 转给被共用的 `FakeHost`
    fn rebase(&self, deadline: tokio::time::Instant) {
        self.0.rebase(deadline);
    }

    async fn finish(&self) -> Result<(), DeployError> {
        self.0.finish().await
    }
}

/// 一条订阅
fn sub(handler: &str) -> Subscription {
    Subscription {
        mod_name: "demo".to_string(),
        mod_version: 1,
        event: "core/mcp.tool.call",
        phase: Some(Phase::Guard),
        handler: handler.to_string(),
        timeout_ms: 50,
        registration_seq: 1,
    }
}

/// 一次调用的约束，按派发器的方式建
///
/// 截止时刻取订阅期限与调用方剩余预算的较小值
fn cx_for(sub_timeout_ms: u64, caller_turn: Duration) -> CallCx {
    CallCx {
        deadline: tokio::time::Instant::now()
            + caller_turn.min(Duration::from_millis(sub_timeout_ms)),
        cancel: tokio_util::sync::CancellationToken::new(),
        mode: CallMode::Intercept,
        scope: Scope::Global,
    }
}

/// 默认订阅是 50 毫秒，调用方剩下的轮次则很宽裕：约束这一次的是订阅自己
fn cx() -> CallCx {
    cx_for(50, Duration::from_secs(10))
}

/// 一次调用的载荷
fn tool_call(url: &str) -> serde_json::Value {
    serde_json::json!({
        "server_name": "web-fetch",
        "tool": "fetch",
        "arguments": { "url": url },
    })
}

#[tokio::test]
async fn a_handler_that_returns_the_old_action_wrapper_is_reported() {
    let package = package(
        r#"export function onCall() {
               return { action: "deny", reason: "example.com is not allowed" };
           }"#,
        r#"{"name":"demo","version":1}"#,
    );
    let host = OneDeployment::of(&package).into_script_host();

    let error = host
        .call(&sub("onCall"), tool_call("https://example.com/a"), cx())
        .await
        .expect_err("deny was removed");
    assert!(
        matches!(error, HostError::Handler(ref message) if message.contains("action wrapper")),
        "the handler error names the removed wrapper: {error:?}"
    );
}

#[tokio::test]
async fn the_entry_is_served_from_the_compiled_bytecode_when_the_deployment_has_it() {
    // 要跑的是安装期编译好的字节码。磁盘上的源码随后被换掉也不该改到结论
    let package = package(
        "export function onCall() { return 'from the bytecode'; }",
        r#"{"name":"demo","version":1}"#,
    );
    let compiled = compile_scripts(package.root.path(), &BTreeSet::from(["onCall".to_string()]))
        .expect("compile the scripts");
    std::fs::write(
        package.root.path().join("scripts").join("index.js"),
        "export function onCall() { return 'from the source'; }",
    )
    .expect("replace the entry");

    let host = OneDeployment::of(&package)
        .with_compiled(compiled.modules)
        .into_script_host();
    let outcome = host
        .call(&sub("onCall"), tool_call("https://a"), cx())
        .await
        .expect("dispatch");
    assert!(
        matches!(outcome, Some(ref value) if value == &serde_json::json!("from the bytecode")),
        "when bytecode is present the entry must be taken from the bytecode, got {outcome:?}"
    );
}

#[tokio::test]
async fn a_state_write_is_flushed_when_the_call_ends() {
    let package = package(
        r#"export function onCall(payload, ctx) {
               ctx.state.set("blocked", 1);
               return;
           }"#,
        r#"{"name":"demo","version":1,"capabilities":["state"]}"#,
    );
    let factory = OneDeployment::of(&package);
    let opened = Arc::clone(&factory.hosts);
    let host = factory.into_script_host();
    let outcome = host
        .call(&sub("onCall"), tool_call("https://a"), cx())
        .await
        .expect("dispatch");
    assert!(outcome.is_none(), "{outcome:?}");

    let opened = opened.lock().expect("lock the state");
    let first = opened.first().expect("the host was opened once");
    assert_eq!(
        first.stored("blocked"),
        Some(serde_json::json!(1)),
        "state written during the call must be flushed when it ends"
    );
    let flushed = first.flushed();
    assert_eq!(
        flushed.len(),
        1,
        "changed keys must be handed to the broadcast: {flushed:?}"
    );
    assert_eq!(flushed[0].0, "blocked");
    assert_eq!(flushed[0].1, serde_json::json!(1));
}

#[tokio::test]
async fn a_state_write_survives_a_handler_that_throws() {
    // 抛错之前写下的那些同样是这次调用的结果，不能跟着错误一起丢
    let package = package(
        r#"export function onCall(payload, ctx) {
               ctx.state.set("attempts", 1);
               throw new Error("upstream is down");
           }"#,
        r#"{"name":"demo","version":1,"capabilities":["state"]}"#,
    );
    let factory = OneDeployment::of(&package);
    let opened = Arc::clone(&factory.hosts);
    let host = factory.into_script_host();
    let outcome = host
        .call(&sub("onCall"), tool_call("https://a"), cx())
        .await;
    assert!(
        matches!(outcome, Err(HostError::Handler(_))),
        "a handler that throws must come back as a failure, got {outcome:?}"
    );

    let opened = opened.lock().expect("lock the state");
    let first = opened.first().expect("the host was opened once");
    assert_eq!(
        first.stored("attempts"),
        Some(serde_json::json!(1)),
        "a call that throws must flush as well"
    );
}

#[tokio::test]
async fn a_state_write_survives_a_call_that_never_returns() {
    // 写下状态之后卡在宿主函数里。被放弃的是工作线程，实现仍然要写回已经写下的值
    let package = package(
        r#"export function onCall(payload, ctx) {
               ctx.state.set("blocked", 1);
               ctx.http.fetchText("https://slow");
               return;
           }"#,
        r#"{"name":"demo","version":1,"capabilities":["state","http"]}"#,
    );
    let factory = OneDeployment::of(&package).with_http_delay(Duration::from_secs(30));
    let opened = Arc::clone(&factory.hosts);
    let host = factory.into_script_host();
    // 约束这一次的是订阅自己那 400 毫秒，调用方剩下的轮次比它宽
    let mut patient = cx_for(400, Duration::from_secs(10));
    patient.mode = CallMode::Request;
    let mut slow = sub("onCall");
    slow.timeout_ms = 400;
    let outcome = host.call(&slow, tool_call("https://a"), patient).await;
    assert!(
        outcome.is_err(),
        "after a timeout the caller gets an error, got {outcome:?}"
    );

    let opened = opened.lock().expect("lock the state");
    let first = opened.first().expect("the host was opened once");
    assert_eq!(
        first.stored("blocked"),
        Some(serde_json::json!(1)),
        "the timeout path must flush as well"
    );
}

#[tokio::test]
async fn a_handler_that_rewrites_the_payload_passes_the_new_value_on() {
    let package = package(
        r#"export function onCall(payload) {
               return {
                   server_name: payload.server_name,
                   tool: payload.tool,
                   arguments: { url: "https://rewritten" },
               };
           }"#,
        r#"{"name":"demo","version":1}"#,
    );
    let host = OneDeployment::of(&package).into_script_host();
    let outcome = host
        .call(&sub("onCall"), tool_call("https://a"), cx())
        .await
        .expect("dispatch");
    match outcome {
        Some(value) => {
            assert_eq!(value["arguments"]["url"], "https://rewritten")
        }
        other => panic!("expected a rewrite, got {other:?}"),
    }
}

#[tokio::test]
async fn a_handler_suspends_calls_by_rewriting_the_payload() {
    let package = package(
        r#"export function onCall(payload) {
               payload.suspended = ["c-1"];
               return payload;
           }"#,
        r#"{"name":"demo","version":1}"#,
    );
    let host = OneDeployment::of(&package).into_script_host();
    let outcome = host
        .call(&sub("onCall"), tool_call("https://a"), cx())
        .await
        .expect("dispatch");
    match outcome {
        Some(value) => assert_eq!(value["suspended"], serde_json::json!(["c-1"])),
        other => panic!("expected a rewrite, got {other:?}"),
    }
}

#[tokio::test]
async fn a_handler_that_uses_an_undeclared_capability_gets_a_type_error() {
    // 清单没有声明 state，因此 ctx.state 在解释器里根本不存在
    let package = package(
        r#"export function onCall(payload, ctx) {
               try { ctx.state.get("k"); return "no throw"; }
               catch (error) { return error.name; }
           }"#,
        r#"{"name":"demo","version":1}"#,
    );
    let host = OneDeployment::of(&package).into_script_host();
    let outcome = host
        .call(&sub("onCall"), tool_call("https://a"), cx())
        .await
        .expect("dispatch");
    assert!(
        matches!(outcome, Some(ref value) if value == &serde_json::json!("TypeError")),
        "an undeclared capability must be a property read failure, got {outcome:?}"
    );
}

#[tokio::test]
async fn a_handler_that_declares_a_capability_that_waits_cannot_reach_it() {
    // 拦截调用里需要等待的能力一律不存在，因此脚本连写都写不出来，宿主也不会被碰到
    let package = package(
        r#"export function onCall(payload, ctx) {
               try { ctx.http.fetchText("https://a"); return; }
               catch (error) { return error.name; }
           }"#,
        r#"{"name":"demo","version":1,"capabilities":["http"]}"#,
    );
    let factory = OneDeployment::of(&package);
    let opened = Arc::clone(&factory.hosts);
    let host = factory.into_script_host();
    let outcome = host
        .call(&sub("onCall"), tool_call("https://a"), cx())
        .await
        .expect("dispatch");
    assert!(
        matches!(outcome, Some(ref value) if value == &serde_json::json!("TypeError")),
        "network access in an intercept call must be absent rather than erroring, got {outcome:?}"
    );
    let opened = opened.lock().expect("lock the state");
    let first = opened.first().expect("the host was opened once");
    assert!(
        first.seen.lock().expect("lock the state").is_empty(),
        "when the property is absent the host side must not be touched at all"
    );
}

#[tokio::test]
async fn a_handler_that_loops_forever_is_interrupted_at_the_deadline() {
    let package = package(
        "export function onCall() { while (true) {} }",
        r#"{"name":"demo","version":1}"#,
    );
    let host = OneDeployment::of(&package).into_script_host();
    let started = std::time::Instant::now();
    let outcome = host
        .call(&sub("onCall"), tool_call("https://a"), cx())
        .await;
    assert!(
        matches!(outcome, Err(HostError::Handler(_))),
        "an infinite loop must be interrupted, got {outcome:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the interruption must be prompt; it actually took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn an_allocation_over_the_interpreter_budget_is_a_catchable_java_script_error() {
    // 解释器的内存上限在每一次建运行时处设下：一个分配循环必须报成一次可捕获的
    // JS 失败，而不是把宿主进程交给 OOM 杀手
    let package = package(
        r#"export function onCall() {
               try {
                   const held = [];
                   while (true) { held.push(new Uint8Array(1 << 20)); }
               } catch (error) {
                   return error.name;
               }
           }"#,
        r#"{"name":"demo","version":1}"#,
    );
    let host = OneDeployment::of(&package).into_script_host();
    let mut call = sub("onCall");
    // 分配循环跑在中断之前，因此给它一个宽裕的预算，让上限先到
    call.timeout_ms = 5_000;
    let outcome = host
        .call(
            &call,
            tool_call("https://a"),
            cx_for(5_000, Duration::from_secs(10)),
        )
        .await;
    match outcome {
        Ok(Some(value)) => assert_eq!(
            value,
            serde_json::json!("InternalError"),
            "an over-limit allocation must reach the JS side as the engine's out-of-memory error"
        ),
        other => panic!("expected a catchable js error, got {other:?}"),
    }
}

#[tokio::test]
async fn a_handler_the_script_does_not_export_is_a_loud_failure() {
    let package = package(
        "export function somethingElse() { return; }",
        r#"{"name":"demo","version":1}"#,
    );
    let host = OneDeployment::of(&package).into_script_host();
    let outcome = host
        .call(&sub("onCall"), tool_call("https://a"), cx())
        .await;
    match outcome {
        Err(HostError::Handler(reason)) => {
            assert!(
                reason.contains("onCall"),
                "the refusal reason must name who is missing: {reason}"
            )
        }
        other => panic!("expected a handler failure, got {other:?}"),
    }
}

#[tokio::test]
async fn an_intercept_handler_may_return_nothing() {
    let package = package(
        "export function onCall() { return undefined; }",
        r#"{"name":"demo","version":1}"#,
    );
    let host = OneDeployment::of(&package).into_script_host();
    let outcome = host
        .call(&sub("onCall"), tool_call("https://a"), cx())
        .await
        .expect("an absent return is a pass");
    assert!(outcome.is_none(), "{outcome:?}");
}

#[tokio::test]
async fn a_mod_that_deadloops_on_every_call_is_suspended() {
    // 期限到点即中断，而中断同样以「处理函数失败」结束。如果它被记成成功，反复
    // 死循环的 mod 每次都把自己攒下的连续超时清零，熔断永远到不了阈值
    let package = package(
        "export function onCall() { while (true) {} }",
        r#"{"name":"demo","version":1}"#,
    );
    let factory = OneDeployment::of(&package);
    let breaker = Arc::clone(&factory.breaker);
    let host = factory.into_script_host();
    for _ in 0..3 {
        let outcome = host
            .call(&sub("onCall"), tool_call("https://a"), cx())
            .await;
        assert!(
            matches!(outcome, Err(HostError::Handler(_))),
            "an infinite loop must be cut off, got {outcome:?}"
        );
    }
    assert!(
        breaker.refuse("demo"),
        "consecutive timeouts reaching the threshold must be blocked"
    );

    let refused = host
        .call(&sub("onCall"), tool_call("https://a"), cx())
        .await;
    match refused {
        Err(HostError::Handler(reason)) => {
            assert!(
                reason.contains("suspended"),
                "the refusal reason must state it: {reason}"
            )
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn an_exhausted_caller_budget_does_not_suspend_the_mod() {
    // 调用方的预算只剩一小截时，这一次调用根本来不及跑完；那不是 mod 的错，
    // 因此它不该攒出连续超时
    let package = package(
        "export function onCall() { while (true) {} }",
        r#"{"name":"demo","version":1}"#,
    );
    let factory = OneDeployment::of(&package);
    let breaker = Arc::clone(&factory.breaker);
    let host = factory.into_script_host();
    // 订阅声明得很宽，因此约束这一次的始终是调用方自己剩下的那一小截
    let mut slow = sub("onCall");
    slow.timeout_ms = 5_000;
    for _ in 0..3 {
        let starved = cx_for(5_000, Duration::from_millis(100));
        let outcome = host.call(&slow, tool_call("https://a"), starved).await;
        assert!(
            outcome.is_err(),
            "an exhausted budget must come back as a failure, got {outcome:?}"
        );
    }
    assert!(
        !breaker.refuse("demo"),
        "what ran out was the caller's budget; the mod must not be suspended"
    );
}

#[tokio::test]
async fn a_failing_flush_does_not_erase_the_breaker_accounting() {
    // 写回失败要说出来，但它不该抹掉这次调用做过什么。判定与计数都先于它落下
    let package = package(
        "export function onCall() { while (true) {} }",
        r#"{"name":"demo","version":1}"#,
    );
    let factory = OneDeployment::of(&package).with_failing_flush();
    let breaker = Arc::clone(&factory.breaker);
    let host = factory.into_script_host();
    for _ in 0..3 {
        let outcome = host
            .call(&sub("onCall"), tool_call("https://a"), cx())
            .await;
        assert!(
            matches!(outcome, Err(HostError::Internal(_))),
            "a failed flush must come back as a host failure, got {outcome:?}"
        );
    }
    assert!(
        breaker.refuse("demo"),
        "a failed flush must not erase the consecutive timeouts already recorded"
    );
}

#[tokio::test]
async fn an_open_failure_is_a_host_failure() {
    let host = ScriptHost::new(ScriptHostOptions {
        factory: Arc::new(NoDeployment),
        pool: Arc::new(ThreadPool::new(2)),
        breaker: Arc::new(Breaker::new(3)),
    });
    let outcome = host
        .call(&sub("onCall"), tool_call("https://a"), cx())
        .await;
    match outcome {
        Err(HostError::Internal(reason)) => {
            assert!(
                reason.contains("is not deployed"),
                "the reason must name what is missing: {reason}"
            )
        }
        other => panic!("expected a host failure, got {other:?}"),
    }
}

#[tokio::test]
async fn a_full_pool_refuses_the_late_call() {
    // 只有一条解释器线程，先到的调用占住它，后到的调用被拒
    let package = package(
        r#"export function onCall(payload, ctx) {
               ctx.http.fetchText("https://slow");
               return;
           }"#,
        r#"{"name":"demo","version":1,"capabilities":["http"]}"#,
    );
    let factory = OneDeployment::of(&package)
        .with_http_delay(Duration::from_secs(30))
        .with_pool(1);
    let host = factory.into_script_host();
    let mut request = cx_for(400, Duration::from_secs(10));
    request.mode = CallMode::Request;
    let mut held = sub("onCall");
    held.timeout_ms = 400;

    // 先被轮到的调用占住名额，第二个在同一轮里就被拒
    let late_sub = sub("onCall");
    let (first, late) = tokio::join!(
        host.call(&held, tool_call("https://a"), request),
        host.call(&late_sub, tool_call("https://a"), cx()),
    );
    assert!(
        first.is_err(),
        "the call holding the slot ends according to its own budget: {first:?}"
    );
    match late {
        Err(HostError::Internal(reason)) => {
            assert!(
                reason.contains("at its limit"),
                "the full-pool reason must state it: {reason}"
            )
        }
        other => panic!("expected a full-pool failure, got {other:?}"),
    }
}

#[tokio::test]
async fn a_topic_scoped_call_keeps_its_scope() {
    // 作用域是隔离键：同一个 mod 在两个话题下写的同一个键互不可见，因此它必须
    // 一路传到 `Deployment` 与状态快照，也要出现在 ctx 上
    let package = package(
        r#"export function onCall(payload, ctx) {
               return ctx.scope.kind;
           }"#,
        r#"{"name":"demo","version":1}"#,
    );
    let factory = OneDeployment::of(&package);
    let scopes = Arc::clone(&factory.scopes);
    let host = factory.into_script_host();
    let mut topic = cx();
    topic.scope = Scope::Topic { topic_id: 7 };
    let outcome = host
        .call(&sub("onCall"), tool_call("https://a"), topic.clone())
        .await
        .expect("dispatch");
    assert!(
        matches!(outcome, Some(ref value) if value == &serde_json::json!("topic")),
        "the scope on ctx must be the one for this call, got {outcome:?}"
    );
    let scopes = scopes.lock().expect("lock the state");
    let opened = scopes.first().expect("open a deployment");
    assert_eq!(
        opened, &topic.scope,
        "`Deployment` and the state snapshot must also be opened under the same scope"
    );
}

#[tokio::test]
async fn an_observe_handler_may_return_nothing() {
    // 观测订阅不要求返回值：它被忽略，缺一个返回值不是实现错误
    let package = package(
        "export function onCall() { }",
        r#"{"name":"demo","version":1}"#,
    );
    let host = OneDeployment::of(&package).into_script_host();
    let mut observing = cx();
    observing.mode = CallMode::Observe;
    let outcome = host
        .call(&sub("onCall"), tool_call("https://a"), observing)
        .await
        .expect("dispatch");
    assert!(
        outcome.is_none(),
        "the return value of an observing subscription is ignored, got {outcome:?}"
    );
}

// 装配：真包、真清单、真字节码、真派发器

/// 一个话题上的顶层操作上下文
fn cx_for_topic(topic_id: i64) -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id },
        Source::core("mcp"),
        Duration::from_secs(5),
    )
}

/// 一条工具调用的载荷
fn tool_call_value(url: &str) -> mcp_events::McpToolCall {
    mcp_events::McpToolCall {
        server_name: "web-fetch".to_string(),
        tool: "fetch".to_string(),
        arguments: serde_json::json!({ "url": url }),
        scope: Scope::Topic { topic_id: 1 },
    }
}

/// 一套按应用的方式装配好的环境
///
/// 次序的唯一权威是组合根，这里镜像它
struct Rig {
    /// 临时目录，随它一起删掉
    _tmp: tempfile::TempDir,
    /// 应用目录
    dirs: AppDirs,
    /// 持久化存储
    storage: Storage,
    /// 帧设施
    hub: Arc<FeedHub>,
    /// 事件总线
    bus: Arc<Dispatcher>,
    /// 帧流的发布入口
    feed: Arc<ModFeed>,
    /// mod 的唯一变更入口
    installer: Installer,
    /// 脚本运行时
    runtime: ScriptRuntime,
}

/// 装配的基座：库、帧设施、总线与应用目录
///
/// 启用集与宿主也留在这里：前者交给安装器，后者等运行时建好才接得上
struct Substrate {
    /// 临时目录，随它一起删掉
    _tmp: tempfile::TempDir,
    /// 应用目录
    dirs: AppDirs,
    /// 持久化存储
    storage: Storage,
    /// 使能集合
    enablement: Arc<Enablement>,
    /// 帧设施
    hub: Arc<FeedHub>,
    /// 事件总线
    bus: Arc<Dispatcher>,
    /// 总线建时固定、脚本运行时建好之后接上的宿主
    deferred: Arc<DeferredHost>,
}

/// 造一套临时目录、库、帧设施、总线与应用目录
///
/// 目录里带上本 crate 与 mcp 的条目，ctx 的 `core/` 事件才能派发
async fn substrate() -> Substrate {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let db = Db::open(&dirs.db_file()).await.expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);

    let enablement = Arc::new(
        Enablement::load(storage.clone())
            .await
            .expect("read the enablement"),
    );
    let hub = FeedHub::open(storage.clone(), None)
        .await
        .expect("open the feed hub");
    // 总线在建立时固定它的宿主，而宿主在脚本运行时建好之后才存在：先把总线建起来
    let deferred = Arc::new(DeferredHost::new());
    let bus = Arc::new(Dispatcher::new(
        usecase::directory()
            .merge(agent::events::directory())
            .merge(conversation::directory())
            .merge(llm::events::directory())
            .merge(mod_events::directory())
            .merge(mcp_events::directory())
            .merge(wind_script::events::directory()),
        enablement.clone(),
        deferred.clone(),
        Limits::default(),
    ));
    bus.attach_observer(hub.clone());
    let seed = storage
        .event_log_max_seq()
        .await
        .expect("read the max sequence");
    if seed > 0 {
        bus.seed_seq(EventSeq(seed));
        hub.seed_high_water(EventSeq(seed));
    }
    Substrate {
        _tmp: tmp,
        dirs,
        storage,
        enablement,
        hub,
        bus,
        deferred,
    }
}

/// 造一套临时目录、库、总线、帧流与脚本运行时
///
/// `ModFeed` 要与安装器共用一个，并由建它的一方登记成基线提供者
async fn rig() -> Rig {
    let base = substrate().await;
    let feed = Arc::new(
        ModFeed::open(base.hub.clone(), base.bus.clone(), base.storage.clone())
            .await
            .expect("open the feed"),
    );
    base.hub.set_baselines(feed.clone());
    let runtime = ScriptRuntime::open(
        base.storage.clone(),
        base.bus.clone(),
        base.dirs.clone(),
        feed.clone(),
    )
    .await
    .expect("create runtime");
    base.deferred.attach(runtime.host());
    let installer = Installer::new(
        base.storage.clone(),
        base.bus.clone(),
        base.dirs.clone(),
        Arc::new(NoServers),
        base.enablement.clone(),
        feed.clone(),
        Arc::new(ScriptCompiler),
    );
    // 重启之后订阅索引是空的，已装 mod 的订阅要在这里回到派发器里
    rebuild_subscriptions(&base.storage, &base.bus, DEFAULT_SUB_TIMEOUT_MS)
        .await
        .expect("rebuild subscriptions");
    Rig {
        _tmp: base._tmp,
        dirs: base.dirs,
        storage: base.storage,
        hub: base.hub,
        bus: base.bus,
        feed,
        installer,
        runtime,
    }
}

/// 把一个包写到约定的安装位置
///
/// 清单里的身份与这一处位置必须一致，安装器据它校验
fn write_package(
    dirs: &AppDirs,
    name: &str,
    version: i64,
    manifest: &str,
    source: &str,
) -> PathBuf {
    let root = dirs.mod_version(name, version);
    let scripts = root.join("scripts");
    std::fs::create_dir_all(&scripts).expect("create dir");
    std::fs::write(root.join("mod.json"), manifest).expect("write the manifest");
    std::fs::write(scripts.join("index.js"), source).expect("write the script");
    root
}

/// 一个包里的清单
fn manifest_of(bus: &Arc<Dispatcher>, package: &Path) -> Manifest {
    let raw = std::fs::read_to_string(package.join("mod.json")).expect("read the manifest");
    parse_manifest(&raw, bus.directory()).expect("the manifest is valid")
}

/// 按安装器装一个包，把它在话题 1 上启用
///
/// 订阅索引不在这里重建。安装与发布已经换掉了清单，重建由安装器自己做
async fn install(rig: &Rig, package: &Path) {
    let manifest = manifest_of(&rig.bus, package);
    let cx = cx_for_topic(1);
    rig.installer.install(&cx, package).await.expect("install");
    rig.installer
        .enable(&cx, 1, &manifest.name)
        .await
        .expect("enable");
}

#[tokio::test]
async fn the_data_plane_is_the_one_the_caller_built() {
    // 运行时不自己建帧流，直接持有交进来的 `ModFeed`
    let rig = substrate().await;
    let feed = Arc::new(
        ModFeed::open(rig.hub.clone(), rig.bus.clone(), rig.storage.clone())
            .await
            .expect("open the feed"),
    );
    rig.hub.set_baselines(feed.clone());
    let runtime = ScriptRuntime::open(
        rig.storage.clone(),
        rig.bus.clone(),
        rig.dirs.clone(),
        feed.clone(),
    )
    .await
    .expect("create runtime");
    assert!(
        Arc::ptr_eq(&runtime.feed(), &feed),
        "the runtime must hold exactly the `ModFeed` passed in, otherwise the baseline provider and the installer would point at different data planes"
    );
}

/// 记下每一条被观测到的事件
#[derive(Default)]
struct Recorder {
    /// 按发生顺序记下（事件名，信封）
    seen: Mutex<Vec<(&'static str, serde_json::Value)>>,
}

#[async_trait::async_trait]
impl BuiltinObserver for Recorder {
    async fn on_event(&self, meta: &EventMeta, event: &serde_json::Value) {
        self.seen
            .lock()
            .expect("lock the state")
            .push((meta.name, event.clone()));
    }
}

impl Recorder {
    /// 某条事件最后一次的信封
    fn last(&self, name: &str) -> Option<serde_json::Value> {
        self.seen
            .lock()
            .expect("lock the state")
            .iter()
            .rev()
            .find(|(event, _)| *event == name)
            .map(|(_, envelope)| envelope.clone())
    }
}

#[tokio::test]
async fn a_mod_can_rewrite_a_named_tool_on_a_named_domain_end_to_end() {
    // 一个真包、真清单、真字节码、真派发器
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,
            "events":{"on":{"core/mcp.tool.call":{"handler":"onCall","phase":"guard"}}}}"#,
        r#"export function onCall(payload) {
               if (payload.arguments.url.includes("example.com")) {
                   payload.arguments.url = "https://blocked.example";
                   return payload;
               }
               return;
           }"#,
    );
    install(&rig, &package).await;

    let verdict = rig
        .bus
        .decide(
            &mcp_events::CORE_MCP_TOOL_CALL,
            &cx_for_topic(1),
            tool_call_value("https://example.com/a"),
        )
        .await;
    assert_eq!(
        verdict.arguments["url"],
        serde_json::json!("https://blocked.example"),
        "the rewritten address is what the chain settles on"
    );

    // 同一个话题下指向别的域名时照常放行
    let passed = rig
        .bus
        .decide(
            &mcp_events::CORE_MCP_TOOL_CALL,
            &cx_for_topic(1),
            tool_call_value("https://docs.rs/a"),
        )
        .await;
    assert_eq!(
        passed,
        tool_call_value("https://docs.rs/a"),
        "other addresses must not be affected"
    );

    // 安装期编译写入数据库。这一版带着字节码与生成它的引擎的指纹
    let installed = rig
        .storage
        .mod_get_by_name("guard")
        .await
        .expect("query")
        .expect("installed");
    let version = rig
        .storage
        .mod_get_version(installed.id, 1)
        .await
        .expect("query")
        .expect("find the version");
    assert!(
        version.compiled.is_some(),
        "bytecode compiled at install time must be written to the database"
    );
    assert_eq!(
        version.engine_fingerprint.as_deref(),
        Some(engine_fingerprint().as_str()),
        "the fingerprint must be recorded together with the engine that produced it"
    );
}

#[tokio::test]
async fn a_state_write_reaches_the_subscriber_of_the_channel_that_declares_it() {
    // 一次调用写下的键是调用方自选的裸键，而订阅按事件的完整名字建立：两者只靠
    // 清单里的 stateKey 对得上，这条用例钉的就是这个对应关系
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,
            "capabilities":["state"],
            "events":{"on":{"core/mcp.tool.call":{"handler":"onCall","phase":"guard"}},
                      "emit":{"items":{"delivery":"stateful","stateKey":"items",
                                       "payload":{"type":"array"}}}}}"#,
        r#"export function onCall(payload, ctx) {
               ctx.state.set("items", [1, 2]);
               return;
           }"#,
    );
    install(&rig, &package).await;
    let mut subscriber = rig
        .hub
        .subscribe(Subscribe {
            scope: Scope::Topic { topic_id: 1 },
            since_seq: None,
            until_seq: None,
            blocks: false,
        })
        .await
        .expect("subscribe")
        .subscriber;

    rig.bus
        .decide(
            &mcp_events::CORE_MCP_TOOL_CALL,
            &cx_for_topic(1),
            tool_call_value("https://a"),
        )
        .await;

    match tokio::time::timeout(Duration::from_secs(5), subscriber.next())
        .await
        .expect("a frame arrives")
        .expect("a frame arrives")
    {
        Frame::Event { envelope } => {
            assert_eq!(envelope["name"], serde_json::json!("m_guard/items"));
            assert_eq!(
                envelope["payload"],
                serde_json::json!([1, 2]),
                "the written value must reach the subscriber unchanged"
            );
        }
        other => panic!("expected an event frame, got {other:?}"),
    }

    // 后连上来的订阅方先收到的当前值里也要有它。这个值只存在于一处帧流里，
    // 因此这一条同时确认「派发与基线用的是同一个 `ModFeed`」
    let mut late = subscribe_topic(&rig, 1).await;
    match next_frame(&mut late).await {
        Frame::Snapshot {
            baseline: Baseline::Stateful { key, value },
        } => {
            assert_eq!(key, "m_guard/items");
            assert_eq!(
                value,
                serde_json::json!([1, 2]),
                "a subscriber that connects later must see the written value"
            );
        }
        other => panic!("expected a state baseline, got {other:?}"),
    }
}

#[tokio::test]
async fn a_notify_from_a_mod_arrives_as_a_core_event_on_its_topic() {
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,
            "events":{"on":{"core/mcp.tool.call":{"handler":"onCall","phase":"guard"}}}}"#,
        r#"export function onCall(payload, ctx) {
               ctx.notify({ text: "example.com blocked it", level: "warn" });
               return;
           }"#,
    );
    install(&rig, &package).await;
    let recorder = Arc::new(Recorder::default());
    rig.bus.attach_observer(recorder.clone());

    rig.bus
        .decide(
            &mcp_events::CORE_MCP_TOOL_CALL,
            &cx_for_topic(1),
            tool_call_value("https://a"),
        )
        .await;

    let event = recorder
        .last("core/mod.notify")
        .expect("record the notify event");
    assert_eq!(event["payload"]["level"], serde_json::json!("warn"));
    assert_eq!(
        event["payload"]["text"],
        serde_json::json!("example.com blocked it")
    );
    assert_eq!(
        event["scope"],
        serde_json::json!({ "kind": "topic", "topic_id": 1 }),
        "the event lands on the topic of this call"
    );
    assert_eq!(
        event["source"]["name"],
        serde_json::json!("guard"),
        "the event is emitted by this mod"
    );
}

#[tokio::test]
async fn a_request_can_draft_a_user_input_through_the_facade() {
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,"capabilities":["agent"],
            "requests":{"draft":{"handler":"onDraft","callableBy":["agent"]}}}"#,
        r#"export function onDraft(input, ctx) {
               ctx.agent.draft("look at " + input.topic);
               return { drafted: true };
           }"#,
    );
    install(&rig, &package).await;
    let recorder = Arc::new(Recorder::default());
    rig.bus.attach_observer(recorder.clone());

    let answer = rig
        .runtime
        .caller_for_agent()
        .call(
            &Scope::Topic { topic_id: 1 },
            "guard",
            1,
            "draft",
            serde_json::json!({ "topic": "rust" }),
        )
        .await
        .expect("call");
    assert_eq!(answer["drafted"], serde_json::json!(true));

    let event = recorder
        .last("core/input.draft")
        .expect("record the draft event");
    assert_eq!(event["payload"]["topic_id"], serde_json::json!(1));
    assert_eq!(event["payload"]["text"], serde_json::json!("look at rust"));
}

#[tokio::test]
async fn a_request_can_read_the_recent_messages_of_its_topic() {
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,"capabilities":["agent"],
            "requests":{"peek":{"handler":"onPeek","callableBy":["agent"]}}}"#,
        r#"export function onPeek(input, ctx) {
               const entries = ctx.agent.transcript();
               return { count: entries.length, first: entries[0] };
           }"#,
    );
    install(&rig, &package).await;
    seed_one_message(&rig.storage).await;

    let answer = rig
        .runtime
        .caller_for_agent()
        .call(
            &Scope::Topic { topic_id: 1 },
            "guard",
            1,
            "peek",
            serde_json::json!({}),
        )
        .await
        .expect("call");
    assert_eq!(
        answer["count"],
        serde_json::json!(1),
        "one message is retrieved"
    );
    assert_eq!(answer["first"]["role"], serde_json::json!("assistant"));
    assert_eq!(
        answer["first"]["text"],
        serde_json::json!("take a look first"),
        "the text comes from this message's body block"
    );
}

/// 在一个话题上摆一条主实例的消息
async fn seed_one_message(storage: &Storage) {
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
    let turn = storage
        .turn_create(TurnCreate {
            instance_id: instance,
            topic_id: 1,
            from_id: None,
            is_boundary: false,
            created_at: None,
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
            created_at: None,
        })
        .await
        .expect("build message");
    storage
        .block_insert(BlockRow {
            id: 1,
            message_id: message,
            seq: 0,
            kind: BlockKind::Text,
            call_id: None,
            revision: 0,
            content: "take a look first".to_string(),
            data: None,
            finished: true,
        })
        .await
        .expect("write block");
}

#[tokio::test]
async fn a_write_over_the_file_limit_is_refused() {
    // 上限与读取相同。读不回来的不必写，而没有上限的写会把磁盘填满
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,"capabilities":["fs"],
            "requests":{"save":{"handler":"onSave","callableBy":["agent"]}}}"#,
        r#"export function onSave(input, ctx) {
               try { ctx.fs.writeText("notes/big.txt", "x".repeat(input.size)); return { name: "no throw" }; }
               catch (error) { return { name: error.name }; }
           }"#,
    );
    install(&rig, &package).await;

    let answer = request(
        &rig,
        "save",
        serde_json::json!({ "size": 4 * 1024 * 1024 + 1 }),
    )
    .await;
    assert_eq!(
        answer["name"],
        serde_json::json!("TooLargeError"),
        "a write over the limit must use the name promised by the contract"
    );
    assert!(
        !rig.dirs.mod_data("guard").join("notes/big.txt").exists(),
        "a refused write leaves no half-written file"
    );
}

#[tokio::test]
async fn a_request_reads_and_writes_inside_its_private_directory_only() {
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,"capabilities":["fs"],
            "requests":{"save":{"handler":"onSave","callableBy":["agent"]}}}"#,
        r#"export function onSave(input, ctx) {
               ctx.fs.writeText("notes/a.txt", input.text);
               const text = ctx.fs.readText("notes/a.txt");
               const entries = ctx.fs.list("notes");
               let missing = "";
               try { ctx.fs.readText("notes/absent.txt"); missing = "no throw"; }
               catch (error) { missing = error.name; }
               let escaped = "";
               try { ctx.fs.readText("../mod.json"); escaped = "no throw"; }
               catch (error) { escaped = error.name; }
               return { text, entries, missing, escaped: escaped };
           }"#,
    );
    install(&rig, &package).await;

    let answer = rig
        .runtime
        .caller_for_agent()
        .call(
            &Scope::Topic { topic_id: 1 },
            "guard",
            1,
            "save",
            serde_json::json!({ "text": "noted" }),
        )
        .await
        .expect("call");
    assert_eq!(
        answer["text"],
        serde_json::json!("noted"),
        "what was written can be read back"
    );
    assert_eq!(
        answer["entries"],
        serde_json::json!(["notes/a.txt"]),
        "listing yields paths relative to the private directory"
    );
    assert_eq!(
        answer["missing"],
        serde_json::json!("NotFoundError"),
        "a missing file must use the name promised by the contract"
    );
    assert_eq!(
        answer["escaped"],
        serde_json::json!("PathError"),
        "a read escaping the private directory must be stopped by the boundary"
    );
    assert!(
        rig.dirs.mod_data("guard").join("notes/a.txt").is_file(),
        "the written file lands in this mod's private directory"
    );
}

#[tokio::test]
async fn an_emit_uses_the_declared_channel_and_refuses_an_undeclared_name() {
    // 名字必须是清单声明过的。清单里的名字不含分隔符，因此带 `core/` 的名字也归入
    // 未声明而拒绝，mod 因此产生不了带该前缀的事件
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,
            "events":{"on":{"core/mcp.tool.call":{"handler":"onCall","phase":"guard"}},
                      "emit":{"items":{"delivery":"stateful","stateKey":"items",
                                       "payload":{"type":"array"}}}}}"#,
        r#"export function onCall(payload, ctx) {
               ctx.emit("items", [1, 2]);
               try { ctx.emit("core/mod.notify", {}); }
               catch (error) { payload.tool = error.name; return payload; }
               return;
           }"#,
    );
    install(&rig, &package).await;
    let mut subscriber = rig
        .hub
        .subscribe(Subscribe {
            scope: Scope::Topic { topic_id: 1 },
            since_seq: None,
            until_seq: None,
            blocks: false,
        })
        .await
        .expect("subscribe")
        .subscriber;

    let verdict = rig
        .bus
        .decide(
            &mcp_events::CORE_MCP_TOOL_CALL,
            &cx_for_topic(1),
            tool_call_value("https://a"),
        )
        .await;
    assert!(
        verdict.tool == "CapabilityError",
        "an undeclared name must report a capability error, got {verdict:?}"
    );

    match tokio::time::timeout(Duration::from_secs(5), subscriber.next())
        .await
        .expect("a frame arrives")
        .expect("a frame arrives")
    {
        Frame::Event { envelope } => {
            assert_eq!(envelope["name"], serde_json::json!("m_guard/items"));
            assert_eq!(envelope["payload"], serde_json::json!([1, 2]));
        }
        other => panic!("expected an event frame, got {other:?}"),
    }
}

// 出网：真的连接、真的并发

/// 用例里认定的响应体上限加一
///
/// 与被测实现同值，两处都写是刻意的
const BODY_OVER_THE_LIMIT: usize = 4 * 1024 * 1024 + 1;

/// 一个最小的 HTTP 服务器
///
/// 跑在 I/O 池上，每条连接一个任务；收满 `party` 条之前不回应任何一条，串行的
/// 客户端因此会卡在第一条连接上
fn http_fixture(party: usize) -> (String, Arc<Barrier>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listen");
    listener
        .set_nonblocking(true)
        .expect("make the listener non-blocking");
    let address = format!(
        "http://{}",
        listener.local_addr().expect("read the local address")
    );
    let gate = Arc::new(Barrier::new(party));
    let holding = Arc::clone(&gate);
    bridge::handle().expect("io handle").spawn(async move {
        let listener = TcpListener::from_std(listener).expect("take the listener");
        while let Ok((stream, _)) = listener.accept().await {
            let gate = Arc::clone(&holding);
            tokio::spawn(async move {
                let _ = answer(stream, gate).await;
            });
        }
    });
    (address, gate)
}

/// 一条连接的处置
///
/// 按路径给答复，收满约定的连接数之后才回
async fn answer(mut stream: TcpStream, gate: Arc<Barrier>) -> std::io::Result<()> {
    let mut head = Vec::new();
    let mut buffer = [0u8; 1024];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        head.extend_from_slice(&buffer[..read]);
        if head.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let request = String::from_utf8_lossy(&head);
    let path = request.split_whitespace().nth(1).unwrap_or("/").to_string();
    gate.wait().await;
    let (status, body) = match path.as_str() {
        "/missing" => ("404 Not Found", b"gone".to_vec()),
        "/big" => ("200 OK", vec![b'x'; BODY_OVER_THE_LIMIT]),
        _ => ("200 OK", b"hello from the fixture".to_vec()),
    };
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.shutdown().await
}

/// 出网用例的清单与脚本
const HTTP_MANIFEST: &str = r#"{"name":"guard","version":1,"capabilities":["http"],
    "requests":{"get":{"handler":"onGet","callableBy":["agent"]},
                "many":{"handler":"onMany","callableBy":["agent"]}}}"#;
const HTTP_SOURCE: &str = r#"export function onGet(input, ctx) {
       try {
           const text = ctx.http.fetchText(input.url, { timeoutMs: 5000 });
           return { text: text };
       } catch (error) {
           return { name: error.name, status: typeof error.status === "number" ? error.status : -1 };
       }
   }
   export function onMany(input, ctx) {
       try {
           const texts = ctx.http.fetchMany(input.urls, { timeoutMs: 5000 });
           return { texts: texts };
       } catch (error) {
           return { name: error.name, texts: [] };
       }
   }"#;

/// 以 agent 的身份、在话题 1 上调一个 request
async fn request(rig: &Rig, name: &str, input: serde_json::Value) -> serde_json::Value {
    rig.runtime
        .caller_for_agent()
        .call(&Scope::Topic { topic_id: 1 }, "guard", 1, name, input)
        .await
        .expect("call")
}

/// 派发一次工具调用
async fn dispatch_tool_call(rig: &Rig, url: &str) -> mcp_events::McpToolCall {
    rig.bus
        .decide(
            &mcp_events::CORE_MCP_TOOL_CALL,
            &cx_for_topic(1),
            tool_call_value(url),
        )
        .await
}

/// 在一个话题上订阅帧
async fn subscribe_topic(rig: &Rig, topic_id: i64) -> wind_core::feed::subscribe::Subscriber {
    rig.hub
        .subscribe(Subscribe {
            scope: Scope::Topic { topic_id },
            since_seq: None,
            until_seq: None,
            blocks: false,
        })
        .await
        .expect("subscribe")
        .subscriber
}

/// 取下一帧，等不到即判失败
async fn next_frame(subscriber: &mut wind_core::feed::subscribe::Subscriber) -> Frame {
    tokio::time::timeout(Duration::from_secs(5), subscriber.next())
        .await
        .expect("a frame arrives")
        .expect("a frame arrives")
}

#[tokio::test]
async fn a_blocking_call_ends_at_its_own_budget_and_returns_its_outcome() {
    // 脚本阻塞在宿主函数上时中断不生效，结束等待的只能是 ctx 调用自带的超时。它比
    // 订阅声明短得多，因此当场抛出 TimeoutError，这一次是返回的而不是失败的
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,"capabilities":["http"],
            "requests":{"slow":{"handler":"onSlow","callableBy":["agent"],"timeoutMs":2000}}}"#,
        r#"export function onSlow(input, ctx) {
               try { ctx.http.fetchText(input.url, { timeoutMs: 50 }); return { name: "no throw" }; }
               catch (error) { return { name: error.name }; }
           }"#,
    );
    install(&rig, &package).await;
    // 收满八条连接才回应，而这一次只连上一条，答复永远不会到
    let (address, _gate) = http_fixture(8);

    let started = std::time::Instant::now();
    let answer = request(
        &rig,
        "slow",
        serde_json::json!({ "url": format!("{address}/ok") }),
    )
    .await;
    assert_eq!(
        answer["name"],
        serde_json::json!("TimeoutError"),
        "what ends this wait must be the timeout that comes with the ctx call"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "it must arrive within this call's own budget; it actually took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn four_blocked_requests_let_go_of_the_pool_at_their_own_budget() {
    // 池只有四条线程；ctx 调用自带的超时若取自打开的宽期限，四条会卡住一小时，
    // 因此 request 自己声明的预算必须传给 ctx
    let rig = rig().await;
    let hanging = write_package(
        &rig.dirs,
        "hang",
        1,
        r#"{"name":"hang","version":1,"capabilities":["http"],
            "requests":{"stall":{"handler":"onStall","callableBy":["agent"],"timeoutMs":200}}}"#,
        r#"export function onStall(input, ctx) {
               ctx.http.fetchText(input.url);
               return { ok: true };
           }"#,
    );
    install(&rig, &hanging).await;
    let quick = write_package(
        &rig.dirs,
        "quick",
        1,
        r#"{"name":"quick","version":1,
            "requests":{"ping":{"handler":"onPing","callableBy":["agent"]}}}"#,
        r#"export function onPing() { return { ok: true }; }"#,
    );
    install(&rig, &quick).await;

    // 收满八条连接才回应，而下面只会连上四条，答复永远不会到
    let (address, _gate) = http_fixture(8);
    let url = format!("{address}/ok");
    let caller = rig.runtime.caller_for_agent();
    let scope = Scope::Topic { topic_id: 1 };

    let held = futures::future::join_all((0..4).map(|_| {
        caller.call(
            &scope,
            "hang",
            1,
            "stall",
            serde_json::json!({ "url": url.clone() }),
        )
    }))
    .await;
    for outcome in &held {
        assert!(
            matches!(outcome, Err(RequestError::Timeout { .. })),
            "a call occupying a thread ends according to its own budget, got {outcome:?}"
        );
    }

    // 预算到点之后四条线程就该被放开，而不是继续等宿主调用
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = caller
        .call(&scope, "quick", 1, "ping", serde_json::json!({}))
        .await;
    assert!(
        matches!(after, Ok(ref value) if value == &serde_json::json!({ "ok": true })),
        "once the budget is reached all four threads must be released, got {after:?}"
    );
}

#[tokio::test]
async fn a_request_fetches_over_a_real_connection() {
    let rig = rig().await;
    let package = write_package(&rig.dirs, "guard", 1, HTTP_MANIFEST, HTTP_SOURCE);
    install(&rig, &package).await;
    let (address, _gate) = http_fixture(1);

    let answer = request(
        &rig,
        "get",
        serde_json::json!({ "url": format!("{address}/ok") }),
    )
    .await;
    assert_eq!(
        answer["text"],
        serde_json::json!("hello from the fixture"),
        "the retrieved body must be the segment written by the server"
    );
}

#[tokio::test]
async fn a_batch_sends_every_request_before_any_response_arrives() {
    // 服务器收满三条连接才回应，而这里一次发出三条：只有真的并发才能同时到齐
    // 串行执行时会在第一条连接上等到自己的上限，答复里因此是一条 TimeoutError
    let rig = rig().await;
    let package = write_package(&rig.dirs, "guard", 1, HTTP_MANIFEST, HTTP_SOURCE);
    install(&rig, &package).await;
    let (address, _gate) = http_fixture(3);

    let answer = request(
        &rig,
        "many",
        serde_json::json!({
            "urls": [format!("{address}/a"), format!("{address}/b"), format!("{address}/c")],
        }),
    )
    .await;
    assert_eq!(
        answer["texts"],
        serde_json::json!([
            "hello from the fixture",
            "hello from the fixture",
            "hello from the fixture"
        ]),
        "all three addresses must be sent before the first response arrives, got {answer:?}"
    );
}

#[tokio::test]
async fn an_error_status_answers_as_an_http_error_carrying_it() {
    let rig = rig().await;
    let package = write_package(&rig.dirs, "guard", 1, HTTP_MANIFEST, HTTP_SOURCE);
    install(&rig, &package).await;
    let (address, _gate) = http_fixture(1);

    let answer = request(
        &rig,
        "get",
        serde_json::json!({ "url": format!("{address}/missing") }),
    )
    .await;
    assert_eq!(answer["name"], serde_json::json!("HttpError"));
    assert_eq!(
        answer["status"],
        serde_json::json!(404),
        "a status code must be present when a response is obtained"
    );
}

#[tokio::test]
async fn a_body_over_the_limit_never_reaches_the_interpreter() {
    let rig = rig().await;
    let package = write_package(&rig.dirs, "guard", 1, HTTP_MANIFEST, HTTP_SOURCE);
    install(&rig, &package).await;
    let (address, _gate) = http_fixture(1);

    let answer = request(
        &rig,
        "get",
        serde_json::json!({ "url": format!("{address}/big") }),
    )
    .await;
    assert_eq!(answer["name"], serde_json::json!("TooLargeError"));
}

#[tokio::test]
async fn a_state_write_survives_a_call_the_user_interrupts() {
    // 用户中断时调用方把宿主那条未来就地丢掉，而 mod 还在跑。被丢掉的只能是等待，
    // 已经写下的值仍然要写回
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,"capabilities":["state"],
            "events":{"on":{"core/mcp.tool.call":{"handler":"onCall","phase":"guard","timeoutMs":1000}}}}"#,
        r#"export function onCall(payload, ctx) {
               ctx.state.set("blocked", 1);
               while (true) {}
           }"#,
    );
    install(&rig, &package).await;

    let cx = cx_for_topic(1);
    let token = cx.cancel().clone();
    let decide = rig.bus.decide(
        &mcp_events::CORE_MCP_TOOL_CALL,
        &cx,
        tool_call_value("https://a"),
    );
    // 这一次调用还在跑的时候中断它
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        token.cancel();
    });
    decide.await;

    let stored = wait_for_state(&rig, "blocked").await;
    assert_eq!(
        stored,
        Some(serde_json::json!(1)),
        "the dropped future must not erase state that was already written"
    );
}

#[tokio::test]
async fn a_state_write_survives_a_call_whose_caller_gives_up() {
    // 观测派发的调用方在外层套了自己的期限，它先于写回到期就会把宿主那条未来丢掉。
    // 被丢掉的只能是等待，已经写下的值仍然要写回。这一条只有走真的派发路径才量得出来
    let (address, _gate) = http_fixture(2);
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,"capabilities":["state","http"],
            "events":{"on":{"core/mod.deleted":{"handler":"onDeleted","timeoutMs":300}}}}"#,
        &format!(
            r#"export function onDeleted(payload, ctx) {{
                   ctx.state.set("blocked", 1);
                   ctx.http.fetchText("{address}/hold", {{ timeoutMs: 30000 }});
               }}"#
        ),
    );
    install(&rig, &package).await;

    // 第二条连接永远不会来，因此这条连接被握住，直到这个宿主调用自己的上限
    rig.bus
        .notify(
            &mod_events::CORE_MOD_DELETED,
            &cx_for_topic(1),
            mod_events::ModDeleted {
                name: "guard".to_string(),
                version: 1,
            },
        )
        .await;

    let stored = wait_for_state(&rig, "blocked").await;
    assert_eq!(
        stored,
        Some(serde_json::json!(1)),
        "the dropped future must not erase state that was already written"
    );
}

/// 等一条状态行出现，等不到即给空
///
/// 写回跑在 I/O 池上，可能比派发器的返回稍晚
async fn wait_for_state(rig: &Rig, key: &str) -> Option<serde_json::Value> {
    let row = rig
        .storage
        .mod_get_by_name("guard")
        .await
        .expect("query")
        .expect("installed");
    let state = &rig.storage;
    for _ in 0..100 {
        if let Some(entry) = state
            .mod_get_state(row.id, "topic", 1, "state", key)
            .await
            .expect("query")
        {
            return Some(entry.value);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

#[tokio::test]
async fn a_deleted_state_key_is_retracted_for_the_subscriber() {
    // 删除在状态下以空值表示，而订阅方必须收到一条说明它没有了的帧。只写入数据库
    // 而不派发，会让在线的订阅方一直停在旧值上
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,
            "capabilities":["state"],
            "events":{"on":{"core/mcp.tool.call":{"handler":"onCall","phase":"guard"}},
                      "emit":{"items":{"delivery":"stateful","stateKey":"items",
                                       "payload":{"type":"array"}}}}}"#,
        r#"export function onCall(payload, ctx) {
               if (payload.arguments.url.includes("drop")) { ctx.state.delete("items"); }
               else { ctx.state.set("items", [1, 2]); }
               return;
           }"#,
    );
    install(&rig, &package).await;
    let mut subscriber = subscribe_topic(&rig, 1).await;

    dispatch_tool_call(&rig, "https://a").await;
    match next_frame(&mut subscriber).await {
        Frame::Event { envelope } => {
            assert_eq!(envelope["name"], serde_json::json!("m_guard/items"));
            assert_eq!(envelope["payload"], serde_json::json!([1, 2]));
        }
        other => panic!("expected the written value, got {other:?}"),
    }

    dispatch_tool_call(&rig, "https://drop").await;
    match next_frame(&mut subscriber).await {
        Frame::Event { envelope } => {
            assert_eq!(envelope["name"], serde_json::json!("m_guard/items"));
            assert!(
                envelope["payload"].is_null(),
                "a retraction is represented by a null value, got {envelope:?}"
            );
        }
        other => panic!("expected a retraction, got {other:?}"),
    }
    assert!(
        rig.feed
            .stateful_baselines(&Scope::Topic { topic_id: 1 })
            .is_empty(),
        "after the retraction it is no longer in the baseline"
    );
}

#[tokio::test]
async fn a_state_value_over_the_limit_is_refused() {
    // 契约在 state.set 上承诺 PayloadTooLargeError
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,
            "capabilities":["state"],
            "events":{"on":{"core/mcp.tool.call":{"handler":"onCall","phase":"guard"}}}}"#,
        r#"export function onCall(payload, ctx) {
               try { ctx.state.set("big", "x".repeat(200000)); return; }
               catch (error) { payload.tool = error.name; return payload; }
           }"#,
    );
    install(&rig, &package).await;

    let verdict = dispatch_tool_call(&rig, "https://a").await;
    assert!(
        verdict.tool == "PayloadTooLargeError",
        "a value over the per-key limit must report this name, got {verdict:?}"
    );
}

#[tokio::test]
async fn an_emitted_payload_over_the_limit_is_refused() {
    // 载荷与一次状态写入走同一份成本。两者都要进帧流，状态派发还要在内存里留一份
    // 当前值。因此上限也是同一个，契约承诺的名字也一样
    let rig = rig().await;
    let package = write_package(
        &rig.dirs,
        "guard",
        1,
        r#"{"name":"guard","version":1,
            "events":{"on":{"core/mcp.tool.call":{"handler":"onCall","phase":"guard"}},
                      "emit":{"items":{"delivery":"stateful","stateKey":"items",
                                       "payload":{"type":"array"}}}}}"#,
        r#"export function onCall(payload, ctx) {
               try { ctx.emit("items", "x".repeat(200000)); return; }
               catch (error) { payload.tool = error.name; return payload; }
           }"#,
    );
    install(&rig, &package).await;

    let verdict = dispatch_tool_call(&rig, "https://a").await;
    assert!(
        verdict.tool == "PayloadTooLargeError",
        "a payload over the limit must report this name, got {verdict:?}"
    );
    assert!(
        rig.feed
            .stateful_baselines(&Scope::Topic { topic_id: 1 })
            .is_empty(),
        "a refused payload is neither written to the database nor kept in memory"
    );
}
