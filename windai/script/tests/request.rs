//! 调用方请求一个 mod 提供一个 request

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant as TokioInstant;
use wind_core::error::CoreError;
use wind_core::llm::ports::ModRequests;
use wind_event::envelope::Scope;
use wind_event::host::CallCx;
use wind_mod::manifest::Manifest;
use wind_mod::manifest::parse as parse_manifest;
use wind_script::circuit::Breaker;
use wind_script::deployment::{DeployError, Deployment, NotDeployed};
use wind_script::facade::{FacadeError, HostApi, HttpRequest, TranscriptEntry};
use wind_script::request::{Caller, RequestCaller, RequestError};
use wind_script::runtime::{CallHost, HostFactory};
use wind_script::threads::ThreadPool;

/// 替身的出网要睡多久
///
/// 卡在宿主函数里的调用不会被中断，要等得够久才分得清
const HTTP_DELAY: Duration = Duration::from_millis(600);

/// 用例留着的观察点
#[derive(Default)]
struct Seen {
    /// 每次打开 `Deployment` 取得的作用域，按先后顺序
    scopes: Mutex<Vec<Scope>>,
    /// `finish` 跑过几次
    finishes: AtomicUsize,
    /// 每次重设后的截止时刻，按先后顺序
    rebased: Mutex<Vec<TokioInstant>>,
}

/// 一次调用期间的宿主替身
///
/// 写下的值先进 `pending`，`finish` 时才写进跨调用共用的表
struct FakeHost {
    /// 已经写回的值，跨调用共用
    stored: Arc<Mutex<HashMap<String, serde_json::Value>>>,
    /// 本次调用写下、等 `finish` 的值
    pending: Mutex<HashMap<String, serde_json::Value>>,
    /// 用例留着的观察点
    seen: Arc<Seen>,
}

impl HostApi for FakeHost {
    fn state_read(&self, key: &str) -> Option<serde_json::Value> {
        if let Some(value) = self.pending.lock().expect("lock the state").get(key) {
            return Some(value.clone());
        }
        self.stored
            .lock()
            .expect("lock the state")
            .get(key)
            .cloned()
    }
    fn state_write(&self, key: &str, value: serde_json::Value) -> Result<(), FacadeError> {
        self.pending
            .lock()
            .expect("lock the state")
            .insert(key.to_string(), value);
        Ok(())
    }
    fn state_delete(&self, key: &str) {
        self.stored.lock().expect("lock the state").remove(key);
        self.pending.lock().expect("lock the state").remove(key);
    }
    fn state_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .pending
            .lock()
            .expect("lock the state")
            .keys()
            .cloned()
            .collect();
        keys.extend(self.stored.lock().expect("lock the state").keys().cloned());
        keys.sort();
        keys.dedup();
        keys
    }
    fn storage_read(&self, key: &str) -> Option<serde_json::Value> {
        self.state_read(key)
    }
    fn storage_write(&self, key: &str, value: serde_json::Value) {
        self.state_write(key, value).expect("write");
    }
    fn storage_delete(&self, key: &str) {
        self.state_delete(key);
    }
    fn storage_keys(&self) -> Vec<String> {
        self.state_keys()
    }
    fn http_fetch(&self, _init: HttpRequest) -> Result<Vec<u8>, FacadeError> {
        std::thread::sleep(HTTP_DELAY);
        Ok(Vec::new())
    }
    fn http_fetch_many(&self, requests: Vec<HttpRequest>) -> Vec<Result<Vec<u8>, FacadeError>> {
        requests
            .into_iter()
            .map(|request| self.http_fetch(request))
            .collect()
    }
    fn agent_prompt(&self, _scope: &Scope, _text: &str) -> Result<(), FacadeError> {
        Ok(())
    }
    fn agent_draft(&self, _scope: &Scope, _text: &str) -> Result<(), FacadeError> {
        Ok(())
    }
    fn agent_spawn(&self, _scope: &Scope, _mode: &str, _task: &str) -> Result<(), FacadeError> {
        Ok(())
    }
    fn agent_transcript(&self, _scope: &Scope, _limit: Option<u32>) -> Vec<TranscriptEntry> {
        Vec::new()
    }
    fn fs_dir(&self, name: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(name)
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
        _name: &str,
        _payload: serde_json::Value,
    ) -> Result<(), FacadeError> {
        Ok(())
    }
    fn notify(&self, _scope: &Scope, _level: &str, _text: &str) -> Result<(), FacadeError> {
        Ok(())
    }
    fn log(&self, _level: &str, _message: &str) {}
}

#[async_trait::async_trait]
impl CallHost for FakeHost {
    /// 记下被重设到的时刻：这一次调用真正拥有的时间由调用方按清单里声明的预算给出
    fn rebase(&self, deadline: TokioInstant) {
        self.seen
            .rebased
            .lock()
            .expect("lock the state")
            .push(deadline);
    }

    async fn finish(&self) -> Result<(), DeployError> {
        // 真实实现会在这里写回状态并广播。替身把本次调用写下的值写进共用的表
        let written: Vec<(String, serde_json::Value)> = self
            .pending
            .lock()
            .expect("lock the state")
            .drain()
            .collect();
        self.stored.lock().expect("lock the state").extend(written);
        self.seen.finishes.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

/// 一个只交出 `Deployment` 的工厂，安装根由它自己持有
struct OneDeployment {
    /// 安装根
    root: tempfile::TempDir,
    /// 该版本的清单
    manifest: Manifest,
    /// 已经写回的状态，跨调用共用
    stored: Arc<Mutex<HashMap<String, serde_json::Value>>>,
    /// 用例留着的观察点
    seen: Arc<Seen>,
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
        self.seen
            .scopes
            .lock()
            .expect("lock the state")
            .push(scope.clone());
        let host = FakeHost {
            stored: Arc::clone(&self.stored),
            pending: Mutex::new(HashMap::new()),
            seen: Arc::clone(&self.seen),
        };
        let deployment = Deployment {
            name: name.to_string(),
            version,
            root: self.root.path().to_path_buf(),
            manifest: self.manifest.clone(),
            // 没有字节码，加载器因此改读源码
            compiled: BTreeMap::new(),
        };
        Ok((deployment, Box::new(host)))
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

/// 一个调用器与用例留着的观察点
struct Rig {
    /// 调用方请求用的调用器
    caller: RequestCaller,
    /// 用例留着的观察点
    seen: Arc<Seen>,
    /// 连续超时的熔断，用例要查它是否已经拦下某个 mod
    breaker: Arc<Breaker>,
}

/// 装配一个调用器与它的观察点，以给定的调用方身份发起
fn rig_of(caller: Caller, source: &str, manifest_raw: &str, threads: usize) -> Rig {
    let root = tempfile::tempdir().expect("create temp dir");
    let scripts = root.path().join("scripts");
    std::fs::create_dir_all(&scripts).expect("create dir");
    std::fs::write(scripts.join("index.js"), source).expect("write the script");
    let manifest = parse_manifest(manifest_raw, &wind_script::events::directory())
        .expect("the manifest is valid");
    let seen = Arc::new(Seen::default());
    let factory = Arc::new(OneDeployment {
        root,
        manifest,
        stored: Arc::new(Mutex::new(HashMap::new())),
        seen: Arc::clone(&seen),
    });
    // 池比用例里的并发大。被放弃的线程在它跑完之前仍占着名额
    let pool = Arc::new(ThreadPool::new(threads));
    let breaker = Arc::new(Breaker::new(3));
    let caller = match caller {
        Caller::Agent => RequestCaller::for_agent(factory, pool, Arc::clone(&breaker)),
        Caller::Ui => RequestCaller::for_ui(factory, pool, Arc::clone(&breaker)),
    };
    Rig {
        caller,
        seen,
        breaker,
    }
}

/// 装配一个调用器，以给定的调用方身份发起
fn caller_for(caller: Caller, source: &str, manifest_raw: &str) -> RequestCaller {
    rig_of(caller, source, manifest_raw, 4).caller
}

/// 装配一个以 UI 为调用方的调用器
fn caller_with(source: &str, manifest_raw: &str) -> RequestCaller {
    caller_for(Caller::Ui, source, manifest_raw)
}

/// 一套装着某个 mod 的装配
struct SeamRig {
    /// agent 用的调用器
    caller: RequestCaller,
    /// 该 mod 的版本
    version: i64,
}

impl SeamRig {
    /// agent 用的调用器
    fn caller_for_agent(&self) -> &RequestCaller {
        &self.caller
    }

    /// 该 mod 的版本
    fn version(&self) -> i64 {
        self.version
    }
}

/// 装一个 agent 可调的 mod，它的 fetch 把输入原样交回
fn rig_with_a_deployed_mod() -> SeamRig {
    SeamRig {
        caller: caller_for(
            Caller::Agent,
            r#"export function fetch(input) { return input; }"#,
            r#"{"name":"alpha","version":1,
                "requests":{"fetch":{"handler":"fetch","callableBy":["agent"]}}}"#,
        ),
        version: 1,
    }
}

#[tokio::test]
async fn a_request_returns_what_its_handler_returned() {
    let caller = caller_with(
        r#"export function fetchTop(input) { return { count: input.limit }; }"#,
        r#"{"name":"demo","version":1,
            "requests":{"fetch":{"handler":"fetchTop","callableBy":["ui"]}}}"#,
    );
    let out = caller
        .call(
            &Scope::Global,
            "demo",
            1,
            "fetch",
            serde_json::json!({ "limit": 3 }),
        )
        .await
        .expect("call");
    assert_eq!(out, serde_json::json!({ "count": 3 }));
}

#[tokio::test]
async fn a_request_the_manifest_does_not_declare_is_refused() {
    let caller = caller_with(
        r#"export function fetchTop() { return 1; }"#,
        r#"{"name":"demo","version":1}"#,
    );
    let error = caller
        .call(&Scope::Global, "demo", 1, "fetch", serde_json::json!({}))
        .await
        .expect_err("must be rejected");
    assert!(
        matches!(error, RequestError::Undeclared { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_caller_that_is_not_declared_is_refused() {
    let caller = caller_with(
        r#"export function fetchTop() { return 1; }"#,
        r#"{"name":"demo","version":1,
            "requests":{"fetch":{"handler":"fetchTop","callableBy":["agent"]}}}"#,
    );
    let error = caller
        .call_as(
            &Scope::Global,
            "demo",
            1,
            "fetch",
            serde_json::json!({}),
            Caller::Ui,
        )
        .await
        .expect_err("must be rejected");
    assert!(
        matches!(error, RequestError::NotCallable { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_handler_that_throws_comes_back_as_a_failure_not_a_value() {
    let caller = caller_with(
        r#"export function fetchTop() { throw new Error("upstream is down"); }"#,
        r#"{"name":"demo","version":1,
            "requests":{"fetch":{"handler":"fetchTop","callableBy":["ui"]}}}"#,
    );
    let error = caller
        .call(&Scope::Global, "demo", 1, "fetch", serde_json::json!({}))
        .await
        .expect_err("must fail");
    assert!(error.to_string().contains("upstream is down"), "{error}");
}

#[tokio::test]
async fn a_request_that_loops_forever_ends_at_its_own_timeout() {
    let caller = caller_with(
        r#"export function fetchTop() { while (true) {} }"#,
        r#"{"name":"demo","version":1,
            "requests":{"fetch":{"handler":"fetchTop","callableBy":["ui"],"timeoutMs":30}}}"#,
    );
    let error = caller
        .call(&Scope::Global, "demo", 1, "fetch", serde_json::json!({}))
        .await
        .expect_err("time out");
    assert!(matches!(error, RequestError::Timeout { .. }), "{error:?}");
}

#[tokio::test]
async fn a_handler_that_returns_nothing_is_a_failure() {
    // 返回值就是调用方取得的结果，交不出可序列化的值即实现错误，而不是悄悄折成
    // null
    let caller = caller_with(
        r#"export function fetchTop() { }"#,
        r#"{"name":"demo","version":1,
            "requests":{"fetch":{"handler":"fetchTop","callableBy":["ui"]}}}"#,
    );
    let error = caller
        .call(&Scope::Global, "demo", 1, "fetch", serde_json::json!({}))
        .await
        .expect_err("must fail");
    assert!(matches!(error, RequestError::Handler(_)), "{error:?}");
}

#[tokio::test]
async fn a_state_write_is_flushed_when_a_request_ends() {
    // 值只在写回之后才可见，写回没跑时第二次请求读不到
    let caller = caller_for(
        Caller::Agent,
        r#"export function keep(input, ctx) { ctx.state.set("kept", input.n); return null; }
           export function read(input, ctx) { return ctx.state.get("kept"); }"#,
        r#"{"name":"demo","version":1,"capabilities":["state"],
            "requests":{"keep":{"handler":"keep","callableBy":["agent"]},
                        "read":{"handler":"read","callableBy":["agent"]}}}"#,
    );
    caller
        .call(
            &Scope::Global,
            "demo",
            1,
            "keep",
            serde_json::json!({ "n": 7 }),
        )
        .await
        .expect("call");
    let read = caller
        .call(&Scope::Global, "demo", 1, "read", serde_json::json!({}))
        .await
        .expect("call");
    assert_eq!(
        read,
        serde_json::json!(7),
        "state written by the previous request must already be in the database"
    );
}

#[tokio::test]
async fn a_state_write_survives_a_request_that_never_returns() {
    // 写下状态之后卡在宿主函数里。被放弃的是工作线程，实现仍然要写回已经写下的值
    let caller = caller_for(
        Caller::Agent,
        r#"export function keep(input, ctx) {
               ctx.state.set("kept", input.n);
               ctx.http.fetchText("https://slow");
               return null;
           }
           export function read(input, ctx) { return ctx.state.get("kept"); }"#,
        r#"{"name":"demo","version":1,"capabilities":["state","http"],
            "requests":{"keep":{"handler":"keep","callableBy":["agent"],"timeoutMs":150},
                        "read":{"handler":"read","callableBy":["agent"]}}}"#,
    );
    let error = caller
        .call(
            &Scope::Global,
            "demo",
            1,
            "keep",
            serde_json::json!({ "n": 7 }),
        )
        .await
        .expect_err("time out");
    assert!(matches!(error, RequestError::Timeout { .. }), "{error:?}");

    let read = caller
        .call(&Scope::Global, "demo", 1, "read", serde_json::json!({}))
        .await
        .expect("call");
    assert_eq!(
        read,
        serde_json::json!(7),
        "the timeout path must flush as well; already-written state must not be lost with it"
    );
}

#[tokio::test]
async fn a_topic_scoped_request_keeps_its_scope() {
    // 作用域是隔离键：同一个 mod 在两个话题下写的同一个键互不可见，因此它必须
    // 一路传到 `Deployment` 与状态快照，也要出现在 ctx 上
    let rig = rig_of(
        Caller::Ui,
        r#"export function where(input, ctx) { ctx.state.set("seen", 1); return ctx.scope.kind; }"#,
        r#"{"name":"demo","version":1,"capabilities":["state"],
            "requests":{"where":{"handler":"where","callableBy":["ui"]}}}"#,
        4,
    );
    let topic = Scope::Topic { topic_id: 7 };
    let out = rig
        .caller
        .call(&topic, "demo", 1, "where", serde_json::json!({}))
        .await
        .expect("call");
    assert_eq!(
        out,
        serde_json::json!("topic"),
        "the scope on ctx must be the one for this call"
    );
    let scopes = rig.seen.scopes.lock().expect("lock the state");
    let opened = scopes.first().expect("open a deployment");
    assert_eq!(
        opened, &topic,
        "`Deployment` and the state snapshot must also be opened under the same scope"
    );
}

#[tokio::test]
async fn a_full_pool_refuses_the_late_request() {
    // 只有一条解释器线程，先到的请求占住它，后到的请求被拒。被拒的请求同样打开
    // 过 `Deployment`，写回照样要跑
    let rig = rig_of(
        Caller::Ui,
        r#"export function slow(input, ctx) { ctx.http.fetchText("https://slow"); return null; }
           export function quick(input) { return 1; }"#,
        r#"{"name":"demo","version":1,"capabilities":["http"],
            "requests":{"slow":{"handler":"slow","callableBy":["ui"],"timeoutMs":150},
                        "quick":{"handler":"quick","callableBy":["ui"]}}}"#,
        1,
    );
    let (held, late) = tokio::join!(
        rig.caller
            .call(&Scope::Global, "demo", 1, "slow", serde_json::json!({})),
        rig.caller
            .call(&Scope::Global, "demo", 1, "quick", serde_json::json!({})),
    );
    assert!(
        matches!(held, Err(RequestError::Timeout { .. })),
        "the call holding the slot ends according to its own budget: {held:?}"
    );
    assert!(
        matches!(late, Err(RequestError::Busy)),
        "a full pool must come back as a full pool, got {late:?}"
    );
    assert_eq!(
        rig.seen.finishes.load(Ordering::Acquire),
        2,
        "a task that never started must flush too; the snapshot it opened must not be left behind"
    );
}

#[tokio::test]
async fn the_host_sees_the_budget_the_request_declares() {
    // 打开的期限只宽到盖住打开这一步，真正的预算写在清单里；ctx 调用自带的超时按
    // 后者重设，否则一次没有自带超时的阻塞调用会占住线程整个宽期限
    let rig = rig_of(
        Caller::Ui,
        r#"export function fetchTop(input) { return 1; }"#,
        r#"{"name":"demo","version":1,
            "requests":{"fetch":{"handler":"fetchTop","callableBy":["ui"],"timeoutMs":150}}}"#,
        2,
    );
    let before = TokioInstant::now();
    rig.caller
        .call(&Scope::Global, "demo", 1, "fetch", serde_json::json!({}))
        .await
        .expect("call");

    let seen = rig.seen.rebased.lock().expect("lock the state");
    let deadline = *seen.first().expect("rebase the deadline");
    let granted = deadline.saturating_duration_since(before);
    assert!(
        granted <= Duration::from_millis(1_000),
        "the deadline the host sees must be the one this request declares, got {granted:?}"
    );
}

#[tokio::test]
async fn repeated_request_timeouts_suspend_the_mod() {
    // 反复拖慢宿主的是同一个 mod，与它从哪个入口进来无关。请求同样计入连续超时，
    // 到阈值之后后续调用被拦下
    let rig = rig_of(
        Caller::Ui,
        r#"export function fetchTop(input, ctx) { ctx.http.fetchText("https://slow"); return 1; }"#,
        r#"{"name":"demo","version":1,"capabilities":["http"],
            "requests":{"fetch":{"handler":"fetchTop","callableBy":["ui"],"timeoutMs":50}}}"#,
        4,
    );
    for _ in 0..3 {
        let outcome = rig
            .caller
            .call(&Scope::Global, "demo", 1, "fetch", serde_json::json!({}))
            .await;
        assert!(
            matches!(outcome, Err(RequestError::Timeout { .. })),
            "a request stuck on a host call must record a timeout, got {outcome:?}"
        );
    }
    assert!(
        rig.breaker.refuse("demo"),
        "consecutive timeouts reaching the threshold must be blocked"
    );

    let refused = rig
        .caller
        .call(&Scope::Global, "demo", 1, "fetch", serde_json::json!({}))
        .await;
    assert!(
        matches!(refused, Err(RequestError::Suspended { .. })),
        "the blocked call must state this reason, got {refused:?}"
    );
}

#[tokio::test]
async fn an_open_failure_comes_back_as_a_deploy_failure() {
    let caller = RequestCaller::for_ui(
        Arc::new(NoDeployment),
        Arc::new(ThreadPool::new(1)),
        Arc::new(Breaker::new(3)),
    );
    let error = caller
        .call(&Scope::Global, "demo", 1, "fetch", serde_json::json!({}))
        .await
        .expect_err("must fail");
    match error {
        RequestError::Deploy(reason) => assert!(
            reason.to_string().contains("is not deployed"),
            "the reason must name what is missing: {reason}"
        ),
        other => panic!("expected a deploy failure, got {other:?}"),
    }
}

#[tokio::test]
async fn the_caller_can_be_used_through_the_chain_seam() {
    let rig = rig_with_a_deployed_mod();
    let seam: &dyn ModRequests = rig.caller_for_agent();
    let value = seam
        .call(
            &Scope::Topic { topic_id: 1 },
            "alpha",
            rig.version(),
            "fetch",
            serde_json::json!({ "q": "x" }),
        )
        .await
        .expect("call");
    assert_eq!(value["q"], serde_json::json!("x"));
}

#[tokio::test]
async fn a_refused_request_comes_back_as_a_denial_not_a_panic() {
    // `ModRequests` 的失败要能被上层如实往上带
    let rig = rig_with_a_deployed_mod();
    let seam: &dyn ModRequests = rig.caller_for_agent();
    let error = seam
        .call(
            &Scope::Topic { topic_id: 1 },
            "alpha",
            rig.version(),
            "no_such_request",
            serde_json::json!({}),
        )
        .await
        .expect_err("must fail");
    assert!(
        matches!(error, CoreError::Denied(_)),
        "a failure on the interface side turns into a denial, got {error:?}"
    );
    assert!(error.to_string().contains("no_such_request"), "{error}");
}
