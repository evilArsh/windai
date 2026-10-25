//! 工具调用路径的拦截与事件记录

use rmcp::ErrorData;
use rmcp::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, ListToolsResult, PaginatedRequestParams,
};
use rmcp::service::{RequestContext, RoleServer};
use std::fs as std_fs;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use wind_event::context::DispatchCx;
use wind_event::def::{EventMeta, Shape};
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::observer::BuiltinObserver;
use wind_event::subscription::{EnabledSet, Phase, Subscription};
use wind_mcp::builtin::{FS, register_builtin, roots_for};
use wind_mcp::registry::{CallOutcome, McpError, Registry};
use wind_mod::dirs::AppDirs;

/// 按脚本应答的 host
#[derive(Clone)]
struct ScriptedHost {
    /// 事件名，命中它就按下面的方式应答
    event: &'static str,
    /// 改写后的载荷
    rewrite: Option<serde_json::Value>,
    /// 被调用的处理函数名
    seen: Arc<Mutex<Vec<String>>>,
}

impl ScriptedHost {
    /// 不做判定的 host
    fn pass() -> Self {
        Self {
            event: "",
            rewrite: None,
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 改写指定事件的 host
    fn rewriting(event: &'static str, value: serde_json::Value) -> Self {
        Self {
            event,
            rewrite: Some(value),
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait::async_trait]
impl ModHost for ScriptedHost {
    async fn call(
        &self,
        sub: &Subscription,
        _payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        self.seen
            .lock()
            .expect("lock the record")
            .push(sub.handler.clone());
        if sub.event != self.event {
            return Ok(None);
        }
        match &self.rewrite {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }
}

/// 替身被调用过几次
fn host_calls(seen: &Arc<Mutex<Vec<String>>>) -> usize {
    seen.lock().expect("lock the record").len()
}

/// 一直启用
struct AllEnabled;

impl ModEnablement for AllEnabled {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::all()
    }
}

/// 记下派发出去的 mcp 事件信封
#[derive(Default, Clone)]
struct Recorder {
    /// 按发生顺序记下的信封
    seen: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl Recorder {
    /// 某条事件发生过几次
    fn count(&self, name: &str) -> usize {
        self.seen
            .lock()
            .expect("lock the record")
            .iter()
            .filter(|event| event["name"] == name)
            .count()
    }

    /// 某条事件最后一次的信封
    #[cfg(unix)]
    fn last(&self, name: &str) -> Option<serde_json::Value> {
        self.seen
            .lock()
            .expect("lock the record")
            .iter()
            .rev()
            .find(|event| event["name"] == name)
            .cloned()
    }
}

#[async_trait::async_trait]
impl BuiltinObserver for Recorder {
    async fn on_event(&self, _meta: &EventMeta, event: &serde_json::Value) {
        self.seen
            .lock()
            .expect("lock the record")
            .push(event.clone());
    }
}

/// 一条拦截订阅
fn subscription(event: &'static str) -> Subscription {
    Subscription {
        mod_name: "guard".into(),
        mod_version: 1,
        event,
        phase: Some(Phase::Guard),
        handler: "onCall".into(),
        timeout_ms: 50,
        registration_seq: 1,
    }
}

/// 装好给定 host 与订阅的注册表，并挂上记录事件的观察者
async fn rig<H>(host: H, subs: Vec<Subscription>) -> (Arc<Registry>, Recorder)
where
    H: ModHost + 'static,
{
    let bus = Arc::new(Dispatcher::new(
        wind_core::usecase::directory().merge(wind_mcp::events::directory()),
        Arc::new(AllEnabled),
        Arc::new(host),
        Limits::default(),
    ));
    bus.rebuild_index(subs);
    let recorder = Recorder::default();
    bus.attach_observer(Arc::new(recorder.clone()));
    (Arc::new(Registry::new(bus)), recorder)
}

/// 一次调用的时间预算
///
/// 建立一条连接是真实 I/O，比事件链的默认量级长。预算耗尽会让拦截链被
/// 整条跳过
const BUDGET: Duration = Duration::from_secs(30);

/// 指向话题 1 的上下文
fn cx() -> DispatchCx {
    cx_in(1)
}

/// 指向给定话题的上下文
fn cx_in(topic_id: i64) -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id },
        Source::core("mcp"),
        BUDGET,
    )
}

/// 带应用目录的装配
///
/// 自带服务的根按话题算出来，因此注册它们需要一份可写的应用目录
async fn rig_with_dirs<H>(
    host: H,
    subs: Vec<Subscription>,
) -> (TempDir, AppDirs, Arc<Registry>, Recorder)
where
    H: ModHost + 'static,
{
    let tmp = TempDir::new().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let (registry, recorder) = rig(host, subs).await;
    (tmp, dirs, registry, recorder)
}

/// 为一个话题注册宿主自带的两个服务
async fn register_servers(registry: &Registry, dirs: &AppDirs, topic_id: i64) {
    let roots = roots_for(dirs, topic_id, &[]);
    register_builtin(registry, &cx_in(topic_id), topic_id, &roots)
        .await
        .expect("register builtin service");
}

/// 读文件的参数，路径按 JSON 字符串给出
fn read_file_args(path: &std::path::Path) -> serde_json::Value {
    serde_json::json!({ "path": path.to_string_lossy() })
}

/// 在握手之后不再正常应答的伪服务
///
/// 伪服务作为资产被直接启动，因此只在能执行脚本的平台上提供。进程内服务端
/// 无法表现传输断开，服务端 task 结束之后客户端会一直等下去
#[cfg(unix)]
mod dying {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use wind_mcp::identity::AssetRoot;
    use wind_mcp::params::{ServerParams, StdioParams};

    /// 伪服务的脚本
    ///
    /// 答完握手之后对每个请求直接退出。之后每个请求都撞在一条已经关闭的
    /// 传输上
    const SCRIPT: &str = r#"#!/usr/bin/env python3
import json, sys
for line in sys.stdin:
    try:
        msg = json.loads(line)
    except Exception:
        continue
    if msg.get("method") == "initialize":
        result = {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "dying", "version": "1"}}
        print(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": result}), flush=True)
    elif "id" in msg:
        sys.exit(0)
"#;

    /// 答完握手就退出的伪服务
    pub fn dying() -> (TempDir, ServerParams, AssetRoot) {
        server()
    }

    /// 伪服务，连同承载资产的临时目录、连接描述与资产根
    fn server() -> (TempDir, ServerParams, AssetRoot) {
        let tmp = TempDir::new().expect("create temp dir");
        // 命令以 ./ 开头，资产位于包根下的 mcp 目录
        let dir = tmp.path().join("mcp").join("dying");
        fs::create_dir_all(&dir).expect("create dir");
        let script = dir.join("serve");
        fs::write(&script, SCRIPT).expect("write");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("execute");
        let params = ServerParams::Stdio(StdioParams {
            name: "dying".into(),
            description: Some("stops answering normally after the handshake".into()),
            command: "./mcp/dying/serve".into(),
            args: Vec::new(),
            env: None,
            cwd: None,
        });
        let root = AssetRoot::new(tmp.path());
        (tmp, params, root)
    }
}

#[tokio::test]
async fn a_rewritten_call_uses_the_rewritten_arguments() {
    let tmp = TempDir::new().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let file = dirs.topic_dir(1).join("note.txt");
    std_fs::create_dir_all(dirs.topic_dir(1)).expect("create dir");
    std_fs::write(&file, "rewritten arguments").expect("write");

    // 被改写之后调用指向自带服务，参数指向这个文件
    let rewritten = serde_json::json!({
        "server_name": FS,
        "tool": "read_file",
        "arguments": read_file_args(&file),
        "scope": { "kind": "topic", "topic_id": 1 }
    });
    let host = ScriptedHost::rewriting("core/mcp.tool.call", rewritten);
    let seen = Arc::clone(&host.seen);
    let (registry, _recorder) = rig(host, vec![subscription("core/mcp.tool.call")]).await;
    let cx = cx();
    // 先建立连接，拦截链要在这段真实 I/O 之后仍然执行
    register_servers(&registry, &dirs, 1).await;

    let outcome = registry
        .call_tool(
            &cx,
            "call_1",
            "clock",
            "get_current_time",
            serde_json::json!({ "timezone": "UTC" }),
        )
        .await
        .expect("call");
    let CallOutcome { arguments, result } = outcome;
    assert_eq!(
        arguments["path"],
        read_file_args(&file)["path"],
        "uses the rewritten arguments"
    );
    assert_eq!(
        result["structuredContent"]["content"],
        serde_json::json!("rewritten arguments"),
        "the rewritten call really reached the service"
    );
    assert_eq!(
        host_calls(&seen),
        1,
        "the stand-in must actually be intercepted, otherwise this test tests nothing"
    );
}

#[tokio::test]
async fn the_builtin_servers_are_listed_for_a_topic() {
    let (registry, _recorder) = rig(
        ScriptedHost::pass(),
        vec![subscription("core/mcp.tool.call")],
    )
    .await;
    let outcome = registry.list_servers(&cx(), 1).await;
    assert!(
        outcome.iter().any(|server| server.name == "fs"),
        "the builtin services exist for every topic"
    );
    assert!(
        outcome.iter().any(|server| server.name == "skills"),
        "the builtin services exist for every topic"
    );
}

#[tokio::test]
async fn every_event_of_this_unit_is_an_intercept_except_the_crash_notice() {
    let directory = wind_mcp::events::directory();
    for name in directory.names() {
        let meta = directory.get(name).expect("query");
        let expected = if name == "core/mcp.crash" {
            Shape::Observe
        } else {
            Shape::Intercept
        };
        assert_eq!(meta.shape, expected, "{name}");
    }
}

#[tokio::test]
async fn a_call_stays_on_the_topic_that_bound_the_name() {
    let (_tmp, dirs, registry, _recorder) = rig_with_dirs(ScriptedHost::pass(), Vec::new()).await;
    let first = cx_in(1);
    let second = cx_in(2);
    // 同名但作用域不同，两个话题各有自己的自带服务
    register_servers(&registry, &dirs, 1).await;
    register_servers(&registry, &dirs, 2).await;
    assert_eq!(
        registry.connection_count(),
        4,
        "each of the two topics has its own two builtin services"
    );

    let first_file = dirs.topic_dir(1).join("note.txt");
    let second_file = dirs.topic_dir(2).join("note.txt");
    std_fs::write(&first_file, "the topic 1 file").expect("write");
    std_fs::write(&second_file, "the topic 2 file").expect("write");

    let first_outcome = registry
        .call_tool(
            &first,
            "call_1",
            FS,
            "read_file",
            read_file_args(&first_file),
        )
        .await
        .expect("call the topic 1 server");
    let CallOutcome { result, .. } = first_outcome;
    assert_eq!(
        result["structuredContent"]["content"],
        serde_json::json!("the topic 1 file"),
        "the topic 1 service reads the file in its own root"
    );

    // 同一个路径在话题 2 上落到另一条连接，该连接的根不属于话题 1
    let second_outcome = registry
        .call_tool(
            &second,
            "call_2",
            FS,
            "read_file",
            read_file_args(&first_file),
        )
        .await
        .expect("call");
    let CallOutcome { result, .. } = second_outcome;
    assert_eq!(
        result["structuredContent"]["bytes"],
        serde_json::json!(0),
        "the topic 2 service cannot see the working directory of topic 1"
    );
}

/// 对每个请求都返回协议错误的伪服务
///
/// 连接始终存在，失败由服务端返回
struct RefusingServer;

impl ServerHandler for RefusingServer {
    /// 调用一律返回协议错误
    fn call_tool(
        &self,
        _request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CallToolResponse, ErrorData>> + '_ {
        std::future::ready(Err(ErrorData::internal_error("the tool failed", None)))
    }

    /// 列举一律返回协议错误
    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, ErrorData>> + '_ {
        std::future::ready(Err(ErrorData::internal_error("the tool failed", None)))
    }
}

#[tokio::test]
async fn a_request_the_server_answers_with_an_error_is_not_a_crash() {
    let (_tmp, _dirs, registry, recorder) = rig_with_dirs(ScriptedHost::pass(), Vec::new()).await;
    let cx = cx();
    registry
        .connect_builtin(&cx, 1, "refusing", "topic:1", RefusingServer)
        .await
        .expect("register");

    // 服务返回的是协议错误，连接仍在
    let called = registry
        .call_tool(
            &cx,
            "call_1",
            "refusing",
            "no_such_tool",
            serde_json::json!({}),
        )
        .await;
    assert!(
        matches!(called, Err(McpError::Connect { .. })),
        "an error answered by the service is returned as a connection failure, got {called:?}"
    );
    let listed = registry.list_tools(&cx, 1).await;
    assert!(
        matches!(listed, Err(McpError::Connect { .. })),
        "an error answered by the service is returned as a connection failure, got {listed:?}"
    );
    assert_eq!(
        recorder.count("core/mcp.crash"),
        0,
        "an error answered by the service is not a dropped connection"
    );
}

/// 连接断开时要宣告崩溃
///
/// 进程内服务端无法表现这件事，服务端 task 结束之后客户端会一直等下去。
/// 用例需要传输真的断开，因此使用一个退出脚本
#[cfg(unix)]
#[tokio::test]
async fn a_failure_on_an_established_connection_is_reported_as_a_crash() {
    let (_assets, params, asset_root) = dying::dying();
    let (registry, recorder) = rig(ScriptedHost::pass(), Vec::new()).await;
    let cx = cx();
    registry
        .attach(&cx, 1, "dying", &params, &asset_root)
        .await
        .expect("connect");
    assert_eq!(
        recorder.count("core/mcp.crash"),
        0,
        "attaching a connection is not a crash"
    );

    // 失败发生在已经建立的句柄上，不是建立阶段
    let called = registry
        .call_tool(
            &cx,
            "call_1",
            "dying",
            "get_current_time",
            serde_json::json!({ "timezone": "UTC" }),
        )
        .await;
    assert!(
        matches!(called, Err(McpError::Connect { .. })),
        "a failed call is returned as a connection failure, got {called:?}"
    );
    let listed = registry.list_tools(&cx, 1).await;
    assert!(
        matches!(listed, Err(McpError::Connect { .. })),
        "a failed list is returned as a connection failure, got {listed:?}"
    );

    assert_eq!(
        recorder.count("core/mcp.crash"),
        2,
        "one failed call and one failed list each announce once"
    );
    let crash = recorder
        .last("core/mcp.crash")
        .expect("find the crash event");
    assert_eq!(crash["payload"]["server_name"], serde_json::json!("dying"));
}
