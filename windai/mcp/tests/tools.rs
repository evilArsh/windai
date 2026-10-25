//! MCP 工具在请求链上的声明与调用

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use wind_core::llm::ports::{McpTools, Tools};
use wind_event::context::DispatchCx;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::subscription::{EnabledSet, Phase, Subscription};
use wind_mcp::builtin::fs::FsServer;
use wind_mcp::registry::Registry;
use wind_mcp::tools::McpToolSource;

/// 一条用例的时间预算
///
/// 建立一条连接比这个预算短得多，但仍比事件链的默认量级长。预算耗尽会让
/// 拦截链被整条跳过
const BUDGET: Duration = Duration::from_secs(30);

/// 把 fs 装到话题 1 上的装配
struct Rig {
    /// 请求链一侧的入口
    source: McpToolSource,
    /// 话题 1 的上下文
    cx: DispatchCx,
    /// 记下被派发过哪些事件名的宿主
    host: Arc<RecordingHost>,
    /// fs 的工作根。持有它以免提前删除
    root: TempDir,
}

/// 记录被调用过哪些事件的宿主
///
/// 宿主只在有订阅者的事件上被调用，记录的事件名就是真的派发过的事件
#[derive(Default)]
struct RecordingHost {
    /// 被调用时的事件名，按发生顺序
    seen: Mutex<Vec<String>>,
}

impl RecordingHost {
    /// 被调用过的事件名，按发生顺序
    fn seen_names(&self) -> Vec<String> {
        self.seen.lock().expect("lock the record").clone()
    }
}

#[async_trait::async_trait]
impl ModHost for RecordingHost {
    async fn call(
        &self,
        sub: &Subscription,
        _payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        self.seen
            .lock()
            .expect("lock the record")
            .push(sub.event.to_string());
        Ok(None)
    }
}

/// 全部订阅者都启用
struct AllEnabled;

impl ModEnablement for AllEnabled {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::all()
    }
}

/// 拦下指定事件的订阅
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

/// 按给定的宿主与订阅创建装配
///
/// 总线使用真实派发器与真实目录，注册表因此与生产环境同源
async fn rig_with(host: Arc<RecordingHost>, subs: Vec<Subscription>) -> Rig {
    let root = TempDir::new().expect("create temp dir");
    let bus = Arc::new(Dispatcher::new(
        wind_core::usecase::directory().merge(wind_mcp::events::directory()),
        Arc::new(AllEnabled),
        host.clone(),
        Limits::default(),
    ));
    bus.rebuild_index(subs);
    let registry = Arc::new(Registry::new(bus));
    let cx = DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id: 1 },
        Source::core("mcp"),
        BUDGET,
    );
    registry
        .connect_builtin(
            &cx,
            1,
            "fs",
            "topic:1",
            FsServer::new(vec![root.path().to_path_buf()]),
        )
        .await
        .expect("register builtin service");
    Rig {
        source: McpToolSource::new(registry),
        cx,
        host,
        root,
    }
}

/// 话题 1 上装着 fs 的装配
async fn rig_with_fs_server() -> Rig {
    rig_with(Arc::new(RecordingHost::default()), Vec::new()).await
}

/// 能拦到工具调用的装配
///
/// 宿主只在有订阅者的事件上被调用，这条订阅是第三个用例的观察点能够成立的
/// 前提
async fn rig_with_recording_host() -> Rig {
    rig_with(
        Arc::new(RecordingHost::default()),
        vec![subscription("core/mcp.tool.call")],
    )
    .await
}

#[tokio::test]
async fn every_tool_of_the_topic_carries_its_exposed_name() {
    let rig = rig_with_fs_server().await;
    let declared = rig.source.declarations(&rig.cx, 1).await.expect("list");
    let read = declared
        .iter()
        .find(|tool| tool.origin.exposed() == "fs__read_file")
        .expect("find the fs read tool");
    match &read.declaration {
        Tools::Function(function) => {
            assert_eq!(
                function.name,
                read.origin.exposed(),
                "the name must already have the shape {{server}}__{{tool}}, which is exactly what the allowlist compares"
            );
            assert!(
                function.parameters.is_some(),
                "the parameter schema must be handed out together with the declaration"
            );
        }
    }
}

#[tokio::test]
async fn a_tool_call_comes_back_as_an_outcome() {
    let rig = rig_with_fs_server().await;
    let path = rig.root.path().join("a.txt");
    std::fs::write(&path, "body").expect("write");
    let outcome = rig
        .source
        .call(
            &rig.cx,
            "c-1",
            "fs",
            "read_file",
            serde_json::json!({ "path": path.to_string_lossy() }),
        )
        .await
        .expect("call");
    let result = outcome;
    assert_eq!(
        result["structuredContent"]["content"],
        serde_json::json!("body"),
        "the execution result must be brought back unchanged"
    );
}

#[tokio::test]
async fn the_call_still_goes_through_the_registrys_own_interception() {
    // 这个接口只改变调用由谁发起，不改变调用经过谁。调用仍从注册表派发
    let rig = rig_with_recording_host().await;
    let _ = rig
        .source
        .call(
            &rig.cx,
            "c-1",
            "fs",
            "read_file",
            serde_json::json!({ "path": "a.txt" }),
        )
        .await
        .expect("call");
    assert!(
        rig.host
            .seen_names()
            .iter()
            .any(|name| name == "core/mcp.tool.call"),
        "this interface must not bypass the registry and dispatch on its own"
    );
}
