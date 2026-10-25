//! 九条事件与注册表

use std::fs as std_fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use wind_core::db::Db;
use wind_core::storage::Storage;
use wind_event::context::DispatchCx;
use wind_event::def::{EventMeta, Mount, Shape};
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::observer::BuiltinObserver;
use wind_event::subscription::{EnabledSet, Phase, Subscription};
use wind_mcp::builtin::fs::FsServer;
use wind_mcp::builtin::{BuiltinRoots, FS, SKILLS, register_builtin, roots_for};
use wind_mcp::events::{self, Transport};
#[cfg(unix)]
use wind_mcp::identity::AssetRoot;
use wind_mcp::registry::{CallOutcome, McpError, Registry};
use wind_mcp::runtime::ModServers;
use wind_mod::dirs::AppDirs;
use wind_mod::installer::TopicServers;
use wind_mod::manifest::Manifest;

#[test]
fn the_directory_holds_the_nine_events_of_this_unit() {
    let directory = events::directory();
    assert_eq!(directory.len(), 9);
    let intercepts = [
        "core/mcp.attach",
        "core/mcp.detach",
        "core/mcp.start",
        "core/mcp.stop",
        "core/mcp.server.list",
        "core/mcp.tool.list",
        "core/mcp.tool.call",
        "core/mcp.tool.result",
    ];
    for name in intercepts {
        let meta = directory
            .get(name)
            .unwrap_or_else(|| panic!("missing event {name}"));
        assert_eq!(meta.shape, Shape::Intercept, "{name} can be rewritten");
        assert!(meta.log, "{name} must be logged");
        let mount = if name == "core/mcp.attach" || name == "core/mcp.detach" {
            Mount::UseCase
        } else {
            Mount::Subsystem
        };
        assert_eq!(meta.mount, mount, "the mount point of {name}");
    }
    let crash = directory.get("core/mcp.crash").expect("must exist");
    assert_eq!(crash.shape, Shape::Observe);
    assert!(crash.log);
    assert_eq!(
        crash.mount,
        Mount::Subsystem,
        "the crash notice is mounted inside the subsystem"
    );
}

#[test]
fn a_tool_call_payload_carries_the_scope_it_was_made_in() {
    let payload = events::McpToolCall {
        server_name: "fs".into(),
        tool: "read_file".into(),
        arguments: serde_json::json!({ "path": "a.txt" }),
        scope: wind_event::envelope::Scope::Topic { topic_id: 3 },
    };
    let value = serde_json::to_value(&payload).expect("serialize");
    assert_eq!(value["server_name"], serde_json::json!("fs"));
    assert_eq!(value["scope"]["kind"], serde_json::json!("topic"));
    assert_eq!(value["scope"]["topic_id"], serde_json::json!(3));
}

#[test]
fn the_attach_payload_names_a_topic() {
    let payload = events::McpAttach {
        topic_id: 1,
        server_name: "fetch".into(),
    };
    let value = serde_json::to_value(&payload).expect("serialize");
    assert_eq!(value["topic_id"], serde_json::json!(1));
    assert_eq!(value["server_name"], serde_json::json!("fetch"));
}

/// 不做任何否决的 host。本任务只测 MCP 自己的链路
struct NoopHost;

#[async_trait::async_trait]
impl ModHost for NoopHost {
    async fn call(
        &self,
        _sub: &Subscription,
        _payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        Ok(None)
    }
}

/// 记录拦到的载荷的 host
struct WatchingHost {
    /// 按发生顺序记下的载荷
    payloads: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}

impl WatchingHost {
    /// 只做记录的 host
    fn new() -> Self {
        Self {
            payloads: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// 最近一条载荷里指定字段的取值
    fn last(&self, field: &str) -> String {
        self.payloads
            .lock()
            .expect("lock the record")
            .last()
            .and_then(|payload| payload[field].as_str())
            .unwrap_or_default()
            .to_string()
    }

    /// 见过的载荷里指定字段的全部取值，按发生顺序
    fn values(&self, field: &str) -> Vec<String> {
        self.payloads
            .lock()
            .expect("lock the record")
            .iter()
            .filter_map(|payload| payload[field].as_str().map(str::to_string))
            .collect()
    }
}

#[async_trait::async_trait]
impl ModHost for WatchingHost {
    async fn call(
        &self,
        _sub: &Subscription,
        payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        self.payloads.lock().expect("lock the record").push(payload);
        Ok(None)
    }
}

/// 全部启用的使能视图。需要配一条订阅才能拦住事件
struct AllEnabled;

impl ModEnablement for AllEnabled {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::all()
    }
}

/// 记录派发出去的 mcp 事件名
#[derive(Default, Clone)]
struct Recorder {
    /// 按发生顺序记下的事件名
    seen: Arc<std::sync::Mutex<Vec<String>>>,
}

impl Recorder {
    /// 某条事件发生过几次
    fn count(&self, name: &str) -> usize {
        self.seen
            .lock()
            .expect("lock the record")
            .iter()
            .filter(|seen| seen.as_str() == name)
            .count()
    }
}

#[async_trait::async_trait]
impl BuiltinObserver for Recorder {
    async fn on_event(&self, _meta: &EventMeta, event: &serde_json::Value) {
        self.seen
            .lock()
            .expect("lock the record")
            .push(event["name"].as_str().unwrap_or_default().to_string());
    }
}

/// 不拦任何事件的注册表
async fn rig() -> (Arc<Registry>, Recorder) {
    rig_with(Arc::new(NoopHost), Vec::new()).await
}

/// 按给定 host 与订阅装起来的注册表
async fn rig_with(host: Arc<dyn ModHost>, subs: Vec<Subscription>) -> (Arc<Registry>, Recorder) {
    let bus = Arc::new(Dispatcher::new(
        wind_core::usecase::directory().merge(wind_mcp::events::directory()),
        Arc::new(AllEnabled),
        host,
        Limits::default(),
    ));
    bus.rebuild_index(subs);
    let recorder = Recorder::default();
    bus.attach_observer(Arc::new(recorder.clone()));
    (Arc::new(Registry::new(bus)), recorder)
}

/// 拦下指定事件的订阅
fn subscription(event: &'static str) -> Subscription {
    Subscription {
        mod_name: "guard".into(),
        mod_version: 1,
        event,
        phase: Some(Phase::Guard),
        handler: "onStop".into(),
        timeout_ms: 50,
        registration_seq: 1,
    }
}

/// 一条用例的时间预算
///
/// 建立一条连接比这个预算短得多，但仍比事件链的默认量级长。预算耗尽会让
/// 拦截链被整条跳过
const BUDGET: Duration = Duration::from_secs(30);

/// 指向给定话题的上下文
fn cx_in(topic_id: i64) -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id },
        Source::core("mcp"),
        BUDGET,
    )
}

/// 指向话题 1 的上下文
fn cx() -> DispatchCx {
    cx_in(1)
}

/// 带应用目录的装配
///
/// 自带服务的根按话题算出，注册它们需要可写的应用目录
async fn rig_with_dirs() -> (TempDir, AppDirs, Arc<Registry>, Recorder) {
    let tmp = TempDir::new().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let (registry, recorder) = rig().await;
    (tmp, dirs, registry, recorder)
}

/// 一个话题下自带服务的根
///
/// 调用方的话题没有启用任何 mod，技能根只有公共目录
fn roots_of(dirs: &AppDirs, topic_id: i64) -> BuiltinRoots {
    roots_for(dirs, topic_id, &[])
}

/// 为一个话题注册宿主自带的两个服务
async fn register_servers(registry: &Registry, dirs: &AppDirs, topic_id: i64) {
    let roots = roots_of(dirs, topic_id);
    register_builtin(registry, &cx_in(topic_id), topic_id, &roots)
        .await
        .expect("register builtin service");
}

/// 在技能根里放一个 `SKILL.md`
fn write_skill(root: &Path, name: &str) {
    std_fs::create_dir_all(root).expect("create dir");
    std_fs::write(
        root.join("SKILL.md"),
        format!("---\nname: {name}\ndescription: description of {name}\n---\n"),
    )
    .expect("write");
}

/// 通过一次技能列举查看某个话题的 `skills` 服务可见哪些目录
///
/// 返回的结构化结果是技能清单，根之外的目录会被服务拒绝
async fn list_through_skills(
    registry: &Registry,
    cx: &DispatchCx,
    dir: &Path,
) -> Result<serde_json::Value, McpError> {
    let outcome = registry
        .call_tool(
            cx,
            "call_1",
            SKILLS,
            "skills_list",
            serde_json::json!({ "dir": dir.to_string_lossy() }),
        )
        .await?;
    let CallOutcome { result, .. } = outcome;
    Ok(result["structuredContent"].clone())
}

/// 通过一次读文件确认某个话题的 `fs` 服务使用哪个根
///
/// 返回的结构化结果里有 `content` 与 `bytes` 两个字段
async fn read_through_fs(registry: &Registry, cx: &DispatchCx, path: &Path) -> serde_json::Value {
    let outcome = registry
        .call_tool(
            cx,
            "call_1",
            FS,
            "read_file",
            serde_json::json!({ "path": path.to_string_lossy() }),
        )
        .await
        .expect("call");
    let CallOutcome { result, .. } = outcome;
    result["structuredContent"].clone()
}

#[tokio::test]
async fn two_topics_share_one_connection_by_identity() {
    let (_tmp, dirs, registry, _recorder) = rig_with_dirs().await;
    let roots = roots_of(&dirs, 1);
    // 两个话题引用同一个实例。身份由名字与作用域给出，与引用它的名字无关。
    // 第二次注册替换的是连接本身，两个话题仍然指向同一槽位
    for topic_id in [1, 2] {
        registry
            .connect_builtin(
                &cx_in(topic_id),
                topic_id,
                FS,
                "topic:1",
                FsServer::new(vec![roots.workspace.clone()]),
            )
            .await
            .expect("register");
    }
    assert_eq!(
        registry.connection_count(),
        1,
        "one connection per identity"
    );
    assert_eq!(
        registry.reference_count(FS),
        2,
        "each of the two topics holds one reference"
    );
}

/// 同一身份的两条并发注册只建立一条连接
///
/// 后到的注册进来时前一次仍在建立，它等待前一次完成，而不是自己再建立一条
#[tokio::test]
async fn two_concurrent_registrations_of_one_identity_build_one_connection() {
    let (_tmp, dirs, registry, recorder) = rig_with_dirs().await;
    let roots = roots_of(&dirs, 1);
    let cx = cx_in(1);
    let scope = "topic:1";
    let one = registry.connect_builtin(
        &cx,
        1,
        FS,
        scope,
        FsServer::new(vec![roots.workspace.clone()]),
    );
    let two = registry.connect_builtin(
        &cx,
        1,
        FS,
        scope,
        FsServer::new(vec![roots.workspace.clone()]),
    );

    let (one, two) = tokio::join!(one, two);
    one.expect("register");
    two.expect("register");

    assert_eq!(
        recorder.count("core/mcp.start"),
        1,
        "one identity starts only once"
    );
    assert_eq!(
        recorder.count("core/mcp.attach"),
        2,
        "both registrations announced the binding"
    );
    assert_eq!(
        registry.connection_count(),
        1,
        "only one connection remains"
    );
    assert_eq!(
        registry.reference_count(FS),
        1,
        "both registrations land on the same topic"
    );
}

#[tokio::test]
async fn the_connection_drops_only_after_the_last_reference() {
    let (_tmp, dirs, registry, _recorder) = rig_with_dirs().await;
    let roots = roots_of(&dirs, 1);
    let first = cx_in(1);
    let second = cx_in(2);
    for topic_id in [1, 2] {
        registry
            .connect_builtin(
                &cx_in(topic_id),
                topic_id,
                FS,
                "topic:1",
                FsServer::new(vec![roots.workspace.clone()]),
            )
            .await
            .expect("register");
    }

    registry.detach(&first, 1, FS).await;
    assert_eq!(
        registry.connection_count(),
        1,
        "one reference still remains, so it cannot be dropped"
    );
    registry.detach(&second, 2, FS).await;
    assert_eq!(
        registry.connection_count(),
        0,
        "it drops only after the last reference is released"
    );
}

#[tokio::test]
async fn attaching_and_detaching_are_announced() {
    let (_tmp, dirs, registry, recorder) = rig_with_dirs().await;
    register_servers(&registry, &dirs, 1).await;
    registry.detach(&cx(), 1, FS).await;
    let seen = recorder.seen.lock().expect("lock the record").clone();
    assert!(seen.contains(&"core/mcp.attach".to_string()), "{seen:?}");
    assert!(seen.contains(&"core/mcp.detach".to_string()), "{seen:?}");
}

/// 一个话题只能看见它自己启用过的 mod 的技能目录
///
/// 技能根按话题算出，由启用过的 mod 的 `skills` 目录与公共目录组成。其他话题
/// 的 mod 目录不在其中，`skills` 服务必须拒绝这个目录
#[tokio::test]
async fn a_topic_cannot_read_another_topics_mod_skills() {
    let (tmp, dirs, registry, _recorder) = rig_with_dirs().await;
    let alpha = AppDirs::under(tmp.path()).mod_version("alpha", 1);
    let beta = AppDirs::under(tmp.path()).mod_version("beta", 3);
    write_skill(&alpha.join("skills"), "alpha-skill");
    write_skill(&beta.join("skills"), "beta-skill");

    // 话题 1 启用 alpha，话题 2 启用 beta
    register_builtin(
        &registry,
        &cx_in(1),
        1,
        &roots_for(&dirs, 1, std::slice::from_ref(&alpha)),
    )
    .await
    .expect("register builtin service");
    register_builtin(
        &registry,
        &cx_in(2),
        2,
        &roots_for(&dirs, 2, std::slice::from_ref(&beta)),
    )
    .await
    .expect("register builtin service");

    // 自己启用过的 mod 的技能目录可见
    let mine = list_through_skills(&registry, &cx_in(1), &alpha.join("skills"))
        .await
        .expect("list the skills of the enabled mod");
    let names: Vec<&str> = mine
        .as_array()
        .expect("the skill list is an array")
        .iter()
        .filter_map(|meta| meta["name"].as_str())
        .collect();
    assert_eq!(
        names,
        vec!["alpha-skill"],
        "enabled mod skill directories are listed first"
    );

    // 别的话题的 mod 技能目录必须被拒
    let refused = list_through_skills(&registry, &cx_in(1), &beta.join("skills")).await;
    assert!(
        matches!(refused, Err(McpError::Connect { .. })),
        "topic 1 must not see the skill directory of a mod enabled by topic 2, got {refused:?}"
    );
}

/// 技能根之内的符号链接不会带出根外的技能
///
/// 直接访问该目录会被沙箱拒绝，经技能根内的链接绕入同样不得放行
#[cfg(unix)]
#[tokio::test]
async fn a_link_inside_a_skill_root_does_not_escape_it() {
    let (_tmp, dirs, registry, _recorder) = rig_with_dirs().await;
    let outside = TempDir::new().expect("create temp dir");
    write_skill(&outside.path().join("secret"), "outside-skill");
    register_servers(&registry, &dirs, 1).await;
    let public = dirs.public_skills();
    write_skill(&public.join("inner"), "inner-skill");
    std::os::unix::fs::symlink(outside.path(), public.join("leak")).expect("create the symlink");

    let listed = list_through_skills(&registry, &cx_in(1), &public)
        .await
        .expect("list inside the skill root");
    let names: Vec<&str> = listed
        .as_array()
        .expect("the skill list is an array")
        .iter()
        .filter_map(|meta| meta["name"].as_str())
        .collect();
    assert_eq!(
        names,
        vec!["inner-skill"],
        "the skill outside the root must not appear in the list"
    );

    // 该目录本身仍然不在任何技能根之内
    let refused = list_through_skills(&registry, &cx_in(1), outside.path()).await;
    assert!(
        matches!(refused, Err(McpError::Connect { .. })),
        "a directory outside the roots must be refused, got {refused:?}"
    );
}

/// 自带服务的注册与 mod 声明的服务走同一组事件
///
/// 注册宣告 attach 与 start，解绑宣告 detach 与 stop。自带服务没有专属
/// 事件，每一次启停都经过同一条拦截链
#[tokio::test]
async fn registering_a_builtin_announces_the_same_lifecycle() {
    let (_tmp, dirs, registry, recorder) = rig_with_dirs().await;
    let roots = roots_of(&dirs, 1);
    let scope = "topic:1";
    registry
        .connect_builtin(
            &cx(),
            1,
            FS,
            scope,
            FsServer::new(vec![roots.workspace.clone()]),
        )
        .await
        .expect("register");
    registry.detach(&cx(), 1, FS).await;
    let seen = recorder.seen.lock().expect("lock the record").clone();
    for name in [
        "core/mcp.attach",
        "core/mcp.start",
        "core/mcp.detach",
        "core/mcp.stop",
    ] {
        assert!(seen.contains(&name.to_string()), "missing {name}: {seen:?}");
    }
}

/// 注册落在哪个话题上只由传入的话题给出
///
/// 身份串里的作用域与绑定同源。上下文只为派发出去的事件确定信封，上下文
/// 指向其他话题也不会把这次注册记到那个话题上
#[tokio::test]
async fn a_builtin_is_bound_to_the_topic_it_is_given() {
    let (_tmp, dirs, registry, _recorder) = rig_with_dirs().await;
    let roots = roots_of(&dirs, 1);
    registry
        .connect_builtin(
            &cx_in(9),
            1,
            FS,
            "topic:1",
            FsServer::new(vec![roots.workspace.clone()]),
        )
        .await
        .expect("register");
    assert_eq!(
        registry.server_names(1),
        vec![FS.to_string()],
        "the binding lands on the topic that was passed in"
    );
    assert!(
        registry.server_names(9).is_empty(),
        "nothing is on the topic the context points at"
    );
}

/// 同一个身份的再次注册替换连接，而不是再建立一条
///
/// 自带服务的根随话题的启用集合变化，保留旧连接会让新根不生效。替换之后
/// 引用仍在，两个话题都指向表里的同一槽位
#[tokio::test]
async fn registering_the_same_identity_again_replaces_its_connection() {
    let (_tmp, dirs, registry, recorder) = rig_with_dirs().await;
    let roots = roots_of(&dirs, 1);
    let first = cx_in(1);
    let second = cx_in(2);
    for topic_id in [1, 2] {
        registry
            .connect_builtin(
                &cx_in(topic_id),
                topic_id,
                FS,
                "topic:1",
                FsServer::new(vec![roots.workspace.clone()]),
            )
            .await
            .expect("register");
    }
    assert_eq!(
        registry.connection_count(),
        1,
        "the same identity leaves only one connection"
    );
    assert_eq!(
        registry.reference_count(FS),
        2,
        "both topics still point at it"
    );
    let seen = recorder.seen.lock().expect("lock the record").clone();
    assert!(
        seen.contains(&"core/mcp.stop".to_string()),
        "the replaced connection must announce its teardown: {seen:?}"
    );

    registry.detach(&first, 1, FS).await;
    assert_eq!(
        registry.connection_count(),
        1,
        "another topic still references it"
    );
    registry.detach(&second, 2, FS).await;
    assert_eq!(
        registry.connection_count(),
        0,
        "it drops only after the last reference is released"
    );
}

#[tokio::test]
async fn rebinding_a_name_releases_the_connection_it_left() {
    let (_tmp, dirs, registry, recorder) = rig_with_dirs().await;
    let old_root = dirs.topic_dir(1);
    let new_root = dirs.topic_dir(2);
    std_fs::create_dir_all(&old_root).expect("create dir");
    std_fs::create_dir_all(&new_root).expect("create dir");
    std_fs::write(old_root.join("early.txt"), "the old root").expect("write");
    std_fs::write(new_root.join("late.txt"), "the new root").expect("write");

    registry
        .connect_builtin(
            &cx(),
            1,
            FS,
            "topic:1",
            FsServer::new(vec![old_root.clone()]),
        )
        .await
        .expect("register");
    assert_eq!(registry.connection_count(), 1);

    // 同一个键指向另一条连接。同名但作用域不同就是另一个实例
    registry
        .connect_builtin(
            &cx(),
            1,
            FS,
            "topic:1#2",
            FsServer::new(vec![new_root.clone()]),
        )
        .await
        .expect("rebind the name");
    assert_eq!(
        registry.connection_count(),
        1,
        "the old connection must be released"
    );
    let seen = recorder.seen.lock().expect("lock the record").clone();
    assert!(
        seen.contains(&"core/mcp.stop".to_string()),
        "the teardown of the old connection must be announced: {seen:?}"
    );

    // 该键现在指向新的连接。新连接的根已换，旧根里的文件不再可见
    let read = read_through_fs(&registry, &cx(), &new_root.join("late.txt")).await;
    assert_eq!(
        read["content"],
        serde_json::json!("the new root"),
        "a file in the new root can be read"
    );
    let refused = read_through_fs(&registry, &cx(), &old_root.join("early.txt")).await;
    assert_eq!(
        refused["bytes"],
        serde_json::json!(0),
        "the old root is now outside its sandbox"
    );
}

#[tokio::test]
async fn the_listed_servers_carry_their_own_transport() {
    let (_tmp, dirs, registry, _recorder) = rig_with_dirs().await;

    // 还没注册过的话题上，自带服务按宿主给出的说明在列
    let servers = registry.list_servers(&cx(), 1).await;
    let skills = servers
        .iter()
        .find(|server| server.name == SKILLS)
        .expect("find the builtin server");
    assert_eq!(
        skills.transport,
        Transport::Builtin,
        "an in-process service"
    );
    assert_eq!(
        skills.description.as_deref(),
        Some("provided by the host"),
        "the description comes from the host"
    );
    assert!(
        !skills.running,
        "it is not running on a topic that has never been registered; reporting it as running would make subscribers think calls can go through"
    );

    // 注册之后，一条服务只占一个槽位，仍然报告为进程内
    register_servers(&registry, &dirs, 1).await;
    let servers = registry.list_servers(&cx(), 1).await;
    let names: Vec<&str> = servers.iter().map(|server| server.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["fs", "skills"],
        "a registered builtin occupies only one slot"
    );
    assert!(
        servers
            .iter()
            .all(|server| server.transport == Transport::Builtin),
        "both are in-process connections"
    );
    assert!(
        servers.iter().all(|server| server.running),
        "both are connected"
    );
}

/// 停止事件的理由说明实际发生了什么
///
/// 重注册不是换绑，订阅方读到的理由要与实际动作一致
#[tokio::test]
async fn the_stop_reason_names_what_happened() {
    let host = Arc::new(WatchingHost::new());
    let (registry, _recorder) = rig_with(
        Arc::clone(&host) as Arc<dyn ModHost>,
        vec![subscription("core/mcp.stop")],
    )
    .await;
    let tmp = TempDir::new().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let scope = "topic:1";

    // 先注册一次，再按同一个身份注册一次
    for _ in 0..2 {
        registry
            .connect_builtin(&cx(), 1, FS, scope, FsServer::new(vec![dirs.topic_dir(1)]))
            .await
            .expect("register");
    }
    assert_eq!(
        host.last("reason"),
        "topic 1 registered fs again",
        "the reason for re-registering must not be phrased as a rebind"
    );

    // 同一个键指向另一个实例，这才是换绑
    registry
        .connect_builtin(
            &cx(),
            1,
            FS,
            "topic:1#2",
            FsServer::new(vec![dirs.topic_dir(2)]),
        )
        .await
        .expect("rebind the name");
    assert_eq!(
        host.last("reason"),
        "topic 1 bound fs to another connection",
        "the reason for a rebind must say clearly that it is a rebind"
    );
}

/// 建不出自带服务的根目录时，错误要说明原因
#[tokio::test]
async fn a_root_that_cannot_be_created_is_named() {
    let (_tmp, dirs, registry, _recorder) = rig_with_dirs().await;
    // 话题目录该在的位置上放一个文件，建目录必然失败
    std_fs::write(dirs.root().join("topics"), "not a directory").expect("write");

    let refused = register_builtin(&registry, &cx_in(1), 1, &roots_of(&dirs, 1)).await;
    assert!(
        matches!(refused, Err(McpError::Directory { .. })),
        "failing to create the root must be returned as a directory creation failure, got {refused:?}"
    );
    assert_eq!(
        registry.connection_count(),
        0,
        "the root was never created, so there should be no connection"
    );
}

#[test]
fn the_scan_roots_follow_the_topic() {
    let tmp = TempDir::new().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let one = AppDirs::under(tmp.path()).mod_version("alpha", 1);
    let two = AppDirs::under(tmp.path()).mod_version("beta", 3);
    std_fs::create_dir_all(one.join("skills")).expect("create dir");
    std_fs::create_dir_all(two.join("skills")).expect("create dir");

    let roots = roots_for(&dirs, 7, &[one.clone(), two.clone()]);
    let BuiltinRoots { skills, workspace } = roots;
    assert_eq!(
        skills,
        vec![one.join("skills"), two.join("skills"), dirs.public_skills()],
        "enabled mod skill directories are listed first, then the public directory"
    );
    assert_eq!(
        workspace,
        dirs.topic_dir(7),
        "the root of fs is the working directory of that topic"
    );
}

#[test]
fn a_topic_with_no_mods_still_gets_the_public_directory() {
    let tmp = TempDir::new().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let roots = roots_for(&dirs, 1, &[]);
    assert_eq!(roots.skills, vec![dirs.public_skills()]);
}

#[test]
fn the_builtin_names_are_stable() {
    assert_eq!(wind_mcp::builtin::NAMES, &["fs", "skills"]);
}

/// 带 `ModServers` 的装配，包含临时目录、应用目录、注册表、存储与事件记录
type Wiring = (
    TempDir,
    AppDirs,
    Arc<Registry>,
    Arc<ModServers>,
    Storage,
    Recorder,
);

/// 不拦任何事件的装配
async fn with_mod() -> Wiring {
    with_mod_hosted(Arc::new(NoopHost), Vec::new()).await
}

/// 按给定 host 与订阅装起来的装配
///
/// `ModServers` 与注册表共用总线和存储，与组合根一致
async fn with_mod_hosted(host: Arc<dyn ModHost>, subs: Vec<Subscription>) -> Wiring {
    let tmp = TempDir::new().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let bus = Arc::new(Dispatcher::new(
        wind_core::usecase::directory()
            .merge(wind_mcp::events::directory())
            .merge(wind_mod::events::directory()),
        Arc::new(AllEnabled),
        host,
        Limits::default(),
    ));
    bus.rebuild_index(subs);
    let recorder = Recorder::default();
    bus.attach_observer(Arc::new(recorder.clone()));
    let registry = Arc::new(Registry::new(Arc::clone(&bus)));
    let db = Db::open(&dirs.db_file()).await.expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);
    let mods = Arc::new(ModServers::new(
        Arc::clone(&registry),
        storage.clone(),
        dirs.clone(),
        bus,
    ));
    (tmp, dirs, registry, mods, storage, recorder)
}

/// 只经过形状层解析的清单
///
/// 校验保证两种连接方式互斥且必有其一，绕开校验的用例要自己构造清单
fn unchecked_manifest(body: &str) -> Manifest {
    serde_json::from_str(body).expect("parse the manifest")
}

/// 把一个 mod 放到约定的安装位置并启用到一个话题
///
/// `ModServers` 读取存储里的清单，这里使用同一个存储入口
async fn install_and_enable(
    storage: &Storage,
    dirs: &AppDirs,
    topic_id: i64,
    name: &str,
    body: &str,
) -> i64 {
    std_fs::create_dir_all(dirs.mod_version(name, 1)).expect("create dir");
    let id = storage
        .mod_install(name, body, None)
        .await
        .expect("install");
    storage.mod_enable(topic_id, id).await.expect("enable");
    id
}

#[tokio::test]
async fn a_topic_without_mods_has_only_the_hosts_servers() {
    let (_tmp, _dirs, registry, _mods, _storage, _recorder) = with_mod().await;
    let servers = registry.list_servers(&cx(), 1).await;
    let names: Vec<&str> = servers.iter().map(|server| server.name.as_str()).collect();
    assert_eq!(names, vec!["fs", "skills"]);
}

/// 能答完握手并记录自己如何启动的伪服务
///
/// 伪服务作为资产被直接启动，因此只在能执行脚本的平台上提供
#[cfg(unix)]
mod serving {
    use super::*;
    use std::collections::HashMap;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use wind_mcp::params::{ServerParams, StdioParams};

    /// 伪服务的脚本
    ///
    /// 先把工作目录与被调用的路径写进记录文件，再答握手，然后一直等待下一条
    /// 消息
    const SCRIPT: &str = r#"#!/usr/bin/env python3
import json, os, sys
with open(os.environ["WIND_TEST_RECORD"], "w") as f:
    json.dump({"cwd": os.getcwd(), "command": sys.argv[0]}, f)
for line in sys.stdin:
    try:
        msg = json.loads(line)
    except Exception:
        continue
    if msg.get("method") == "initialize":
        result = {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "search", "version": "1"}}
        print(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": result}), flush=True)
"#;

    /// 把伪服务装进一个 mod 版本的资产根，返回绝对路径
    pub fn place(dirs: &AppDirs, name: &str, version: i64) -> std::path::PathBuf {
        let path = dirs
            .mod_version(name, version)
            .join("mcp")
            .join("search")
            .join("serve");
        fs::create_dir_all(path.parent().expect("find the parent directory")).expect("create dir");
        fs::write(&path, SCRIPT).expect("write");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("execute");
        path
    }

    /// 一份按绝对路径启动伪服务的连接描述
    ///
    /// 命令不以 `./` 开头，因此它不指向任何随包分发的资产
    pub fn command(program: &Path, record: &Path) -> ServerParams {
        ServerParams::Stdio(StdioParams {
            name: "search".into(),
            description: None,
            command: program.to_string_lossy().into_owned(),
            args: Vec::new(),
            env: Some(HashMap::from([(
                "WIND_TEST_RECORD".to_string(),
                record.to_string_lossy().into_owned(),
            )])),
            cwd: None,
        })
    }
}

/// 启用 mod 之后，它声明的服务接到话题上
///
/// 命令是资产根里的相对路径，注册表按该 mod 版本的根解析成绝对路径再启动。
/// 工作目录是该 mod 的私有目录，不是宿主进程的当前工作目录
#[cfg(unix)]
#[tokio::test]
async fn enabling_a_mod_brings_its_declared_servers_to_the_topic() {
    let (_tmp, dirs, registry, mods, _storage, _recorder) = with_mod().await;
    let script = serving::place(&dirs, "demo", 1);
    let record = dirs.root().join("record.json");
    let manifest = unchecked_manifest(&format!(
        r#"{{"name":"demo","version":1,"mcpServers":
            {{"search":{{"command":"./mcp/search/serve","env":{{"WIND_TEST_RECORD":"{}"}}}}}}}}"#,
        record.display()
    ));

    mods.apply(&cx(), 1, std::slice::from_ref(&manifest))
        .await
        .expect("connect");

    let names = registry.server_names(1);
    assert!(names.contains(&"search".to_string()), "{names:?}");
    assert!(
        names.contains(&FS.to_string()),
        "builtin services still exist: {names:?}"
    );
    assert!(names.contains(&SKILLS.to_string()), "{names:?}");

    let recorded: serde_json::Value =
        serde_json::from_str(&std_fs::read_to_string(&record).expect("read the service record"))
            .expect("parse the record as json");
    assert_eq!(
        recorded["command"],
        serde_json::json!(script.to_string_lossy()),
        "a relative command is resolved against the asset root to an absolute path before starting"
    );
    assert_eq!(
        recorded["cwd"],
        serde_json::json!(dirs.mod_data("demo").to_string_lossy()),
        "the working directory is the private directory of that mod"
    );
}

/// 同一个话题下两个名字指向同一个资产时共用一条连接
///
/// 名字只是引用键。两个名字的命令与资产内容都相同，因此属于同一个身份
#[cfg(unix)]
#[tokio::test]
async fn two_names_of_one_topic_share_one_connection() {
    let (_tmp, dirs, registry, mods, _storage, _recorder) = with_mod().await;
    serving::place(&dirs, "demo", 1);
    let record = dirs.root().join("record.json");
    let manifest = unchecked_manifest(&format!(
        r#"{{"name":"demo","version":1,"mcpServers":
            {{"a":{{"command":"./mcp/search/serve","env":{{"WIND_TEST_RECORD":"{0}"}}}},
              "b":{{"command":"./mcp/search/serve","env":{{"WIND_TEST_RECORD":"{0}"}}}}}}}}"#,
        record.display()
    ));

    mods.apply(&cx(), 1, std::slice::from_ref(&manifest))
        .await
        .expect("connect");

    let names = registry.server_names(1);
    assert!(names.contains(&"a".to_string()), "{names:?}");
    assert!(names.contains(&"b".to_string()), "{names:?}");
    assert_eq!(
        registry.connection_count(),
        names.len() - 1,
        "four names occupy only three connections: two names share one, and the host's own services each occupy one: {names:?}"
    );
}

/// 由系统 PATH 查找或绝对路径启动的命令能真正接上
///
/// 身份取完整的命令行。命令行不同的命令是两条独立连接
#[cfg(unix)]
#[tokio::test]
async fn a_command_line_reaches_the_connection_layer() {
    let (_tmp, dirs, registry, _mods, _storage, _recorder) = with_mod().await;
    let first = serving::place(&dirs, "demo", 1);
    let second = serving::place(&dirs, "other", 1);
    let root = AssetRoot::new(dirs.mod_version("demo", 1));
    let one_record = dirs.root().join("record-1.json");
    let two_record = dirs.root().join("record-2.json");

    registry
        .attach(
            &cx(),
            1,
            "one",
            &serving::command(&first, &one_record),
            &root,
        )
        .await
        .expect("connect");
    registry
        .attach(
            &cx(),
            1,
            "two",
            &serving::command(&second, &two_record),
            &root,
        )
        .await
        .expect("connect");
    assert_eq!(
        registry.connection_count(),
        2,
        "different command lines mean two connections"
    );

    // 同一条命令行换个名字接上，使用已经建立的连接
    registry
        .attach(
            &cx(),
            1,
            "three",
            &serving::command(&first, &one_record),
            &root,
        )
        .await
        .expect("connect");
    assert_eq!(
        registry.connection_count(),
        2,
        "the same command line shares one connection"
    );
    assert_eq!(
        registry.server_names(1),
        vec!["one".to_string(), "three".to_string(), "two".to_string()],
        "all three names are present, two of which point at the same connection"
    );
}

/// 重算只移除不再声明的服务，自带服务不在其中
///
/// 自带服务的根随使能集合变化，`register_builtin` 会重新注册它们。先解绑再
/// 注册会多一轮停止与启动，订阅方会看到一次没有必要的 detach
#[tokio::test]
async fn a_recompute_tears_down_only_the_servers_that_are_gone() {
    let host = Arc::new(WatchingHost::new());
    let (_tmp, dirs, registry, mods, _storage, _recorder) = with_mod_hosted(
        Arc::clone(&host) as Arc<dyn ModHost>,
        vec![subscription("core/mcp.detach")],
    )
    .await;
    register_servers(&registry, &dirs, 1).await;
    registry
        .connect_builtin(
            &cx(),
            1,
            "search",
            "topic:1",
            FsServer::new(vec![dirs.topic_dir(1)]),
        )
        .await
        .expect("register");

    // 不再有启用的 mod。声明过 search 的那个 mod 已经停用
    mods.apply(&cx(), 1, &[])
        .await
        .expect("recompute the servers");

    assert_eq!(
        registry.server_names(1),
        vec![FS.to_string(), SKILLS.to_string()],
        "services that are no longer declared must be detached, while builtin services stay as they were"
    );
    assert_eq!(
        host.values("server_name"),
        vec!["search".to_string()],
        "the recompute must actually reach unbinding, and builtin services do not take part in unbinding"
    );
}

/// 越出资产根的命令要报错，不能原样放行
///
/// 原样放行会把解析基准交给宿主进程的当前工作目录，而该目录是任意的
#[tokio::test]
async fn a_relative_command_that_leaves_the_asset_root_is_refused() {
    let (_tmp, _dirs, registry, mods, _storage, _recorder) = with_mod().await;
    let manifest = unchecked_manifest(
        r#"{"name":"demo","version":1,"mcpServers":{"search":{"command":"./../serve"}}}"#,
    );

    let refused = mods.apply(&cx(), 1, std::slice::from_ref(&manifest)).await;
    match refused {
        // 兜底分支必须在这里拒绝。交给注册表的话，报的是找不到名为 .. 的资产
        Err(McpError::Servers { context, reason }) => {
            assert_eq!(
                context, "search",
                "the error must say which declaration it is about"
            );
            assert!(reason.contains("./../serve"), "{reason}");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert!(
        registry.server_names(1).is_empty(),
        "a refused declaration must not leave a binding on the topic"
    );
}

/// 两种连接方式都没给的声明在连接阶段失败，而不是 panic
///
/// 清单校验保证两种情况互斥且必有其一，绕开校验的调用方才造得出这种声明
#[tokio::test]
async fn a_declaration_with_neither_command_nor_url_fails_at_connect_instead_of_panicking() {
    let (_tmp, _dirs, registry, mods, _storage, _recorder) = with_mod().await;
    let manifest = unchecked_manifest(r#"{"name":"demo","version":1,"mcpServers":{"search":{}}}"#);

    let refused = mods.apply(&cx(), 1, std::slice::from_ref(&manifest)).await;
    assert!(
        matches!(refused, Err(McpError::Connect { .. })),
        "a fallback declaration must make the connect phase fail with a clear error, got {refused:?}"
    );
    assert!(registry.server_names(1).is_empty());
}

/// 声明的服务要经过同一条拦截链
///
/// 一个 mod 可以拒绝某个服务绑到一个话题上，被拒之后不建立连接
#[tokio::test]
async fn the_servers_of_a_topic_follow_its_enabled_mods() {
    let (_tmp, dirs, registry, mods, storage, _recorder) = with_mod().await;
    let skills = dirs.mod_version("demo", 1).join("skills");
    write_skill(&skills, "demo-skill");
    let id = install_and_enable(&storage, &dirs, 1, "demo", r#"{"name":"demo","version":1}"#).await;

    mods.attach(&cx(), 1, "demo").await.expect("connect");
    let listed = list_through_skills(&registry, &cx(), &skills)
        .await
        .expect("list the skills of the enabled mod");
    let names: Vec<&str> = listed
        .as_array()
        .expect("the skill list is an array")
        .iter()
        .filter_map(|meta| meta["name"].as_str())
        .collect();
    assert_eq!(names, vec!["demo-skill"]);

    storage.mod_disable(1, id).await.expect("disable");
    mods.detach(&cx(), 1, "demo").await.expect("detach the mod");
    let refused = list_through_skills(&registry, &cx(), &skills).await;
    assert!(
        matches!(refused, Err(McpError::Connect { .. })),
        "its skill directory is no longer visible after disabling, got {refused:?}"
    );
}

/// 发版之后话题换到新版本的清单
///
/// 启用集合没变，但清单已经更换，由它派生出来的扫描根要重算
#[tokio::test]
async fn publishing_a_new_version_moves_the_topic_to_it() {
    let (_tmp, dirs, registry, mods, storage, _recorder) = with_mod().await;
    let old = dirs.mod_version("demo", 1).join("skills");
    write_skill(&old, "old-skill");
    let id = install_and_enable(&storage, &dirs, 1, "demo", r#"{"name":"demo","version":1}"#).await;
    mods.attach(&cx(), 1, "demo").await.expect("connect");

    let new = dirs.mod_version("demo", 2).join("skills");
    write_skill(&new, "new-skill");
    storage
        .mod_publish(id, 2, r#"{"name":"demo","version":2}"#, None)
        .await
        .expect("publish");
    storage
        .mod_set_current_version(id, 2)
        .await
        .expect("switch to the new version");

    mods.refresh(&cx(), 1).await.expect("recompute the servers");

    let listed = list_through_skills(&registry, &cx(), &new)
        .await
        .expect("list the new skill dir");
    let names: Vec<&str> = listed
        .as_array()
        .expect("the skill list is an array")
        .iter()
        .filter_map(|meta| meta["name"].as_str())
        .collect();
    assert_eq!(names, vec!["new-skill"]);
    let refused = list_through_skills(&registry, &cx(), &old).await;
    assert!(
        matches!(refused, Err(McpError::Connect { .. })),
        "the old version's manifest is no longer visible, got {refused:?}"
    );
}

/// 清单读不出来时报错，不能跳过这个 mod
///
/// 跳过会让这个话题少掉一批服务而不报错
#[tokio::test]
async fn a_manifest_that_cannot_be_read_is_an_error() {
    let (_tmp, dirs, _registry, mods, storage, _recorder) = with_mod().await;
    install_and_enable(&storage, &dirs, 1, "demo", "{ this is not a manifest }").await;

    let refused = mods.attach(&cx(), 1, "demo").await;
    let reason = refused.expect_err("refuse an unreadable manifest");
    assert!(
        reason.contains("demo"),
        "the error must say which mod it is about: {reason}"
    );
}
