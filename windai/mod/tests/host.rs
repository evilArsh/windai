//! 安装、使能与数据面的端到端流程

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use wind_core::db::Db;
use wind_core::feed::frame::{Baseline, Frame};
use wind_core::feed::hub::FeedHub;
use wind_core::feed::subscribe::{Subscribe, Subscriber};
use wind_core::storage::Storage;
use wind_event::context::DispatchCx;
use wind_event::def::EventMeta;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{EventSeq, Origin, Scope, Source};
use wind_event::host::{CallCx, CallMode, HostError, ModEnablement, ModHost};
use wind_event::observer::BuiltinObserver;
use wind_event::subscription::Subscription;
use wind_mod::compiler::{CompiledScripts, Compiler, NoCompiler};
use wind_mod::dirs::AppDirs;
use wind_mod::enablement::Enablement;
use wind_mod::events;
use wind_mod::feed::{FeedError, ModFeed};
use wind_mod::installer::{InstallError, Installer, NoServers, TopicServers};
use wind_mod::manifest::{self, Manifest};
use wind_mod::state::StateSnapshot;

/// 一个建好表、落在临时目录里的空库
async fn store() -> (TempDir, Storage) {
    let dir = TempDir::new().expect("create temp dir");
    let db = Db::open(&dir.path().join("t.db"))
        .await
        .expect("open database");
    db.init_schema().await.expect("create table");
    (dir, Storage::new(db))
}

#[test]
fn the_directory_holds_six_events_and_none_of_them_can_be_denied() {
    let directory = events::directory();
    assert_eq!(directory.len(), 6);
    for name in [
        "core/mod.install",
        "core/mod.installed",
        "core/mod.update",
        "core/mod.updated",
        "core/mod.delete",
        "core/mod.deleted",
    ] {
        let meta = directory
            .get(name)
            .unwrap_or_else(|| panic!("missing event {name}"));
        assert_eq!(meta.shape, wind_event::def::Shape::Observe, "{name}");
        assert!(meta.log, "{name} must be logged");
        assert_eq!(meta.mount, wind_event::def::Mount::Subsystem, "{name}");
    }
}

#[test]
fn the_update_event_declares_the_two_versions_and_no_manifest() {
    let directory = events::directory();
    let meta = directory.get("core/mod.update").expect("find the event");
    let schema = (meta.schema)().to_string();
    assert!(
        schema.contains("from_version"),
        "the payload must carry the version before publishing"
    );
    assert!(
        schema.contains("to_version"),
        "the payload must carry the version after publishing"
    );
    assert!(
        !schema.contains("manifest"),
        "a publish event carries no manifest; the manifest is the install event payload"
    );
}

#[test]
fn the_installed_payload_carries_the_ui_entry() {
    let payload = events::ModInstalled {
        name: "demo".into(),
        version: 1,
        manifest: serde_json::json!({ "name": "demo", "version": 1 }),
        ui_entry: Some("windai://mods/demo/1/ui/index.js".into()),
    };
    let value = serde_json::to_value(&payload).expect("serialize");
    assert_eq!(value["name"], serde_json::json!("demo"));
    assert_eq!(
        value["ui_entry"],
        serde_json::json!("windai://mods/demo/1/ui/index.js")
    );
}

#[test]
fn the_updated_payload_carries_the_ui_entry() {
    let payload = events::ModUpdated {
        name: "demo".into(),
        from_version: 1,
        to_version: 2,
        ui_entry: Some("windai://mods/demo/2/ui/index.js".into()),
    };
    let value = serde_json::to_value(&payload).expect("serialize");
    assert_eq!(value["from_version"], serde_json::json!(1));
    assert_eq!(
        value["ui_entry"],
        serde_json::json!("windai://mods/demo/2/ui/index.js")
    );
}

#[test]
fn a_mod_without_a_ui_entry_omits_the_key() {
    let payload = events::ModInstalled {
        name: "demo".into(),
        version: 1,
        manifest: serde_json::json!({ "name": "demo", "version": 1 }),
        ui_entry: None,
    };
    let value = serde_json::to_value(&payload).expect("serialize");
    assert!(
        value.get("ui_entry").is_none(),
        "no empty key is left when there is no component; the frontend uses this to tell whether there is a UI"
    );
}

#[tokio::test]
async fn the_view_starts_empty_and_follows_the_configuration() {
    let (_dir, store) = store().await;
    let id = store
        .mod_install("demo", "{}", None)
        .await
        .expect("install");
    let enablement = Arc::new(Enablement::load(store.clone()).await.expect("load"));

    assert!(
        !enablement
            .enabled(&Scope::Topic { topic_id: 1 })
            .contains("demo")
    );

    store.mod_enable(1, id).await.expect("enable");
    enablement.refresh(&store).await.expect("refresh the view");
    assert!(
        enablement
            .enabled(&Scope::Topic { topic_id: 1 })
            .contains("demo")
    );
    assert!(
        !enablement
            .enabled(&Scope::Topic { topic_id: 2 })
            .contains("demo"),
        "the enable set is isolated per topic"
    );
}

#[tokio::test]
async fn the_global_scope_has_no_mods() {
    let (_dir, store) = store().await;
    let id = store
        .mod_install("demo", "{}", None)
        .await
        .expect("install");
    store.mod_enable(1, id).await.expect("enable");
    let enablement = Enablement::load(store.clone()).await.expect("load");
    assert!(
        enablement.enabled(&Scope::Global).is_empty(),
        "mods are an enable set on topics; the global scope has none"
    );
}

#[tokio::test]
async fn a_disable_is_visible_at_once() {
    let (_dir, store) = store().await;
    let id = store
        .mod_install("demo", "{}", None)
        .await
        .expect("install");
    store.mod_enable(1, id).await.expect("enable");
    let enablement = Enablement::load(store.clone()).await.expect("load");
    store.mod_disable(1, id).await.expect("disable");
    enablement.refresh(&store).await.expect("refresh the view");
    assert!(
        !enablement
            .enabled(&Scope::Topic { topic_id: 1 })
            .contains("demo")
    );
}

/// 在话题上启用一个 mod，再把使能视图刷新一次
async fn enable_then_refresh(store: Storage, view: Arc<Enablement>, topic_id: i64, mod_id: i64) {
    store.mod_enable(topic_id, mod_id).await.expect("enable");
    view.refresh(&store).await.expect("refresh the view");
}

/// 两次刷新各自写入，视图最终必须同时包含两者的结果
///
/// 刷新会读库再整体替换。两个并发的刷新若让先读的一方后写，视图就会停在较旧的
/// 快照上
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_refreshes_keep_both_results() {
    let (_dir, store) = store().await;
    let view = Arc::new(Enablement::load(store.clone()).await.expect("load"));

    for round in 1..=8i64 {
        let first = format!("mod-{round}-a");
        let second = format!("mod-{round}-b");
        let first_id = store
            .mod_install(&first, "{}", None)
            .await
            .expect("install");
        let second_id = store
            .mod_install(&second, "{}", None)
            .await
            .expect("install");

        let (a, b) = tokio::join!(
            tokio::spawn(enable_then_refresh(
                store.clone(),
                view.clone(),
                round,
                first_id
            )),
            tokio::spawn(enable_then_refresh(
                store.clone(),
                view.clone(),
                round,
                second_id
            )),
        );
        a.expect("join the task");
        b.expect("join the task");

        let enabled = view.enabled(&Scope::Topic { topic_id: round });
        assert!(
            enabled.contains(&first) && enabled.contains(&second),
            "round {round} dropped a refresh"
        );
    }
}

/// 一个不调用任何 mod 的宿主
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

/// 话题服务的一次调用
#[derive(Debug, Clone, PartialEq)]
enum ServerCall {
    /// 启用一个 mod，值为（话题标识，mod 名）
    Attach(i64, String),
    /// 停用一个 mod，值为（话题标识，mod 名）
    Detach(i64, String),
    /// 重算一个话题的派生服务集合，值为话题标识
    Refresh(i64),
}

/// 记下话题服务收到的每一次调用
#[derive(Default)]
struct RecordingServers {
    /// 按发生顺序记下每一次调用
    seen: Mutex<Vec<ServerCall>>,
}

#[async_trait::async_trait]
impl TopicServers for RecordingServers {
    async fn attach(&self, _cx: &DispatchCx, topic_id: i64, mod_name: &str) -> Result<(), String> {
        self.seen
            .lock()
            .expect("lock the record")
            .push(ServerCall::Attach(topic_id, mod_name.to_string()));
        Ok(())
    }

    async fn detach(&self, _cx: &DispatchCx, topic_id: i64, mod_name: &str) -> Result<(), String> {
        self.seen
            .lock()
            .expect("lock the record")
            .push(ServerCall::Detach(topic_id, mod_name.to_string()));
        Ok(())
    }

    async fn refresh(&self, _cx: &DispatchCx, topic_id: i64) -> Result<(), String> {
        self.seen
            .lock()
            .expect("lock the record")
            .push(ServerCall::Refresh(topic_id));
        Ok(())
    }
}

/// 把一个包放到约定的安装位置，返回它的根
fn windai_test_package(dirs: &AppDirs, name: &str, version: i64, body: &str) -> std::path::PathBuf {
    let dir = dirs.mod_version(name, version);
    std::fs::create_dir_all(&dir).expect("create dir");
    std::fs::write(dir.join("mod.json"), body).expect("write the manifest");
    dir
}

/// 一套装配好的环境
///
/// 安装器与数据面（`ModFeed`）共用同一份存储与帧设施。删除一个 mod 要清掉它的
/// 内存状态，因此两边必须是同一个实例
struct Harness {
    /// 临时目录，随它一起删掉
    tmp: TempDir,
    /// 应用目录
    dirs: AppDirs,
    /// 底层存储
    storage: Storage,
    /// mod 的唯一变更入口
    installer: Installer,
    /// 事件总线
    bus: Arc<Dispatcher>,
    /// 帧设施
    hub: Arc<FeedHub>,
    /// 数据面
    feed: Arc<ModFeed>,
}

/// 按给定的上限与编译接口装配一套环境
async fn assemble(
    servers: Arc<dyn TopicServers>,
    limits: Limits,
    compiler: Arc<dyn Compiler>,
) -> Harness {
    assemble_in(
        TempDir::new().expect("create temp dir"),
        servers,
        limits,
        compiler,
    )
    .await
}

/// 在给定的临时目录上装配一套环境
///
/// 目录由调用方持有，不在函数内创建。跨重启的用例要用同一个目录装配两次。
///
/// 装配步骤与组合根一致。先建库建表，再建总线与帧设施，把帧设施挂成观察者，用
/// 日志里的最大序号作为两者的序号起点，最后建数据面（`ModFeed`）与安装器。目录按
/// 同一份聚合列表拼出。组合根若改了步骤，这里也要改。
async fn assemble_in(
    tmp: TempDir,
    servers: Arc<dyn TopicServers>,
    limits: Limits,
    compiler: Arc<dyn Compiler>,
) -> Harness {
    let dirs = AppDirs::under(tmp.path());
    let db = Db::open(&dirs.db_file()).await.expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);
    let enablement = Arc::new(Enablement::load(storage.clone()).await.expect("load"));
    let bus = Arc::new(Dispatcher::new(
        wind_core::usecase::directory()
            .merge(wind_core::agent::events::directory())
            .merge(wind_core::conversation::directory())
            .merge(events::directory()),
        enablement.clone(),
        Arc::new(NoopHost),
        limits,
    ));
    let hub = FeedHub::open(storage.clone(), None).await.expect("open");
    bus.attach_observer(hub.clone());
    // 日志为空时序号从零起
    let seed = storage
        .event_log_max_seq()
        .await
        .expect("read the max sequence");
    if seed > 0 {
        bus.seed_seq(EventSeq(seed));
        hub.seed_high_water(EventSeq(seed));
    }
    let feed = Arc::new(
        ModFeed::open(hub.clone(), bus.clone(), storage.clone())
            .await
            .expect("open"),
    );
    let installer = Installer::new(
        storage.clone(),
        bus.clone(),
        dirs.clone(),
        servers,
        enablement,
        feed.clone(),
        compiler,
    );
    Harness {
        tmp,
        dirs,
        storage,
        installer,
        bus,
        hub,
        feed,
    }
}

/// 一套可用的装配
async fn rig(servers: Arc<RecordingServers>) -> (TempDir, AppDirs, Installer, Arc<Dispatcher>) {
    let Harness {
        tmp,
        dirs,
        installer,
        bus,
        ..
    } = assemble(servers, Limits::default(), Arc::new(NoCompiler)).await;
    (tmp, dirs, installer, bus)
}

impl Harness {
    /// mod 的唯一变更入口
    fn installer(&self) -> &Installer {
        &self.installer
    }

    /// 底层存储
    fn storage(&self) -> &Storage {
        &self.storage
    }
}

/// 以给定的编译接口装配一套环境
async fn rig_with_compiler(compiler: Arc<dyn Compiler>) -> Harness {
    assemble(
        Arc::new(RecordingServers::default()),
        Limits::default(),
        compiler,
    )
    .await
}

/// 指向话题 1 的上下文
fn cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id: 1 },
        Source::core("mod"),
        std::time::Duration::from_millis(50),
    )
}

#[tokio::test]
async fn installing_a_package_registers_it_and_announces_it() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, _bus) = rig(servers).await;
    let package = windai_test_package(&dirs, "demo", 1, r#"{"name":"demo","version":1}"#);

    let id = installer.install(&cx(), &package).await.expect("install");
    assert!(id > 0);
    let row = installer
        .get("demo")
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(row.current_version, 1);
}

#[tokio::test]
async fn installing_a_name_twice_is_refused() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, _bus) = rig(servers).await;
    let package = windai_test_package(&dirs, "demo", 1, r#"{"name":"demo","version":1}"#);
    installer.install(&cx(), &package).await.expect("install");

    let error = installer
        .install(&cx(), &package)
        .await
        .expect_err("install the same name twice");
    assert!(
        matches!(error, InstallError::AlreadyInstalled { .. }),
        "{error}"
    );
}

#[tokio::test]
async fn a_package_whose_name_disagrees_with_its_path_is_refused() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, _bus) = rig(servers).await;
    let package = windai_test_package(&dirs, "demo", 1, r#"{"name":"other","version":1}"#);
    let error = installer
        .install(&cx(), &package)
        .await
        .expect_err("install package at wrong path");
    assert!(matches!(error, InstallError::Mismatch { .. }), "{error}");
}

#[tokio::test]
async fn updating_publishes_a_new_version_and_keeps_the_old_one() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, _bus) = rig(servers).await;
    installer
        .install(
            &cx(),
            &windai_test_package(&dirs, "demo", 1, r#"{"name":"demo","version":1}"#),
        )
        .await
        .expect("install");
    installer
        .update(
            &cx(),
            &windai_test_package(&dirs, "demo", 2, r#"{"name":"demo","version":2}"#),
        )
        .await
        .expect("publish");

    assert_eq!(
        installer
            .get("demo")
            .await
            .expect("query")
            .expect("must exist")
            .current_version,
        2
    );
    assert_eq!(installer.versions("demo").await.expect("query").len(), 2);
}

#[tokio::test]
async fn publishing_a_version_that_does_not_advance_is_refused() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, _bus) = rig(servers).await;
    installer
        .install(
            &cx(),
            &windai_test_package(&dirs, "demo", 1, r#"{"name":"demo","version":1}"#),
        )
        .await
        .expect("install");
    installer
        .update(
            &cx(),
            &windai_test_package(&dirs, "demo", 3, r#"{"name":"demo","version":3}"#),
        )
        .await
        .expect("publish");

    for version in [3, 2] {
        let package = windai_test_package(
            &dirs,
            "demo",
            version,
            &format!(r#"{{"name":"demo","version":{version}}}"#),
        );
        let error = installer
            .update(&cx(), &package)
            .await
            .expect_err("publish a non-advancing version");
        assert!(matches!(error, InstallError::Mismatch { .. }), "{error}");
    }
    assert_eq!(
        installer
            .get("demo")
            .await
            .expect("query")
            .expect("must exist")
            .current_version,
        3,
        "a refused publish must not change the current version"
    );
    assert_eq!(
        installer.versions("demo").await.expect("query").len(),
        2,
        "a refused publish must not leave a version row"
    );
}

#[tokio::test]
async fn updating_a_name_that_is_not_installed_is_refused() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, _bus) = rig(servers).await;
    let package = windai_test_package(&dirs, "demo", 2, r#"{"name":"demo","version":2}"#);
    let error = installer
        .update(&cx(), &package)
        .await
        .expect_err("publish an uninstalled mod");
    assert!(
        matches!(error, InstallError::NotInstalled { .. }),
        "{error}"
    );
}

#[tokio::test]
async fn deleting_removes_the_rows_but_keeps_the_private_directory() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, _bus) = rig(servers).await;
    installer
        .install(
            &cx(),
            &windai_test_package(&dirs, "demo", 1, r#"{"name":"demo","version":1}"#),
        )
        .await
        .expect("install");
    std::fs::create_dir_all(dirs.mod_data("demo")).expect("create dir");
    std::fs::write(dirs.mod_data("demo").join("keep"), b"x").expect("write");

    installer.delete(&cx(), "demo").await.expect("delete");
    assert!(installer.get("demo").await.expect("query").is_none());
    assert!(
        dirs.mod_data("demo").join("keep").is_file(),
        "the private directory is shared across versions and is not deleted automatically on uninstall"
    );
}

#[tokio::test]
async fn enabling_attaches_the_topics_servers_and_disabling_detaches_them() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, _bus) = rig(servers.clone()).await;
    installer
        .install(
            &cx(),
            &windai_test_package(&dirs, "demo", 1, r#"{"name":"demo","version":1}"#),
        )
        .await
        .expect("install");

    installer.enable(&cx(), 1, "demo").await.expect("enable");
    installer.disable(&cx(), 1, "demo").await.expect("disable");

    let seen = servers.seen.lock().expect("lock the record").clone();
    assert_eq!(
        seen,
        vec![
            ServerCall::Attach(1, "demo".to_string()),
            ServerCall::Detach(1, "demo".to_string())
        ]
    );
}

#[tokio::test]
async fn publishing_a_version_refreshes_every_topic_the_mod_is_enabled_on() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, _bus) = rig(servers.clone()).await;
    installer
        .install(
            &cx(),
            &windai_test_package(&dirs, "demo", 1, r#"{"name":"demo","version":1}"#),
        )
        .await
        .expect("install");
    installer.enable(&cx(), 1, "demo").await.expect("enable");
    installer.enable(&cx(), 2, "demo").await.expect("enable");
    assert_eq!(
        servers.seen.lock().expect("lock the record").clone(),
        vec![
            ServerCall::Attach(1, "demo".to_string()),
            ServerCall::Attach(2, "demo".to_string())
        ],
        "enabling only attaches the servers and does not recompute"
    );

    installer
        .update(
            &cx(),
            &windai_test_package(&dirs, "demo", 2, r#"{"name":"demo","version":2}"#),
        )
        .await
        .expect("publish");
    assert_eq!(
        servers.seen.lock().expect("lock the record").clone(),
        vec![
            ServerCall::Attach(1, "demo".to_string()),
            ServerCall::Attach(2, "demo".to_string()),
            ServerCall::Refresh(1),
            ServerCall::Refresh(2),
        ],
        "publishing replaces the manifest, so every topic that enabled it must be recomputed"
    );
}

#[tokio::test]
async fn enabling_an_unknown_mod_is_refused() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, _dirs, installer, _bus) = rig(servers).await;
    let error = installer
        .enable(&cx(), 1, "absent")
        .await
        .expect_err("must be rejected");
    assert!(
        matches!(error, InstallError::NotInstalled { .. }),
        "{error}"
    );
}

/// 记下生命周期事件的载荷，供断言事件里宣告了什么
#[derive(Default)]
struct RecordingObserver {
    /// 按发生顺序记下（事件名，载荷）
    seen: Mutex<Vec<(&'static str, serde_json::Value)>>,
}

#[async_trait::async_trait]
impl BuiltinObserver for RecordingObserver {
    async fn on_event(&self, meta: &EventMeta, event: &serde_json::Value) {
        self.seen
            .lock()
            .expect("lock the record")
            .push((meta.name, event.clone()));
    }
}

impl RecordingObserver {
    /// 取出某个事件各条记录宣告的组件入口地址
    fn announced_ui_entries(&self, event: &str) -> Vec<Option<String>> {
        self.seen
            .lock()
            .expect("lock the record")
            .iter()
            .filter(|(name, _)| *name == event)
            .map(|(_, event)| {
                event["payload"]
                    .get("ui_entry")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            })
            .collect()
    }
}

#[tokio::test]
async fn installing_a_package_that_does_not_start_at_version_one_is_refused() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, _bus) = rig(servers).await;
    let package = windai_test_package(&dirs, "demo", 2, r#"{"name":"demo","version":2}"#);

    let error = installer
        .install(&cx(), &package)
        .await
        .expect_err("install at a later version");
    assert!(matches!(error, InstallError::Mismatch { .. }), "{error}");
}

#[tokio::test]
async fn deleting_a_mod_detaches_it_from_every_topic_it_was_enabled_on() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, _bus) = rig(servers.clone()).await;
    installer
        .install(
            &cx(),
            &windai_test_package(&dirs, "demo", 1, r#"{"name":"demo","version":1}"#),
        )
        .await
        .expect("install");
    installer.enable(&cx(), 1, "demo").await.expect("enable");
    installer.enable(&cx(), 2, "demo").await.expect("enable");

    installer.delete(&cx(), "demo").await.expect("delete");

    let seen = servers.seen.lock().expect("lock the record").clone();
    assert_eq!(
        seen,
        vec![
            ServerCall::Attach(1, "demo".to_string()),
            ServerCall::Attach(2, "demo".to_string()),
            ServerCall::Detach(1, "demo".to_string()),
            ServerCall::Detach(2, "demo".to_string()),
        ],
        "deletion must detach the mod from every topic it was enabled on"
    );
}

#[tokio::test]
async fn the_ui_entry_is_announced_only_when_the_component_exists() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, bus) = rig(servers).await;
    let observer = Arc::new(RecordingObserver::default());
    bus.attach_observer(observer.clone());

    let with_ui = windai_test_package(&dirs, "demo", 1, r#"{"name":"demo","version":1}"#);
    std::fs::create_dir_all(with_ui.join("ui")).expect("create dir");
    std::fs::write(with_ui.join("ui").join("index.js"), b"export default {}").expect("write");
    installer.install(&cx(), &with_ui).await.expect("install");
    installer
        .install(
            &cx(),
            &windai_test_package(&dirs, "bare", 1, r#"{"name":"bare","version":1}"#),
        )
        .await
        .expect("install");

    assert_eq!(
        observer.announced_ui_entries("core/mod.installed"),
        vec![Some("windai://mods/demo/1/ui/index.js".to_string()), None,],
        "the address is announced only when the component exists"
    );
}

#[tokio::test]
async fn a_published_version_announces_the_ui_entry_too() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, bus) = rig(servers).await;
    let observer = Arc::new(RecordingObserver::default());
    bus.attach_observer(observer.clone());

    let first = windai_test_package(&dirs, "demo", 1, r#"{"name":"demo","version":1}"#);
    installer.install(&cx(), &first).await.expect("install");
    let second = windai_test_package(&dirs, "demo", 2, r#"{"name":"demo","version":2}"#);
    std::fs::create_dir_all(second.join("ui")).expect("create dir");
    std::fs::write(second.join("ui").join("index.js"), b"export default {}").expect("write");
    installer.update(&cx(), &second).await.expect("publish");

    assert_eq!(
        observer.announced_ui_entries("core/mod.updated"),
        vec![Some("windai://mods/demo/2/ui/index.js".to_string())],
        "the frontend also needs the component address on the update path"
    );
}

// 数据面投递

/// 一条状态投递通道的清单
const STATEFUL: &str = r#"{"name":"demo","version":1,
    "capabilities":["state"],
    "events":{"emit":{"notify":{"delivery":"stateful","stateKey":"items","payload":{"type":"array"}}}}}"#;

/// 一条提醒投递通道的清单
const NOTICE: &str = r#"{"name":"demo","version":1,
    "events":{"emit":{"ping":{"delivery":"notice","payload":{"type":"array"}}}}}"#;

/// 一条订阅请求，只收实时事件
fn subscribe(topic_id: i64) -> Subscribe {
    Subscribe {
        scope: Scope::Topic { topic_id },
        since_seq: None,
        until_seq: None,
        blocks: false,
    }
}

/// 读出包的清单原文，供解析
fn package_manifest(path: &std::path::Path) -> String {
    std::fs::read_to_string(path.join("mod.json")).expect("read the manifest")
}

/// 按清单装好一个包，并把它的清单解析出来
async fn install_and_parse(harness: &Harness, body: &str) -> Manifest {
    let package = windai_test_package(&harness.dirs, "demo", 1, body);
    harness
        .installer
        .install(&cx(), &package)
        .await
        .expect("install");
    manifest::parse(&package_manifest(&package), harness.bus.directory())
        .expect("the manifest is valid")
}

/// 已安装的 demo 的标识
async fn demo_id(harness: &Harness) -> i64 {
    harness
        .installer
        .get("demo")
        .await
        .expect("query")
        .expect("exists")
        .id
}

/// 取下一帧，等待超过上限即判失败
///
/// 上限把本该有帧却没有的情况变成一次断言失败，测试因此不会一直等下去
async fn next_frame(subscriber: &mut Subscriber) -> Frame {
    tokio::time::timeout(Duration::from_secs(5), subscriber.next())
        .await
        .expect("a frame arrives")
        .expect("a frame arrives")
}

/// 在短窗口内取一帧，用于断言此刻不该有帧
async fn frame_within(subscriber: &mut Subscriber) -> Option<Frame> {
    tokio::time::timeout(Duration::from_millis(200), subscriber.next())
        .await
        .ok()
        .flatten()
}

#[tokio::test]
async fn a_stateful_channel_writes_the_value_and_pushes_a_frame() {
    let servers = Arc::new(RecordingServers::default());
    let harness = assemble(servers, Limits::default(), Arc::new(NoCompiler)).await;
    let manifest = install_and_parse(&harness, STATEFUL).await;

    let opening = harness
        .hub
        .subscribe(subscribe(1))
        .await
        .expect("subscribe");
    let mut subscriber = opening.subscriber;

    harness
        .feed
        .emit(&cx(), 1, &manifest, "notify", serde_json::json!([1, 2]))
        .await
        .expect("deliver");

    match next_frame(&mut subscriber).await {
        Frame::Event { envelope } => {
            assert_eq!(envelope["name"], serde_json::json!("m_demo/notify"));
            assert_eq!(envelope["payload"], serde_json::json!([1, 2]));
        }
        other => panic!("expected an event frame, got {other:?}"),
    }
    let stored = harness
        .storage
        .mod_get_state(
            demo_id(&harness).await,
            "topic",
            1,
            "state",
            "m_demo/notify",
        )
        .await
        .expect("query")
        .expect("the state is written");
    assert_eq!(stored.value, serde_json::json!([1, 2]));
}

#[tokio::test]
async fn a_new_subscriber_gets_the_current_value_as_a_baseline() {
    let servers = Arc::new(RecordingServers::default());
    let harness = assemble(servers, Limits::default(), Arc::new(NoCompiler)).await;
    let manifest = install_and_parse(&harness, STATEFUL).await;
    harness
        .installer
        .enable(&cx(), 1, "demo")
        .await
        .expect("enable");
    harness
        .feed
        .emit(&cx(), 1, &manifest, "notify", serde_json::json!([7]))
        .await
        .expect("deliver");

    harness.hub.set_baselines(harness.feed.clone());
    let opening = harness
        .hub
        .subscribe(subscribe(1))
        .await
        .expect("subscribe");
    let mut subscriber = opening.subscriber;

    match next_frame(&mut subscriber).await {
        Frame::Snapshot {
            baseline: Baseline::Stateful { key, value },
        } => {
            assert_eq!(key, "m_demo/notify");
            assert_eq!(value, serde_json::json!([7]));
        }
        other => panic!("expected a state baseline, got {other:?}"),
    }
}

#[tokio::test]
async fn a_notice_channel_pushes_without_touching_the_store() {
    let servers = Arc::new(RecordingServers::default());
    let harness = assemble(servers, Limits::default(), Arc::new(NoCompiler)).await;
    let manifest = install_and_parse(&harness, NOTICE).await;

    harness
        .feed
        .emit(&cx(), 1, &manifest, "ping", serde_json::json!([1]))
        .await
        .expect("deliver");
    assert!(
        harness
            .storage
            .mod_list_state(demo_id(&harness).await)
            .await
            .expect("query")
            .is_empty(),
        "only the increment is pushed; nothing is written to the database"
    );
}

#[tokio::test]
async fn emitting_a_channel_that_was_not_declared_is_refused() {
    let servers = Arc::new(RecordingServers::default());
    let harness = assemble(servers, Limits::default(), Arc::new(NoCompiler)).await;
    let manifest = install_and_parse(&harness, r#"{"name":"demo","version":1}"#).await;

    let error = harness
        .feed
        .emit(&cx(), 1, &manifest, "absent", serde_json::json!([1]))
        .await
        .expect_err("emit an undeclared channel");
    assert!(
        matches!(error, FeedError::Undeclared { .. }),
        "an undeclared channel must not be delivered, actual {error}"
    );
}

#[tokio::test]
async fn a_payload_that_does_not_match_its_declared_shape_is_refused() {
    let servers = Arc::new(RecordingServers::default());
    let harness = assemble(servers, Limits::default(), Arc::new(NoCompiler)).await;
    let manifest = install_and_parse(&harness, STATEFUL).await;

    let error = harness
        .feed
        .emit(&cx(), 1, &manifest, "notify", serde_json::json!({ "n": 1 }))
        .await
        .expect_err("accept a mismatched payload");
    assert!(
        matches!(error, FeedError::Payload { .. }),
        "a payload that does not match its declared shape must be refused, actual {error}"
    );
}

#[tokio::test]
async fn a_mod_over_its_emit_rate_keeps_the_value_but_loses_the_frame() {
    let servers = Arc::new(RecordingServers::default());
    let harness = assemble(
        servers,
        Limits {
            per_window: 1,
            window: Duration::from_millis(300),
            ..Limits::default()
        },
        Arc::new(NoCompiler),
    )
    .await;
    let manifest = install_and_parse(&harness, STATEFUL).await;

    let opening = harness
        .hub
        .subscribe(subscribe(1))
        .await
        .expect("subscribe");
    let mut subscriber = opening.subscriber;

    harness
        .feed
        .emit(&cx(), 1, &manifest, "notify", serde_json::json!([1]))
        .await
        .expect("deliver");
    // 清掉此前积累的日志记录，包括安装的两条与放行的一条
    drop(harness.hub.take_pending());
    harness
        .feed
        .emit(&cx(), 1, &manifest, "notify", serde_json::json!([2]))
        .await
        .expect("deliver");

    assert!(
        harness.hub.take_pending().is_empty(),
        "the over-limit entry does not enter the event log"
    );

    let first_seq = match next_frame(&mut subscriber).await {
        Frame::Event { envelope } => {
            assert_eq!(
                envelope["payload"],
                serde_json::json!([1]),
                "the over-limit entry does not enter the frame stream"
            );
            envelope["seq"].as_u64().expect("read the frame sequence")
        }
        other => panic!("expected an event frame, got {other:?}"),
    };
    assert!(
        frame_within(&mut subscriber).await.is_none(),
        "the over-limit entry is not pushed as a frame"
    );

    let stored = harness
        .storage
        .mod_get_state(
            demo_id(&harness).await,
            "topic",
            1,
            "state",
            "m_demo/notify",
        )
        .await
        .expect("query")
        .expect("the state is written");
    assert_eq!(
        stored.value,
        serde_json::json!([2]),
        "over-limit only drops the frame; the final value is still written to the database"
    );

    // 换一个窗口再产生一条事件。它的序号紧接在放行事件之后，说明超限的事件没有
    // 取到编号
    tokio::time::sleep(Duration::from_millis(350)).await;
    harness
        .feed
        .emit(&cx(), 1, &manifest, "notify", serde_json::json!([3]))
        .await
        .expect("deliver");
    match next_frame(&mut subscriber).await {
        Frame::Event { envelope } => assert_eq!(
            envelope["seq"].as_u64().expect("read the frame sequence"),
            first_seq + 1,
            "the over-limit entry consumed no sequence number"
        ),
        other => panic!("expected an event frame, got {other:?}"),
    }
}

#[tokio::test]
async fn a_stateful_channel_merges_in_the_queue_instead_of_disconnecting() {
    let servers = Arc::new(RecordingServers::default());
    let harness = assemble(
        servers,
        Limits {
            per_window: 1000,
            ..Limits::default()
        },
        Arc::new(NoCompiler),
    )
    .await;
    let manifest = install_and_parse(&harness, STATEFUL).await;

    let opening = harness
        .hub
        .subscribe(subscribe(1))
        .await
        .expect("subscribe");
    let mut subscriber = opening.subscriber;

    // 队列上限 256，先投满再多投一帧
    for index in 0..257 {
        harness
            .feed
            .emit(&cx(), 1, &manifest, "notify", serde_json::json!([index]))
            .await
            .expect("deliver");
    }

    match next_frame(&mut subscriber).await {
        Frame::Event { envelope } => assert_eq!(
            envelope["payload"],
            serde_json::json!([1]),
            "stateful overflow drops the oldest; the earliest frame gives way"
        ),
        other => panic!("expected an event frame, got {other:?}"),
    }
}

#[tokio::test]
async fn deleting_a_mod_clears_its_values_from_the_baseline() {
    let servers = Arc::new(RecordingServers::default());
    let harness = assemble(servers, Limits::default(), Arc::new(NoCompiler)).await;
    let manifest = install_and_parse(&harness, STATEFUL).await;
    let scope = Scope::Topic { topic_id: 1 };

    harness
        .feed
        .emit(&cx(), 1, &manifest, "notify", serde_json::json!([5]))
        .await
        .expect("deliver");
    assert_eq!(
        harness.feed.stateful_baselines(&scope),
        vec![("m_demo/notify".to_string(), serde_json::json!([5]))],
        "after delivery it is in the baseline"
    );

    harness
        .installer
        .delete(&cx(), "demo")
        .await
        .expect("delete");
    assert!(
        harness.feed.stateful_baselines(&scope).is_empty(),
        "after deletion it is no longer in the baseline"
    );
}

#[tokio::test]
async fn a_restart_hands_out_the_values_it_finds_in_the_store() {
    // 重启后内存是空的，基线只能来自库里已有的行
    let servers = Arc::new(RecordingServers::default());
    let harness = assemble(servers, Limits::default(), Arc::new(NoCompiler)).await;
    let manifest = install_and_parse(&harness, STATEFUL).await;
    harness
        .feed
        .emit(&cx(), 1, &manifest, "notify", serde_json::json!([3]))
        .await
        .expect("deliver");

    let restarted = ModFeed::open(
        harness.hub.clone(),
        harness.bus.clone(),
        harness.storage.clone(),
    )
    .await
    .expect("open");
    assert_eq!(
        restarted.stateful_baselines(&Scope::Topic { topic_id: 1 }),
        vec![("m_demo/notify".to_string(), serde_json::json!([3]))]
    );
}

#[tokio::test]
async fn a_restart_leaves_out_the_keys_that_no_channel_declares() {
    // 调用边界写入的键不属于任何通道，重启后不得当作基线交出去
    let servers = Arc::new(RecordingServers::default());
    let harness = assemble(servers, Limits::default(), Arc::new(NoCompiler)).await;
    let manifest = install_and_parse(&harness, STATEFUL).await;
    harness
        .storage
        .mod_put_state(
            demo_id(&harness).await,
            "topic",
            1,
            "state",
            "items",
            serde_json::json!([9]),
        )
        .await
        .expect("write");
    harness
        .feed
        .emit(&cx(), 1, &manifest, "notify", serde_json::json!([3]))
        .await
        .expect("deliver");

    let restarted = ModFeed::open(
        harness.hub.clone(),
        harness.bus.clone(),
        harness.storage.clone(),
    )
    .await
    .expect("open");
    assert_eq!(
        restarted.stateful_baselines(&Scope::Topic { topic_id: 1 }),
        vec![("m_demo/notify".to_string(), serde_json::json!([3]))],
        "only state channels declared in the manifest are baselines"
    );
}

#[tokio::test]
async fn the_snapshot_reads_once_and_writes_on_flush() {
    let (_dir, store) = store().await;
    let id = store
        .mod_install("demo", "{}", None)
        .await
        .expect("install");
    store
        .mod_put_state(id, "topic", 1, "state", "items", serde_json::json!([1]))
        .await
        .expect("write");

    let mut snapshot = StateSnapshot::open(store.clone(), id, Scope::Topic { topic_id: 1 })
        .await
        .expect("open");
    assert_eq!(snapshot.state("items"), Some(&serde_json::json!([1])));
    assert_eq!(snapshot.state("absent"), None);

    snapshot.put_state("items", serde_json::json!([1, 2]));
    snapshot.put_storage("cursor", serde_json::json!(5));
    assert_eq!(
        snapshot.state("items"),
        Some(&serde_json::json!([1, 2])),
        "changes are immediately visible to this call"
    );

    let flushed = snapshot.flush().await.expect("write the state");
    assert_eq!(
        flushed.changed_state,
        vec![("items".to_string(), serde_json::json!([1, 2]))],
        "only external state is broadcast"
    );
    assert_eq!(
        store
            .mod_get_state(id, "topic", 1, "state", "items")
            .await
            .expect("query")
            .expect("exists")
            .value,
        serde_json::json!([1, 2])
    );
    assert_eq!(
        store
            .mod_get_state(id, "topic", 1, "storage", "cursor")
            .await
            .expect("query")
            .expect("exists")
            .value,
        serde_json::json!(5)
    );
}

#[tokio::test]
async fn an_untouched_key_is_not_written_back() {
    let (_dir, store) = store().await;
    let id = store
        .mod_install("demo", "{}", None)
        .await
        .expect("install");
    store
        .mod_put_state(id, "global", 0, "storage", "cursor", serde_json::json!(1))
        .await
        .expect("write");

    let before = store.statement_count();
    let snapshot = StateSnapshot::open(store.clone(), id, Scope::Global)
        .await
        .expect("open");
    assert_eq!(
        store.statement_count(),
        before + 1,
        "opening reads the database once and does not query per key"
    );

    let flushed = snapshot.flush().await.expect("write the state");
    assert!(flushed.changed_state.is_empty());
    assert!(
        flushed.changed_storage.is_empty(),
        "untouched keys are not rewritten"
    );
    assert_eq!(
        store.statement_count(),
        before + 1,
        "untouched keys issue not a single statement"
    );
}

#[tokio::test]
async fn a_flush_batches_upserts_and_per_kind_deletes() {
    let (_dir, store) = store().await;
    let id = store
        .mod_install("demo", "{}", None)
        .await
        .expect("install");
    store
        .mod_put_state(id, "topic", 1, "state", "gone", serde_json::json!(1))
        .await
        .expect("write");
    store
        .mod_put_state(id, "topic", 1, "storage", "cursor", serde_json::json!(1))
        .await
        .expect("write");

    let mut snapshot = StateSnapshot::open(store.clone(), id, Scope::Topic { topic_id: 1 })
        .await
        .expect("open");
    snapshot.put_state("a", serde_json::json!(1));
    snapshot.put_state("b", serde_json::json!(2));
    snapshot.put_storage("c", serde_json::json!(3));
    snapshot.remove_state("gone");
    snapshot.remove_storage("cursor");

    let before = store.statement_count();
    let flushed = snapshot.flush().await.expect("write the state");
    assert_eq!(
        store.statement_count(),
        before + 3,
        "one multi-row upsert plus two kind-grouped deletes"
    );
    assert_eq!(
        flushed.changed_state.len(),
        3,
        "a and b written, gone deleted"
    );
    assert_eq!(
        flushed.changed_storage.len(),
        2,
        "c written, cursor deleted"
    );

    for key in ["a", "b"] {
        assert!(
            store
                .mod_get_state(id, "topic", 1, "state", key)
                .await
                .expect("query")
                .is_some()
        );
    }
    for (kind, key) in [("state", "gone"), ("storage", "cursor")] {
        assert!(
            store
                .mod_get_state(id, "topic", 1, kind, key)
                .await
                .expect("query")
                .is_none()
        );
    }
    assert!(
        store
            .mod_get_state(id, "topic", 1, "storage", "c")
            .await
            .expect("query")
            .is_some()
    );
}

#[tokio::test]
async fn the_two_scopes_do_not_see_each_other() {
    let (_dir, store) = store().await;
    let id = store
        .mod_install("demo", "{}", None)
        .await
        .expect("install");
    store
        .mod_put_state(id, "topic", 1, "state", "items", serde_json::json!("one"))
        .await
        .expect("write");

    let snapshot = StateSnapshot::open(store.clone(), id, Scope::Topic { topic_id: 2 })
        .await
        .expect("open");
    assert_eq!(
        snapshot.state("items"),
        None,
        "different isolation keys cannot see each other"
    );
}

#[tokio::test]
async fn a_key_with_no_declared_channel_is_still_persisted() {
    let servers = Arc::new(RecordingServers::default());
    let (_tmp, dirs, installer, _bus) = rig(servers).await;
    let package = windai_test_package(
        &dirs,
        "demo",
        1,
        r#"{"name":"demo","version":1,
            "capabilities":["state"],
            "events":{"emit":{"notify":{"delivery":"stateful","stateKey":"items",
                                        "payload":{"type":"string"}}}}}"#,
    );
    installer.install(&cx(), &package).await.expect("install");

    let db = Db::open(&dirs.db_file()).await.expect("open database");
    let storage = Storage::new(db);
    let id = installer
        .get("demo")
        .await
        .expect("query")
        .expect("exists")
        .id;
    let mut snapshot = StateSnapshot::open(storage.clone(), id, Scope::Topic { topic_id: 1 })
        .await
        .expect("open");
    snapshot.put_state("private.note", serde_json::json!("x"));
    let flushed = snapshot.flush().await.expect("write the state");
    assert_eq!(
        flushed.changed_state,
        vec![("private.note".to_string(), serde_json::json!("x"))]
    );
    assert!(
        storage
            .mod_get_state(id, "topic", 1, "state", "private.note")
            .await
            .expect("query")
            .is_some(),
        "keys with no channel declared are still written to the database as usual"
    );
}

#[tokio::test]
async fn a_removed_key_is_deleted_on_flush() {
    let (_dir, store) = store().await;
    let id = store
        .mod_install("demo", "{}", None)
        .await
        .expect("install");
    store
        .mod_put_state(id, "global", 0, "state", "items", serde_json::json!(1))
        .await
        .expect("write");

    let mut snapshot = StateSnapshot::open(store.clone(), id, Scope::Global)
        .await
        .expect("open");
    snapshot.remove_state("items");
    let flushed = snapshot.flush().await.expect("write the state");
    assert_eq!(
        flushed.changed_state,
        vec![("items".to_string(), serde_json::Value::Null)]
    );
    assert!(
        store
            .mod_get_state(id, "global", 0, "state", "items")
            .await
            .expect("query")
            .is_none(),
        "deletion must reach the database"
    );
}

#[tokio::test]
async fn the_assembly_wires_the_frame_facility_and_carries_the_sequence_across_restarts() {
    let first = assemble_in(
        TempDir::new().expect("create temp dir"),
        Arc::new(NoServers),
        Limits::default(),
        Arc::new(NoCompiler),
    )
    .await;
    first
        .bus
        .decide(
            &wind_core::usecase::topic::CORE_TOPIC_CREATE,
            &cx(),
            wind_core::models::TopicCreate {
                label: "work".into(),
                ..Default::default()
            },
        )
        .await;
    first.hub.flush().await.expect("write the state");
    let high = first.hub.high_water();
    assert!(high.0 > 0);
    // 目录要留给下一次装配，其余部件随 first 一起释放
    let Harness { tmp, .. } = first;

    let again = assemble_in(
        tmp,
        Arc::new(NoServers),
        Limits::default(),
        Arc::new(NoCompiler),
    )
    .await;
    assert_eq!(
        again.bus.current_seq(),
        high,
        "after a restart the sequence continues from the numbers already in the database"
    );
}

#[tokio::test]
async fn a_resumed_subscription_reads_the_history_that_survived_a_restart() {
    let first = assemble_in(
        TempDir::new().expect("create temp dir"),
        Arc::new(NoServers),
        Limits::default(),
        Arc::new(NoCompiler),
    )
    .await;
    for version in 1..=3 {
        first
            .bus
            .notify(
                &events::CORE_MOD_DELETED,
                &cx(),
                events::ModDeleted {
                    name: "demo".into(),
                    version,
                },
            )
            .await;
    }
    first.hub.flush().await.expect("write the state");
    // 目录要留给下一次装配，其余部件随 first 一起释放
    let Harness { tmp, .. } = first;

    let again = assemble_in(
        tmp,
        Arc::new(NoServers),
        Limits::default(),
        Arc::new(NoCompiler),
    )
    .await;
    let mut subscriber = again
        .hub
        .subscribe(Subscribe {
            scope: Scope::Topic { topic_id: 1 },
            since_seq: Some(EventSeq(0)),
            until_seq: None,
            blocks: false,
        })
        .await
        .expect("subscribe")
        .subscriber;

    let mut history = Vec::new();
    for _ in 0..3 {
        match next_frame(&mut subscriber).await {
            Frame::Event { envelope } => history.push((
                envelope["seq"].as_u64().expect("read the sequence number"),
                envelope["payload"]["version"]
                    .as_i64()
                    .expect("read the payload version"),
            )),
            other => panic!("expected a history frame, got {other:?}"),
        }
    }
    assert_eq!(
        history,
        vec![(1, 1), (2, 2), (3, 3)],
        "after a restart the backfill must reconnect to the log"
    );
    assert!(
        frame_within(&mut subscriber).await.is_none(),
        "no other frames should be backfilled after the backfill segment"
    );
}

#[tokio::test]
async fn deleting_a_mod_takes_its_value_out_of_the_baseline_the_assembly_serves() {
    let harness = assemble(Arc::new(NoServers), Limits::default(), Arc::new(NoCompiler)).await;
    let package = windai_test_package(&harness.dirs, "demo", 1, STATEFUL);
    harness
        .installer
        .install(&cx(), &package)
        .await
        .expect("install");
    harness
        .installer
        .enable(&cx(), 1, "demo")
        .await
        .expect("enable");
    let manifest = manifest::parse(&package_manifest(&package), harness.bus.directory())
        .expect("the manifest is valid");
    harness
        .feed
        .emit(&cx(), 1, &manifest, "notify", serde_json::json!([7]))
        .await
        .expect("deliver");

    // 数据面（`ModFeed`）要登记成帧设施的基线提供者，订阅方才能收到当前值
    harness.hub.set_baselines(harness.feed.clone());
    let mut subscriber = harness
        .hub
        .subscribe(subscribe(1))
        .await
        .expect("subscribe")
        .subscriber;
    match next_frame(&mut subscriber).await {
        Frame::Snapshot {
            baseline: Baseline::Stateful { key, value },
        } => {
            assert_eq!(key, "m_demo/notify");
            assert_eq!(value, serde_json::json!([7]));
        }
        other => panic!("expected a state baseline, got {other:?}"),
    }

    harness
        .installer
        .delete(&cx(), "demo")
        .await
        .expect("delete");
    let mut subscriber = harness
        .hub
        .subscribe(subscribe(1))
        .await
        .expect("subscribe")
        .subscriber;
    assert!(
        frame_within(&mut subscriber).await.is_none(),
        "after deletion it should no longer be in the baseline"
    );
}

#[tokio::test]
async fn a_subscription_that_starts_after_the_state_exists_gets_the_baseline_then_the_increments() {
    // 状态通道在订阅建立之前已经产生过一帧。此后订阅方先收基线，再收增量
    let harness = assemble(Arc::new(NoServers), Limits::default(), Arc::new(NoCompiler)).await;
    let package = windai_test_package(
        &harness.dirs,
        "demo",
        1,
        r#"{"name":"demo","version":1,
            "capabilities":["state","http"],
            "events":{"emit":{"items":{"delivery":"stateful","stateKey":"items",
                                       "payload":{"type":"array"}}}}}"#,
    );
    harness
        .installer
        .install(&cx(), &package)
        .await
        .expect("install");
    harness
        .installer
        .enable(&cx(), 1, "demo")
        .await
        .expect("enable");
    let manifest = manifest::parse(&package_manifest(&package), harness.bus.directory())
        .expect("the manifest is valid");
    harness
        .feed
        .emit(&cx(), 1, &manifest, "items", serde_json::json!([1]))
        .await
        .expect("deliver");

    // 数据面（`ModFeed`）要登记成帧设施的基线提供者，订阅方才能收到此前积累的当前值
    harness.hub.set_baselines(harness.feed.clone());
    let mut subscriber = harness
        .hub
        .subscribe(subscribe(1))
        .await
        .expect("subscribe")
        .subscriber;
    match next_frame(&mut subscriber).await {
        Frame::Snapshot {
            baseline: Baseline::Stateful { key, value },
        } => {
            assert_eq!(key, "m_demo/items");
            assert_eq!(value, serde_json::json!([1]));
        }
        other => panic!("expected the state baseline first, got {other:?}"),
    }

    harness
        .feed
        .emit(&cx(), 1, &manifest, "items", serde_json::json!([1, 2]))
        .await
        .expect("deliver");
    match next_frame(&mut subscriber).await {
        Frame::Event { envelope } => {
            assert_eq!(envelope["name"], serde_json::json!("m_demo/items"));
            assert_eq!(
                envelope["payload"],
                serde_json::json!([1, 2]),
                "changes produced after the baseline arrive as event frames"
            );
        }
        other => panic!("expected an increment next, got {other:?}"),
    }

    let set = wind_mod::capability::available(&manifest.capabilities, CallMode::Observe);
    assert!(
        set.has(wind_mod::capability::Prop::Http),
        "declared means present"
    );
    assert!(
        !set.has(wind_mod::capability::Prop::Fs),
        "undeclared means absent"
    );
}

#[tokio::test]
async fn the_assembly_serves_one_directory_with_no_entry_lost_in_the_merge() {
    let harness = assemble(Arc::new(NoServers), Limits::default(), Arc::new(NoCompiler)).await;
    let parts = [
        wind_core::usecase::directory(),
        wind_core::agent::events::directory(),
        wind_core::conversation::directory(),
        events::directory(),
    ];
    let total: usize = parts.iter().map(|part| part.len()).sum();
    assert_eq!(
        harness.bus.directory().len(),
        total,
        "a merge that drops same-named entries would shrink the total"
    );
}

/// 记录被编译过哪些包的替身
struct RecordingCompiler {
    /// 每次编译的 (安装根, 处理函数集合)
    seen: Mutex<Vec<(std::path::PathBuf, BTreeSet<String>)>>,
    /// 是否拒绝
    refuse: bool,
}

impl Compiler for RecordingCompiler {
    fn compile(
        &self,
        root: &std::path::Path,
        handlers: &BTreeSet<String>,
    ) -> Result<Option<CompiledScripts>, String> {
        self.seen
            .lock()
            .expect("lock the record")
            .push((root.to_path_buf(), handlers.clone()));
        if self.refuse {
            return Err("refused by the test".to_string());
        }
        Ok(Some(CompiledScripts {
            modules: std::collections::BTreeMap::from([(
                "scripts/index.js".to_string(),
                vec![1, 2, 3],
            )]),
            fingerprint: "test-engine".to_string(),
        }))
    }
}

#[tokio::test]
async fn installing_compiles_the_scripts_and_records_them_with_the_version() {
    let compiler = Arc::new(RecordingCompiler {
        seen: Mutex::new(Vec::new()),
        refuse: false,
    });
    let rig = rig_with_compiler(compiler.clone()).await;
    let package = windai_test_package(
        &rig.dirs,
        "demo",
        1,
        r#"{"name":"demo","version":1,
            "requests":{"fetch":{"handler":"fetchTop"}}}"#,
    );
    rig.installer()
        .install(&cx(), &package)
        .await
        .expect("install");

    // 取一份记录，锁不跨过下面的 await
    let seen = compiler.seen.lock().expect("lock the record").clone();
    assert_eq!(seen.len(), 1, "one install compiles only once");
    assert_eq!(
        seen[0].0, package,
        "the package compiled is the one just written to the database"
    );
    assert!(
        seen[0].1.contains("fetchTop"),
        "the handler in the manifest must be handed to the compiler: {:?}",
        seen[0].1
    );

    let row = rig
        .storage()
        .mod_get_by_name("demo")
        .await
        .expect("query")
        .expect("installed");
    let version = rig
        .storage()
        .mod_get_version(row.id, 1)
        .await
        .expect("query")
        .expect("read the version row");
    assert_eq!(version.engine_fingerprint.as_deref(), Some("test-engine"));
    assert_eq!(
        version.compiled,
        Some(serde_json::json!({ "scripts/index.js": [1, 2, 3] }))
    );
}

#[tokio::test]
async fn a_package_the_compiler_refuses_is_not_installed() {
    let compiler = Arc::new(RecordingCompiler {
        seen: Mutex::new(Vec::new()),
        refuse: true,
    });
    let rig = rig_with_compiler(compiler).await;
    let package = windai_test_package(
        &rig.dirs,
        "demo",
        1,
        r#"{"name":"demo","version":1,
            "requests":{"fetch":{"handler":"fetchTop"}}}"#,
    );

    let error = rig
        .installer()
        .install(&cx(), &package)
        .await
        .expect_err("install a refused package");
    assert!(error.to_string().contains("refused"), "{error}");

    assert!(
        rig.storage()
            .mod_get_by_name("demo")
            .await
            .expect("query")
            .is_none(),
        "the refusal happens before writing to the database, so no mods row is left"
    );
}

#[tokio::test]
async fn a_deployment_without_a_script_runtime_leaves_the_compiled_columns_empty() {
    let rig = rig_with_compiler(Arc::new(NoCompiler)).await;
    let package = windai_test_package(&rig.dirs, "demo", 1, r#"{"name":"demo","version":1}"#);
    rig.installer()
        .install(&cx(), &package)
        .await
        .expect("install");

    let row = rig
        .storage()
        .mod_get_by_name("demo")
        .await
        .expect("query")
        .expect("installed");
    let version = rig
        .storage()
        .mod_get_version(row.id, 1)
        .await
        .expect("query")
        .expect("read the version row");
    assert!(
        version.compiled.is_none(),
        "this column should not be written when never compiled"
    );
    assert!(
        version.engine_fingerprint.is_none(),
        "the fingerprint should not be written when never compiled: an empty fingerprint looks like a compiled artifact"
    );
}
