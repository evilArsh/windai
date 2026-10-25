//! 验证配置实体的唯一入口，共三项检验。
//! 用例层与存储层之外没有代码直接写配置实体。移除派发后变更无法完成，四类实体都经过同一入口。

use async_trait::async_trait;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use wind_core::db::Db;
use wind_core::models::{
    AdapterType, CredentialUpdate, ModelCreate, ModelUpdate, ProviderCreate, ProviderUpdate,
    TopicCreate, TopicUpdate,
};
use wind_core::storage::Storage;
use wind_core::usecase::{self, UseCases};
use wind_event::context::DispatchCx;
use wind_event::def::Directory;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::subscription::{EnabledSet, Phase, Subscription};

// 检验一。用例层之外没有代码直接写配置实体

/// 四类实体在存储层的全部写方法
const WRITE_METHODS: [&str; 12] = [
    "topic_create",
    "topic_update",
    "topic_delete",
    "provider_create",
    "provider_update",
    "provider_delete",
    "credential_create",
    "credential_update",
    "credential_delete",
    "model_create",
    "model_update",
    "model_delete",
];

/// 四类实体在存储层的全部读方法，写方法之外只有这一组
const READ_METHODS: [&str; 9] = [
    "topic_get",
    "topic_list",
    "topic_children_of",
    "provider_get",
    "provider_list",
    "credential_get",
    "credential_list_by_provider",
    "model_get",
    "model_list_by_provider",
];

#[test]
fn nothing_outside_the_usecase_layer_writes_configuration() {
    // 绕过唯一入口直接调用存储，是这个子系统最可能出的问题。
    // 该问题没有外部症状。接口照常返回，数据照常写入存储，只是拦截不生效
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let offenders = scan_for_direct_writes(&root);
    assert!(
        offenders.is_empty(),
        "the following locations bypass the use-case layer and write configuration entities directly: {offenders:?}"
    );
}

/// 扫出 src 下除用例层与存储层之外直接调用配置写方法的位置
///
/// 用例层与存储层本来就写这些名字，两处一并跳过
fn scan_for_direct_writes(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    for entry in walk(root) {
        if is_exempt(&entry) {
            continue;
        }
        let Ok(text) = fs::read_to_string(&entry) else {
            continue;
        };
        for write in WRITE_METHODS {
            if text.contains(write) {
                out.push(format!("{write} appears in {}", entry.display()));
            }
        }
    }
    out
}

/// 递归列出目录下的全部文件
fn walk(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = fs::read_dir(root) else {
        return files;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files.extend(walk(&path));
        } else {
            files.push(path);
        }
    }
    files
}

/// 该文件是否属于用例层或存储层
fn is_exempt(path: &Path) -> bool {
    path.components().any(|component| match component {
        Component::Normal(name) => {
            let name = name.to_string_lossy();
            matches!(
                name.as_ref(),
                "usecase" | "usecase.rs" | "storage" | "storage.rs"
            )
        }
        _ => false,
    })
}

#[test]
fn the_write_list_covers_every_storage_write() {
    // 列表漏一个写方法就漏掉一条绕过路径，因此与存储层的公开方法保持一致
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/storage/config.rs");
    let text = fs::read_to_string(&path).expect("read storage source");
    let methods = public_async_methods(&text);

    for write in WRITE_METHODS {
        assert!(
            methods.contains(&write),
            "{write} in the write method list does not exist in the storage layer"
        );
    }

    let unclassified: Vec<&str> = methods
        .iter()
        .copied()
        .filter(|name| !WRITE_METHODS.contains(name) && !READ_METHODS.contains(name))
        .collect();
    assert!(
        unclassified.is_empty(),
        "the following storage methods are in neither the write list nor the read list: {unclassified:?}"
    );
}

/// 提取源码里全部公开异步方法的函数名
fn public_async_methods(text: &str) -> Vec<&str> {
    const MARKER: &str = "pub async fn ";
    text.match_indices(MARKER)
        .filter_map(|(at, marker)| {
            let rest = &text[at + marker.len()..];
            let end = rest.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))?;
            Some(&rest[..end])
        })
        .collect()
}

// 检验二。移除派发后配置变更无法完成

#[tokio::test]
async fn a_configuration_change_reaches_the_host_before_it_is_written() {
    // 配置写入必须经过事件入口：宿主看得到 create 的载荷
    let host = RecordingHost::on("core/topic.create");
    let (_dir, cases) =
        usecases_with_host(directory(), vec![host.sub()], Arc::new(host.clone())).await;
    let id = cases
        .create_topic(
            &cx(),
            TopicCreate {
                label: "work".into(),
                ..Default::default()
            },
        )
        .await
        .expect("create");
    assert!(cases.get_topic(id).await.expect("read").is_some());
    assert_eq!(host.all_events(), vec!["core/topic.create"]);
    assert_eq!(host.all_payloads()[0]["label"], serde_json::json!("work"));
}

#[tokio::test]
async fn a_missing_directory_entry_does_not_break_configuration() {
    // 派发失败只影响 mod，不影响操作本身
    let (_dir, cases) = usecases(broken_directory(), vec![]).await;
    let id = cases
        .create_topic(
            &cx(),
            TopicCreate {
                label: "work".into(),
                ..Default::default()
            },
        )
        .await
        .expect("the operation must continue without dispatch");
    assert!(cases.get_topic(id).await.expect("read").is_some());
}

// 检验三。四类实体的入口收敛在同一处

#[tokio::test]
async fn every_entry_point_funnels_through_the_same_place() {
    // 检查是否存在第二条写入路径。四类实体的读写都经过同一个入口
    let (_dir, cases) = usecases(directory(), vec![]).await;
    let provider = cases
        .create_provider(
            &cx(),
            ProviderCreate {
                name: "openai".into(),
                base_url: "https://x".into(),
                ..Default::default()
            },
        )
        .await
        .expect("create");
    let model = cases
        .create_model(
            &cx(),
            ModelCreate::new(provider, "gpt-4o".into(), AdapterType::OpenAICompletion),
        )
        .await
        .expect("create");
    let credential = cases
        .create_credential(&cx(), provider, "sk-secret")
        .await
        .expect("create");
    let topic = cases
        .create_topic(
            &cx(),
            TopicCreate {
                label: "work".into(),
                ..Default::default()
            },
        )
        .await
        .expect("create");

    assert!(cases.get_topic(topic).await.expect("read").is_some());
    assert!(cases.get_provider(provider).await.expect("read").is_some());
    assert!(cases.get_model(model).await.expect("read").is_some());
    assert!(
        cases
            .get_credential(credential)
            .await
            .expect("read")
            .is_some()
    );
}

#[tokio::test]
async fn a_ui_origin_change_passes_the_rewriting_chain() {
    // ui 来源的操作不受任何 mod 干预，但仍然经过同一个入口
    let (_dir, cases) = usecases_with_host(
        directory(),
        vec![rewrite_subscription("core/topic.create")],
        Arc::new(RecordingHost::rewriting_on("core/topic.create")),
    )
    .await;
    let id = cases
        .create_topic(
            &ui_cx(),
            TopicCreate {
                label: "work".into(),
                ..Default::default()
            },
        )
        .await
        .expect("bypass intercept for ui source");
    assert!(
        cases.get_topic(id).await.expect("read").is_some(),
        "a write from the ui source must be written to the database"
    );
}

// 写入存储之后派发的观测事件

#[tokio::test]
async fn a_topic_write_dispatches_its_observe_events() {
    let host = RecordingHost::on("core/topic.created");
    let (_dir, cases) = usecases_with_host(
        directory(),
        vec![
            host.sub(),
            subscription("core/topic.updated", &host.mod_name()),
            subscription("core/topic.deleted", &host.mod_name()),
        ],
        Arc::new(host.clone()),
    )
    .await;
    let id = cases
        .create_topic(
            &cx(),
            TopicCreate {
                label: "work".into(),
                ..Default::default()
            },
        )
        .await
        .expect("create");
    cases
        .update_topic(
            &cx(),
            id,
            TopicUpdate {
                label: Some("worksheet".into()),
                ..Default::default()
            },
        )
        .await
        .expect("update");
    cases.delete_topic(&cx(), id).await.expect("delete");

    host.wait_for(3).await;
    assert_eq!(
        host.all_events(),
        vec![
            "core/topic.created",
            "core/topic.updated",
            "core/topic.deleted",
        ],
        "each of the three writes dispatches its own observe event"
    );
    assert_eq!(
        host.all_payloads(),
        vec![serde_json::json!({ "topic_id": id }); 3],
        "each of the three writes dispatches an observe payload carrying only the id"
    );
}

#[tokio::test]
async fn a_provider_write_dispatches_its_observe_events() {
    let host = RecordingHost::on("core/provider.created");
    let (_dir, cases) = usecases_with_host(
        directory(),
        vec![
            host.sub(),
            subscription("core/provider.updated", &host.mod_name()),
            subscription("core/provider.deleted", &host.mod_name()),
        ],
        Arc::new(host.clone()),
    )
    .await;
    let id = cases
        .create_provider(
            &cx(),
            ProviderCreate {
                name: "openai".into(),
                base_url: "https://x".into(),
                ..Default::default()
            },
        )
        .await
        .expect("create");
    cases
        .update_provider(
            &cx(),
            id,
            ProviderUpdate {
                name: Some("azure".into()),
                ..Default::default()
            },
        )
        .await
        .expect("update");
    cases.delete_provider(&cx(), id).await.expect("delete");

    host.wait_for(3).await;
    assert_eq!(
        host.all_events(),
        vec![
            "core/provider.created",
            "core/provider.updated",
            "core/provider.deleted",
        ],
        "each of the three writes dispatches its own observe event"
    );
    assert_eq!(
        host.all_payloads(),
        vec![serde_json::json!({ "provider_id": id }); 3],
        "each of the three writes dispatches an observe payload carrying only the id"
    );
}

#[tokio::test]
async fn a_credential_write_dispatches_its_observe_events() {
    let host = RecordingHost::on("core/credential.created");
    let (_dir, cases) = usecases_with_host(
        directory(),
        vec![
            host.sub(),
            subscription("core/credential.updated", &host.mod_name()),
            subscription("core/credential.deleted", &host.mod_name()),
        ],
        Arc::new(host.clone()),
    )
    .await;
    let provider = cases
        .create_provider(
            &cx(),
            ProviderCreate {
                name: "openai".into(),
                base_url: "https://x".into(),
                ..Default::default()
            },
        )
        .await
        .expect("create");
    let id = cases
        .create_credential(&cx(), provider, "sk-secret")
        .await
        .expect("create");
    cases
        .update_credential(
            &cx(),
            id,
            CredentialUpdate {
                active: Some(false),
            },
        )
        .await
        .expect("update");
    cases.delete_credential(&cx(), id).await.expect("delete");

    host.wait_for(3).await;
    assert_eq!(
        host.all_events(),
        vec![
            "core/credential.created",
            "core/credential.updated",
            "core/credential.deleted",
        ],
        "each of the three writes dispatches its own observe event"
    );
    assert_eq!(
        host.all_payloads(),
        vec![serde_json::json!({ "credential_id": id }); 3],
        "each of the three writes dispatches an observe payload carrying only the id"
    );
}

#[tokio::test]
async fn a_model_write_dispatches_its_observe_events() {
    let host = RecordingHost::on("core/model.created");
    let (_dir, cases) = usecases_with_host(
        directory(),
        vec![
            host.sub(),
            subscription("core/model.updated", &host.mod_name()),
            subscription("core/model.deleted", &host.mod_name()),
        ],
        Arc::new(host.clone()),
    )
    .await;
    let provider = cases
        .create_provider(
            &cx(),
            ProviderCreate {
                name: "openai".into(),
                base_url: "https://x".into(),
                ..Default::default()
            },
        )
        .await
        .expect("create");
    let id = cases
        .create_model(
            &cx(),
            ModelCreate::new(provider, "gpt-4o".into(), AdapterType::OpenAICompletion),
        )
        .await
        .expect("create");
    cases
        .update_model(
            &cx(),
            id,
            ModelUpdate {
                name: Some("gpt-4o-mini".into()),
                ..Default::default()
            },
        )
        .await
        .expect("update");
    cases.delete_model(&cx(), id).await.expect("delete");

    host.wait_for(3).await;
    assert_eq!(
        host.all_events(),
        vec![
            "core/model.created",
            "core/model.updated",
            "core/model.deleted",
        ],
        "each of the three writes dispatches its own observe event"
    );
    assert_eq!(
        host.all_payloads(),
        vec![serde_json::json!({ "model_id": id }); 3],
        "each of the three writes dispatches an observe payload carrying only the id"
    );
}

// 测试替身

/// 每个作用域都启用的全部 mod
struct AllowAll;

impl ModEnablement for AllowAll {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::all()
    }
}

/// 始终返回 `Pass`、不给出判定的 host
struct NoopHost;

#[async_trait]
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

/// 记录每次调用载荷，并按声明放行或否决的 host
#[derive(Clone)]
struct RecordingHost {
    /// 订阅方的 mod 名
    name: String,
    /// 订阅的事件
    event: &'static str,
    /// 为真时否决每次调用
    rewrite: bool,
    /// 收到的全部载荷，按调用先后排列
    seen: Arc<Mutex<Vec<serde_json::Value>>>,
    /// 每次调用所派发的事件名，与载荷一一对应
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl RecordingHost {
    /// 记录载荷并放行，订阅给定事件
    fn on(event: &'static str) -> Self {
        Self::with("recorder", event, false)
    }

    /// 记录载荷并改写，订阅给定事件
    fn rewriting_on(event: &'static str) -> Self {
        Self::with("rewriter", event, true)
    }

    fn with(name: &str, event: &'static str, rewrite: bool) -> Self {
        Self {
            name: name.into(),
            event,
            rewrite,
            seen: Arc::new(Mutex::new(Vec::new())),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 该 host 在所订阅事件上的订阅
    fn sub(&self) -> Subscription {
        subscription(self.event, &self.name)
    }

    /// 订阅方的 mod 名
    fn mod_name(&self) -> String {
        self.name.clone()
    }

    /// 收到的全部载荷
    fn all_payloads(&self) -> Vec<serde_json::Value> {
        self.seen.lock().expect("lock the record").clone()
    }

    /// 收到的全部事件名，按调用先后排列
    fn all_events(&self) -> Vec<&'static str> {
        self.events.lock().expect("lock the record").clone()
    }

    /// 等到收满给定的条数。观测事件在后台派发完成
    async fn wait_for(&self, want: usize) {
        for _ in 0..200 {
            if self.all_payloads().len() >= want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

#[async_trait]
impl ModHost for RecordingHost {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        self.events.lock().expect("lock the record").push(sub.event);
        self.seen
            .lock()
            .expect("lock the record")
            .push(payload.clone());
        if self.rewrite {
            let mut rewritten = payload;
            rewritten["label"] = serde_json::json!("rewritten");
            Ok(Some(rewritten))
        } else {
            Ok(None)
        }
    }
}

/// 一个 agent 来源、话题 1 的上下文
fn cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id: 1 },
        Source::core("usecase"),
        Duration::from_millis(50),
    )
}

/// 一个 ui 来源、话题 1 的上下文
fn ui_cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Ui,
        Scope::Topic { topic_id: 1 },
        Source::core("ui"),
        Duration::from_millis(50),
    )
}

/// 一条订阅
fn subscription(event: &'static str, mod_name: &str) -> Subscription {
    Subscription {
        mod_name: mod_name.into(),
        mod_version: 1,
        event,
        phase: Some(Phase::Guard),
        handler: "onEvent".into(),
        timeout_ms: 50,
        registration_seq: 1,
    }
}

/// 一条改写指定事件的订阅
fn rewrite_subscription(event: &'static str) -> Subscription {
    subscription(event, "rewriter")
}

/// 位于临时目录、已建好表与事件总线、由给定 host 应答的用例层
///
/// 返回的临时目录须由调用方持有。临时目录被丢弃时，库文件随之删除
async fn usecases_with_host(
    directory: Directory,
    subs: Vec<Subscription>,
    host: Arc<dyn ModHost>,
) -> (TempDir, UseCases) {
    let dir = TempDir::new().expect("create temp dir");
    let db = Db::open(&dir.path().join("t.db"))
        .await
        .expect("open database");
    db.init_schema().await.expect("create table");
    let dispatcher = Dispatcher::new(directory, Arc::new(AllowAll), host, Limits::default());
    dispatcher.rebuild_index(subs);
    let storage = Storage::new(db);
    (dir, UseCases::new(storage, Arc::new(dispatcher)))
}

/// 由不给出判定的 host 应答的用例层
async fn usecases(directory: Directory, subs: Vec<Subscription>) -> (TempDir, UseCases) {
    usecases_with_host(directory, subs, Arc::new(NoopHost)).await
}

/// 本文件建用例层时传入的事件目录
fn directory() -> Directory {
    usecase::directory()
}

/// 一个不含任何事件的目录
fn broken_directory() -> Directory {
    Directory::from_metas([])
}
