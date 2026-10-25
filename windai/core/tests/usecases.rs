//! 验证用例层对配置实体的写入、拦截改写与事件目录。

use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use wind_core::db::Db;
use wind_core::models::{
    AdapterType, AllowPolicy, CredentialUpdate, ModelCreate, ModelUpdate, ProviderCreate,
    ProviderUpdate, TopicCreate, TopicUpdate,
};
use wind_core::storage::Storage;
use wind_core::usecase::error::UseCaseError;
use wind_core::usecase::{self, UseCases};
use wind_event::context::DispatchCx;
use wind_event::def::{Directory, Shape};
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::subscription::{EnabledSet, Phase, Subscription};

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

/// 记录每次调用载荷的 host
#[derive(Clone)]
struct RecordingHost {
    /// 订阅方的 mod 名
    name: String,
    /// 订阅的拦截事件
    event: &'static str,
    /// 收到的全部载荷，按调用先后排列
    seen: Arc<Mutex<Vec<serde_json::Value>>>,
    /// 每次调用所派发的事件名，与载荷一一对应
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl RecordingHost {
    /// 放行并记录载荷，订阅话题修改
    fn new() -> Self {
        Self::with("recorder", "core/topic.update")
    }

    /// 放行并记录载荷，订阅给定的拦截事件
    fn on(event: &'static str) -> Self {
        Self::with("recorder", event)
    }

    fn with(name: &str, event: &'static str) -> Self {
        Self {
            name: name.into(),
            event,
            seen: Arc::new(Mutex::new(Vec::new())),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 最近一次调用收到的载荷
    fn last_payload(&self) -> serde_json::Value {
        self.seen
            .lock()
            .expect("lock the record")
            .last()
            .cloned()
            .expect("call at least once")
    }

    /// 收到的全部载荷
    fn all_payloads(&self) -> Vec<serde_json::Value> {
        self.seen.lock().expect("lock the record").clone()
    }

    /// 收到的全部事件名，按调用先后排列
    fn all_events(&self) -> Vec<&'static str> {
        self.events.lock().expect("lock the record").clone()
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
        self.seen.lock().expect("lock the record").push(payload);
        Ok(None)
    }
}

/// 指向话题 1 的 agent 上下文
fn cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id: 1 },
        Source::core("usecase"),
        Duration::from_millis(50),
    )
}

/// 一条拦截订阅
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

/// 一个在某个拦截事件上应答的 host
trait InterceptHost {
    /// 应答的 mod 名
    fn mod_name(&self) -> String;
    /// 订阅的拦截事件
    fn event(&self) -> &'static str;
}

impl InterceptHost for RecordingHost {
    fn mod_name(&self) -> String {
        self.name.clone()
    }

    fn event(&self) -> &'static str {
        self.event
    }
}

/// 改写载荷后放行的 host
#[derive(Clone)]
struct RewritingHost {
    /// 订阅方的 mod 名
    name: String,
    /// 订阅的拦截事件
    event: &'static str,
    /// 对载荷的改写
    rewrite: Arc<dyn Fn(&mut serde_json::Value) + Send + Sync>,
}

impl RewritingHost {
    /// 在给定事件上按给定函数改写载荷
    fn on(
        event: &'static str,
        rewrite: impl Fn(&mut serde_json::Value) + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: "rewriter".into(),
            event,
            rewrite: Arc::new(rewrite),
        }
    }
}

impl InterceptHost for RewritingHost {
    fn mod_name(&self) -> String {
        self.name.clone()
    }

    fn event(&self) -> &'static str {
        self.event
    }
}

#[async_trait]
impl ModHost for RewritingHost {
    async fn call(
        &self,
        _sub: &Subscription,
        mut payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        (self.rewrite)(&mut payload);
        Ok(Some(payload))
    }
}

/// 让给定 host 参与所订阅事件的拦截
fn sub(host: &impl InterceptHost) -> Subscription {
    subscription(host.event(), &host.mod_name())
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

#[tokio::test]
async fn the_use_case_layer_builds_on_the_intercept_events() {
    let (_dir, _layer) = usecases(directory(), vec![]).await;
    let dir = directory();
    assert_eq!(
        dir.len(),
        24,
        "six events for each of the four entity kinds"
    );
    for name in [
        "core/topic.create",
        "core/topic.update",
        "core/topic.delete",
        "core/provider.create",
        "core/provider.update",
        "core/provider.delete",
        "core/credential.create",
        "core/credential.update",
        "core/credential.delete",
        "core/model.create",
        "core/model.update",
        "core/model.delete",
    ] {
        let meta = dir.get(name).expect("find catalog entry");
        assert_eq!(
            meta.shape,
            Shape::Intercept,
            "{name} must be an intercept event"
        );
        let schema = (meta.schema)();
        assert_eq!(
            schema["type"],
            serde_json::json!("object"),
            "{name} schema must be an object"
        );
    }
}

#[tokio::test]
async fn creating_a_topic_goes_through_the_entry() {
    let (_dir, cases) = usecases(directory(), vec![]).await;
    let id = cases
        .create_topic(
            &cx(),
            TopicCreate {
                label: "work".into(),
                ..Default::default()
            },
        )
        .await
        .expect("succeeds");
    let topic = cases
        .get_topic(id)
        .await
        .expect("read")
        .expect("must exist");
    assert_eq!(topic.label, "work");
    assert!(
        topic.tool_approval_policy.allow == vec!["*".to_string()],
        "a new topic defaults to allowing everything"
    );
}

#[tokio::test]
async fn an_update_carries_the_submitted_fields() {
    let host = RecordingHost::new();
    let (_dir, cases) =
        usecases_with_host(directory(), vec![sub(&host)], Arc::new(host.clone())).await;
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
    let seen = host.last_payload();
    assert_eq!(seen["topic_id"], serde_json::json!(id));
    assert_eq!(seen["data"]["label"], serde_json::json!("worksheet"));
}

#[tokio::test]
async fn a_provider_round_trips_through_the_entry() {
    let (_dir, cases) = usecases(directory(), vec![]).await;
    let id = cases
        .create_provider(
            &cx(),
            ProviderCreate {
                name: "openai".into(),
                base_url: "https://api.openai.com/v1".into(),
                ..Default::default()
            },
        )
        .await
        .expect("succeeds");
    let provider = cases
        .get_provider(id)
        .await
        .expect("read")
        .expect("must exist");
    assert_eq!(provider.name, "openai");
    assert_eq!(provider.base_url, "https://api.openai.com/v1");
}

#[tokio::test]
async fn a_credential_round_trips_through_the_entry() {
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
    let id = cases
        .create_credential(&cx(), provider, "sk-secret")
        .await
        .expect("succeeds");
    assert!(cases.get_credential(id).await.expect("read").is_some());
}

#[tokio::test]
async fn no_event_payload_ever_carries_the_secret() {
    // 经过真实的派发路径，载荷经 serde 序列化后交给 host
    let host = RecordingHost::on("core/credential.create");
    let (_dir, cases) = usecases_with_host(
        directory(),
        vec![
            sub(&host),
            subscription("core/credential.update", &host.mod_name()),
            subscription("core/credential.delete", &host.mod_name()),
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

    assert_eq!(
        host.all_events(),
        vec![
            "core/credential.create",
            "core/credential.update",
            "core/credential.delete",
        ],
        "each write must dispatch its own event"
    );
    let payloads = host.all_payloads();
    assert_eq!(
        payloads.len(),
        3,
        "each of the three credential events should deliver one payload, otherwise the assertion is vacuous"
    );
    for payload in &payloads {
        let text = serde_json::to_string(payload).expect("serialize");
        assert!(
            !text.contains("sk-secret"),
            "the secret leaked into the event payload: {text}"
        );
    }
}

#[tokio::test]
async fn toggling_a_credential_carries_the_submitted_fields() {
    let host = RecordingHost::on("core/credential.update");
    let (_dir, cases) =
        usecases_with_host(directory(), vec![sub(&host)], Arc::new(host.clone())).await;
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
    let seen = host.last_payload();
    assert_eq!(seen["credential_id"], serde_json::json!(id));
    assert_eq!(seen["data"]["active"], serde_json::json!(false));
    assert!(
        seen["data"].get("key").is_none(),
        "the update payload must not carry the key"
    );
    let credential = cases
        .get_credential(id)
        .await
        .expect("read")
        .expect("must exist");
    assert!(
        !credential.active,
        "an allowed change should be written to the database"
    );
    assert_eq!(
        credential.key, "sk-secret",
        "the key in storage is still the original"
    );
}

#[tokio::test]
async fn a_model_round_trips_through_the_entry() {
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
    let id = cases
        .create_model(
            &cx(),
            ModelCreate::new(provider, "gpt-4o".into(), AdapterType::OpenAICompletion),
        )
        .await
        .expect("succeeds");
    let model = cases
        .get_model(id)
        .await
        .expect("read")
        .expect("must exist");
    assert_eq!(model.name, "gpt-4o");
    assert_eq!(model.adapter, AdapterType::OpenAICompletion);
    assert_eq!(model.provider_id, provider);
}

#[tokio::test]
async fn deleting_a_provider_keeps_its_models() {
    // 模型列只引用提供商标识，两者之间没有级联约束
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
    cases
        .create_model(
            &cx(),
            ModelCreate::new(provider, "gpt-4o".into(), AdapterType::OpenAICompletion),
        )
        .await
        .expect("create");
    cases
        .delete_provider(&cx(), provider)
        .await
        .expect("delete provider");

    let models = cases.list_models_by_provider(provider).await.expect("read");
    assert_eq!(
        models.len(),
        1,
        "deleting a provider does not take its models with it"
    );
    assert_eq!(
        models[0].provider_id, provider,
        "the model still points at the original provider"
    );
}

// 被拦截链改写后的值才是写入存储的值

#[tokio::test]
async fn a_rewritten_topic_update_is_what_lands_in_the_database() {
    // 写入存储的是载荷中的值，不是调用方的实参。改写必须一路传递到存储
    let host = RewritingHost::on("core/topic.update", |payload| {
        payload["data"]["label"] = serde_json::json!("rewritten topic");
        payload["data"]["tool_approval_policy"]["allow"] = serde_json::json!(["read_file"]);
    });
    let (_dir, cases) = usecases_with_host(directory(), vec![sub(&host)], Arc::new(host)).await;
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
    cases
        .update_topic(
            &cx(),
            id,
            TopicUpdate {
                tool_approval_policy: Some(AllowPolicy {
                    allow: vec!["*".into()],
                }),
                ..Default::default()
            },
        )
        .await
        .expect("update");
    let topic = cases
        .get_topic(id)
        .await
        .expect("read")
        .expect("must exist");
    assert_eq!(topic.label, "rewritten topic");
    assert_eq!(
        topic.tool_approval_policy.allow,
        vec!["read_file".to_string()]
    );
}

#[tokio::test]
async fn a_rewritten_provider_update_is_what_lands_in_the_database() {
    let host = RewritingHost::on("core/provider.update", |payload| {
        payload["data"]["name"] = serde_json::json!("rewritten provider");
        payload["data"]["base_url"] = serde_json::json!("https://rewritten.example");
    });
    let (_dir, cases) = usecases_with_host(directory(), vec![sub(&host)], Arc::new(host)).await;
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
    cases
        .update_provider(
            &cx(),
            id,
            ProviderUpdate {
                base_url: Some("https://other.example".into()),
                ..Default::default()
            },
        )
        .await
        .expect("update");
    let provider = cases
        .get_provider(id)
        .await
        .expect("read")
        .expect("must exist");
    assert_eq!(provider.name, "rewritten provider");
    assert_eq!(provider.base_url, "https://rewritten.example");
}

#[tokio::test]
async fn a_rewritten_credential_update_is_what_lands_in_the_database() {
    let host = RewritingHost::on("core/credential.update", |payload| {
        payload["data"]["active"] = serde_json::json!(false);
    });
    let (_dir, cases) = usecases_with_host(directory(), vec![sub(&host)], Arc::new(host)).await;
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
        .update_credential(&cx(), id, CredentialUpdate { active: Some(true) })
        .await
        .expect("update");
    let credential = cases
        .get_credential(id)
        .await
        .expect("read")
        .expect("must exist");
    assert!(
        !credential.active,
        "the rewrite overrode the enabled value the caller gave"
    );
}

#[tokio::test]
async fn a_rewritten_model_update_is_what_lands_in_the_database() {
    let host = RewritingHost::on("core/model.update", |payload| {
        payload["data"]["name"] = serde_json::json!("rewritten model");
        payload["data"]["adapter"] = serde_json::json!("openai_response");
    });
    let (_dir, cases) = usecases_with_host(directory(), vec![sub(&host)], Arc::new(host)).await;
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
    cases
        .update_model(
            &cx(),
            id,
            ModelUpdate {
                adapter: Some(AdapterType::OpenAICompletion),
                ..Default::default()
            },
        )
        .await
        .expect("update");
    let model = cases
        .get_model(id)
        .await
        .expect("read")
        .expect("must exist");
    assert_eq!(model.name, "rewritten model");
    assert_eq!(model.adapter, AdapterType::OpenAIResponse);
}

// 缺失的实体是一个事实，不是一次拒绝

/// 断言一次写操作把缺失的目标报成 `NotFound`
fn assert_not_found(entity: &str, result: Result<(), UseCaseError>) {
    match result {
        Err(UseCaseError::NotFound { entity: got, id }) => {
            assert_eq!(got, entity, "entity kind in the error");
            assert_eq!(id, 9999, "entity id in the error");
        }
        other => panic!("expected NotFound for missing {entity}, got {other:?}"),
    }
}

#[tokio::test]
async fn an_absent_target_reports_not_found_on_every_write() {
    let (_dir, cases) = usecases(directory(), vec![]).await;
    let cx = cx();
    let missing = 9999;
    let policy = AllowPolicy {
        allow: vec!["*".into()],
    };

    assert_not_found(
        "topic",
        cases
            .update_topic(
                &cx,
                missing,
                TopicUpdate {
                    label: Some("work".into()),
                    ..Default::default()
                },
            )
            .await,
    );
    assert_not_found(
        "topic",
        cases
            .update_topic(
                &cx,
                missing,
                TopicUpdate {
                    tool_approval_policy: Some(policy),
                    ..Default::default()
                },
            )
            .await,
    );
    assert_not_found("topic", cases.delete_topic(&cx, missing).await);
    assert_not_found(
        "provider",
        cases
            .update_provider(
                &cx,
                missing,
                ProviderUpdate {
                    name: Some("openai".into()),
                    ..Default::default()
                },
            )
            .await,
    );
    assert_not_found(
        "provider",
        cases
            .update_provider(
                &cx,
                missing,
                ProviderUpdate {
                    base_url: Some("https://x".into()),
                    ..Default::default()
                },
            )
            .await,
    );
    assert_not_found("provider", cases.delete_provider(&cx, missing).await);
    assert_not_found(
        "credential",
        cases
            .update_credential(
                &cx,
                missing,
                CredentialUpdate {
                    active: Some(false),
                },
            )
            .await,
    );
    assert_not_found("credential", cases.delete_credential(&cx, missing).await);
    assert_not_found(
        "model",
        cases
            .update_model(
                &cx,
                missing,
                ModelUpdate {
                    name: Some("gpt-4o".into()),
                    ..Default::default()
                },
            )
            .await,
    );
    assert_not_found(
        "model",
        cases
            .update_model(
                &cx,
                missing,
                ModelUpdate {
                    adapter: Some(AdapterType::OpenAICompletion),
                    ..Default::default()
                },
            )
            .await,
    );
    assert_not_found("model", cases.delete_model(&cx, missing).await);
}

// 事件目录中本单元产出的部分

#[test]
fn the_configuration_family_declares_twenty_four_events() {
    let dir = directory();
    assert_eq!(
        dir.len(),
        24,
        "six events for each of the four entity kinds"
    );
}

#[test]
fn each_entity_has_three_intercepts_and_three_observes() {
    let dir = directory();
    for entity in ["topic", "provider", "credential", "model"] {
        for action in ["create", "update", "delete"] {
            let name = format!("core/{entity}.{action}");
            let meta = dir.get(&name).expect("read event definition");
            assert!(meta.is_intercept(), "{name} must be interceptable");

            let observed = format!("core/{entity}.{action}d");
            let meta = dir.get(&observed).expect("read event definition");
            assert!(meta.is_observe(), "{observed} must be observe-only");
        }
    }
}

#[test]
fn every_configuration_event_enters_the_log() {
    // 判据取全量。目录中的每一条都必须写入事件日志，抽查漏掉一条就漏掉一类
    let dir = directory();
    let mut count = 0;
    for name in dir.names() {
        assert!(
            dir.get(name).expect("look up catalog name").log,
            "{name} must enter the event log"
        );
        count += 1;
    }
    assert_eq!(
        count, 24,
        "six per each of the four entity kinds, all must be checked"
    );
}

#[test]
fn the_intercepts_and_observes_of_one_entity_are_distinct_events() {
    // 每个动作的拦截与观测是两条各自独立的名字，而不是一条带动作字段的事件
    let dir = directory();
    for entity in ["topic", "provider", "credential", "model"] {
        for action in ["create", "update", "delete"] {
            let intercept = format!("core/{entity}.{action}");
            let observe = format!("core/{entity}.{action}d");
            assert_ne!(intercept, observe);
            assert!(dir.get(&intercept).is_some());
            assert!(dir.get(&observe).is_some());
        }
    }
}

// 观测事件在写入存储之后派发

/// 轮询直到条件成立。观测事件在后台派发完成
async fn wait_until(mut ready: impl FnMut() -> bool) {
    for _ in 0..200 {
        if ready() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn a_write_dispatches_its_observe_event_after_the_row_lands() {
    let host = RecordingHost::on("core/topic.created");
    let (_dir, cases) = usecases_with_host(
        directory(),
        vec![
            sub(&host),
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

    wait_until(|| host.all_payloads().len() == 3).await;
    assert_eq!(
        host.all_events(),
        vec![
            "core/topic.created",
            "core/topic.updated",
            "core/topic.deleted",
        ],
        "the three specified names each dispatch to their own observe event, not a shared name"
    );
    let payloads = host.all_payloads();
    assert_eq!(
        payloads.len(),
        3,
        "each of the three writes dispatches one observe event"
    );
    for payload in &payloads {
        assert_eq!(
            payload,
            &serde_json::json!({ "topic_id": id }),
            "the observe payload carries only the entity id"
        );
    }
}
