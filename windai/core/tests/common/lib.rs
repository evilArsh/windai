//! 集成测试共用的环境装配、播种与事件订阅
//!
//! 每个测试目标只用其中一部分，未被用到的项不算缺陷
#![allow(dead_code)]

use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use wind_core::agent::handler::{Agent, AgentHandle, AgentOptions};
use wind_core::feed::{self, FeedError, FeedHandle, FeedOptions, ShutdownReport};
use wind_core::llm::LlmRunner;
use wind_core::llm::domain::{LlmDomain, LlmSpec};
use wind_core::resources::ResourceService;
use wind_event::{
    CallCx, CallError, Dispatcher, Limits, ModCaller, ModRoster, Observer, Subscription,
};
use wind_mcp::McpOptions;
use wind_mod::installer::Installer;
use wind_mod::resources::Resources;
use wind_mod::roster::Roster;
use wind_storage::Storage;
use wind_wire::AppDirs;
use wind_wire::event::FeedEvent;
use wind_wire::{AdapterType, AgentRole, Body, Content, PendingKind, Role};

/// 按 .env 的 RUST_LOG 打开日志
pub fn init_log() {
    dotenvy::dotenv().expect("仓库根目录的 .env 必须可读");
    let _ = env_logger::builder()
        .parse_env(env_logger::Env::default().default_filter_or("info"))
        .try_init();
}

/// 一次测试独占的应用目录与数据库
pub struct Fixture {
    pub storage: Storage,
    pub dirs: AppDirs,
}

impl Fixture {
    /// 每次运行都从空的目录与数据库开始
    pub async fn open(name: &str) -> Self {
        let root = dirs::home_dir()
            .expect("需要 home 目录")
            .join(".windai_test")
            .join(name);
        let _ = tokio::fs::remove_dir_all(&root).await;
        tokio::fs::create_dir_all(root.join("mods"))
            .await
            .expect("建测试目录");
        Self::with_dirs(AppDirs::under(root)).await
    }

    /// 复用 `.windai_test/mods`：只重建数据库与话题，手改的 mod 包保留
    pub async fn shared_mods() -> Self {
        let dirs = AppDirs::under(
            dirs::home_dir()
                .expect("需要 home 目录")
                .join(".windai_test"),
        );
        tokio::fs::create_dir_all(dirs.mods_root())
            .await
            .expect("建 mods 目录");
        let _ = tokio::fs::remove_dir_all(dirs.topics_root()).await;
        Self::with_dirs(dirs).await
    }

    async fn with_dirs(dirs: AppDirs) -> Self {
        for suffix in ["", "-wal", "-shm"] {
            let mut file = dirs.db_file().into_os_string();
            file.push(suffix);
            let _ = tokio::fs::remove_file(PathBuf::from(file)).await;
        }
        let storage = Storage::open(&dirs.db_file())
            .await
            .expect("打开测试数据库");
        Self { storage, dirs }
    }
}

/// 按 .env 建提供商、凭证与模型
pub async fn seed_provider(fixture: &Fixture) -> i64 {
    let storage = &fixture.storage;
    let provider = storage
        .create_provider("dotenv", &env("TEST_BASE_URL"))
        .await
        .expect("建提供商");
    storage
        .create_credential(provider, &env("TEST_KEY"))
        .await
        .expect("建凭证");
    let endpoint = std::env::var("TEST_ENDPOINT")
        .ok()
        .filter(|endpoint| !endpoint.is_empty());
    let config = serde_json::json!({ "stream": env("TEST_STREAM") == "true" });
    storage
        .create_model(
            provider,
            &env("TEST_MODEL"),
            adapter(),
            endpoint.as_deref(),
            Some(&config),
        )
        .await
        .expect("建模型")
}

/// 等待首轮输入的主实例
pub struct Root {
    pub topic: i64,
    pub instance: i64,
    /// 话题工作区，内建文件工具的沙箱根
    pub workspace: PathBuf,
}

/// 建话题与主实例
pub async fn seed_topic(fixture: &Fixture, label: &str, model: i64) -> Root {
    let storage = &fixture.storage;
    let workspace = fixture.dirs.topics_root().join(label);
    tokio::fs::create_dir_all(&workspace)
        .await
        .expect("建话题工作区");
    let topic = storage
        .create_topic(
            None,
            label,
            Some(model),
            None,
            Some(workspace.to_string_lossy().as_ref()),
        )
        .await
        .expect("建话题");
    let instance = storage
        .create_agent_instance(topic, None, AgentRole::Main, None, "running", None)
        .await
        .expect("建主实例");
    Root {
        topic,
        instance,
        workspace,
    }
}

/// 放一条待消费输入，返回它的标识
pub async fn seed_pending(storage: &Storage, instance: i64, prompt: &str) -> i64 {
    storage
        .create_pending(
            instance,
            PendingKind::User,
            vec![Content::new_text(prompt.to_string())],
            None,
        )
        .await
        .expect("放一条待消费输入")
}

/// 挂上 LlmRunner 并投放主节点
pub async fn start_agent(
    bus: &Bus,
    storage: &Storage,
    root: &Root,
    pending: i64,
) -> AgentHandle<LlmDomain> {
    let runner = Arc::new(LlmRunner::new(
        storage.clone(),
        Arc::clone(&bus.dispatcher),
        bus.services.clone(),
        bus.services.clone(),
    ));
    let handle = Agent::<LlmDomain>::start(AgentOptions {
        topic_id: root.topic,
        storage: storage.clone(),
        dispatcher: Arc::clone(&bus.dispatcher),
        runner,
        restored: Vec::new(),
        cancel: CancellationToken::new(),
    });
    handle
        .spawn(
            root.instance,
            None,
            LlmSpec {
                instance_id: root.instance,
                message_id: None,
                pending_id: Some(pending),
                turn_id: None,
            },
            false,
        )
        .await
        .expect("投放主节点");
    handle
}

/// feed、派发总线、资源适配与订阅端的组合
pub struct Bus {
    pub dispatcher: Arc<Dispatcher>,
    pub events: feed::Subscription,
    pub services: Arc<ResourceService>,
    outlet: FeedHandle,
    runtime: tokio::task::JoinHandle<std::result::Result<ShutdownReport, FeedError>>,
}

impl Bus {
    pub async fn open(fixture: &Fixture) -> Self {
        let dirs = fixture.dirs.clone();
        let (outlet, runtime) = feed::open(fixture.storage.clone(), FeedOptions::default())
            .await
            .expect("打开 feed");

        let roster = Arc::new(Roster::load(&fixture.storage).await.expect("读启用集合"));
        let directory = wind_event::directory()
            .merge(wind_core::agent::events::directory())
            .merge(wind_core::llm::events::directory())
            .merge(wind_mod::events::directory())
            .merge(wind_mcp::events::directory());
        let dispatcher = Arc::new(Dispatcher::new(
            directory,
            Arc::clone(&roster) as Arc<dyn ModRoster>,
            Arc::new(NoCaller),
            Arc::new(outlet.clone()),
            Limits::default(),
        ));

        let registry = wind_mcp::init(McpOptions::new(dirs.mods_root()), Arc::clone(&dispatcher));
        let installer = Installer::new(
            fixture.storage.clone(),
            Arc::clone(&dispatcher),
            roster,
            dirs.clone(),
            None,
        );
        let services = Arc::new(ResourceService::new(
            installer,
            Resources::new(fixture.storage.clone(), dirs.clone()),
            fixture.storage.clone(),
            registry,
            dirs.topics_root(),
        ));

        let observer_handle: Arc<dyn Observer> = dispatcher.clone();
        let runtime = tokio::spawn(runtime.run(vec![Arc::downgrade(&observer_handle)]));
        let events = outlet.subscribe().await.expect("订阅 feed").subscription;

        Self {
            dispatcher,
            events,
            services,
            outlet,
            runtime,
        }
    }

    pub async fn close(self) {
        self.services.shutdown().await.expect("关闭资源适配");
        self.outlet.shutdown().await.expect("关闭 feed");
        self.runtime
            .await
            .expect("feed 任务不能 panic")
            .expect("feed 必须干净退出");
    }
}

/// 没有脚本引擎时的 mod 调用端
pub struct NoCaller;

#[async_trait::async_trait]
impl ModCaller for NoCaller {
    async fn call(
        &self,
        _sub: &Subscription,
        _payload: Value,
        _cx: CallCx,
    ) -> Result<Option<Value>, CallError> {
        Err(CallError::Internal("mod calls are unavailable".to_string()))
    }
}

/// 取一条事件并按给定级别打印
pub async fn next_event(events: &mut feed::Subscription, level: log::Level) -> Arc<FeedEvent> {
    let event = timeout(Duration::from_secs(180), events.recv())
        .await
        .expect("事件必须在期限内到达")
        .expect("feed 不能提前关闭")
        .expect("feed 不能提前关闭");
    if event.envelope.name != wind_mcp::events::CORE_MCP_LOG.name {
        let text = match event.envelope.payload.as_ref() {
            Some(payload) => format!("{} {}\n{}", event.key, event.envelope.name, pretty(payload)),
            None => format!("{} {}", event.key, event.envelope.name),
        };
        log::log!(level, "{text}");
    }
    event
}

/// 多行缩进的 JSON，便于直接读结构
fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

/// 整棵树已静止
pub fn is_sealed(event: &FeedEvent, instance_id: i64) -> bool {
    event.envelope.name == wind_core::agent::events::CORE_AGENT_SEALED.name
        && event
            .envelope
            .payload
            .as_ref()
            .and_then(|payload| payload.get("instance_id"))
            .and_then(Value::as_i64)
            == Some(instance_id)
}

/// 指定实例的终态相位
pub fn terminal_phase(event: &FeedEvent, instance_id: i64) -> Option<String> {
    if event.envelope.name != wind_core::agent::events::CORE_AGENT_STATUS.name {
        return None;
    }
    let payload = event.envelope.payload.as_ref()?;
    if payload.get("instance_id")?.as_i64()? != instance_id {
        return None;
    }
    let phase = payload.get("phase")?.as_str()?;
    matches!(phase, "done" | "failed" | "aborted" | "cancelled").then(|| phase.to_string())
}

/// 取实例助手消息里的全部文本
pub async fn assistant_reply(storage: &Storage, instance: i64) -> String {
    let mut reply = String::new();
    for row in storage
        .list_instance_context(instance)
        .await
        .expect("读回上下文")
    {
        if row.block.role == Role::Assistant
            && let Body::Text { content } = row.block.body
        {
            reply.push_str(&content);
        }
    }
    reply
}

/// 取实例收到的全部工具结果
pub async fn tool_results(storage: &Storage, instance: i64) -> Vec<String> {
    let mut results = Vec::new();
    for row in storage
        .list_instance_context(instance)
        .await
        .expect("读回上下文")
    {
        if let Body::ToolResult { call_res, .. } = row.block.body {
            results.push(call_res.to_string());
        }
    }
    results
}

/// 取实例发出过的工具名
pub async fn tool_calls(storage: &Storage, instance: i64) -> Vec<String> {
    let mut calls = Vec::new();
    for row in storage
        .list_instance_context(instance)
        .await
        .expect("读回上下文")
    {
        if let Body::ToolCall { call_name, .. } = row.block.body {
            calls.push(call_name);
        }
    }
    calls
}

fn adapter() -> AdapterType {
    match env("TEST_ADAPTER").as_str() {
        "openai_completion" | "OpenAICompletion" => AdapterType::OpenAICompletion,
        "openai_response" | "OpenAIResponses" => AdapterType::OpenAIResponse,
        other => panic!("TEST_ADAPTER 不是可用适配器: {other}"),
    }
}

fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!(".env 缺少 {key}"))
}
