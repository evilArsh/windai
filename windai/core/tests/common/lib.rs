//! 集成测试共用的播种、替身与事件订阅
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
use wind_core::error::CoreError;
use wind_core::feed::{self, FeedError, FeedHandle, FeedOptions, ShutdownReport};
use wind_core::llm::domain::{LlmDomain, LlmSpec};
use wind_core::llm::events::CORE_LLM_REQUEST;
use wind_core::llm::tools::{DeclaredTool, ToolOrigin};
use wind_core::llm::{LlmRunner, McpService, ModService};
use wind_event::{
    CallCx, CallError, DispatchCx, Dispatcher, EnabledSet, Limits, ModCaller, ModRoster, Scope,
    Subscription,
};
use wind_storage::Storage;
use wind_wire::event::FeedEvent;
use wind_wire::tool::{FunctionTool, Tools};
use wind_wire::{AdapterType, AgentRole, Body, Content, PendingKind, Role};

/// 事件替身的 mod 名
pub const SPY_MOD: &str = "spy";

/// 工具替身的 mod 名
pub const FS_MOD: &str = "fs";

/// 列 home 目录的 request 名
pub const LIST_REQUEST: &str = "list_home";

/// 按 .env 的 RUST_LOG 打开日志
pub fn init_log() {
    dotenvy::dotenv().expect("仓库根目录的 .env 必须可读");
    let _ = env_logger::builder()
        .parse_env(env_logger::Env::default().default_filter_or("info"))
        .try_init();
}

/// 每次运行都从空的数据库开始
pub async fn open_storage(name: &str) -> Storage {
    let dir = dirs::home_dir()
        .expect("需要 home 目录")
        .join(".windai_test");
    tokio::fs::create_dir_all(&dir).await.expect("建测试目录");
    let path = dir.join(format!("{name}.db"));
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.clone().into_os_string();
        file.push(suffix);
        let _ = tokio::fs::remove_file(PathBuf::from(file)).await;
    }
    Storage::open(&path).await.expect("打开测试数据库")
}

/// 按 .env 建提供商、凭证与模型
pub async fn seed_provider(storage: &Storage) -> i64 {
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

/// 建话题、主实例与一条待消费输入，返回话题、实例与输入标识
pub async fn seed_root(
    storage: &Storage,
    label: &str,
    model: i64,
    prompt: &str,
) -> (i64, i64, i64) {
    let topic = storage
        .create_topic(None, label, Some(model), None)
        .await
        .expect("建话题");
    let instance = storage
        .create_agent_instance(topic, None, AgentRole::Main, None, "running", None)
        .await
        .expect("建主实例");
    let pending = storage
        .create_pending(
            instance,
            PendingKind::User,
            vec![Content::new_text(prompt.to_string())],
            None,
        )
        .await
        .expect("放一条待消费输入");
    (topic, instance, pending)
}

/// 挂上 LlmRunner 并投放主节点
pub async fn start_agent(
    storage: &Storage,
    dispatcher: Arc<Dispatcher>,
    (topic, instance, pending): (i64, i64, i64),
) -> AgentHandle<LlmDomain> {
    let runner = Arc::new(LlmRunner::new(
        storage.clone(),
        Arc::clone(&dispatcher),
        Arc::new(Services),
        Arc::new(Services),
    ));
    let handle = Agent::<LlmDomain>::start(AgentOptions {
        topic_id: topic,
        storage: storage.clone(),
        dispatcher,
        runner,
        restored: Vec::new(),
        cancel: CancellationToken::new(),
    });
    handle
        .spawn(
            instance,
            None,
            LlmSpec {
                instance_id: instance,
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

/// feed、派发总线与订阅端的组合
pub struct Bus {
    pub dispatcher: Arc<Dispatcher>,
    pub events: feed::Subscription,
    outlet: FeedHandle,
    runtime: tokio::task::JoinHandle<std::result::Result<ShutdownReport, FeedError>>,
}

impl Bus {
    pub async fn open(storage: &Storage) -> Self {
        let (outlet, runtime) = feed::open(storage.clone(), FeedOptions::default())
            .await
            .expect("打开 feed");
        let runtime = tokio::spawn(runtime.run(Vec::new()));
        let events = outlet.subscribe().await.expect("订阅 feed").subscription;
        let directory = wind_event::directory()
            .merge(wind_core::agent::events::directory())
            .merge(wind_core::llm::events::directory());
        let dispatcher = Arc::new(Dispatcher::new(
            directory,
            Arc::new(Spy),
            Arc::new(Spy),
            Arc::new(outlet.clone()),
            Limits::default(),
        ));
        dispatcher.rebuild_index(spy_subscriptions());
        Self {
            dispatcher,
            events,
            outlet,
            runtime,
        }
    }

    pub async fn close(self) {
        self.outlet.shutdown().await.expect("关闭 feed");
        self.runtime
            .await
            .expect("feed 任务不能 panic")
            .expect("feed 必须干净退出");
    }
}

/// 取一条事件并按 RUST_LOG 打印
pub async fn next_event(events: &mut feed::Subscription) -> Arc<FeedEvent> {
    let event = timeout(Duration::from_secs(180), events.recv())
        .await
        .expect("事件必须在期限内到达")
        .expect("feed 不能提前关闭")
        .expect("feed 不能提前关闭");
    match event.envelope.payload.as_ref() {
        Some(payload) => log::info!("{} {}\n{}", event.key, event.envelope.name, pretty(payload)),
        None => log::info!("{} {}", event.key, event.envelope.name),
    }
    event
}

/// 多行缩进的 JSON，便于直接读结构
fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
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

/// 事件派发端替身：订阅 llm 请求，把请求体写进日志
pub struct Spy;

impl ModRoster for Spy {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::from_names([SPY_MOD.to_string()])
    }
}

#[async_trait::async_trait]
impl ModCaller for Spy {
    async fn call(
        &self,
        sub: &Subscription,
        payload: Value,
        _cx: CallCx,
    ) -> Result<Option<Value>, CallError> {
        log::info!("[spy] {}\n{}", sub.event, pretty(&payload));
        Ok(None)
    }
}

fn spy_subscriptions() -> Vec<Subscription> {
    vec![
        Subscription {
            mod_name: SPY_MOD.to_string(),
            mod_version: 0,
            event: CORE_LLM_REQUEST.name,
            handler: "log".to_string(),
            timeout_ms: 5_000,
            rank: 0,
        },
        Subscription {
            mod_name: SPY_MOD.to_string(),
            mod_version: 0,
            event: wind_core::llm::events::CORE_RULE_APPLY.name,
            handler: "log".to_string(),
            timeout_ms: 5_000,
            rank: 0,
        },
    ]
}

/// llm 侧替身：一条提示词与一个列 home 目录的工具
pub struct Services;

#[async_trait::async_trait]
impl ModService for Services {
    async fn prompt_fragments(&self, _topic_id: i64) -> Result<Vec<String>, CoreError> {
        Ok(vec!["回答保持简短".to_string()])
    }

    async fn agent_tools(&self, _topic_id: i64) -> Result<Vec<DeclaredTool>, CoreError> {
        Ok(vec![list_home_tool()])
    }

    async fn version_of(&self, _mod_name: &str) -> Result<i64, CoreError> {
        Ok(1)
    }

    async fn call(
        &self,
        _scope: &Scope,
        _mod_name: &str,
        _version: i64,
        request: &str,
        _input: Value,
    ) -> Result<Value, CoreError> {
        match request {
            LIST_REQUEST => Ok(list_home()),
            other => Err(CoreError::Denied(format!("unknown request {other}"))),
        }
    }
}

#[async_trait::async_trait]
impl McpService for Services {
    async fn declarations(
        &self,
        _cx: &DispatchCx,
        _topic_id: i64,
    ) -> Result<Vec<DeclaredTool>, CoreError> {
        Ok(Vec::new())
    }

    async fn call(
        &self,
        _cx: &DispatchCx,
        _call_id: &str,
        _server: &str,
        _tool: &str,
        _arguments: Value,
    ) -> Result<Value, CoreError> {
        Ok(Value::Null)
    }
}

fn list_home_tool() -> DeclaredTool {
    let origin = ToolOrigin::ModRequest {
        mod_name: FS_MOD.to_string(),
        request: LIST_REQUEST.to_string(),
    };
    DeclaredTool {
        declaration: Tools::Function(FunctionTool {
            name: origin.exposed(),
            description: Some(
                "List the entry names of the current user's home directory (~)".to_string(),
            ),
            parameters: Some(serde_json::json!({ "type": "object", "properties": {} })),
            strict: None,
        }),
        origin,
    }
}

fn list_home() -> Value {
    serde_json::json!({
        "path": dirs::home_dir().expect("需要 home 目录").to_string_lossy(),
        "entries": home_entries(),
    })
}

/// home 目录的全部条目名，已排序
pub fn home_entries() -> Vec<String> {
    let home = dirs::home_dir().expect("需要 home 目录");
    let mut entries: Vec<String> = std::fs::read_dir(home)
        .expect("读 home 目录")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    entries.sort();
    entries
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
