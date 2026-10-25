//! 验证一次运行只占一个 turn。工具循环的每一轮与子实例的任务轮都写回同一个 turn。

#[path = "common/lib.rs"]
mod common;

use common::store;
use std::io;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use wind_core::CoreError;
use wind_core::agent::behavior::{Behavior, ChildResult, Ctx, Input, Outcome};
use wind_core::agent::domain::{AgentDomain, AgentOutcome, AgentSpec};
use wind_core::agent::phase_codec;
use wind_core::agent::tree::Terminal;
use wind_core::agent::tree::phase::Phase;
use wind_core::conversation::assembler::ProjectionInvalidations;
use wind_core::conversation::writer::BlockWriter;
use wind_core::feed::hub::FeedHub;
use wind_core::llm::ports::{DeclaredTool, McpTools, ModRequests, Tools, TopicMods};
use wind_core::llm::tools::ToolOrigin;
use wind_core::llm::turn::{AgentRunner, DEFAULT_MAX_ROUNDS};
use wind_core::models::{
    AdapterType, AgentInstanceCreate, AgentPhase, AgentRole, ModelCreate, ProviderCreate,
    TopicCreate,
};
use wind_core::storage::Storage;
use wind_core::usecase::UseCases;
use wind_event::context::DispatchCx;
use wind_event::def::Directory;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::Scope;
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::subscription::{EnabledSet, Subscription};
use wind_wire::tool::FunctionTool;

/// 一次运行使用的夹具
struct Rig {
    /// 临时目录须被持有。一被丢弃，库文件随之删除
    _dir: TempDir,
    /// 存储，用来读已写入的 turn
    storage: Storage,
    /// 被驱动的 `AgentRunner`
    runner: AgentRunner,
    /// 本轮的实例标识
    instance_id: i64,
    /// 已派生的子实例启动参数
    child: Option<AgentSpec>,
}

impl Rig {
    /// 执行一轮。首次启动，不带待消费输入
    async fn run_one_round(&self) -> Result<Outcome<AgentDomain>, String> {
        let spec = AgentSpec {
            instance_id: self.instance_id,
            message_id: None,
            pending_id: None,
            turn_id: None,
        };
        self.run_spec(spec, 1).await
    }

    /// 让派生出的子实例运行一次
    async fn run_child_once(&self) -> Result<Outcome<AgentDomain>, String> {
        let spec = self.child.clone().expect("the child was derived");
        self.run_spec(spec, 2).await
    }

    /// 按给定的启动参数运行一次
    async fn run_spec(&self, spec: AgentSpec, job: u64) -> Result<Outcome<AgentDomain>, String> {
        let ctx = Ctx::new(spec.instance_id, job, CancellationToken::new());
        match self.runner.run(ctx, Input::Fresh(spec)).await {
            Outcome::Failed(failure) => Err(failure.error),
            outcome => Ok(outcome),
        }
    }

    /// 让派生出的子实例运行完毕，并把子实例的 `Phase` 写入存储供父节点对账
    async fn run_child_done(&self) -> Result<AgentOutcome, String> {
        let spec = self.child.clone().expect("the child was derived");
        let outcome = self.run_spec(spec.clone(), 2).await?;
        let Outcome::Completed(done) = outcome else {
            return Err(format!(
                "the child instance must finish, actually {outcome:?}"
            ));
        };
        let stored = phase_codec::encode(&Phase::Done(done.clone())).expect("encode");
        self.storage
            .agent_save_phase(
                spec.instance_id,
                stored.name.parse().expect("encoded phase"),
                stored
                    .payload
                    .as_ref()
                    .map(serde_json::Value::to_string)
                    .as_deref(),
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(done)
    }

    /// 子任务结束后以 `Children` 重入父节点
    async fn run_children_once(
        &self,
        child: i64,
        done: AgentOutcome,
    ) -> Result<Outcome<AgentDomain>, String> {
        let input = Input::Children(vec![ChildResult {
            node: child,
            terminal: Terminal::Done(done),
        }]);
        let ctx = Ctx::new(self.instance_id, 3, CancellationToken::new());
        match self.runner.run(ctx, input).await {
            Outcome::Failed(failure) => Err(failure.error),
            outcome => Ok(outcome),
        }
    }

    /// 一个实例写入了几个 turn
    async fn turn_count(&self, instance_id: i64) -> usize {
        self.storage
            .turn_list_by_instance(instance_id)
            .await
            .expect("read this instance's turn")
            .len()
    }

    /// 子实例写入了几个 turn
    async fn turn_count_of_child(&self) -> usize {
        let child = self.child.as_ref().expect("the child was derived");
        self.turn_count(child.instance_id).await
    }

    /// 建一套夹具。含临时库、真实总线、假接口与按次数应答的服务
    ///
    /// `replies` 按请求次序取，用完之后重复使用最后一项
    async fn build(replies: Vec<Reply>, tools: Vec<(&str, &str)>) -> Self {
        let (dir, storage) = store().await;
        let server = Server::serve(replies).await;
        // 提供商、模型、话题与实例都由夹具直接写入存储。装配过程不属于本文件的验证范围
        let provider = storage
            .provider_create(ProviderCreate {
                name: "deepseek".into(),
                base_url: server.base_url(),
                ..Default::default()
            })
            .await
            .expect("create provider");
        let model = storage
            .model_create(ModelCreate::new(
                provider,
                "deepseek-chat".into(),
                AdapterType::OpenAICompletion,
            ))
            .await
            .expect("build model");
        let topic_id = storage
            .topic_create(TopicCreate {
                label: "test".into(),
                ..Default::default()
            })
            .await
            .expect("create topic");
        // 话题绑定模型。存储层没有提供对应的写入接口，因此夹具直接更新 `model_id` 列
        sqlx::query("UPDATE topics SET model_id = ? WHERE id = ?")
            .bind(model)
            .bind(topic_id)
            .execute(storage.pool())
            .await
            .expect("bind the model");
        let instance_id = storage
            .agent_create(AgentInstanceCreate {
                parent_id: None,
                topic_id,
                role: AgentRole::Main,
                mode: None,
                phase: AgentPhase::Running,
                phase_payload: None,
            })
            .await
            .expect("create instance");

        let hub = FeedHub::open(storage.clone(), None)
            .await
            .expect("open hub");
        let bus = Arc::new(Dispatcher::new(
            directory(),
            Arc::new(NoEnablement),
            Arc::new(PassHost),
            Limits::default(),
        ));
        let runner = AgentRunner::new(
            BlockWriter::new(
                hub,
                bus.clone(),
                storage.clone(),
                ProjectionInvalidations::default(),
            ),
            UseCases::new(storage.clone(), bus.clone()),
            bus,
            Arc::new(NoMods),
            Some(Arc::new(FakeMcp::declaring(tools))),
            Arc::new(NoRequests),
            storage.clone(),
            DEFAULT_MAX_ROUNDS,
        );
        Self {
            _dir: dir,
            storage: storage.clone(),
            runner,
            instance_id,
            child: None,
        }
    }
}

/// 一个回一次工具调用、再回一段文本的模型
async fn rig_returning_one_tool_call_then_text() -> Rig {
    Rig::build(
        vec![
            calls_reply(&[("c-1", "fs__read", r#"{"path":"/tmp/a"}"#)]),
            text_reply("done reading"),
        ],
        vec![("fs", "read")],
    )
    .await
}

/// 一个已经派生出一个同步子实例的模型
async fn rig_with_a_spawned_child() -> Rig {
    let mut rig = Rig::build(
        vec![
            calls_reply(&[(
                "c-1",
                "windai_spawn_agent",
                r#"{"mode":"sync","task":"check the weather"}"#,
            )]),
            text_reply("ok"),
            text_reply("continue"),
        ],
        Vec::new(),
    )
    .await;
    let outcome = rig.run_one_round().await.expect("run to completion");
    let Outcome::Spawn { spawn } = outcome else {
        panic!("expected spawn result for tree");
    };
    assert_eq!(spawn.len(), 1, "one call spawns once");
    rig.child = Some(spawn[0].spec.clone());
    rig
}

#[tokio::test]
async fn a_tool_loop_stays_in_one_turn() {
    let rig = rig_returning_one_tool_call_then_text().await;
    let _ = rig.run_one_round().await.expect("run to completion");
    assert_eq!(
        rig.turn_count(1).await,
        1,
        "the tool loop must not create a new turn per round"
    );
}

/// 子任务结束后的续写属于派生调用所在的 turn，不另开 turn
#[tokio::test]
async fn a_children_reentry_stays_in_the_turn_that_spawned() {
    let rig = rig_with_a_spawned_child().await;
    let child = rig
        .child
        .as_ref()
        .expect("the child was derived")
        .instance_id;
    let done = rig.run_child_done().await.expect("finish child run");
    let outcome = rig
        .run_children_once(child, done)
        .await
        .expect("resume parent");
    assert!(matches!(outcome, Outcome::Completed(_)));
    assert_eq!(
        rig.turn_count(rig.instance_id).await,
        1,
        "the continuation must stay in the round where the spawn call happened and must not open a new round"
    );
}

#[tokio::test]
async fn a_child_task_round_uses_the_turn_its_task_message_started() {
    let rig = rig_with_a_spawned_child().await;
    let _ = rig.run_child_once().await.expect("run to completion");
    let child_turns = rig.turn_count_of_child().await;
    assert_eq!(
        child_turns, 1,
        "the child instance's task and reply are in the same turn"
    );
}

// 服务端的一次应答

/// 一段应答体
struct Reply {
    /// 写回去的字节
    body: String,
}

impl Reply {
    /// 构造一段完整应答
    fn whole(body: String) -> Self {
        Self { body }
    }
}

/// 流结束的哨兵行
const DONE_LINE: &str = "data: [DONE]\n\n";

/// 一片增量拼成的数据行
fn delta_line(delta: serde_json::Value) -> String {
    let chunk = serde_json::json!({
        "id": "x",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "deepseek-chat",
        "choices": [{ "index": 0, "delta": delta }],
    });
    format!("data: {chunk}\n\n")
}

/// 一片文本增量的内容
fn text_delta(text: &str) -> serde_json::Value {
    serde_json::json!({ "role": "assistant", "content": text })
}

/// 构造一段文本增量的 SSE
fn text_reply(text: &str) -> Reply {
    Reply::whole(format!("{}{DONE_LINE}", delta_line(text_delta(text))))
}

/// 构造一段工具调用增量的 SSE，一条调用占一片
fn calls_reply(calls: &[(&str, &str, &str)]) -> Reply {
    let body: String = calls
        .iter()
        .map(|(id, name, arguments)| delta_line(call_delta(id, name, arguments)))
        .collect();
    Reply::whole(format!("{body}{DONE_LINE}"))
}

/// 一片工具调用增量的内容
fn call_delta(id: &str, name: &str, arguments: &str) -> serde_json::Value {
    serde_json::json!({
        "role": "assistant",
        "tool_calls": [{
            "index": 0,
            "id": id,
            "type": "function",
            "function": { "name": name, "arguments": arguments },
        }],
    })
}

/// 一个按请求次数给出不同响应体的最小 HTTP 应答器
struct Server {
    /// 监听的基地址
    base_url: String,
}

impl Server {
    /// 服务的基地址
    fn base_url(&self) -> String {
        self.base_url.clone()
    }

    /// 启动服务并逐个处理连接
    ///
    /// 只服务本文件的用例。不分块，不支持 keep-alive，写完即关闭连接
    async fn serve(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
        let base_url = format!(
            "http://{}",
            listener.local_addr().expect("has a local address")
        );
        let mut replies = replies.into_iter();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let _ = read_request(&mut socket).await;
                if let Some(reply) = replies.next() {
                    let _ = respond(&mut socket, &reply.body).await;
                }
                let _ = socket.shutdown().await;
            }
        });
        Self { base_url }
    }
}

/// 把一次请求读完，头部与实体都读掉
///
/// 不读实体时，请求方会在仍在发送时看到连接被关闭
async fn read_request(socket: &mut TcpStream) -> Option<String> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(end) = head_end(&buffer) {
            let head = String::from_utf8_lossy(&buffer[..end]).into_owned();
            let wanted = end + body_len(&head);
            if buffer.len() >= wanted {
                return Some(String::from_utf8_lossy(&buffer[end..wanted]).into_owned());
            }
        }
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

/// 头部结束的位置，即空行之后的偏移
fn head_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|at| at + 4)
}

/// 头部里声明的请求体长度
fn body_len(head: &str) -> usize {
    head.lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .find_map(|(_, value)| value.trim().parse().ok())
        .unwrap_or(0)
}

/// 写回一次应答。先写头部再写实体，随后关闭连接
async fn respond(socket: &mut TcpStream, body: &str) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    socket.flush().await
}

// 总线上的替身

/// 不启用任何 mod
struct NoEnablement;

impl ModEnablement for NoEnablement {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::none()
    }
}

/// 一律放行的宿主
struct PassHost;

#[async_trait::async_trait]
impl ModHost for PassHost {
    async fn call(
        &self,
        _sub: &Subscription,
        _payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        Ok(None)
    }
}

/// 合并 llm 与会话目录得到的事件目录
fn directory() -> Directory {
    wind_core::llm::directory().merge(wind_core::conversation::directory())
}

// 三个接口的替身

/// 不声明任何片段与工具的 `TopicMods`
struct NoMods;

#[async_trait::async_trait]
impl TopicMods for NoMods {
    async fn prompt_fragments(&self, _topic_id: i64) -> Result<Vec<String>, CoreError> {
        Ok(Vec::new())
    }

    async fn agent_tools(&self, _topic_id: i64) -> Result<Vec<DeclaredTool>, CoreError> {
        Ok(Vec::new())
    }

    async fn version_of(&self, _mod_name: &str) -> Result<i64, CoreError> {
        Ok(1)
    }
}

/// 按工具名声明工具、并把调用参数原样交回的 `McpTools` 实现
struct FakeMcp {
    /// 本服务里可用的工具
    declared: Vec<DeclaredTool>,
}

impl FakeMcp {
    /// 按 (服务名, 工具名) 构造一份声明
    fn declaring(tools: Vec<(&str, &str)>) -> Self {
        let declared = tools
            .into_iter()
            .map(|(server, tool)| DeclaredTool {
                origin: ToolOrigin::Mcp {
                    server: server.to_string(),
                    tool: tool.to_string(),
                },
                declaration: Tools::Function(FunctionTool {
                    name: format!("{server}__{tool}"),
                    description: None,
                    parameters: None,
                    strict: None,
                }),
            })
            .collect();
        Self { declared }
    }
}

#[async_trait::async_trait]
impl McpTools for FakeMcp {
    async fn declarations(
        &self,
        _cx: &DispatchCx,
        _topic_id: i64,
    ) -> Result<Vec<DeclaredTool>, CoreError> {
        Ok(self.declared.clone())
    }

    async fn call(
        &self,
        _cx: &DispatchCx,
        _call_id: &str,
        _server: &str,
        tool: &str,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        Ok(serde_json::json!({ "tool": tool, "arguments": arguments }))
    }
}

/// 不会被调用的 `ModRequests` 实现
struct NoRequests;

#[async_trait::async_trait]
impl ModRequests for NoRequests {
    async fn call(
        &self,
        _scope: &Scope,
        _mod_name: &str,
        _version: i64,
        _request: &str,
        _input: serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        Err(CoreError::Denied(
            "this test has no request runtime".to_string(),
        ))
    }
}
