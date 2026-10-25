//! 验证一轮请求的驱动。跨三个接口完成一次往返。

#[path = "common/lib.rs"]
mod common;

use common::store;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use wind_core::CoreError;
use wind_core::agent::behavior::{Behavior, Ctx, Input, Outcome};
use wind_core::agent::domain::{AgentDomain, AgentSpec};
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
use wind_event::def::{Directory, EventMeta};
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::Scope;
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::observer::BuiltinObserver;
use wind_event::subscription::{EnabledSet, Phase as SubPhase, Subscription};
use wind_wire::tool::FunctionTool;

/// 一次运行使用的夹具
struct Rig {
    /// 临时目录须被持有。一被丢弃，库文件随之删除
    _dir: TempDir,
    /// 库，用来读已提交的块
    storage: Storage,
    /// 被驱动的 `AgentRunner`
    runner: AgentRunner,
    /// 模型应答的来源
    server: Server,
    /// 记录被派发过的载荷的宿主
    host: Arc<Host>,
    /// 见过的每一条事件
    observer: Arc<Observer>,
    /// 本轮的实例标识
    instance_id: i64,
    /// 本轮写入的消息标识。给定时接着写该消息
    message_id: Option<i64>,
}

impl Rig {
    /// 执行一轮
    ///
    /// 失败从 `Outcome::Failed` 中取出。调用方是树，错误只出现在判定里
    async fn run_one_round(&self) -> Result<Outcome<AgentDomain>, String> {
        let ctx = Ctx::new(self.instance_id, 1, CancellationToken::new());
        let input = Input::Fresh(AgentSpec {
            instance_id: self.instance_id,
            message_id: self.message_id,
            pending_id: None,
            turn_id: None,
        });
        match self.runner.run(ctx, input).await {
            Outcome::Failed(failure) => Err(failure.error),
            outcome => Ok(outcome),
        }
    }

    /// 模型被请求了几次
    fn rounds(&self) -> usize {
        self.server.requests()
    }

    /// 被派发过的 (事件名, 载荷)
    fn seen_payloads(&self) -> Vec<(String, serde_json::Value)> {
        self.host.seen()
    }

    /// 服务端收到过的请求体，按次序
    fn sent_bodies(&self) -> Vec<String> {
        self.server.bodies()
    }

    /// 已提交的块数，按 `core/block.committed` 数
    fn committed_block_count(&self) -> usize {
        self.observer.count("core/block.committed")
    }

    /// 已定稿的消息数，按 `core/message.seal` 数
    fn sealed_message_count(&self) -> usize {
        self.observer.count("core/message.seal")
    }

    /// 某条事件被派发到宿主过几次
    fn dispatch_count(&self, name: &str) -> usize {
        self.host
            .seen()
            .iter()
            .filter(|(event, _)| event == name)
            .count()
    }

    /// 第二轮上下文里助手消息的文本
    ///
    /// 取自第二次 `core/llm.request` 的载荷，即上一轮返回的消息
    fn next_round_context_text(&self) -> String {
        let requests: Vec<serde_json::Value> = self
            .host
            .seen()
            .into_iter()
            .filter(|(name, _)| name == "core/llm.request")
            .map(|(_, payload)| payload)
            .collect();
        let second = requests.get(1).expect("dispatch llm.request twice");
        let contexts = second["request"]["contexts"]
            .as_array()
            .expect("read unified message sequence");
        contexts
            .iter()
            .find(|message| message["role"] == serde_json::json!("assistant"))
            .and_then(|message| message["content"].as_array())
            .and_then(|content| content.iter().find_map(|item| item["data"].as_str()))
            .expect("find assistant text message")
            .to_string()
    }

    /// 存储中已提交的块拼成的文本
    async fn committed_blocks_text(&self) -> String {
        let turns = self
            .storage
            .turn_list_by_instance(self.instance_id)
            .await
            .expect("read this instance's turn");
        let message_ids = self
            .storage
            .message_list_by_turns(&turns.iter().map(|turn| turn.id).collect::<Vec<_>>())
            .await
            .expect("read messages of turn")
            .iter()
            .map(|message| message.id)
            .collect::<Vec<_>>();
        self.storage
            .block_list_by_messages(&message_ids)
            .await
            .expect("read the committed block")
            .iter()
            .map(|block| block.content.clone())
            .collect::<Vec<_>>()
            .join("\n")
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
        let observer = Arc::new(Observer::default());
        let host = Arc::new(Host::default());
        let bus = Arc::new(Dispatcher::new(
            directory(),
            Arc::new(AllEnabled),
            host.clone(),
            Limits::default(),
        ));
        bus.rebuild_index(subscriptions());
        bus.attach_observer(observer.clone());

        let runner = AgentRunner::new(
            BlockWriter::new(
                hub,
                bus.clone(),
                storage.clone(),
                ProjectionInvalidations::default(),
            ),
            UseCases::new(storage.clone(), bus.clone()),
            bus.clone(),
            Arc::new(FakeMods),
            Some(Arc::new(FakeMcp::declaring(tools))),
            Arc::new(FakeRequests),
            storage.clone(),
            DEFAULT_MAX_ROUNDS,
        );
        Self {
            _dir: dir,
            storage: storage.clone(),
            runner,
            server,
            host,
            observer,
            instance_id,
            message_id: None,
        }
    }
}

/// 一个只回一段文本的模型
async fn rig_returning_text(text: &str) -> Rig {
    Rig::build(vec![text_reply(text)], Vec::new()).await
}

/// 一个回一次派生调用的模型
async fn rig_returning_spawn_call() -> Rig {
    Rig::build(
        vec![
            calls_reply(&[(
                "c-1",
                "windai_spawn_agent",
                r#"{"mode":"sync","task":"check the weather"}"#,
            )]),
            text_reply("ok"),
        ],
        Vec::new(),
    )
    .await
}

/// 一个流到第二片就断掉的模型
async fn rig_whose_stream_breaks_after_two_chunks() -> Rig {
    Rig::build(
        vec![broken_reply(&["first chunk", "second chunk"])],
        Vec::new(),
    )
    .await
}

/// 一个回两条工具调用的模型，两条都声明在 MCP 接口上
async fn rig_returning_two_calls_and_recording_payloads() -> Rig {
    Rig::build(
        vec![
            calls_reply(&[
                ("c-1", "fs__read", r#"{"path":"/tmp/a"}"#),
                ("c-2", "fs__write", r#"{"path":"/tmp/b"}"#),
            ]),
            text_reply("all done"),
        ],
        vec![("fs", "read"), ("fs", "write")],
    )
    .await
}

/// 一个把一段文本分成两片流回来的模型
async fn rig_returning_two_chunks_of_text() -> Rig {
    Rig::build(
        vec![text_chunks_reply(&["first chunk", "second chunk"])],
        Vec::new(),
    )
    .await
}

/// 一个宿主会改掉整流后消息文本的模型
///
/// 第一轮除文本外还带一条工具调用，因此这一轮之后还有第二轮。第二轮按给定的文本结束
async fn rig_whose_host_rewrites_the_response_message(rewritten: &str) -> Rig {
    let rig = Rig::build(
        vec![
            text_and_call_reply("original text", ("c-1", "fs__read", r#"{"path":"/tmp/a"}"#)),
            text_reply("ok"),
        ],
        vec![("fs", "read")],
    )
    .await;
    rig.host.rewrite_response_with(rewritten);
    rig
}

#[tokio::test]
async fn a_round_without_tool_calls_completes_in_one_pass() {
    let rig = rig_returning_text("What time is it? I don't know").await;
    let outcome = rig.run_one_round().await.expect("run to completion");
    assert!(matches!(outcome, Outcome::Completed(_)));
    assert_eq!(rig.rounds(), 1);
}

#[tokio::test]
async fn a_spawn_call_ends_the_round_with_a_spawn_outcome() {
    // 本轮不得为派生调用生成 tool_result，也不得继续下一轮
    let rig = rig_returning_spawn_call().await;
    let outcome = rig.run_one_round().await.expect("run to completion");
    assert!(
        matches!(outcome, Outcome::Spawn { .. }),
        "a spawn must be handed back to the tree"
    );
    assert_eq!(rig.rounds(), 1);

    let Outcome::Spawn { spawn } = outcome else {
        panic!("expected a spawn result for the tree");
    };
    assert_eq!(spawn.len(), 1, "one call spawns once");
    assert_eq!(
        spawn[0].spec.instance_id, spawn[0].id,
        "the instance id and the node id are the same value"
    );
    assert!(
        !spawn[0].detached,
        "sync mode enters the parent node's waiting barrier"
    );
}

#[tokio::test]
async fn a_stream_that_fails_midway_still_commits_and_seals_what_arrived() {
    let rig = rig_whose_stream_breaks_after_two_chunks().await;
    let error = rig.run_one_round().await.expect_err("must fail");
    assert!(error_message(&error).contains("transfer"), "{error:?}");
    assert!(
        rig.committed_block_count() > 0,
        "already produced blocks must be committed"
    );
    assert_eq!(rig.sealed_message_count(), 1, "the message must be sealed");
}

#[tokio::test]
async fn the_two_intervention_points_see_their_own_side_of_the_request() {
    // `rule.apply` 收到提供商结构，`llm.request` 收到统一结构
    let rig = rig_returning_two_calls_and_recording_payloads().await;
    let _ = rig.run_one_round().await.expect("run to completion");

    let seen = rig.seen_payloads();
    let rule = seen
        .iter()
        .find(|(name, _)| *name == "core/rule.apply")
        .expect("dispatch request");
    let request = seen
        .iter()
        .find(|(name, _)| *name == "core/llm.request")
        .expect("dispatch request");
    assert!(
        rule.1.get("adapter").is_some(),
        "the provider side carries the adapter name: {}",
        rule.1
    );
    assert!(
        rule.1["body"]["model"].is_string(),
        "the provider side is already its own request body"
    );
    assert!(
        request.1["request"]["contexts"].is_array(),
        "the unified side carries a message sequence, not the provider request body: {}",
        request.1
    );

    // 两侧的改写都必须进入实际请求。统一结构侧的改写经适配器进入请求体，
    // 提供商结构侧的改写直接位于请求体中
    let sent = rig.sent_bodies();
    let first = sent.first().expect("send request");
    assert!(
        first.contains(ADDED_BY_HOST),
        "a rewrite on the unified side must go through the adapter into the sent request body: {first}"
    );
    assert!(
        first.contains(ADDED_FIELD),
        "a rewrite on the provider side must be right in the sent request body: {first}"
    );
}

#[tokio::test]
async fn a_round_dispatches_the_response_event_exactly_once() {
    let rig = rig_returning_two_chunks_of_text().await;
    let _ = rig.run_one_round().await.expect("run to completion");
    assert_eq!(
        rig.dispatch_count("core/llm.response"),
        1,
        "once after rectification, not per fragment"
    );
}

#[tokio::test]
async fn the_rewritten_response_is_what_the_next_round_uses() {
    // 宿主的改写影响下一轮上下文与工具采纳，但改不了已经进流的块
    let rig = rig_whose_host_rewrites_the_response_message("rewritten").await;
    let _ = rig.run_one_round().await.expect("run to completion");
    assert_eq!(rig.next_round_context_text(), "rewritten");
    assert!(
        rig.committed_blocks_text().await.contains("original"),
        "blocks that already entered the stream are not rewritten retroactively"
    );
}

fn error_message(error: &str) -> &str {
    error
}

// 服务端的一次应答

/// 一次应答，包含实体与声明长度
struct Reply {
    /// 写回去的字节
    body: String,
    /// 头部声明的长度。与实体不符时，请求方会在中途读到错误
    declared: usize,
}

impl Reply {
    /// 构造一段完整应答
    fn whole(body: String) -> Self {
        let declared = body.len();
        Self { body, declared }
    }

    /// 构造一段中途断掉的应答。声明长度大于实体，写完实体即关闭连接
    fn broken(body: String) -> Self {
        let declared = body.len() + 64;
        Self { body, declared }
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

/// 一段文本的 SSE，分几片给出
fn text_chunks_reply(chunks: &[&str]) -> Reply {
    let body: String = chunks
        .iter()
        .map(|text| delta_line(text_delta(text)))
        .collect();
    Reply::whole(format!("{body}{DONE_LINE}"))
}

/// 一段文本加一条工具调用的 SSE，文本在前
fn text_and_call_reply(text: &str, call: (&str, &str, &str)) -> Reply {
    let (id, name, arguments) = call;
    Reply::whole(format!(
        "{}{}{DONE_LINE}",
        delta_line(text_delta(text)),
        delta_line(call_delta(id, name, arguments))
    ))
}

/// 构造一段工具调用增量的 SSE，一条调用占一片
fn calls_reply(calls: &[(&str, &str, &str)]) -> Reply {
    let body: String = calls
        .iter()
        .map(|(id, name, arguments)| delta_line(call_delta(id, name, arguments)))
        .collect();
    Reply::whole(format!("{body}{DONE_LINE}"))
}

/// 构造一段写完就断的 SSE。没有结束哨兵，声明长度大于实发字节
fn broken_reply(chunks: &[&str]) -> Reply {
    let body: String = chunks
        .iter()
        .map(|text| delta_line(text_delta(text)))
        .collect();
    Reply::broken(body)
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
    /// 收到的请求次数
    requests: Arc<AtomicUsize>,
    /// 收到过的请求体，按次序
    bodies: Arc<Mutex<Vec<String>>>,
    /// 监听的基地址
    base_url: String,
}

impl Server {
    /// 服务的基地址
    fn base_url(&self) -> String {
        self.base_url.clone()
    }

    /// 收到过几次请求
    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    /// 收到过的请求体，按次序
    fn bodies(&self) -> Vec<String> {
        self.bodies.lock().expect("the record is usable").clone()
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
        let requests = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&requests);
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&bodies);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let index = counted.fetch_add(1, Ordering::SeqCst);
                let reply = replies.get(index).or_else(|| replies.last());
                if let Some(request) = read_request(&mut socket).await
                    && let Ok(mut bodies) = recorded.lock()
                {
                    bodies.push(request);
                }
                if let Some(reply) = reply {
                    let _ = respond(&mut socket, &reply.body, reply.declared).await;
                }
                let _ = socket.shutdown().await;
            }
        });
        Self {
            requests,
            bodies,
            base_url,
        }
    }
}

/// 把一次请求读完，头部与实体都读掉，返回实体原文
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
async fn respond(socket: &mut TcpStream, body: &str, declared: usize) -> io::Result<()> {
    let head =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {declared}\r\nConnection: close\r\n\r\n");
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    socket.flush().await
}

// 总线上的替身

/// 见过的事件。每到达一条记一条
#[derive(Default)]
struct Observer {
    /// (事件名, 载荷)
    seen: Mutex<Vec<(String, serde_json::Value)>>,
}

impl Observer {
    /// 某条事件见过几次
    fn count(&self, name: &str) -> usize {
        self.seen
            .lock()
            .expect("the record is usable")
            .iter()
            .filter(|(event, _)| event == name)
            .count()
    }
}

#[async_trait::async_trait]
impl BuiltinObserver for Observer {
    async fn on_event(&self, _meta: &EventMeta, event: &serde_json::Value) {
        let name = event["name"].as_str().unwrap_or_default().to_string();
        self.seen
            .lock()
            .expect("the record is usable")
            .push((name, event["payload"].clone()));
    }
}

/// 记录每次派发的载荷，并在两个接口上各改写一处
///
/// 两处改写用于确认改写确实生效。统一结构侧增加一条消息，提供商结构侧增加一个
/// 字段。两者形状不同，若在同一位置派发就会暴露
///
/// 整流后消息的文本另有一处可选改写，默认不启用。该改写只进入下一轮上下文，
/// 改不了已经进流的块，工具调用保持原样，使这一轮之后还有一轮
#[derive(Default)]
struct Host {
    /// 被派发过的 (事件名, 载荷)
    seen: Mutex<Vec<(String, serde_json::Value)>>,
    /// 给定时把整流后消息的文本改成该值
    rewritten_response: Mutex<Option<String>>,
}

impl Host {
    /// 被派发过的 (事件名, 载荷)
    fn seen(&self) -> Vec<(String, serde_json::Value)> {
        self.seen.lock().expect("the record is usable").clone()
    }

    /// 让宿主把整流后消息的文本改成给定内容
    fn rewrite_response_with(&self, text: &str) {
        *self
            .rewritten_response
            .lock()
            .expect("the record is usable") = Some(text.to_string());
    }
}

#[async_trait::async_trait]
impl ModHost for Host {
    async fn call(
        &self,
        sub: &Subscription,
        mut payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        self.seen
            .lock()
            .expect("the record is usable")
            .push((sub.event.to_string(), payload.clone()));
        match sub.event {
            "core/rule.apply" => payload["body"]["rewritten_by"] = serde_json::json!("rule"),
            "core/llm.request" => {
                let contexts = payload["request"]["contexts"]
                    .as_array_mut()
                    .expect("read unified message sequence");
                contexts.push(serde_json::json!({
                    "role": "user",
                    "content": [{ "type": "text", "data": ADDED_BY_HOST }],
                    "reasoning_content": null,
                    "created_at": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "tool_calls": null,
                }));
            }
            "core/llm.response" => {
                let rewritten = self
                    .rewritten_response
                    .lock()
                    .expect("the record is usable")
                    .clone();
                match rewritten {
                    Some(text) => {
                        payload["message"]["content"] =
                            serde_json::json!([{ "type": "text", "data": text }]);
                    }
                    None => return Ok(None),
                }
            }
            "core/llm.toolcall" => return Ok(None),
            _ => return Ok(None),
        }
        Ok(Some(payload))
    }
}

/// 统一结构侧改写加入的文本
const ADDED_BY_HOST: &str = "a sentence added by the host";

/// 提供商结构侧改写加入的字段名
const ADDED_FIELD: &str = "rewritten_by";

/// 全部订阅者都启用
struct AllEnabled;

impl ModEnablement for AllEnabled {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::all()
    }
}

/// 链上事件的目录
///
/// 会话目录也须并入。块与定稿的事件由写者派发
fn directory() -> Directory {
    wind_core::llm::directory().merge(wind_core::conversation::directory())
}

/// 对本文件关心的每一条链上事件各建一条订阅
///
/// 只有存在订阅者时派发才会经过宿主，因此这些订阅同时用于探测事件是否被派发
fn subscriptions() -> Vec<Subscription> {
    [
        "core/prompt.collect",
        "core/rule.apply",
        "core/llm.request",
        "core/llm.response",
        "core/llm.toolcall",
    ]
    .into_iter()
    .enumerate()
    .map(|(seq, event)| Subscription {
        mod_name: "watcher".into(),
        mod_version: 1,
        event,
        phase: Some(SubPhase::Guard),
        handler: "onEvent".into(),
        timeout_ms: 1000,
        registration_seq: seq as u64,
    })
    .collect()
}

// 三个接口的替身

/// 不声明任何片段与工具的 `TopicMods`
struct FakeMods;

#[async_trait::async_trait]
impl TopicMods for FakeMods {
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

/// 按工具名声明工具、并记录每次调用的 `McpTools` 实现
struct FakeMcp {
    /// 本服务里可用的工具
    declared: Vec<DeclaredTool>,
    /// 被调用过的调用标识
    called: Mutex<Vec<String>>,
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
        Self {
            declared,
            called: Mutex::new(Vec::new()),
        }
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
        call_id: &str,
        _server: &str,
        tool: &str,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        self.called
            .lock()
            .expect("the record is usable")
            .push(call_id.to_string());
        Ok(serde_json::json!({ "tool": tool, "arguments": arguments }))
    }
}

/// 一律原样返回输入的 `ModRequests` 实现
struct FakeRequests;

#[async_trait::async_trait]
impl ModRequests for FakeRequests {
    async fn call(
        &self,
        _scope: &Scope,
        _mod_name: &str,
        _version: i64,
        _request: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        Ok(input)
    }
}
