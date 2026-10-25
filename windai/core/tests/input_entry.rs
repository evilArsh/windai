//! 验证用户任务入口。`drain` 把队列拼成一条 user 消息，该消息成为本轮的 `from_id`。

#[path = "common/lib.rs"]
mod common;

use common::store;
use std::io;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use wind_core::CoreError;
use wind_core::agent::behavior::{Behavior, Ctx, Input, Outcome};
use wind_core::agent::domain::{AgentDomain, AgentSpec};
use wind_core::conversation::assembler::{ContextProjection, ProjectionInvalidations};
use wind_core::conversation::writer::BlockWriter;
use wind_core::feed::hub::FeedHub;
use wind_core::llm::ports::{DeclaredTool, ModRequests, TopicMods};
use wind_core::llm::turn::{AgentRunner, DEFAULT_MAX_ROUNDS};
use wind_core::models::{
    AdapterType, AgentInstanceCreate, AgentMode, AgentPhase, AgentRole, BlockKind,
    Message as MessageRow, ModelCreate, PendingCreate, PendingKind, ProviderCreate, Role,
    TopicCreate,
};
use wind_core::storage::Storage;
use wind_core::usecase::UseCases;
use wind_event::def::Directory;
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::Scope;
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::subscription::{EnabledSet, Subscription};
use wind_wire::message::Content;

/// 本用例的实例标识
const INSTANCE: i64 = 1;

/// 一次运行使用的夹具
struct Rig {
    /// 临时目录须被持有。一被丢弃，库文件随之删除
    _dir: TempDir,
    /// 存储，用来读已写入的轮、消息与块
    storage: Storage,
    /// 被驱动的 `AgentRunner`
    runner: AgentRunner,
    /// 模型应答的来源
    server: Server,
    /// 本轮的实例标识
    instance_id: i64,
    /// 待消费的队列行
    pending: Vec<i64>,
}

impl Rig {
    /// 执行一轮。已取走的整队输入由 `Idle` 交给运行者
    async fn run_one_round(&self) -> Result<Outcome<AgentDomain>, String> {
        let specs = self
            .pending
            .iter()
            .map(|pending_id| AgentSpec {
                instance_id: self.instance_id,
                message_id: None,
                pending_id: Some(*pending_id),
                turn_id: None,
            })
            .collect();
        let ctx = Ctx::new(self.instance_id, 1, CancellationToken::new());
        match self.runner.run(ctx, Input::Idle(specs)).await {
            Outcome::Failed(failure) => Err(failure.error),
            outcome => Ok(outcome),
        }
    }

    /// 执行一轮。首次启动携带一行待消费输入
    async fn run_one_fresh(&self) -> Result<Outcome<AgentDomain>, String> {
        let spec = AgentSpec {
            instance_id: self.instance_id,
            message_id: None,
            pending_id: self.pending.first().copied(),
            turn_id: None,
        };
        let ctx = Ctx::new(self.instance_id, 1, CancellationToken::new());
        match self.runner.run(ctx, Input::Fresh(spec)).await {
            Outcome::Failed(failure) => Err(failure.error),
            outcome => Ok(outcome),
        }
    }

    /// 执行一轮。首次启动并指向给定的队列行
    async fn run_fresh_pointing_at(&self, pending_id: i64) -> Result<Outcome<AgentDomain>, String> {
        let spec = AgentSpec {
            instance_id: self.instance_id,
            message_id: None,
            pending_id: Some(pending_id),
            turn_id: None,
        };
        let ctx = Ctx::new(self.instance_id, 1, CancellationToken::new());
        match self.runner.run(ctx, Input::Fresh(spec)).await {
            Outcome::Failed(failure) => Err(failure.error),
            outcome => Ok(outcome),
        }
    }

    /// 一个实例的 user 消息，按轮与轮内序号排列
    async fn user_messages_of(&self, instance_id: i64) -> Vec<MessageRow> {
        let turn_ids: Vec<i64> = self
            .storage
            .turn_list_by_instance(instance_id)
            .await
            .expect("read this instance's turn")
            .into_iter()
            .map(|turn| turn.id)
            .collect();
        self.storage
            .message_list_by_turns(&turn_ids)
            .await
            .expect("read messages of turn")
            .into_iter()
            .filter(|message| message.role == Role::User)
            .collect()
    }

    /// 一条消息已提交的块中的文本
    async fn text_of(&self, message: &MessageRow) -> String {
        self.storage
            .block_list_by_message(message.id)
            .await
            .expect("read the committed block")
            .iter()
            .filter(|block| block.kind == BlockKind::Text)
            .map(|block| block.content.clone())
            .collect::<Vec<_>>()
            .join("")
    }

    /// 最新一轮中的 user 消息
    async fn latest_turn_and_user(&self, instance_id: i64) -> (i64, i64) {
        let turn = self
            .storage
            .turn_list_by_instance(instance_id)
            .await
            .expect("read this instance's turn")
            .pop()
            .expect("find at least one turn");
        let message = self
            .storage
            .message_list_by_turns(&[turn.id])
            .await
            .expect("read messages of turn")
            .into_iter()
            .find(|message| message.role == Role::User)
            .expect("find user message");
        (turn.id, message.id)
    }

    /// 一轮的 `from_id`
    async fn turn_from_id(&self, turn_id: i64) -> Option<i64> {
        self.storage
            .turn_list_by_instance(self.instance_id)
            .await
            .expect("read this instance's turn")
            .into_iter()
            .find(|turn| turn.id == turn_id)
            .and_then(|turn| turn.from_id)
    }

    /// 建一套夹具。含临时库、真实总线、假接口与只回一段文本的服务
    async fn build(instance_id: i64, payloads: &[&str]) -> Self {
        let (dir, storage) = store().await;
        let server = Server::serve("received").await;
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
        let created = storage
            .agent_create(AgentInstanceCreate {
                parent_id: None,
                topic_id,
                role: AgentRole::Main,
                mode: Some(AgentMode::Sync),
                phase: AgentPhase::Done,
                phase_payload: None,
            })
            .await
            .expect("create instance");
        assert_eq!(
            created, instance_id,
            "the instance of this test lands on the given id"
        );

        let mut pending = Vec::with_capacity(payloads.len());
        for text in payloads {
            let id = storage
                .pending_insert(PendingCreate {
                    instance_id,
                    kind: PendingKind::User,
                    source_id: None,
                    payload: [Content::new_text((*text).to_string())].to_vec(),
                })
                .await
                .expect("enqueue");
            pending.push(id);
        }

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
            None,
            Arc::new(NoRequests),
            storage.clone(),
            DEFAULT_MAX_ROUNDS,
        );
        Self {
            _dir: dir,
            storage: storage.clone(),
            runner,
            server,
            instance_id,
            pending,
        }
    }

    /// 模型收到过的请求体
    fn request_bodies(&self) -> Vec<String> {
        self.server.bodies()
    }
}

/// 一条输入已入队、实例空闲的夹具
async fn rig_with_one_pending(instance_id: i64) -> Rig {
    Rig::build(instance_id, &["first"]).await
}

/// 两行输入位于同一队列的夹具
async fn rig_with_two_pending(instance_id: i64) -> Rig {
    Rig::build(instance_id, &["first", "second"]).await
}

#[tokio::test]
async fn all_pending_sources_become_exactly_one_user_message() {
    let rig = rig_with_two_pending(INSTANCE).await;
    let _ = rig.run_one_round().await.expect("run to completion");
    let users = rig.user_messages_of(INSTANCE).await;
    assert_eq!(
        users.len(),
        1,
        "multiple sources are composed into exactly one user message"
    );
    let text = rig.text_of(&users[0]).await;
    assert!(text.contains("first"));
    assert!(text.contains("second"));
}

#[tokio::test]
async fn a_failed_block_write_leaves_the_input_for_the_next_run() {
    let rig = rig_with_one_pending(INSTANCE).await;
    sqlx::query(
        "CREATE TRIGGER refuse_block BEFORE INSERT ON block \
         BEGIN SELECT RAISE(ABORT, 'refused'); END",
    )
    .execute(rig.storage.pool())
    .await
    .expect("install the refusing trigger");

    assert!(
        rig.run_one_round().await.is_err(),
        "the refused block write must fail the round"
    );
    assert_eq!(
        rig.storage
            .pending_list_by_instance(INSTANCE)
            .await
            .expect("query")
            .len(),
        1,
        "a failed round must leave the input pending"
    );

    let projection = ContextProjection::new(
        rig.storage.clone(),
        INSTANCE,
        ProjectionInvalidations::default(),
    );
    let context = projection.context(None).await.expect("assemble");
    assert!(
        context.is_empty(),
        "the orphan user message must stay out of the context: {context:?}"
    );

    sqlx::query("DROP TRIGGER refuse_block")
        .execute(rig.storage.pool())
        .await
        .expect("drop the refusing trigger");
    rig.run_one_round()
        .await
        .expect("the next run consumes the input");

    let context = projection.context(None).await.expect("assemble");
    let users: Vec<_> = context
        .iter()
        .filter(|message| message.role == Role::User)
        .collect();
    assert_eq!(users.len(), 1, "the input is visible exactly once");
    let text: String = users[0]
        .content
        .iter()
        .filter_map(|content| match content {
            Content::Text { data } => Some(data.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        text.contains("first"),
        "the content survives the retry: {text}"
    );
}

#[tokio::test]
async fn the_composed_user_message_is_the_turn_from_id() {
    let rig = rig_with_one_pending(INSTANCE).await;
    let _ = rig.run_one_round().await.expect("run to completion");
    let (turn_id, message_id) = rig.latest_turn_and_user(INSTANCE).await;
    assert_eq!(rig.turn_from_id(turn_id).await, Some(message_id));
}

#[tokio::test]
async fn a_later_round_sees_the_reply_of_the_earlier_one() {
    let rig = rig_with_one_pending(INSTANCE).await;
    let _ = rig.run_one_fresh().await.expect("run to completion");

    let second = rig
        .storage
        .pending_insert(PendingCreate {
            instance_id: INSTANCE,
            kind: PendingKind::User,
            source_id: None,
            payload: [Content::new_text("second".to_string())].to_vec(),
        })
        .await
        .expect("enqueue");
    let _ = rig
        .run_fresh_pointing_at(second)
        .await
        .expect("run to completion");

    let bodies = rig.request_bodies();
    assert_eq!(bodies.len(), 2, "each round makes one request");
    assert!(
        bodies[1].contains("received"),
        "the reply committed in the earlier turn is still part of the later request"
    );
    assert!(bodies[1].contains("first"));
    assert!(bodies[1].contains("second"));
}

/// 其他实例的队列行不得因标识相同而拼进本实例
#[tokio::test]
async fn a_foreign_pending_row_is_not_composed_into_this_instance() {
    let rig = rig_with_one_pending(INSTANCE).await;
    let other = rig
        .storage
        .agent_create(AgentInstanceCreate {
            parent_id: Some(INSTANCE),
            topic_id: 1,
            role: AgentRole::Child,
            mode: Some(AgentMode::Sync),
            phase: AgentPhase::Done,
            phase_payload: None,
        })
        .await
        .expect("create another instance");
    let foreign = rig
        .storage
        .pending_insert(PendingCreate {
            instance_id: other,
            kind: PendingKind::User,
            source_id: None,
            payload: [Content::new_text("someone else's input".into())].to_vec(),
        })
        .await
        .expect("enqueue");

    let _ = rig
        .run_fresh_pointing_at(foreign)
        .await
        .expect("run to completion");

    assert!(
        rig.user_messages_of(INSTANCE).await.is_empty(),
        "another instance's queue row must not be composed into this instance's context"
    );
    assert_eq!(
        rig.storage
            .pending_list_by_instance(other)
            .await
            .expect("read the queue")
            .len(),
        1,
        "rows that do not belong to this instance must not be consumed"
    );
}

#[tokio::test]
async fn a_fresh_launch_composes_the_pending_row_it_was_handed() {
    let rig = rig_with_one_pending(INSTANCE).await;
    let _ = rig.run_one_fresh().await.expect("run to completion");
    let users = rig.user_messages_of(INSTANCE).await;
    assert_eq!(
        users.len(),
        1,
        "the fresh-launch row must be composed into exactly one user message"
    );
    assert!(rig.text_of(&users[0]).await.contains("first"));
    let (turn_id, message_id) = rig.latest_turn_and_user(INSTANCE).await;
    assert_eq!(
        rig.turn_from_id(turn_id).await,
        Some(message_id),
        "the composed message is this turn's from_id"
    );
}

/// 一个只回一段文本的最小 HTTP 应答器
struct Server {
    /// 监听的基地址
    base_url: String,
    /// 收到的请求体
    bodies: Arc<Mutex<Vec<String>>>,
}

impl Server {
    /// 服务的基地址
    fn base_url(&self) -> String {
        self.base_url.clone()
    }

    /// 收到过的请求体
    fn bodies(&self) -> Vec<String> {
        self.bodies
            .lock()
            .map(|bodies| bodies.clone())
            .unwrap_or_default()
    }

    /// 启动服务并逐个处理连接
    ///
    /// 只服务本文件的用例。不分块，不支持 keep-alive，写完即关闭连接
    async fn serve(text: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
        let base_url = format!(
            "http://{}",
            listener.local_addr().expect("has a local address")
        );
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&bodies);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                if let Some(body) = read_request(&mut socket).await
                    && let Ok(mut bodies) = recorded.lock()
                {
                    bodies.push(body);
                }
                let _ = respond(&mut socket, text).await;
                let _ = socket.shutdown().await;
            }
        });
        Self { base_url, bodies }
    }
}

/// 流结束的哨兵行
const DONE_LINE: &str = "data: [DONE]\n\n";

/// 构造一段文本增量的 SSE 应答
fn text_reply(text: &str) -> String {
    let chunk = serde_json::json!({
        "id": "x",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "deepseek-chat",
        "choices": [{ "index": 0, "delta": { "role": "assistant", "content": text } }],
    });
    format!("data: {chunk}\n\n{DONE_LINE}")
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
    let reply = text_reply(body);
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        reply.len()
    );
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(reply.as_bytes()).await?;
    socket.flush().await
}

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
