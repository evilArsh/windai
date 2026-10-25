//! 验证派生调用的配对。父子关系由 `spawn_calls` 的显式映射给出，不依赖屏障顺序。

#[path = "common/lib.rs"]
mod common;

use common::store;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use wind_core::CoreError;
use wind_core::agent::behavior::{Behavior, Ctx, Input, Outcome, SpawnRequest};
use wind_core::agent::domain::{AgentDomain, AgentSpec};
use wind_core::agent::phase_codec;
use wind_core::agent::tree::phase::Phase;
use wind_core::conversation::assembler::ProjectionInvalidations;
use wind_core::conversation::writer::BlockWriter;
use wind_core::feed::hub::FeedHub;
use wind_core::llm::ports::{DeclaredTool, ModRequests, TopicMods};
use wind_core::llm::turn::{AgentRunner, DEFAULT_MAX_ROUNDS};
use wind_core::models::{
    AdapterType, AgentInstanceCreate, AgentMode, AgentPhase, AgentRole, BlockKind, BlockRow,
    MessageCreate, ModelCreate, ProviderCreate, Role, TopicCreate, TurnCreate,
};
use wind_core::storage::Storage;
use wind_core::usecase::UseCases;
use wind_event::def::{Directory, EventMeta};
use wind_event::dispatch::{Dispatcher, Limits};
use wind_event::envelope::Scope;
use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
use wind_event::observer::BuiltinObserver;
use wind_event::subscription::{EnabledSet, Subscription};
use wind_stream::id::BlockId;

/// 一次运行使用的夹具
struct Rig {
    /// 临时目录须被持有。一被丢弃，库文件随之删除
    _dir: TempDir,
    /// 存储，用来读已写入的轮、消息与块
    storage: Storage,
    /// 被驱动的 `AgentRunner`
    runner: AgentRunner,
    /// 本次运行的话题
    topic_id: i64,
    /// 与该话题共享的帧设施
    hub: Arc<FeedHub>,
    /// 派发器
    #[allow(dead_code)]
    bus: Arc<Dispatcher>,
    /// 记录总线派发的事件
    recorder: Recorder,
    /// 父实例
    parent_id: i64,
    /// 父实例的首条用户消息
    seed_message_id: i64,
    /// 上一轮返回给树的派生请求
    spawns: Mutex<Vec<SpawnRequest<AgentDomain>>>,
    /// 服务端按到达顺序收到的请求体
    requests: Arc<Mutex<Vec<String>>>,
}

impl Rig {
    /// 在一个实例上执行一次
    async fn run_at(&self, spec: AgentSpec) -> Result<Outcome<AgentDomain>, String> {
        let ctx = Ctx::new(spec.instance_id, 1, CancellationToken::new());
        match self.runner.run(ctx, Input::Fresh(spec)).await {
            Outcome::Failed(failure) => Err(failure.error),
            outcome => Ok(outcome),
        }
    }

    /// 在父实例上执行一次
    async fn run_parent_once(&self) -> Result<Outcome<AgentDomain>, String> {
        self.run_at(AgentSpec {
            instance_id: self.parent_id,
            message_id: None,
            pending_id: None,
            turn_id: None,
        })
        .await
    }

    /// 让父实例执行一轮，并记录返回给树的派生请求
    async fn spawn_once(&self) -> Result<(), String> {
        let outcome = self.run_parent_once().await?;
        let Outcome::Spawn { spawn } = outcome else {
            return Err(format!("the parent node must spawn, actually {outcome:?}"));
        };
        let mut stored = self.spawns.lock().expect("lock spawn requests");
        *stored = spawn;
        Ok(())
    }

    /// 让本轮派生的子实例各执行完一轮，再把结论对账回父节点
    async fn run_child_rounds(&self) -> Result<(), String> {
        let spawns = {
            let mut stored = self.spawns.lock().expect("lock spawn requests");
            std::mem::take(&mut *stored)
        };
        for request in spawns {
            let child_id = request.id;
            let outcome = self.run_at(request.spec).await?;
            let Outcome::Completed(done) = outcome else {
                return Err(format!(
                    "child instance {child_id} must finish, actually {outcome:?}"
                ));
            };
            // `Phase` 由树保存。测试替树写入该行，因为对账读取的是子实例的 `Phase` 与回复
            let stored = phase_codec::encode(&Phase::Done(done)).expect("encode");
            let payload = stored.payload.as_ref().map(serde_json::Value::to_string);
            self.storage
                .agent_save_phase(
                    child_id,
                    stored.name.parse().expect("encoded phase"),
                    payload.as_deref(),
                )
                .await
                .map_err(|error| error.to_string())?;
        }
        // 树的下一轮在 `serve` 开头对账。测试直接调用同一段算法，使断言只关注配对结果
        self.runner
            .report_spawns(self.parent_id)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    /// 一个实例已提交的工具结果，按调用标识索引
    async fn spawn_results_of(&self, instance_id: i64) -> HashMap<String, serde_json::Value> {
        let turns = self
            .storage
            .turn_list_by_instance(instance_id)
            .await
            .expect("read this instance's turn");
        let turn_ids: Vec<i64> = turns.iter().map(|turn| turn.id).collect();
        let messages = self
            .storage
            .message_list_by_turns(&turn_ids)
            .await
            .expect("read messages of turn");
        let message_ids: Vec<i64> = messages.iter().map(|message| message.id).collect();
        self.storage
            .block_list_by_messages(&message_ids)
            .await
            .expect("read the committed block")
            .into_iter()
            .filter(|block| block.kind == BlockKind::ToolResult)
            .filter_map(|block| match (block.call_id, block.data) {
                (Some(call_id), Some(data)) => Some((call_id, data)),
                _ => None,
            })
            .collect()
    }

    /// 建一套夹具。含临时库、真实总线、假接口与按次数应答的服务
    async fn build(replies: Vec<Reply>) -> Self {
        let (dir, storage) = store().await;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server = Server::serve(replies, requests.clone()).await;
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
        let parent_id = storage
            .agent_create(AgentInstanceCreate {
                parent_id: None,
                topic_id,
                role: AgentRole::Main,
                mode: Some(AgentMode::Sync),
                phase: AgentPhase::Running,
                phase_payload: None,
            })
            .await
            .expect("create parent instance");

        // 父实例先有一条用户输入，历史才能以用户消息开头
        let turn_id = storage
            .turn_create(TurnCreate {
                instance_id: parent_id,
                topic_id,
                from_id: None,
                is_boundary: false,
                created_at: None,
            })
            .await
            .expect("seed the parent turn");
        let message_id = storage
            .message_create(MessageCreate {
                turn_id,
                role: Role::User,
                model_id: None,
                finish_reason: None,
                input_tokens: 0,
                output_tokens: 0,
                created_at: None,
            })
            .await
            .expect("seed the parent message");
        storage
            .block_insert(BlockRow {
                id: 1,
                message_id,
                seq: 0,
                kind: BlockKind::Text,
                call_id: None,
                revision: 1,
                content: "start".into(),
                data: None,
                finished: true,
            })
            .await
            .expect("seed the parent block");

        let hub = FeedHub::open(storage.clone(), None)
            .await
            .expect("open hub");
        let bus = Arc::new(Dispatcher::new(
            directory(),
            Arc::new(AllEnabled),
            Arc::new(PassHost),
            Limits::default(),
        ));
        let recorder = Recorder::default();
        bus.attach_observer(Arc::new(recorder.clone()));
        let runner = AgentRunner::new(
            BlockWriter::new(
                hub.clone(),
                bus.clone(),
                storage.clone(),
                ProjectionInvalidations::default(),
            ),
            UseCases::new(storage.clone(), bus.clone()),
            bus.clone(),
            Arc::new(FakeMods),
            None,
            Arc::new(FakeRequests),
            storage.clone(),
            DEFAULT_MAX_ROUNDS,
        );
        Self {
            _dir: dir,
            storage: storage.clone(),
            runner,
            topic_id,
            hub,
            bus,
            recorder,
            parent_id,
            seed_message_id: message_id,
            spawns: Mutex::new(Vec::new()),
            requests,
        }
    }
}

/// 父节点一次派生两个同步子任务的夹具
async fn rig_with_two_spawn_calls(first: &str, second: &str) -> Rig {
    let rig = Rig::build(vec![
        calls_reply(&[
            (
                first,
                "windai_spawn_agent",
                r#"{"mode":"sync","task":"look up the first thing"}"#,
            ),
            (
                second,
                "windai_spawn_agent",
                r#"{"mode":"sync","task":"look up the second thing"}"#,
            ),
        ]),
        text_reply("first answer"),
        text_reply("second answer"),
    ])
    .await;
    rig.spawn_once().await.expect("spawn two children");
    rig
}

/// 父节点一次派生一个后台子任务的夹具
async fn rig_with_background_spawn(call_id: &str) -> Rig {
    let rig = Rig::build(vec![
        calls_reply(&[(
            call_id,
            "windai_spawn_agent",
            r#"{"mode":"background","task":"write in the background"}"#,
        )]),
        text_reply("continue"),
    ])
    .await;
    rig.spawn_once().await.expect("spawn background child");
    rig
}

/// 父节点一次派生，并在同一批里调用一个不在清单里的工具
async fn rig_with_a_spawn_and_another_call() -> Rig {
    Rig::build(vec![
        calls_reply(&[
            (
                "c-1",
                "windai_spawn_agent",
                r#"{"mode":"sync","task":"look up the first thing"}"#,
            ),
            ("c-2", "no_such_tool", "{}"),
        ]),
        text_reply("received"),
    ])
    .await
}

#[tokio::test]
async fn a_spawn_batch_keeps_the_other_calls_results() {
    let rig = rig_with_a_spawn_and_another_call().await;
    rig.spawn_once().await.expect("spawn child");
    assert!(
        rig.spawn_results_of(1).await.contains_key("c-2"),
        "the second call in the same batch really ran and must leave a tool_result"
    );

    rig.run_child_rounds().await.expect("finish child run");
    rig.run_parent_once().await.expect("resume parent");
    let requests = rig.requests.lock().expect("read request record");
    let last = requests.last().expect("send at least one request");
    assert!(
        last.contains("c-2"),
        "the next model request must carry this tool result: {last}"
    );
}

#[tokio::test]
async fn two_spawn_calls_pair_with_their_own_children() {
    let rig = rig_with_two_spawn_calls("c-1", "c-2").await;
    rig.run_child_rounds().await.expect("run to completion");
    let results = rig.spawn_results_of(1).await;
    assert_eq!(results["c-1"], "first answer");
    assert_eq!(results["c-2"], "second answer");
}

#[tokio::test]
async fn a_background_call_always_gets_a_started_result() {
    let rig = rig_with_background_spawn("c-1").await;
    let _ = rig.run_parent_once().await.expect("run to completion");
    assert_eq!(rig.spawn_results_of(1).await["c-1"]["phase"], "running");
}

#[tokio::test]
async fn a_failed_second_task_write_rolls_back_the_first() {
    let rig = Rig::build(vec![
        calls_reply(&[
            (
                "c-1",
                "windai_spawn_agent",
                r#"{"mode":"sync","task":"first task"}"#,
            ),
            (
                "c-2",
                "windai_spawn_agent",
                r#"{"mode":"sync","task":"second task"}"#,
            ),
        ]),
        text_reply("unused"),
    ])
    .await;
    // 父块 id 为 1，累加器种子为 1；父应答按每个工具调用各开一块，取 2 与 3；
    // 两个子任务块依次取 4 与 5。预占 5 让第二个 write_task 的 block_insert 失败
    rig.storage
        .block_insert(BlockRow {
            id: 5,
            message_id: rig.seed_message_id,
            seq: 1,
            kind: BlockKind::Text,
            call_id: None,
            revision: 1,
            content: "occupied".into(),
            data: None,
            finished: true,
        })
        .await
        .expect("occupy the second task block id");

    let error = rig
        .run_parent_once()
        .await
        .expect_err("the second task block collides with the occupied id");

    assert!(
        error.contains("UNIQUE"),
        "the batch must fail on the occupied block id, actually: {error}"
    );

    let agents = rig
        .storage
        .agent_list_by_topic(rig.topic_id)
        .await
        .expect("list the topic's agents");
    assert_eq!(
        agents.iter().map(|agent| agent.id).collect::<Vec<_>>(),
        vec![rig.parent_id],
        "the first child must not survive the failed batch"
    );
    let calls = rig
        .storage
        .spawn_call_list_by_parent(rig.parent_id)
        .await
        .expect("list the parent's spawn calls");
    assert!(
        calls.is_empty(),
        "the spawn mapping must roll back with its child: {calls:?}"
    );

    let (turns, messages, blocks) = rows_outside(&rig.storage, rig.topic_id, rig.parent_id).await;
    assert_eq!(
        (turns, messages, blocks),
        (0, 0, 0),
        "a failed batch must leave no turn, message or block of the child"
    );

    let user_opens: Vec<serde_json::Value> = rig
        .recorder
        .payloads("core/block.open")
        .into_iter()
        .filter(|payload| payload["role"] == serde_json::json!("user"))
        .collect();
    assert!(
        user_opens.is_empty(),
        "a rolled back task must not announce its block: {user_opens:?}"
    );

    let committed = rig.recorder.payloads("core/block.committed");
    for task in ["first task", "second task"] {
        assert!(
            !committed
                .iter()
                .any(|payload| payload["content"] == serde_json::json!(task)),
            "a rolled back task must not commit its block: {task}"
        );
    }

    let snapshot = rig.hub.snapshot_blocks(&Scope::Topic {
        topic_id: rig.topic_id,
    });
    for id in [BlockId(4), BlockId(5)] {
        assert!(
            !snapshot.iter().any(|candidate| candidate.id == id),
            "a rolled back task block {id:?} must stay out of the accumulator"
        );
    }
}

#[tokio::test]
async fn an_unknown_spawn_mode_rolls_back_the_first_spawn() {
    let rig = Rig::build(vec![
        calls_reply(&[
            (
                "c-1",
                "windai_spawn_agent",
                r#"{"mode":"sync","task":"first"}"#,
            ),
            (
                "c-2",
                "windai_spawn_agent",
                r#"{"mode":"bogus","task":"second"}"#,
            ),
        ]),
        text_reply("unused"),
    ])
    .await;

    let result = rig.run_parent_once().await;
    assert!(
        result.is_err(),
        "the second spawn names an unknown mode, so the batch must fail"
    );

    let agents = rig
        .storage
        .agent_list_by_topic(rig.topic_id)
        .await
        .expect("list the topic's agents");
    assert_eq!(
        agents.iter().map(|agent| agent.id).collect::<Vec<_>>(),
        vec![rig.parent_id],
        "the first child must not survive the failed batch"
    );
    let calls = rig
        .storage
        .spawn_call_list_by_parent(rig.parent_id)
        .await
        .expect("list the parent's spawn calls");
    assert!(
        calls.is_empty(),
        "the spawn mapping must roll back with its child: {calls:?}"
    );

    let user_opens: Vec<serde_json::Value> = rig
        .recorder
        .payloads("core/block.open")
        .into_iter()
        .filter(|payload| payload["role"] == serde_json::json!("user"))
        .collect();
    assert!(
        user_opens.is_empty(),
        "a rolled back task must not announce its block: {user_opens:?}"
    );

    let (turns, messages, blocks) = rows_outside(&rig.storage, rig.topic_id, rig.parent_id).await;
    assert_eq!(
        (turns, messages, blocks),
        (0, 0, 0),
        "a failed batch must leave no turn, message or block of the child"
    );
}

/// 统计话题内不属于该实例的轮、消息与块
async fn rows_outside(storage: &Storage, topic_id: i64, instance_id: i64) -> (i64, i64, i64) {
    let turns = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM turn WHERE topic_id = ? AND instance_id <> ?",
    )
    .bind(topic_id)
    .bind(instance_id)
    .fetch_one(storage.pool())
    .await
    .expect("count the turns outside the instance");
    let messages = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM message m JOIN turn t ON m.turn_id = t.id \
         WHERE t.topic_id = ? AND t.instance_id <> ?",
    )
    .bind(topic_id)
    .bind(instance_id)
    .fetch_one(storage.pool())
    .await
    .expect("count the messages outside the instance");
    let blocks = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM block b JOIN message m ON b.message_id = m.id \
         JOIN turn t ON m.turn_id = t.id WHERE t.topic_id = ? AND t.instance_id <> ?",
    )
    .bind(topic_id)
    .bind(instance_id)
    .fetch_one(storage.pool())
    .await
    .expect("count the blocks outside the instance");
    (turns, messages, blocks)
}

#[tokio::test]
async fn a_successful_spawn_writes_task_rows_then_announces() {
    let rig = rig_with_two_spawn_calls("c-1", "c-2").await;
    let children: Vec<(i64, i64)> = {
        let spawns = rig.spawns.lock().expect("lock spawn requests");
        assert_eq!(spawns.len(), 2, "both calls spawn");
        spawns
            .iter()
            .map(|request| {
                (
                    request.id,
                    request.spec.turn_id.expect("a spawned child gets a turn"),
                )
            })
            .collect()
    };
    let tasks = ["look up the first thing", "look up the second thing"];
    let events = rig.recorder.events();
    for ((child_id, turn_id), task) in children.iter().zip(tasks) {
        let turns = rig
            .storage
            .turn_list_by_instance(*child_id)
            .await
            .expect("read the child's turns");
        assert_eq!(turns.len(), 1, "a spawned child starts with one turn");
        let turn = &turns[0];
        assert_eq!(turn.id, *turn_id);

        let messages = rig
            .storage
            .message_list_by_turns(&[*turn_id])
            .await
            .expect("read the child's messages");
        assert_eq!(messages.len(), 1, "the task is exactly one user message");
        let message = &messages[0];
        assert_eq!(message.seq, 0, "a new turn's task message starts at seq 0");
        assert_eq!(message.role, Role::User);
        assert_eq!(
            turn.from_id,
            Some(message.id),
            "the turn points at its task message"
        );

        let blocks = rig
            .storage
            .block_list_by_messages(&[message.id])
            .await
            .expect("read the task block");
        assert_eq!(blocks.len(), 1, "the task is exactly one text block");
        let block = &blocks[0];
        assert_eq!(block.seq, 0, "the task block is the message's first block");
        assert_eq!(block.revision, 1);
        assert!(block.finished, "the task block is final from the start");
        assert_eq!(block.kind, BlockKind::Text);
        assert_eq!(block.content, *task, "the block carries the task text");
        let block_id = serde_json::json!(block.id.to_string());

        let open_at = events
            .iter()
            .position(|event| {
                event["name"] == serde_json::json!("core/block.open")
                    && event["payload"]["block_id"] == block_id
            })
            .unwrap_or_else(|| panic!("the task block {} must open", block.id));
        let committed_at = events
            .iter()
            .position(|event| {
                event["name"] == serde_json::json!("core/block.committed")
                    && event["payload"]["block_id"] == block_id
            })
            .unwrap_or_else(|| panic!("the task block {} must commit", block.id));
        assert!(
            open_at < committed_at,
            "the open of block {} must arrive before its committed",
            block.id
        );
        let open = &events[open_at]["payload"];
        assert_eq!(open["role"], serde_json::json!("user"));
        assert_eq!(open["of_message"], serde_json::json!(message.id));
        assert!(
            !events.iter().any(|event| {
                event["name"] == serde_json::json!("core/block.delta")
                    && event["payload"]["block_id"] == block_id
            }),
            "a task block never streams fragments"
        );
    }
}

#[tokio::test]
async fn a_spawned_task_block_never_enters_the_hub_snapshot() {
    let rig = rig_with_two_spawn_calls("c-1", "c-2").await;
    let turns: Vec<i64> = {
        let spawns = rig.spawns.lock().expect("lock spawn requests");
        spawns
            .iter()
            .map(|request| request.spec.turn_id.expect("a spawned child gets a turn"))
            .collect()
    };
    let messages = rig
        .storage
        .message_list_by_turns(&turns)
        .await
        .expect("read the children's messages");
    let message_ids: Vec<i64> = messages.iter().map(|message| message.id).collect();
    let task_blocks = rig
        .storage
        .block_list_by_messages(&message_ids)
        .await
        .expect("read the task blocks");
    assert_eq!(task_blocks.len(), 2);

    let snapshot = rig.hub.snapshot_blocks(&Scope::Topic {
        topic_id: rig.topic_id,
    });
    for block in task_blocks {
        assert!(
            !snapshot
                .iter()
                .any(|candidate| candidate.id == BlockId(block.id as u64)),
            "task block {} must stay out of the accumulator",
            block.id
        );
    }
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

/// 构造一段文本增量的 SSE
fn text_reply(text: &str) -> Reply {
    Reply::whole(format!(
        "{}{DONE_LINE}",
        delta_line(serde_json::json!({ "role": "assistant", "content": text }))
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
    async fn serve(replies: Vec<Reply>, requests: Arc<Mutex<Vec<String>>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
        let base_url = format!(
            "http://{}",
            listener.local_addr().expect("has a local address")
        );
        tokio::spawn(async move {
            let mut index = 0;
            while let Ok((mut socket, _)) = listener.accept().await {
                let reply = replies.get(index).or_else(|| replies.last());
                index += 1;
                if let Some(request) = read_request(&mut socket).await
                    && let Ok(mut all) = requests.lock()
                {
                    all.push(request);
                }
                if let Some(reply) = reply {
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

/// 记录派发给内建观察者的信封，供断言核对
///
/// 克隆共享同一份记录
#[derive(Default, Clone)]
struct Recorder {
    /// 按收到顺序记下的信封
    seen: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl Recorder {
    /// 按收到顺序返回全部信封
    fn events(&self) -> Vec<serde_json::Value> {
        self.seen.lock().expect("lock the record").clone()
    }

    /// 某个事件名记下的载荷，按序
    fn payloads(&self, name: &str) -> Vec<serde_json::Value> {
        self.events()
            .into_iter()
            .filter(|event| event["name"] == serde_json::json!(name))
            .map(|event| event["payload"].clone())
            .collect()
    }
}

#[async_trait::async_trait]
impl BuiltinObserver for Recorder {
    async fn on_event(&self, _meta: &EventMeta, event: &serde_json::Value) {
        self.seen
            .lock()
            .expect("lock the record")
            .push(event.clone());
    }
}

/// 全部订阅者都启用
struct AllEnabled;

impl ModEnablement for AllEnabled {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::all()
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

/// 不会被调用的 `ModRequests` 实现
struct FakeRequests;

#[async_trait::async_trait]
impl ModRequests for FakeRequests {
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
