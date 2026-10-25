//! 后台子任务的终态结果成为父节点的下一条 user 输入
//!
//! 每棵树一个 watcher 订阅自己的事件流。后台子节点进入终态后，结果被写进父节点的
//! 待消费队列；父节点已在取消一侧时只释放交付义务，不写队列。启动扫描补上进程退出
//! 时尚未交出的结果

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Semaphore;
use tokio::sync::broadcast;
use tokio::time::{sleep, timeout};
use wind_core::agent::domain::{AgentDomain, AgentSpec};
use wind_core::agent::events::AgentCreate;
use wind_core::agent::tree::TreeEvent;
use wind_core::agent::tree::phase::Phase;
use wind_core::db::Db;
use wind_core::models::{
    AdapterType, AgentInstanceCreate, AgentMode, AgentPhase, AgentRole, BlockKind, BlockRow,
    Message, MessageCreate, ModelCreate, ProviderCreate, Role, TopicCreate, TurnCreate,
};
use wind_core::storage::Storage;
use wind_event::context::DispatchCx;
use wind_event::envelope::{Origin, Scope, Source};
use wind_mod::dirs::AppDirs;
use wind_runtime::{Assembly, TreeHandle};

/// 等一件事发生的时限
const PATIENCE: Duration = Duration::from_secs(5);

const POLL: Duration = Duration::from_millis(10);

/// 本装配使用的话题
const TOPIC: i64 = 1;

/// 后台子任务返回的结论
const CONCLUSION: &str = "child task conclusion";

/// 一次操作所处的上下文
fn cx(topic_id: i64) -> DispatchCx {
    DispatchCx::root(
        Origin::Internal,
        Scope::Topic { topic_id },
        Source::core("runtime"),
        Duration::from_millis(50),
    )
}

/// 一套后台子任务的夹具
struct Rig {
    /// 临时目录必须被持有，否则库文件随目录一起删除
    _tmp: TempDir,
    /// 被驱动的装配
    assembly: Assembly,
    tree: TreeHandle,
    parent_id: i64,
    child_id: i64,
    /// 放行子任务应答的信号量
    release: Arc<Semaphore>,
    /// 从建树起就订阅的事件流
    events: AsyncMutex<broadcast::Receiver<TreeEvent<AgentDomain>>>,
}

impl Rig {
    /// 一个实例当前的 user 消息
    async fn user_messages_now(&self, instance_id: i64) -> Vec<Message> {
        let storage = self.assembly.storage();
        let turn_ids: Vec<i64> = storage
            .turn_list_by_instance(instance_id)
            .await
            .expect("read turn")
            .into_iter()
            .map(|turn| turn.id)
            .collect();
        storage
            .message_list_by_turns(&turn_ids)
            .await
            .expect("read message")
            .into_iter()
            .filter(|message| message.role == Role::User)
            .collect()
    }

    /// 一个节点当前的 Phase
    async fn phase_of(&self, instance_id: i64) -> Option<Phase<AgentDomain>> {
        self.tree
            .hub
            .snapshot()
            .await
            .expect("query the snapshot")
            .into_iter()
            .find(|node| node.id == instance_id)
            .map(|node| node.phase)
    }

    /// 这个实例已经写入数据库的待消费行数
    async fn pending_rows(&self, instance_id: i64) -> usize {
        self.assembly
            .storage()
            .pending_list_by_instance(instance_id)
            .await
            .expect("read the queue")
            .len()
    }

    /// 一次读到的消息、Phase，以及树是否稳定
    async fn settled(&self, instance_id: i64) -> (Vec<Message>, Option<Phase<AgentDomain>>, bool) {
        let users = self.user_messages_now(instance_id).await;
        let phase = self.phase_of(instance_id).await;
        let released = self
            .tree
            .hub
            .undelivered_nodes()
            .await
            .map(|nodes| nodes.is_empty())
            .unwrap_or(false);
        let quiet = released && self.pending_rows(instance_id).await == 0;
        (users, phase, quiet)
    }

    /// 等一个实例的下一轮 user 消息出现并稳定
    ///
    /// 实例已在取消一侧时不会再有结果，立即返回空表
    async fn user_messages_of(&self, instance_id: i64) -> Vec<Message> {
        let polled = timeout(PATIENCE, async {
            loop {
                let (users, phase, quiet) = self.settled(instance_id).await;
                let terminal = phase.as_ref().is_some_and(Phase::is_terminal);
                if !users.is_empty() && quiet && terminal {
                    return users;
                }
                let dropped = matches!(
                    phase,
                    Some(Phase::Cancelled(_)) | Some(Phase::Cancelling(_)) | Some(Phase::Aborted)
                );
                if users.is_empty() && quiet && dropped {
                    return users;
                }
                sleep(POLL).await;
            }
        })
        .await;
        match polled {
            Ok(users) => users,
            Err(_) => self.user_messages_now(instance_id).await,
        }
    }

    /// 一条消息的正文，按块的顺序拼接
    async fn text_of(&self, message: &Message) -> String {
        self.assembly
            .storage()
            .block_list_by_message(message.id)
            .await
            .expect("read the blocks")
            .into_iter()
            .filter(|block| block.kind == BlockKind::Text)
            .map(|block| block.content)
            .collect()
    }

    /// 放行子任务的一次应答，并等它的结果交付完成
    async fn finish_child(&self) -> Result<(), String> {
        self.release.add_permits(1);
        timeout(PATIENCE, async {
            loop {
                let terminal = self
                    .phase_of(self.child_id)
                    .await
                    .is_some_and(|phase| phase.is_terminal());
                let released = self
                    .tree
                    .hub
                    .undelivered_nodes()
                    .await
                    .map(|nodes| nodes.is_empty())
                    .unwrap_or(false);
                if terminal && released {
                    return;
                }
                sleep(POLL).await;
            }
        })
        .await
        .map_err(|_| "the child task did not finish within the deadline".to_string())
    }

    /// 树是否曾经宣告安静
    async fn tree_sealed(&self) -> bool {
        let mut events = self.events.lock().await;
        timeout(PATIENCE, async {
            loop {
                match events.recv().await {
                    Ok(TreeEvent::Sealed) => return true,
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return false,
                }
            }
        })
        .await
        .unwrap_or(false)
    }
}

/// 在一个刚建好的库上创建话题与父实例
///
/// 父实例处于终态，恢复之后没有正在运行的 worker。子实例行必须等树恢复之后再建：
/// 恢复期间存在的 `running` 行没有对应的 worker，会被收敛成 `aborted`
async fn seeded_root(storage: &Storage, parent_phase: &str, parent_payload: &str) -> i64 {
    let topic_id = storage
        .topic_create(TopicCreate {
            label: "test".into(),
            ..Default::default()
        })
        .await
        .expect("create topic");
    assert_eq!(topic_id, TOPIC, "the topic in this case is the first one");
    storage
        .agent_create(AgentInstanceCreate {
            parent_id: None,
            topic_id,
            role: AgentRole::Main,
            mode: Some(AgentMode::Sync),
            phase: parent_phase.parse().expect("known phase"),
            phase_payload: Some(parent_payload.to_string()),
        })
        .await
        .expect("create parent instance")
}

/// 建表、写入话题与实例，再把子实例插进树里运行
///
/// 子任务的一次模型应答被信号量拦住，夹具里的子实例因此确实在运行
async fn rig_with_a_running_background_child(parent_phase: &str, parent_payload: &str) -> Rig {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let server = Server::serve().await;
    let db = Db::open(&dirs.db_file()).await.expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);
    let provider = storage
        .provider_create(ProviderCreate {
            name: "local".into(),
            base_url: server.base_url(),
            ..Default::default()
        })
        .await
        .expect("create provider");
    let model = storage
        .model_create(ModelCreate::new(
            provider,
            "local-model".into(),
            AdapterType::OpenAICompletion,
        ))
        .await
        .expect("build model");
    let parent_id = seeded_root(&storage, parent_phase, parent_payload).await;
    sqlx::query("UPDATE topics SET model_id = ? WHERE id = ?")
        .bind(model)
        .bind(TOPIC)
        .execute(storage.pool())
        .await
        .expect("bind the model");

    let assembly = Assembly::open(dirs.clone(), None, None, None)
        .await
        .expect("assemble");
    let tree = assembly.tree_of(TOPIC).await.expect("build tree");
    let events = AsyncMutex::new(tree.hub.subscribe());
    // 子实例行与配对在树恢复之后才写入数据库。恢复不会看到它，该子实例由下面的派生启动
    let child_id = storage
        .agent_create(AgentInstanceCreate {
            parent_id: Some(parent_id),
            topic_id: TOPIC,
            role: AgentRole::Child,
            mode: Some(AgentMode::Background),
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("create child instance");
    storage
        .spawn_call_insert(parent_id, "c-1", child_id)
        .await
        .expect("record the pair");
    tree.hub
        .spawn_at_with_spec(
            &cx(TOPIC),
            child_id,
            &AgentCreate {
                topic_id: TOPIC,
                parent_id: Some(parent_id),
                mode: AgentMode::Background.to_string(),
                task: None,
            },
            AgentSpec {
                instance_id: child_id,
                message_id: None,
                pending_id: None,
                turn_id: None,
            },
        )
        .await
        .expect("spawn the child node");
    // 建好夹具时子实例必须还在运行，否则信号量之后的代码没有被执行
    assert!(
        matches!(
            tree.hub
                .snapshot()
                .await
                .expect("query the snapshot")
                .into_iter()
                .find(|node| node.id == child_id)
                .map(|node| node.phase),
            Some(Phase::Running)
        ),
        "the child instance in the fixture must stay running"
    );
    Rig {
        _tmp: tmp,
        assembly,
        tree,
        parent_id,
        child_id,
        release: server.release(),
        events,
    }
}

/// 一个父实例已被取消的后台子任务夹具
async fn rig_with_a_cancelled_parent() -> Rig {
    rig_with_a_running_background_child("cancelled", "\"External\"").await
}

/// 一个父实例空闲、子实例正在运行的后台子任务夹具
async fn rig_with_an_idle_parent() -> Rig {
    rig_with_a_running_background_child("done", r#"{"message_id":1}"#).await
}

#[tokio::test]
async fn a_finished_background_child_becomes_the_parents_next_user_input() {
    let rig = rig_with_an_idle_parent().await;
    rig.finish_child().await.expect("finish the child");
    let users = rig.user_messages_of(rig.parent_id).await;
    assert_eq!(
        users.len(),
        1,
        "each background result assembles into exactly one user message"
    );
    assert!(
        rig.text_of(&users[0]).await.contains(CONCLUSION),
        "the parent's next input must carry the child task's conclusion"
    );
}

#[tokio::test]
async fn a_cancelled_parent_drops_the_result_but_the_tree_still_seals() {
    let rig = rig_with_a_cancelled_parent().await;
    rig.finish_child().await.expect("finish the child");
    assert!(
        rig.user_messages_of(rig.parent_id).await.is_empty(),
        "when the parent is cancelled the result must not be written to its queue"
    );
    assert!(
        rig.tree_sealed().await,
        "after the delivery obligation is released the tree must announce quiet"
    );
}

/// 一个已经写入数据库、尚未交出的后台结果：父实例终态，子实例终态并带有一条回复消息
async fn seed_an_undelivered_result(storage: &Storage) -> (i64, i64) {
    let topic_id = storage
        .topic_create(TopicCreate {
            label: "test".into(),
            ..Default::default()
        })
        .await
        .expect("create topic");
    let parent_id = storage
        .agent_create(AgentInstanceCreate {
            parent_id: None,
            topic_id,
            role: AgentRole::Main,
            mode: Some(AgentMode::Sync),
            phase: AgentPhase::Done,
            phase_payload: Some(r#"{"message_id":1}"#.to_string()),
        })
        .await
        .expect("create parent instance");
    let child_id = storage
        .agent_create(AgentInstanceCreate {
            parent_id: Some(parent_id),
            topic_id,
            role: AgentRole::Child,
            mode: Some(AgentMode::Background),
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("create child instance");
    let turn_id = storage
        .turn_create(TurnCreate {
            instance_id: child_id,
            topic_id,
            from_id: None,
            is_boundary: false,
            created_at: None,
        })
        .await
        .expect("open the turn");
    let message_id = storage
        .message_create(MessageCreate {
            turn_id,
            role: Role::Assistant,
            model_id: None,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: None,
        })
        .await
        .expect("build message");
    storage
        .block_insert(BlockRow {
            id: 1,
            message_id,
            seq: 0,
            kind: BlockKind::Text,
            call_id: None,
            revision: 0,
            content: CONCLUSION.into(),
            data: None,
            finished: true,
        })
        .await
        .expect("write block");
    let payload = format!(r#"{{"message_id":{message_id}}}"#);
    storage
        .agent_save_phase(child_id, AgentPhase::Done, Some(&payload))
        .await
        .expect("save the phase");
    storage
        .spawn_call_insert(parent_id, "c-1", child_id)
        .await
        .expect("record the pair");
    (parent_id, child_id)
}

/// 取消一侧的结果不写队列行，但仍要记为已处理，否则启动扫描会一直列出它
#[tokio::test]
async fn a_cancel_side_background_result_is_marked_handled_without_a_row() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let db = Db::open(&dirs.db_file()).await.expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);
    let (parent_id, child_id) = seed_an_undelivered_result(&storage).await;
    storage
        .agent_save_phase(parent_id, AgentPhase::Cancelled, Some("\"External\""))
        .await
        .expect("move the parent to cancel side");

    let assembly = Assembly::open(dirs.clone(), None, None, None)
        .await
        .expect("assemble");
    let _tree = assembly.tree_of(TOPIC).await.expect("build tree");

    let mapping = storage
        .spawn_call_get_by_child(child_id)
        .await
        .expect("read the pair")
        .expect("read the spawn pair");
    assert!(
        mapping.result_delivered_at.is_some(),
        "a cancel-side result must be marked handled, otherwise the startup scan would list it forever"
    );
    assert!(
        storage
            .pending_list_by_instance(parent_id)
            .await
            .expect("read the queue")
            .is_empty(),
        "a cancel-side parent must not receive a queue row"
    );
    assert!(
        storage
            .spawn_call_list_undelivered_background(TOPIC)
            .await
            .expect("list the undelivered results")
            .is_empty(),
        "a handled result must not enter the startup scan again"
    );
}

#[tokio::test]
async fn the_startup_scan_delivers_a_terminal_background_result_once() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let db = Db::open(&dirs.db_file()).await.expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);
    let (parent_id, child_id) = seed_an_undelivered_result(&storage).await;

    let assembly = Assembly::open(dirs.clone(), None, None, None)
        .await
        .expect("assemble");
    let tree = assembly.tree_of(TOPIC).await.expect("build tree");
    // 扫描在取树返回之前跑完，标记已写入数据库，待扫描的列表为空
    let mapping = storage
        .spawn_call_get_by_child(child_id)
        .await
        .expect("read the pair")
        .expect("read the spawn pair");
    assert!(
        mapping.result_delivered_at.is_some(),
        "the startup scan must record that the result was delivered"
    );
    assert!(
        storage
            .spawn_call_list_undelivered_background(TOPIC)
            .await
            .expect("list the undelivered results")
            .is_empty(),
        "the same result must not be scanned twice"
    );

    // 父实例的下一轮把结果组装成 user 消息。这次运行没有可用模型，消息仍然先写入数据库
    let users = timeout(PATIENCE, async {
        loop {
            let turn_ids: Vec<i64> = storage
                .turn_list_by_instance(parent_id)
                .await
                .expect("read turn")
                .into_iter()
                .map(|turn| turn.id)
                .collect();
            let users: Vec<Message> = storage
                .message_list_by_turns(&turn_ids)
                .await
                .expect("read message")
                .into_iter()
                .filter(|message| message.role == Role::User)
                .collect();
            if !users.is_empty() {
                return users;
            }
            sleep(POLL).await;
        }
    })
    .await
    .expect("deliver a user message");
    assert_eq!(users.len(), 1);
    // 消息行先于它的块写入数据库，正文要等块提交之后再读
    let text = timeout(PATIENCE, async {
        loop {
            let text: String = storage
                .block_list_by_message(users[0].id)
                .await
                .expect("read the blocks")
                .into_iter()
                .filter(|block| block.kind == BlockKind::Text)
                .map(|block| block.content)
                .collect();
            if text.contains(CONCLUSION) {
                return text;
            }
            sleep(POLL).await;
        }
    })
    .await
    .expect("write the delivered text");
    assert!(
        text.contains(CONCLUSION),
        "the redelivered text must come from the child task's reply"
    );

    // 重开一次，树重新恢复并再扫一遍，结果仍然只有一条
    tree.hub.shutdown().await.expect("stop the tree");
    drop(assembly);
    let again = Assembly::open(dirs, None, None, None)
        .await
        .expect("reopen the assembly");
    let _ = again.tree_of(TOPIC).await.expect("rebuild the tree");
    let turn_ids: Vec<i64> = again
        .storage()
        .turn_list_by_instance(parent_id)
        .await
        .expect("read turn")
        .into_iter()
        .map(|turn| turn.id)
        .collect();
    let users: Vec<Message> = again
        .storage()
        .message_list_by_turns(&turn_ids)
        .await
        .expect("read message")
        .into_iter()
        .filter(|message| message.role == Role::User)
        .collect();
    assert_eq!(
        users.len(),
        1,
        "a rescan must not assemble another user message"
    );
    again.shutdown().await.expect("stop");
}

/// 一次 SSE 应答
fn text_reply(text: &str) -> String {
    format!(
        "{}{DONE}",
        delta_line(serde_json::json!({ "role": "assistant", "content": text }))
    )
}

/// 流结束的哨兵行
const DONE: &str = "data: [DONE]\n\n";

/// 由一个增量片段拼成的数据行
fn delta_line(delta: serde_json::Value) -> String {
    let chunk = serde_json::json!({
        "id": "x",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "local-model",
        "choices": [{ "index": 0, "delta": delta }],
    });
    format!("data: {chunk}\n\n")
}

/// 一个按请求次序应答的最小 HTTP 应答器
///
/// 第一次请求来自后台子任务，它停在信号量上，等用例放行之后返回结论。之后的请求
/// 都来自父节点的下一轮，立即应答
struct Server {
    base_url: String,
    /// 放行第一次请求的信号量
    release: Arc<Semaphore>,
}

impl Server {
    fn base_url(&self) -> String {
        self.base_url.clone()
    }

    /// 放行子任务应答的信号量
    fn release(&self) -> Arc<Semaphore> {
        Arc::clone(&self.release)
    }

    /// 启动服务并逐个处理连接
    async fn serve() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
        let base_url = format!(
            "http://{}",
            listener.local_addr().expect("has a local address")
        );
        let release = Arc::new(Semaphore::new(0));
        let gate = Arc::clone(&release);
        tokio::spawn(async move {
            let mut index = 0usize;
            while let Ok((mut socket, _)) = listener.accept().await {
                drain_request(&mut socket).await;
                let reply = if index == 0 {
                    let _permit = gate.acquire().await;
                    text_reply(CONCLUSION)
                } else {
                    text_reply("the parent has received it")
                };
                index += 1;
                let _ = respond(&mut socket, &reply).await;
                let _ = socket.shutdown().await;
            }
        });
        Self { base_url, release }
    }
}

/// 把一次请求读完，头部与实体都要读取
async fn drain_request(socket: &mut TcpStream) {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let Ok(read) = socket.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(end) = head_end(&buffer) {
            let head = String::from_utf8_lossy(&buffer[..end]);
            if buffer.len() >= end + body_len(&head) {
                return;
            }
        }
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

/// 写回一次应答。先写头部再写实体，之后关闭连接
async fn respond(socket: &mut TcpStream, body: &str) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    socket.flush().await
}
