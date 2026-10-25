//! 用户任务的提交入口。空闲时立即开一轮，忙时入队并宣告受理

use std::future::pending;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout};
use wind_core::agent::domain::AgentSpec;
use wind_core::agent::events::AgentCreate;
use wind_core::agent::hub::SubmitOutcome;
use wind_core::agent::tree::CancelOrigin;
use wind_core::agent::tree::phase::Phase;
use wind_core::db::Db;
use wind_core::models::{
    AdapterType, AgentInstanceCreate, AgentMode, AgentPhase, AgentRole, ModelCreate, PendingCreate,
    PendingKind, ProviderCreate, Role, TopicCreate,
};
use wind_core::storage::Storage;
use wind_event::context::DispatchCx;
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModHost};
use wind_event::subscription::Subscription;
use wind_mod::dirs::AppDirs;
use wind_runtime::{Assembly, RuntimeError};
use wind_wire::message::Content;

/// 订阅受理与创建事件的清单
const GUARD_MANIFEST: &str = r#"{"name":"guard","version":1,
    "events":{"on":{
        "core/input.accepted":{"handler":"onAccepted"},
        "core/agent.create":{"handler":"onCreate","phase":"guard"}
    }}}"#;

const TOPIC: i64 = 1;

/// 等一条异步派发事件出现的时限
const PATIENCE: Duration = Duration::from_secs(2);

const POLL: Duration = Duration::from_millis(5);

/// 记下逐次派发的事件名
#[derive(Default)]
struct Recorder {
    /// 收到的事件名，按到达顺序
    seen: Mutex<Vec<String>>,
}

impl Recorder {
    fn seen_names(&self) -> Vec<String> {
        self.seen
            .lock()
            .map(|seen| seen.clone())
            .unwrap_or_default()
    }

    /// 某个事件名被派发的次数
    fn count_of(&self, event: &str) -> usize {
        self.seen_names()
            .iter()
            .filter(|name| name.as_str() == event)
            .count()
    }
}

#[async_trait::async_trait]
impl ModHost for Recorder {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        if let Ok(mut seen) = self.seen.lock() {
            seen.push(sub.event.to_string());
        }
        // 只记录事件名，载荷原样放回
        Ok(Some(payload))
    }
}

/// 一次提交使用的操作上下文
fn submit_cx(topic_id: i64) -> DispatchCx {
    DispatchCx::root(
        Origin::Internal,
        Scope::Topic { topic_id },
        Source::core("runtime"),
        Duration::from_millis(50),
    )
}

/// 建表、安装一个订阅受理事件的 mod，并使能本话题
async fn seeded(dirs: &AppDirs) -> Storage {
    let db = Db::open(&dirs.db_file()).await.expect("open database");
    db.init_schema().await.expect("create table");
    let storage = Storage::new(db);
    let mod_id = storage
        .mod_install("guard", GUARD_MANIFEST, None)
        .await
        .expect("install the mod");
    storage
        .mod_enable(TOPIC, mod_id)
        .await
        .expect("enable the mod");
    storage
}

/// 一套以装配本身驱动的夹具
struct Rig {
    /// 临时目录必须被持有，否则库文件随目录一起删除
    _tmp: TempDir,
    /// 被驱动的装配
    assembly: Assembly,
    /// 本用例自己持有的记录宿主
    host: Arc<Recorder>,
}

impl Rig {
    /// 本话题上的待消费行数
    async fn pending_rows(&self, topic_id: i64) -> usize {
        let instances = self
            .assembly
            .storage()
            .agent_list_by_topic(topic_id)
            .await
            .expect("read instance");
        let ids: Vec<i64> = instances.iter().map(|instance| instance.id).collect();
        self.assembly
            .storage()
            .pending_list_by_instances(&ids)
            .await
            .expect("read the queue")
            .len()
    }

    /// 本话题的实例已经组装出的 user 消息数
    async fn user_messages(&self, topic_id: i64) -> usize {
        let instances = self
            .assembly
            .storage()
            .agent_list_by_topic(topic_id)
            .await
            .expect("read instance");
        let Some(instance) = instances.first() else {
            return 0;
        };
        let turn_ids: Vec<i64> = self
            .assembly
            .storage()
            .turn_list_by_instance(instance.id)
            .await
            .expect("read turn")
            .into_iter()
            .map(|turn| turn.id)
            .collect();
        self.assembly
            .storage()
            .message_list_by_turns(&turn_ids)
            .await
            .expect("read message")
            .iter()
            .filter(|message| message.role == Role::User)
            .count()
    }

    async fn cancel(&self, instance_id: i64) -> Result<(), RuntimeError> {
        self.assembly.cancel(TOPIC, instance_id).await
    }

    /// 等一个事件名出现在记录里，超时返回当时的记录
    async fn wait_for_name(&self, name: &str) -> Vec<String> {
        timeout(PATIENCE, async {
            loop {
                let seen = self.host.seen_names();
                if seen.iter().any(|event| event == name) {
                    return seen;
                }
                sleep(POLL).await;
            }
        })
        .await
        .unwrap_or_else(|_| self.host.seen_names())
    }
}

/// 一个还没有任何实例的话题，第一次提交时创建根实例
async fn rig() -> Rig {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    seeded(&dirs).await;

    let host = Arc::new(Recorder::default());
    let assembly = Assembly::open(dirs, None, Some(host.clone()), None)
        .await
        .expect("assemble");
    Rig {
        _tmp: tmp,
        assembly,
        host,
    }
}

/// 一个上一轮已经结束的根实例，提交时处于空闲
async fn rig_with_a_done_root() -> Rig {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let storage = seeded(&dirs).await;
    storage
        .agent_create(AgentInstanceCreate {
            parent_id: None,
            topic_id: TOPIC,
            role: AgentRole::Main,
            mode: Some(AgentMode::Sync),
            phase: AgentPhase::Done,
            phase_payload: Some(r#"{"message_id": 1}"#.to_string()),
        })
        .await
        .expect("create the root instance");

    let host = Arc::new(Recorder::default());
    let assembly = Assembly::open(dirs, None, Some(host.clone()), None)
        .await
        .expect("assemble");
    Rig {
        _tmp: tmp,
        assembly,
        host,
    }
}

/// 一个停留在运行中的根实例
///
/// 模型请求发往一个只接受连接、永不回应的服务，这一轮不会自行结束
async fn rig_with_a_busy_root() -> Rig {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let storage = seeded(&dirs).await;
    let topic_id = storage
        .topic_create(TopicCreate {
            label: "test".into(),
            ..Default::default()
        })
        .await
        .expect("create topic");
    assert_eq!(
        topic_id, TOPIC,
        "the root instance in this case is on topic 1"
    );
    let base_url = hanging_server().await;
    let provider = storage
        .provider_create(ProviderCreate {
            name: "local".into(),
            base_url,
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
    sqlx::query("UPDATE topics SET model_id = ? WHERE id = ?")
        .bind(model)
        .bind(topic_id)
        .execute(storage.pool())
        .await
        .expect("bind the model");

    let host = Arc::new(Recorder::default());
    let assembly = Assembly::open(dirs, None, Some(host.clone()), None)
        .await
        .expect("assemble");
    let tree = assembly.tree_of(topic_id).await.expect("build tree");
    let root = assembly
        .storage()
        .agent_ensure_main(topic_id, AgentMode::Sync, AgentPhase::Running)
        .await
        .expect("create the root row");
    tree.hub
        .spawn_at(
            &submit_cx(topic_id),
            root,
            &AgentCreate {
                topic_id,
                parent_id: None,
                mode: AgentMode::Sync.to_string(),
                task: None,
            },
        )
        .await
        .expect("spawn the root node");

    let nodes = tree.hub.snapshot().await.expect("query the snapshot");
    assert!(
        nodes
            .iter()
            .any(|node| node.id == root && matches!(node.phase, Phase::Running)),
        "the root node must stay running, otherwise the queuing premise does not hold"
    );
    Rig {
        _tmp: tmp,
        assembly,
        host,
    }
}

/// 启动一个只接受连接、永不回应的服务
///
/// 请求停在等待响应的状态，直到传输层的读超时
async fn hanging_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("has a local address")
    );
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                // 读取请求之后一直占用连接
                let mut buffer = [0u8; 4096];
                let _ = socket.read(&mut buffer).await;
                pending::<()>().await;
            });
        }
    });
    base_url
}

#[tokio::test]
async fn an_idle_instance_launches_without_an_accepted_event() {
    let rig = rig().await;
    let outcome = rig
        .assembly
        .submit(1, vec![Content::new_text("hello".into())])
        .await
        .expect("commit");
    assert_eq!(outcome, SubmitOutcome::Launched);
    assert!(
        rig.host
            .seen_names()
            .iter()
            .all(|name| name != "core/input.accepted"),
        "a submit that starts a turn immediately must not dispatch an accepted event, actual deliveries: {:?}",
        rig.host.seen_names()
    );
    // 待消费行会被 drain 消费。等它变成 user 消息，再断言输入恰好记录一次
    let users = timeout(PATIENCE, async {
        loop {
            if rig.pending_rows(1).await == 0 {
                return rig.user_messages(1).await;
            }
            sleep(POLL).await;
        }
    })
    .await
    .expect("consume the pending row");
    assert_eq!(
        users, 1,
        "each accepted input assembles into exactly one user message"
    );
}

#[tokio::test]
async fn an_existing_terminal_instance_launches_without_an_accepted_event() {
    let rig = rig_with_a_done_root().await;
    let outcome = rig
        .assembly
        .submit(1, vec![Content::new_text("again".into())])
        .await
        .expect("commit");
    assert_eq!(outcome, SubmitOutcome::Launched);
    assert!(
        rig.host
            .seen_names()
            .iter()
            .all(|name| name != "core/input.accepted"),
        "a submit that starts a turn immediately must not dispatch an accepted event, actual deliveries: {:?}",
        rig.host.seen_names()
    );
    // 待消费行会被 drain 消费。等它变成 user 消息，再断言输入恰好记录一次
    let users = timeout(PATIENCE, async {
        loop {
            if rig.pending_rows(1).await == 0 {
                return rig.user_messages(1).await;
            }
            sleep(POLL).await;
        }
    })
    .await
    .expect("consume the pending row");
    assert_eq!(
        users, 1,
        "each accepted input assembles into exactly one user message"
    );
}

#[tokio::test]
async fn an_existing_root_does_not_run_the_create_intercept_again() {
    let rig = rig_with_a_done_root().await;
    let before = rig.host.count_of("core/agent.create");
    let _ = rig
        .assembly
        .submit(TOPIC, vec![Content::new_text("again".into())])
        .await
        .expect("commit");
    assert_eq!(
        before,
        rig.host.count_of("core/agent.create"),
        "the create intercept must not be dispatched again when the root instance already exists"
    );
}

#[tokio::test]
async fn a_busy_instance_queues_and_reports_accepted() {
    let rig = rig_with_a_busy_root().await;
    let outcome = rig
        .assembly
        .submit(1, vec![Content::new_text("continue".into())])
        .await
        .expect("commit");
    assert_eq!(outcome, SubmitOutcome::Queued);
    let seen = rig.wait_for_name("core/input.accepted").await;
    assert!(
        seen.contains(&"core/input.accepted".to_string()),
        "a queued input must announce acceptance externally, actual deliveries: {seen:?}"
    );
    assert_eq!(rig.pending_rows(1).await, 1);
}

#[tokio::test]
async fn a_submit_to_a_cancelling_node_is_dropped_and_its_row_removed() {
    let rig = rig_with_a_busy_root().await;
    let tree = rig.assembly.tree_of(TOPIC).await.expect("build tree");
    tree.hub
        .cancel(&submit_cx(TOPIC), 1, CancelOrigin::External)
        .await
        .expect("request cancellation");

    let outcome = rig
        .assembly
        .submit(TOPIC, vec![Content::new_text("too late".into())])
        .await
        .expect("commit");
    assert_eq!(
        outcome,
        SubmitOutcome::Dropped,
        "a cancelling node does not accept input"
    );
    assert_eq!(
        rig.pending_rows(TOPIC).await,
        0,
        "a dropped submit must not leave a queue row"
    );
    assert!(
        rig.host
            .seen_names()
            .iter()
            .all(|name| name != "core/input.accepted"),
        "a dropped submit must not announce acceptance, actual deliveries: {:?}",
        rig.host.seen_names()
    );
}

#[tokio::test]
async fn cancelling_a_root_drops_descendant_pending_rows() {
    let rig = rig_with_a_busy_root().await;
    let tree = rig.assembly.tree_of(TOPIC).await.expect("build tree");
    let root = tree
        .hub
        .snapshot()
        .await
        .expect("query the snapshot")
        .into_iter()
        .find(|node| node.parent.is_none())
        .expect("find the root node")
        .id;
    let child = rig
        .assembly
        .storage()
        .agent_create(AgentInstanceCreate {
            parent_id: Some(root),
            topic_id: TOPIC,
            role: AgentRole::Child,
            mode: Some(AgentMode::Sync),
            phase: AgentPhase::Running,
            phase_payload: None,
        })
        .await
        .expect("create child instance");
    tree.hub
        .spawn_at_with_spec(
            &submit_cx(TOPIC),
            child,
            &AgentCreate {
                topic_id: TOPIC,
                parent_id: Some(root),
                mode: AgentMode::Sync.to_string(),
                task: None,
            },
            AgentSpec {
                instance_id: child,
                message_id: None,
                pending_id: None,
                turn_id: None,
            },
        )
        .await
        .expect("spawn the child node");
    // 子节点还在运行。一条后台结果排在它的队列里
    let pending = rig
        .assembly
        .storage()
        .pending_insert(PendingCreate {
            instance_id: child,
            kind: PendingKind::BackgroundResult,
            source_id: None,
            payload: [Content::new_text("background result".into())].to_vec(),
        })
        .await
        .expect("enqueue");
    assert_eq!(
        tree.hub
            .submit(
                child,
                AgentSpec {
                    instance_id: child,
                    message_id: None,
                    pending_id: Some(pending),
                    turn_id: None,
                },
            )
            .await
            .expect("commit"),
        SubmitOutcome::Queued,
        "this row must stay in the queue while the child node is running"
    );

    rig.cancel(root).await.expect("cancel the instance");
    assert_eq!(
        rig.assembly
            .storage()
            .pending_list_by_instance(child)
            .await
            .expect("read the queue")
            .len(),
        0,
        "cancelling the root must clear the descendant queue rows already written to the database"
    );
}

#[tokio::test]
async fn cancelling_an_instance_drops_its_queued_inputs() {
    let rig = rig_with_a_busy_root().await;
    let _ = rig
        .assembly
        .submit(1, vec![Content::new_text("stop running".into())])
        .await
        .expect("commit");
    rig.cancel(1).await.expect("cancel the instance");
    assert_eq!(rig.pending_rows(1).await, 0);
}
