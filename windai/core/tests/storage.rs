use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use std::path::PathBuf;
use std::str::FromStr;
use wind_ai::message::{Content, Message as AiMessage, ReqConfig, Role};
use wind_core::WindCore;
use wind_core::error::CoreError;
use wind_core::models::*;
use wind_core::schema::init_schema;
use wind_core::storage::message::MessageStorage;
use wind_mcp::client::TransportType;

/// 临时文件数据库，用于需要真实文件持久化的并发/跨连接测试。
#[allow(dead_code)]
struct TempDbFile {
    path: PathBuf,
}

#[allow(dead_code)]
impl TempDbFile {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "windai-storage-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Self { path }
    }

    fn path(&self) -> &PathBuf {
        &self.path
    }

    fn remove_files(&self) {
        for path in [
            self.path.clone(),
            self.path.with_extension("db-wal"),
            self.path.with_extension("db-shm"),
        ] {
            for _ in 0..5 {
                match std::fs::remove_file(&path) {
                    Ok(_) => break,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
                    Err(_) => std::thread::sleep(std::time::Duration::from_millis(20)),
                }
            }
        }
    }
}

impl Drop for TempDbFile {
    fn drop(&mut self) {
        self.remove_files();
    }
}

/// 创建内存数据库并初始化 WindCore。
///
/// `sqlite::memory:` 作用域绑定到单个物理连接，因此使用 `max_connections(1)`
/// 保证所有查询都命中同一个内存数据库。
async fn setup() -> WindCore {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")
        .unwrap()
        .shared_cache(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    WindCore::init_with_pool(pool).await.unwrap()
}

/// 创建文件持久化的连接池，供需要真实跨连接共享的测试使用。
#[allow(dead_code)]
async fn setup_file_pool(file: &TempDbFile) -> SqlitePool {
    let options = SqliteConnectOptions::new()
        .filename(file.path())
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .unwrap();
    init_schema(&pool).await.unwrap();
    wind_core::storage::init_id_generator(0);
    pool
}

#[allow(dead_code)]
fn assert_not_found<T: std::fmt::Debug>(result: wind_core::error::Result<T>) {
    assert!(
        matches!(result, Err(CoreError::RowNotFound(_))),
        "expected NotFound, got {result:?}"
    );
}

// ---------------------------------------------------------------------------
// 消息构造辅助
// ---------------------------------------------------------------------------

/// 构造一条用户消息。`from_id = None` 表示用户消息。
fn user_msg(
    binding_id: i64,
    model_id: i64,
    text: &str,
    is_boundary: bool,
    is_exclude: bool,
) -> CreateMessage {
    CreateMessage {
        from_id: None,
        stream: false,
        content: vec![AiMessage::new_simple(
            Role::User,
            vec![Content::new_text(text.into())],
            None,
        )],
        model_id,
        binding_id,
        is_boundary,
        is_exclude,
        input_tokens: 5,
        output_tokens: 0,
    }
}

/// 构造一条助手消息，`from_id` 指向其配对的用户消息。
fn asst_msg(
    binding_id: i64,
    model_id: i64,
    from_id: i64,
    text: &str,
    is_boundary: bool,
    is_exclude: bool,
) -> CreateMessage {
    CreateMessage {
        from_id: Some(from_id),
        stream: false,
        content: vec![AiMessage::new_simple(
            Role::Assistant,
            vec![Content::new_text(text.into())],
            None,
        )],
        model_id,
        binding_id,
        is_boundary,
        is_exclude,
        input_tokens: 0,
        output_tokens: 10,
    }
}

/// 在 topic 下创建一对 user-assistant 消息，返回 (user_id, assistant_id)。
async fn create_pair(
    msg: &MessageStorage,
    binding_id: i64,
    model_id: i64,
    user_text: &str,
    assistant_text: &str,
    user_exclude: bool,
    assistant_exclude: bool,
) -> (i64, i64) {
    let user = msg
        .create(user_msg(
            binding_id,
            model_id,
            user_text,
            false,
            user_exclude,
        ))
        .await
        .unwrap();
    let assistant = msg
        .create(asst_msg(
            binding_id,
            model_id,
            user.id,
            assistant_text,
            false,
            assistant_exclude,
        ))
        .await
        .unwrap();
    (user.id, assistant.id)
}

/// 一组基础消息：两对 user-assistant 消息 + 一条 boundary 消息。
struct BoundarySequence {
    u1: i64,
    a1: i64,
    u2: i64,
    a2: i64,
    b: i64,
}

/// 创建两对 user-assistant 消息，并在 `position` 处插入一条 `is_boundary = true` 的消息。
///
/// position（按创建顺序，id 单调递增）：
/// - 0: boundary 在第一个 user 之前
/// - 1: boundary 在第一个 user 与第一个 assistant 之间
/// - 2: boundary 在第一个 assistant 与第二个 user 之间
/// - 3: boundary 在第二个 user 与第二个 assistant 之间
/// - 4: boundary 在第二个 assistant 之后
async fn create_boundary_sequence(
    msg: &MessageStorage,
    topic_id: i64,
    model_id: i64,
    position: usize,
) -> BoundarySequence {
    let mut s = BoundarySequence {
        u1: 0,
        a1: 0,
        u2: 0,
        a2: 0,
        b: 0,
    };
    match position {
        0 => {
            s.b = msg
                .create(user_msg(topic_id, model_id, "boundary", true, false))
                .await
                .unwrap()
                .id;
            s.u1 = msg
                .create(user_msg(topic_id, model_id, "q1", false, false))
                .await
                .unwrap()
                .id;
            s.a1 = msg
                .create(asst_msg(topic_id, model_id, s.u1, "a1", false, false))
                .await
                .unwrap()
                .id;
            s.u2 = msg
                .create(user_msg(topic_id, model_id, "q2", false, false))
                .await
                .unwrap()
                .id;
            s.a2 = msg
                .create(asst_msg(topic_id, model_id, s.u2, "a2", false, false))
                .await
                .unwrap()
                .id;
        }
        1 => {
            s.u1 = msg
                .create(user_msg(topic_id, model_id, "q1", false, false))
                .await
                .unwrap()
                .id;
            s.b = msg
                .create(user_msg(topic_id, model_id, "boundary", true, false))
                .await
                .unwrap()
                .id;
            s.a1 = msg
                .create(asst_msg(topic_id, model_id, s.u1, "a1", false, false))
                .await
                .unwrap()
                .id;
            s.u2 = msg
                .create(user_msg(topic_id, model_id, "q2", false, false))
                .await
                .unwrap()
                .id;
            s.a2 = msg
                .create(asst_msg(topic_id, model_id, s.u2, "a2", false, false))
                .await
                .unwrap()
                .id;
        }
        2 => {
            s.u1 = msg
                .create(user_msg(topic_id, model_id, "q1", false, false))
                .await
                .unwrap()
                .id;
            s.a1 = msg
                .create(asst_msg(topic_id, model_id, s.u1, "a1", false, false))
                .await
                .unwrap()
                .id;
            s.b = msg
                .create(user_msg(topic_id, model_id, "boundary", true, false))
                .await
                .unwrap()
                .id;
            s.u2 = msg
                .create(user_msg(topic_id, model_id, "q2", false, false))
                .await
                .unwrap()
                .id;
            s.a2 = msg
                .create(asst_msg(topic_id, model_id, s.u2, "a2", false, false))
                .await
                .unwrap()
                .id;
        }
        3 => {
            s.u1 = msg
                .create(user_msg(topic_id, model_id, "q1", false, false))
                .await
                .unwrap()
                .id;
            s.a1 = msg
                .create(asst_msg(topic_id, model_id, s.u1, "a1", false, false))
                .await
                .unwrap()
                .id;
            s.u2 = msg
                .create(user_msg(topic_id, model_id, "q2", false, false))
                .await
                .unwrap()
                .id;
            s.b = msg
                .create(user_msg(topic_id, model_id, "boundary", true, false))
                .await
                .unwrap()
                .id;
            s.a2 = msg
                .create(asst_msg(topic_id, model_id, s.u2, "a2", false, false))
                .await
                .unwrap()
                .id;
        }
        _ => {
            s.u1 = msg
                .create(user_msg(topic_id, model_id, "q1", false, false))
                .await
                .unwrap()
                .id;
            s.a1 = msg
                .create(asst_msg(topic_id, model_id, s.u1, "a1", false, false))
                .await
                .unwrap()
                .id;
            s.u2 = msg
                .create(user_msg(topic_id, model_id, "q2", false, false))
                .await
                .unwrap()
                .id;
            s.a2 = msg
                .create(asst_msg(topic_id, model_id, s.u2, "a2", false, false))
                .await
                .unwrap()
                .id;
            s.b = msg
                .create(user_msg(topic_id, model_id, "boundary", true, false))
                .await
                .unwrap()
                .id;
        }
    }
    s
}

/// 创建一个普通的根 Topic。
async fn create_root_topic(
    topic_storage: &wind_core::storage::topic::TopicStorage,
    label: &str,
) -> Topic {
    topic_storage
        .create(CreateTopic {
            parent_id: None,
            label: label.into(),
            icon: None,
        })
        .await
        .unwrap()
}

/// 创建一个 AgentDefinition；`owner_topic_id` 为 `Some` 表示 topic 专属 Agent。
async fn create_agent_def(
    agent: &wind_core::storage::agent::AgentStorage,
    key: &str,
    owner_topic_id: Option<i64>,
) -> AgentDefinition {
    agent
        .create_definition(CreateAgentDefinition {
            key: key.into(),
            name: key.into(),
            description: key.into(),
            owner_topic_id,
            cloned_from_id: None,
            active: Some(true),
            data: AgentDefinitionData::default(),
        })
        .await
        .unwrap()
}

/// 为 `topic_id` 绑定 `agent_id`，返回 binding。
async fn bind_agent(
    agent: &wind_core::storage::agent::AgentStorage,
    topic_id: i64,
    agent_id: i64,
    role: AgentRole,
) -> AgentInstance {
    agent
        .create_instance(CreateInstance {
            topic_id,
            agent_id,
            role,
            model_id: None,
            chat_config_id: None,
            enabled: Some(true),
        })
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

/// AgentDefinition / AgentBinding 的 CRUD 与查询。
#[tokio::test]
async fn agent_binding_crud() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    // create_definition：创建三个随机的 AgentDefinition
    let def_a = agent
        .create_definition(CreateAgentDefinition {
            key: "agent-a".into(),
            name: "Agent A".into(),
            description: "first agent".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: Some(true),
            data: AgentDefinitionData::default(),
        })
        .await
        .unwrap();
    let def_b = agent
        .create_definition(CreateAgentDefinition {
            key: "agent-b".into(),
            name: "Agent B".into(),
            description: "second agent".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: Some(true),
            data: AgentDefinitionData::default(),
        })
        .await
        .unwrap();
    let def_c = agent
        .create_definition(CreateAgentDefinition {
            key: "agent-c".into(),
            name: "Agent C".into(),
            description: "third agent".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: Some(true),
            data: AgentDefinitionData::default(),
        })
        .await
        .unwrap();

    // create：创建一个 Topic
    let topic = create_root_topic(topics, "root").await;

    // create_binding：在 topic 下创建三个 binding，分别绑定三个 definition；第一个为 Main
    let binding_main = agent
        .create_instance(CreateInstance {
            agent_id: def_a.id,
            role: AgentRole::Main,
            model_id: None,
            chat_config_id: None,
            enabled: Some(true),
            topic_id: topic.id,
        })
        .await
        .unwrap();
    let binding_b = agent
        .create_instance(CreateInstance {
            topic_id: topic.id,
            agent_id: def_b.id,
            role: AgentRole::Child,
            model_id: None,
            chat_config_id: None,
            enabled: Some(true),
        })
        .await
        .unwrap();
    let binding_c = agent
        .create_instance(CreateInstance {
            topic_id: topic.id,
            agent_id: def_c.id,
            role: AgentRole::Child,
            model_id: None,
            chat_config_id: None,
            enabled: Some(true),
        })
        .await
        .unwrap();

    // get_binding / get_definition：通过 id 找回创建的 binding / definition
    let got = agent.get_instance(binding_main.id).await.unwrap().unwrap();
    assert_eq!(got.id, binding_main.id);
    assert_eq!(got.topic_id, topic.id);
    assert_eq!(got.agent_id, def_a.id);
    assert_eq!(got.role, AgentRole::Main);

    let got_def = agent.get_definition(def_b.id).await.unwrap().unwrap();
    assert_eq!(got_def.id, def_b.id);
    assert_eq!(got_def.key, "agent-b");
    assert_eq!(got_def.name, "Agent B");

    // list_definitions_by_topic：只有 role != main 且 enabled 的绑定会关联出 definition
    let defs = agent.list_sub_definitions_by_topic(topic.id).await.unwrap();
    assert_eq!(defs.len(), 2, "main 绑定的 definition 不应出现在列表中");
    let mut def_ids: Vec<i64> = defs.iter().map(|d| d.id).collect();
    def_ids.sort_unstable();
    assert_eq!(def_ids, vec![def_b.id, def_c.id]);

    // get_definition_by_key：通过 key 查找 definition
    let by_key = agent
        .get_definition_by_key("agent-c")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(by_key.id, def_c.id);

    // get_binding_by_agent_id：通过 topic + agent 查找 binding
    let found = agent
        .get_binding_by_agent_id(topic.id, def_b.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.id, binding_b.id);
    assert_eq!(found.agent_id, def_b.id);
    assert_eq!(found.topic_id, topic.id);

    let found_c = agent
        .get_binding_by_agent_id(topic.id, def_c.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found_c.id, binding_c.id);
    assert_eq!(found_c.role, AgentRole::Child);
}

/// 子话题查询：list_child_topics 只返回 parent_id 匹配的直接子话题，且不改变 list_topics 语义。
#[tokio::test]
async fn topic_child_listing() {
    let core = setup().await;
    let topics = core.storage().topic();

    // 两个根话题
    let root_a = create_root_topic(topics, "root-a").await;
    let root_b = create_root_topic(topics, "root-b").await;

    // root-a 下建两个子话题
    let child_a = topics
        .create(CreateTopic {
            parent_id: Some(root_a.id),
            label: "child-a".into(),
            icon: None,
        })
        .await
        .unwrap();
    let child_b = topics
        .create(CreateTopic {
            parent_id: Some(root_a.id),
            label: "child-b".into(),
            icon: None,
        })
        .await
        .unwrap();

    // root-a 的子话题按创建顺序返回
    let children = topics.list_child_topics(root_a.id).await.unwrap();
    let child_ids: Vec<i64> = children.iter().map(|t| t.id).collect();
    assert_eq!(child_ids, vec![child_a.id, child_b.id]);
    assert!(children.iter().all(|t| t.parent_id == Some(root_a.id)));

    // root-b 无子话题
    assert!(
        topics
            .list_child_topics(root_b.id)
            .await
            .unwrap()
            .is_empty()
    );

    // list_topics 只返回根话题，子话题不出现在根列表中
    let root_ids: Vec<i64> = topics
        .list_topics()
        .await
        .unwrap()
        .iter()
        .map(|t| t.id)
        .collect();
    assert!(root_ids.contains(&root_a.id));
    assert!(root_ids.contains(&root_b.id));
    assert!(!root_ids.contains(&child_a.id));
}

/// 消息 is_excluded 标志对 list_by_binding / list_contexts 的影响。
/// tips: 该测试中，消息被手动设置为is_excluded = true, 而不是成对消息被删除后自动设置另一个消息
#[tokio::test]
async fn message_excluded_crud() {
    let core = setup().await;
    let topics = core.storage().topic();
    let msg = core.storage().message();
    let model_id = 1;

    // ---- case1: 两对 user-assistant 消息，全部 is_excluded = false ----
    {
        let topic = create_root_topic(topics, "case1").await;
        let (u1, a1) = create_pair(msg, topic.id, model_id, "q1", "a1", false, false).await;
        let (u2, a2) = create_pair(msg, topic.id, model_id, "q2", "a2", false, false).await;

        // list_by_binding：数量、id 与创建顺序
        let list = msg.list_by_instance(topic.id).await.unwrap();
        assert_eq!(list.len(), 4);
        let ids: Vec<i64> = list.iter().map(|m| m.id).collect();
        assert_eq!(ids, vec![u1, a1, u2, a2]);

        // list_contexts：无排除、无 boundary，全部消息作为上下文
        let ctx = msg.list_contexts(topic.id).await.unwrap();
        assert_eq!(ctx.len(), 4);
        let ctx_ids: Vec<i64> = ctx.iter().map(|m| m.id).collect();
        assert_eq!(ctx_ids, vec![u1, a1, u2, a2]);
    }

    // ---- case2: 两对消息，随机将其中一条 is_excluded = true ----
    // 遍历每一条消息作为被排除者，等价于覆盖所有"随机"位置
    for excluded in ["u1", "a1", "u2", "a2"] {
        let topic = create_root_topic(topics, &format!("case2-{excluded}")).await;
        let (u1, a1) = create_pair(
            msg,
            topic.id,
            model_id,
            "q1",
            "a1",
            excluded == "u1",
            excluded == "a1",
        )
        .await;
        let (u2, a2) = create_pair(
            msg,
            topic.id,
            model_id,
            "q2",
            "a2",
            excluded == "u2",
            excluded == "a2",
        )
        .await;

        // list_by_binding：排除不影响全量列表
        let list = msg.list_by_instance(topic.id).await.unwrap();
        assert_eq!(list.len(), 4);
        let ids: Vec<i64> = list.iter().map(|m| m.id).collect();
        assert_eq!(ids, vec![u1, a1, u2, a2]);

        // list_contexts：被排除的那一条消息不再作为上下文
        let ctx = msg.list_contexts(topic.id).await.unwrap();
        let ctx_ids: Vec<i64> = ctx.iter().map(|m| m.id).collect();
        let excluded_id = match excluded {
            "u1" => u1,
            "a1" => a1,
            "u2" => u2,
            _ => a2,
        };
        assert_eq!(ctx_ids.len(), 3);
        assert!(
            !ctx_ids.contains(&excluded_id),
            "被排除的消息 {excluded_id} 不应出现在上下文中"
        );
        assert!(
            ctx.iter().all(|m| !m.is_excluded),
            "list_contexts 不应返回 is_excluded=1 的消息"
        );
        for id in [u1, a1, u2, a2] {
            if id != excluded_id {
                assert!(ctx_ids.contains(&id), "消息 {id} 应出现在上下文中");
            }
        }
    }

    // ---- case3: 两对消息，全部 is_excluded = true ----
    {
        let topic = create_root_topic(topics, "case3").await;
        let _ = create_pair(msg, topic.id, model_id, "q1", "a1", true, true).await;
        let _ = create_pair(msg, topic.id, model_id, "q2", "a2", true, true).await;

        // list_by_binding：仍然全部存在
        let list = msg.list_by_instance(topic.id).await.unwrap();
        assert_eq!(list.len(), 4);

        // list_contexts：全部被排除，上下文为空
        let ctx = msg.list_contexts(topic.id).await.unwrap();
        assert!(ctx.is_empty(), "全部被排除时上下文应为空");
    }
}

/// 删除消息后，其配对的 user/assistant 消息被标记 is_excluded。
#[tokio::test]
async fn message_del_excluded_crud() {
    let core = setup().await;
    let topics = core.storage().topic();
    let msg = core.storage().message();
    let model_id = 1;

    // 初始：两对 user-assistant 消息，全部 is_excluded = false
    let topic = create_root_topic(topics, "del").await;
    let (u1, a1) = create_pair(msg, topic.id, model_id, "q1", "a1", false, false).await;
    let (u2, a2) = create_pair(msg, topic.id, model_id, "q2", "a2", false, false).await;

    // list_by_binding：数量、id 与创建顺序
    let list = msg.list_by_instance(topic.id).await.unwrap();
    assert_eq!(list.len(), 4);
    let ids: Vec<i64> = list.iter().map(|m| m.id).collect();
    assert_eq!(ids, vec![u1, a1, u2, a2]);

    // list_contexts：符合函数注释描述——无 boundary、无排除，全部作为上下文
    let ctx = msg.list_contexts(topic.id).await.unwrap();
    assert_eq!(ctx.len(), 4);

    // ---- 场景1: 随机删除一条助手消息 a1 ----
    {
        msg.delete(a1).await.unwrap();

        // 配对的用户消息被标记为 is_excluded = true
        let u1_now = msg.get(u1).await.unwrap().unwrap();
        assert!(u1_now.is_excluded, "删除助手消息后配对的用户消息应被排除");
        // 被删除的消息不再存在
        assert!(msg.get(a1).await.unwrap().is_none());

        // 1. list_contexts：u1-a1 消息对不再出现在上下文中，只剩另一对；
        //    且返回的消息都不含 is_excluded=1
        let ctx = msg.list_contexts(topic.id).await.unwrap();
        let ctx_ids: Vec<i64> = ctx.iter().map(|m| m.id).collect();
        assert_eq!(ctx_ids, vec![u2, a2]);
        assert!(
            ctx.iter().all(|m| !m.is_excluded),
            "list_contexts 不应返回 is_excluded=1 的消息"
        );

        // 2. list_by_binding：所有未删除的消息完全存在（u1 仍存在，但已被排除）
        let list = msg.list_by_instance(topic.id).await.unwrap();
        assert_eq!(list.len(), 3);
        let list_ids: Vec<i64> = list.iter().map(|m| m.id).collect();
        assert!(list_ids.contains(&u1));
        assert!(list_ids.contains(&u2));
        assert!(list_ids.contains(&a2));
        assert!(!list_ids.contains(&a1));
    }

    // ---- 场景2: 随机删除一条用户消息 u1 ----
    {
        // 重新建立独立 topic，避免与场景1相互影响
        let topic = create_root_topic(topics, "del-user").await;
        let (u1, a1) = create_pair(msg, topic.id, model_id, "q1", "a1", false, false).await;
        let (u2, a2) = create_pair(msg, topic.id, model_id, "q2", "a2", false, false).await;

        msg.delete(u1).await.unwrap();

        // 配对的助手消息被标记为 is_excluded = true
        let a1_now = msg.get(a1).await.unwrap().unwrap();
        assert!(a1_now.is_excluded, "删除用户消息后配对的助手消息应被排除");
        assert!(msg.get(u1).await.unwrap().is_none());

        // 1. list_contexts：u1-a1 消息对不再出现在上下文中；
        //    且返回的消息都不含 is_excluded=1
        let ctx = msg.list_contexts(topic.id).await.unwrap();
        let ctx_ids: Vec<i64> = ctx.iter().map(|m| m.id).collect();
        assert_eq!(ctx_ids, vec![u2, a2]);
        assert!(
            ctx.iter().all(|m| !m.is_excluded),
            "list_contexts 不应返回 is_excluded=1 的消息"
        );

        // 2. list_by_binding：所有未删除的消息完全存在
        let list = msg.list_by_instance(topic.id).await.unwrap();
        assert_eq!(list.len(), 3);
        let list_ids: Vec<i64> = list.iter().map(|m| m.id).collect();
        assert!(list_ids.contains(&a1));
        assert!(list_ids.contains(&u2));
        assert!(list_ids.contains(&a2));
        assert!(!list_ids.contains(&u1));
    }
}

/// 插入 is_boundary = true 消息后，list_contexts 只返回边界之后的消息。
#[tokio::test]
async fn message_boundary_crud() {
    let core = setup().await;
    let topics = core.storage().topic();
    let msg = core.storage().message();
    let model_id = 1;

    // 遍历 boundary 的 5 个不同插入位置（覆盖描述中列举的前后位置）
    let labels = [
        "before-u1",
        "between-u1-a1",
        "between-a1-u2",
        "between-u2-a2",
        "after-a2",
    ];
    for (position, label) in labels.iter().enumerate() {
        let topic = create_root_topic(topics, &format!("boundary-{label}")).await;
        let s = create_boundary_sequence(msg, topic.id, model_id, position).await;

        // 按创建顺序排列的全部消息 id（id 随创建单调递增）
        let expected_order: Vec<i64> = match position {
            0 => vec![s.b, s.u1, s.a1, s.u2, s.a2],
            1 => vec![s.u1, s.b, s.a1, s.u2, s.a2],
            2 => vec![s.u1, s.a1, s.b, s.u2, s.a2],
            3 => vec![s.u1, s.a1, s.u2, s.b, s.a2],
            _ => vec![s.u1, s.a1, s.u2, s.a2, s.b],
        };

        // list_by_binding：所有消息完全存在且按 id 顺序
        let list = msg.list_by_instance(topic.id).await.unwrap();
        assert_eq!(list.len(), 5);
        let ids: Vec<i64> = list.iter().map(|m| m.id).collect();
        assert_eq!(
            ids, expected_order,
            "position {position} 下 list_by_binding 顺序错误"
        );

        // list_contexts：只返回 boundary 之后（不含 boundary 自身）且未排除的消息
        let ctx = msg.list_contexts(topic.id).await.unwrap();
        let ctx_ids: Vec<i64> = ctx.iter().map(|m| m.id).collect();
        let expected_ctx: Vec<i64> = expected_order
            .iter()
            .copied()
            .filter(|id| *id > s.b)
            .collect();
        assert_eq!(
            ctx_ids, expected_ctx,
            "position {position} 下 list_contexts 错误"
        );
        assert!(!ctx_ids.contains(&s.b), "boundary 消息自身不应作为上下文");

        // ---- 删除场景：删除第一个助手消息 a1 ----
        msg.delete(s.a1).await.unwrap();

        // 配对的用户消息被标记排除，被删消息不存在
        let u1_now = msg.get(s.u1).await.unwrap().unwrap();
        assert!(u1_now.is_excluded, "position {position} 下 u1 应被排除");
        assert!(msg.get(s.a1).await.unwrap().is_none());

        // list_contexts：boundary 之后、非排除的消息中不再包含 u1-a1 消息对；
        //    且返回的消息都不含 is_excluded=1
        let ctx = msg.list_contexts(topic.id).await.unwrap();
        let ctx_ids: Vec<i64> = ctx.iter().map(|m| m.id).collect();
        let expected: Vec<i64> = expected_order
            .iter()
            .copied()
            .filter(|id| *id > s.b && *id != s.u1 && *id != s.a1)
            .collect();
        assert_eq!(
            ctx_ids, expected,
            "position {position} 删除后 list_contexts 错误"
        );
        assert!(
            ctx.iter().all(|m| !m.is_excluded),
            "position {position} 下 list_contexts 不应返回 is_excluded=1 的消息"
        );

        // list_by_binding：所有未删除的消息完全存在
        let list = msg.list_by_instance(topic.id).await.unwrap();
        assert_eq!(list.len(), 4);
        let list_ids: Vec<i64> = list.iter().map(|m| m.id).collect();
        assert!(!list_ids.contains(&s.a1));
        for id in [s.u1, s.u2, s.a2, s.b] {
            assert!(
                list_ids.contains(&id),
                "position {position} 下消息 {id} 应存在"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 内建 MCP 绑定（builtin_mcp_servers）与保留名
// ---------------------------------------------------------------------------

/// McpStorage::create 拒绝内建保留名，避免与 registry 常驻内建冲突。
#[tokio::test]
async fn mcp_create_rejects_builtin_reserved_name() {
    let core = setup().await;
    let result = core
        .storage()
        .mcp()
        .create(CreateMcpServer {
            r#type: TransportType::Stdio,
            name: wind_mcp::builtin::BUILTIN_FS.name.to_string(),
            url: None,
            description: None,
            command: Some("npx".into()),
            args: None,
            env: None,
        })
        .await;
    assert!(
        matches!(result, Err(CoreError::Validation(_))),
        "got: {result:?}"
    );
}

/// agent 定义内建 MCP 绑定：目录内名字可创建，未知名被 Validation 拒绝。
#[tokio::test]
async fn agent_definition_validates_builtin_binding_names() {
    let core = setup().await;
    let agent = core.storage().agent();

    fn build_def(key: &str, builtins: Vec<BuiltinMcpBinding>) -> CreateAgentDefinition {
        CreateAgentDefinition {
            key: key.into(),
            name: key.into(),
            description: "d".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: None,
            data: AgentDefinitionData {
                builtin_mcp_servers: builtins,
                ..AgentDefinitionData::default()
            },
        }
    }

    let ok = agent
        .create_definition(build_def(
            "with-builtin",
            vec![BuiltinMcpBinding {
                name: wind_mcp::builtin::BUILTIN_FS.name.to_string(),
                allowed_tools: vec![],
                denied_tools: vec![],
                enabled: true,
            }],
        ))
        .await;
    assert!(ok.is_ok(), "valid builtin name should pass: {ok:?}");
}

/// 旧数据缺 builtin_mcp_servers 字段时反序列化回退为空列表。
#[test]
fn agent_definition_data_defaults_builtin_servers() {
    let mut value = serde_json::to_value(AgentDefinitionData::default()).unwrap();
    value.as_object_mut().unwrap().remove("builtin_mcp_servers");
    let data: AgentDefinitionData = serde_json::from_value(value).unwrap();
    assert!(data.builtin_mcp_servers.is_empty());
}

/// 删除 Topic 时级联清理：专属 definition、binding、message、chat_config、审批记录。
#[tokio::test]
async fn topic_delete_cascades() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();
    let msg = core.storage().message();
    let model_id = 1;

    let root = create_root_topic(topics, "cascade-root").await;

    // owner_topic_id 指向该 topic 的 definition 属于"专属 definition"
    let def = agent
        .create_definition(CreateAgentDefinition {
            key: "owned-agent".into(),
            name: "Owned".into(),
            description: "owned by topic".into(),
            owner_topic_id: Some(root.id),
            cloned_from_id: None,
            active: Some(true),
            data: AgentDefinitionData::default(),
        })
        .await
        .unwrap();

    let config = topics
        .create_chat_config(ReqConfig::default())
        .await
        .unwrap();
    let binding = agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            agent_id: def.id,
            role: AgentRole::Main,
            model_id: None,
            chat_config_id: Some(config.id),
            enabled: Some(true),
        })
        .await
        .unwrap();

    let (u1, a1) = create_pair(msg, binding.id, model_id, "q1", "a1", false, false).await;
    let approvals = core
        .storage()
        .approval()
        .create_requests(CreateToolApprovalRequests {
            binding_id: binding.id,
            topic_id: root.id,
            message_id: a1,
            calls: vec![CreateToolApprovalCall {
                tool_call_id: "call-1".into(),
                tool_name: "fs0m0read".into(),
                arguments: serde_json::json!({}),
            }],
        })
        .await
        .unwrap();
    assert_eq!(approvals.len(), 1, "审批记录应创建成功");

    topics.delete_topics(&[root.id]).await.unwrap();

    assert!(
        topics.get_topic(root.id).await.unwrap().is_none(),
        "topic 应被删除"
    );
    assert!(
        agent.get_definition(def.id).await.unwrap().is_none(),
        "topic 专属 definition 应被删除"
    );
    assert!(
        agent.get_instance(binding.id).await.unwrap().is_none(),
        "binding 应被删除"
    );
    assert!(msg.get(u1).await.unwrap().is_none(), "user 消息应被删除");
    assert!(
        msg.get(a1).await.unwrap().is_none(),
        "assistant 消息应被删除"
    );
    assert!(
        topics.get_chat_config(config.id).await.unwrap().is_none(),
        "chat_config 应被删除"
    );
    assert!(
        core.storage()
            .approval()
            .list_by_message(a1)
            .await
            .unwrap()
            .is_empty(),
        "审批记录应被删除"
    );
}

/// 删除 binding 时级联清理 messages、chat_config 与审批记录，topic/definition 保留。
#[tokio::test]
async fn binding_delete_cascades() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();
    let msg = core.storage().message();
    let model_id = 1;

    let root = create_root_topic(topics, "binding-cascade").await;
    let def = agent
        .create_definition(CreateAgentDefinition {
            key: "keep-me".into(),
            name: "KeepMe".into(),
            description: "global agent".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: Some(true),
            data: AgentDefinitionData::default(),
        })
        .await
        .unwrap();

    let config = topics
        .create_chat_config(ReqConfig::default())
        .await
        .unwrap();
    let binding = agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            agent_id: def.id,
            role: AgentRole::Main,
            model_id: None,
            chat_config_id: Some(config.id),
            enabled: Some(true),
        })
        .await
        .unwrap();

    let (u1, a1) = create_pair(msg, binding.id, model_id, "q1", "a1", false, false).await;
    core.storage()
        .approval()
        .create_requests(CreateToolApprovalRequests {
            binding_id: binding.id,
            topic_id: root.id,
            message_id: a1,
            calls: vec![CreateToolApprovalCall {
                tool_call_id: "call-1".into(),
                tool_name: "fs0m0read".into(),
                arguments: serde_json::json!({}),
            }],
        })
        .await
        .unwrap();

    agent.delete_instances(&[binding.id]).await.unwrap();

    assert!(
        agent.get_instance(binding.id).await.unwrap().is_none(),
        "binding 应被删除"
    );
    assert!(msg.get(u1).await.unwrap().is_none(), "user 消息应被删除");
    assert!(
        msg.get(a1).await.unwrap().is_none(),
        "assistant 消息应被删除"
    );
    assert!(
        topics.get_chat_config(config.id).await.unwrap().is_none(),
        "chat_config 应被删除"
    );
    assert!(
        core.storage()
            .approval()
            .list_by_message(a1)
            .await
            .unwrap()
            .is_empty(),
        "审批记录应被删除"
    );
    assert!(
        topics.get_topic(root.id).await.unwrap().is_some(),
        "topic 必须保留"
    );
    assert!(
        agent.get_definition(def.id).await.unwrap().is_some(),
        "definition 必须保留"
    );
}

/// 一个 topic 下同一个 AgentDefinition 只能被一个 binding 使用。
#[tokio::test]
async fn create_binding_rejects_duplicate_agent() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "dup-agent").await;
    let def = agent
        .create_definition(CreateAgentDefinition {
            key: "dup".into(),
            name: "Dup".into(),
            description: "dup".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: Some(true),
            data: AgentDefinitionData::default(),
        })
        .await
        .unwrap();

    agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            agent_id: def.id,
            role: AgentRole::Child,
            model_id: None,
            chat_config_id: None,
            enabled: Some(false),
        })
        .await
        .unwrap();

    // 即便已有 binding 被禁用，也不允许再绑一次
    let err = agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            agent_id: def.id,
            role: AgentRole::Child,
            model_id: None,
            chat_config_id: None,
            enabled: Some(true),
        })
        .await
        .expect_err("重复绑定应被拒绝");
    assert!(
        matches!(err, CoreError::Validation(_)),
        "expected Validation, got {err:?}"
    );
}

/// 一个 topic 下只能有一个主 Agent。
#[tokio::test]
async fn create_binding_rejects_second_main() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "dup-main").await;
    let mut defs = Vec::new();
    for key in ["main-a", "main-b"] {
        defs.push(
            agent
                .create_definition(CreateAgentDefinition {
                    key: key.into(),
                    name: key.into(),
                    description: key.into(),
                    owner_topic_id: None,
                    cloned_from_id: None,
                    active: Some(true),
                    data: AgentDefinitionData::default(),
                })
                .await
                .unwrap(),
        );
    }

    agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            agent_id: defs[0].id,
            role: AgentRole::Main,
            model_id: None,
            chat_config_id: None,
            enabled: Some(true),
        })
        .await
        .unwrap();

    let err = agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            agent_id: defs[1].id,
            role: AgentRole::Main,
            model_id: None,
            chat_config_id: None,
            enabled: Some(true),
        })
        .await
        .expect_err("第二个主 Agent 应被拒绝");
    assert!(
        matches!(err, CoreError::Validation(_)),
        "expected Validation, got {err:?}"
    );
}

/// topic 没有任何专属 definition 时，删除也必须成功。
#[tokio::test]
async fn topic_delete_succeeds_without_owned_definitions() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();
    let msg = core.storage().message();
    let model_id = 1;

    let root = create_root_topic(topics, "no-owned-def").await;
    let def = agent
        .create_definition(CreateAgentDefinition {
            key: "global-agent".into(),
            name: "Global".into(),
            description: "global".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: Some(true),
            data: AgentDefinitionData::default(),
        })
        .await
        .unwrap();
    let binding = agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            agent_id: def.id,
            role: AgentRole::Main,
            model_id: None,
            chat_config_id: None,
            enabled: Some(true),
        })
        .await
        .unwrap();
    let (u1, a1) = create_pair(msg, binding.id, model_id, "q1", "a1", false, false).await;

    topics.delete_topics(&[root.id]).await.unwrap();

    assert!(
        topics.get_topic(root.id).await.unwrap().is_none(),
        "topic 应被删除"
    );
    assert!(
        agent.get_instance(binding.id).await.unwrap().is_none(),
        "binding 应被删除"
    );
    assert!(msg.get(u1).await.unwrap().is_none(), "user 消息应被删除");
    assert!(
        msg.get(a1).await.unwrap().is_none(),
        "assistant 消息应被删除"
    );
    assert!(
        agent.get_definition(def.id).await.unwrap().is_some(),
        "全局 definition 不属于该 topic，不应被删除"
    );
}

/// 删除一个 topic 不能影响其它 topic 的 definition / binding / 消息。
#[tokio::test]
async fn topic_delete_does_not_touch_other_topics() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();
    let msg = core.storage().message();
    let model_id = 1;

    let root_a = create_root_topic(topics, "keep-b-root-a").await;
    let root_b = create_root_topic(topics, "keep-b-root-b").await;

    let def_a = create_agent_def(agent, "keep-b-a", Some(root_a.id)).await;
    let def_b = create_agent_def(agent, "keep-b-b", Some(root_b.id)).await;
    let binding_a = bind_agent(agent, root_a.id, def_a.id, AgentRole::Main).await;
    let binding_b = bind_agent(agent, root_b.id, def_b.id, AgentRole::Main).await;
    let (ua, aa) = create_pair(msg, binding_a.id, model_id, "qa", "aa", false, false).await;
    let (ub, ab) = create_pair(msg, binding_b.id, model_id, "qb", "ab", false, false).await;

    topics.delete_topics(&[root_a.id]).await.unwrap();

    assert!(
        topics.get_topic(root_a.id).await.unwrap().is_none(),
        "topic A 应被删除"
    );
    assert!(
        agent.get_definition(def_a.id).await.unwrap().is_none(),
        "A 的专属 definition 应被删除"
    );
    assert!(
        agent.get_instance(binding_a.id).await.unwrap().is_none(),
        "A 的 binding 应被删除"
    );
    assert!(
        msg.get(ua).await.unwrap().is_none(),
        "A 的 user 消息应被删除"
    );
    assert!(
        msg.get(aa).await.unwrap().is_none(),
        "A 的 assistant 消息应被删除"
    );

    assert!(
        topics.get_topic(root_b.id).await.unwrap().is_some(),
        "topic B 必须保留"
    );
    assert!(
        agent.get_definition(def_b.id).await.unwrap().is_some(),
        "B 的 definition 必须保留"
    );
    assert!(
        agent.get_instance(binding_b.id).await.unwrap().is_some(),
        "B 的 binding 必须保留"
    );
    assert!(
        msg.get(ub).await.unwrap().is_some(),
        "B 的 user 消息必须保留"
    );
    assert!(
        msg.get(ab).await.unwrap().is_some(),
        "B 的 assistant 消息必须保留"
    );
}

/// topic 专属 definition 只能绑定到其所属 topic。
#[tokio::test]
async fn create_binding_rejects_foreign_topic_local_definition() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root_a = create_root_topic(topics, "local-owner-a").await;
    let root_b = create_root_topic(topics, "local-owner-b").await;
    let local_def = create_agent_def(agent, "local-only", Some(root_a.id)).await;

    // 归属 topic 自身可以绑定
    bind_agent(agent, root_a.id, local_def.id, AgentRole::Main).await;

    // 其它 topic 绑定同一个专属 definition 应被拒绝
    let err = agent
        .create_instance(CreateInstance {
            topic_id: root_b.id,
            agent_id: local_def.id,
            role: AgentRole::Main,
            model_id: None,
            chat_config_id: None,
            enabled: Some(true),
        })
        .await
        .expect_err("跨 topic 绑定专属 Agent 应被拒绝");
    assert!(
        matches!(err, CoreError::Validation(_)),
        "expected Validation, got {err:?}"
    );

    // 已有 binding 改绑专属 definition 同样应被拒绝
    let global_def = create_agent_def(agent, "global-in-b", None).await;
    let child = bind_agent(agent, root_b.id, global_def.id, AgentRole::Child).await;
    let err = agent
        .update_instance(
            child.id,
            UpdateInstance {
                agent_id: Some(local_def.id),
                ..Default::default()
            },
        )
        .await
        .expect_err("改绑到其它 topic 的专属 Agent 应被拒绝");
    assert!(
        matches!(err, CoreError::Validation(_)),
        "expected Validation, got {err:?}"
    );
}

/// 一次删除多个 topic，且不级联删除其子 topic。
#[tokio::test]
async fn topic_delete_handles_multiple_ids_and_keeps_child_topics() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root_a = create_root_topic(topics, "root-a").await;
    let root_b = create_root_topic(topics, "root-b").await;
    let child = topics
        .create(CreateTopic {
            parent_id: Some(root_a.id),
            label: "child".into(),
            icon: None,
        })
        .await
        .unwrap();

    // 两个根 topic 各带一个 binding，使 delete_bindings 走多元素 IN (?, ?) 分支
    let mut bindings = Vec::new();
    for (topic_id, key) in [(root_a.id, "multi-a"), (root_b.id, "multi-b")] {
        let def = agent
            .create_definition(CreateAgentDefinition {
                key: key.into(),
                name: key.into(),
                description: key.into(),
                owner_topic_id: None,
                cloned_from_id: None,
                active: Some(true),
                data: AgentDefinitionData::default(),
            })
            .await
            .unwrap();
        bindings.push(
            agent
                .create_instance(CreateInstance {
                    topic_id,
                    agent_id: def.id,
                    role: AgentRole::Main,
                    model_id: None,
                    chat_config_id: None,
                    enabled: Some(true),
                })
                .await
                .unwrap(),
        );
    }

    topics.delete_topics(&[root_a.id, root_b.id]).await.unwrap();

    assert!(
        topics.get_topic(root_a.id).await.unwrap().is_none(),
        "root_a 应被删除"
    );
    assert!(
        topics.get_topic(root_b.id).await.unwrap().is_none(),
        "root_b 应被删除"
    );
    assert!(
        topics.get_topic(child.id).await.unwrap().is_some(),
        "子 topic 不应被级联删除"
    );
    for binding in bindings {
        assert!(
            agent.get_instance(binding.id).await.unwrap().is_none(),
            "binding {} 应被删除",
            binding.id
        );
    }
}

/// update_binding 的 agent_id / role 变更预校验。
#[tokio::test]
async fn update_binding_rejects_conflicts() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "update-conflict").await;
    let mut defs = Vec::new();
    for key in ["upd-a", "upd-b"] {
        defs.push(
            agent
                .create_definition(CreateAgentDefinition {
                    key: key.into(),
                    name: key.into(),
                    description: key.into(),
                    owner_topic_id: None,
                    cloned_from_id: None,
                    active: Some(true),
                    data: AgentDefinitionData::default(),
                })
                .await
                .unwrap(),
        );
    }
    let (def_a, def_b) = (defs[0].id, defs[1].id);

    agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            agent_id: def_a,
            role: AgentRole::Main,
            model_id: None,
            chat_config_id: None,
            enabled: Some(true),
        })
        .await
        .unwrap();
    let child = agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            agent_id: def_b,
            role: AgentRole::Child,
            model_id: None,
            chat_config_id: None,
            enabled: Some(true),
        })
        .await
        .unwrap();

    // 该 topic 已有 Main，改角色应被拒绝
    let err = agent
        .update_instance(
            child.id,
            UpdateInstance {
                role: Some(AgentRole::Main),
                ..Default::default()
            },
        )
        .await
        .expect_err("topic 已有主 Agent，改角色应被拒绝");
    assert!(
        matches!(err, CoreError::Validation(_)),
        "expected Validation, got {err:?}"
    );

    // def_a 已被占用，改绑它应被拒绝
    let err = agent
        .update_instance(
            child.id,
            UpdateInstance {
                agent_id: Some(def_a),
                ..Default::default()
            },
        )
        .await
        .expect_err("def_a 已被占用，改绑应被拒绝");
    assert!(
        matches!(err, CoreError::Validation(_)),
        "expected Validation, got {err:?}"
    );

    // 合法更新不应被预校验误伤
    agent
        .update_instance(
            child.id,
            UpdateInstance {
                role: Some(AgentRole::Child),
                ..Default::default()
            },
        )
        .await
        .unwrap();
}
