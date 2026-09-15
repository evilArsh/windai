use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use std::path::PathBuf;
use std::str::FromStr;
use wind_ai::{
    message::{Content, Message as AiMessage, Role},
    model::AdapterType,
};
use wind_core::{
    WindCore, agent::helper::get_or_create_main_instance, error::CoreError, models::*,
    schema::init_schema, storage::message::MessageStorage,
};

/// 临时文件数据库，用于需要真实文件持久化的并发/跨连接测试
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

/// 创建内存连接池，供需要拿到原始连接执行 SQL 的测试使用
///
/// `sqlite::memory:` 作用域绑定到单个物理连接，因此使用 `max_connections(1)`
/// 保证所有查询都命中同一个内存数据库
async fn setup_pool() -> SqlitePool {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")
        .expect("parse sqlite url")
        .shared_cache(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal);
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("connect sqlite")
}

/// 创建内存数据库并初始化 WindCore
async fn setup() -> WindCore {
    WindCore::init_with_pool(setup_pool().await)
        .await
        .expect("init core")
}

/// 创建文件持久化的连接池，供需要真实跨连接共享的测试使用
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

/// 构造一条用户消息。`from_id = None` 表示用户消息
fn user_msg(
    instance_id: i64,
    model_id: i64,
    text: &str,
    is_boundary: bool,
    is_excluded: bool,
) -> CreateMessage {
    CreateMessage {
        from_id: None,
        content: vec![AiMessage::new_simple(
            Role::User,
            vec![Content::new_text(text.into())],
            None,
        )],
        model_id,
        instance_id,
        is_boundary,
        is_excluded,
        input_tokens: 5,
        output_tokens: 0,
    }
}

/// 构造一条助手消息，`from_id` 指向其配对的用户消息
fn asst_msg(
    instance_id: i64,
    model_id: i64,
    from_id: i64,
    text: &str,
    is_boundary: bool,
    is_excluded: bool,
) -> CreateMessage {
    CreateMessage {
        from_id: Some(from_id),
        content: vec![AiMessage::new_simple(
            Role::Assistant,
            vec![Content::new_text(text.into())],
            None,
        )],
        model_id,
        instance_id,
        is_boundary,
        is_excluded,
        input_tokens: 0,
        output_tokens: 10,
    }
}

/// 在 `instance_id` 作用域下创建一对 user-assistant 消息，返回 (user_id, assistant_id)
async fn create_pair(
    msg: &MessageStorage,
    instance_id: i64,
    model_id: i64,
    user_text: &str,
    assistant_text: &str,
    user_exclude: bool,
    assistant_exclude: bool,
) -> (i64, i64) {
    let user = msg
        .create(user_msg(
            instance_id,
            model_id,
            user_text,
            false,
            user_exclude,
        ))
        .await
        .unwrap();
    let assistant = msg
        .create(asst_msg(
            instance_id,
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

/// 创建一个普通的根 Topic
async fn create_root_topic(
    topic_storage: &wind_core::storage::topic::TopicStorage,
    label: &str,
) -> Topic {
    topic_storage
        .create(CreateTopic {
            parent_id: None,
            label: label.into(),
            icon: None,
            model_id: None,
            tool_approval_policy: None,
        })
        .await
        .unwrap()
}

/// 创建一个 AgentDefinition；`owner_topic_id` 为 `Some` 表示 topic 专属 Agent
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

/// 为 `topic_id` 创建绑定 `agent_id` 的实例，返回该实例
async fn create_agent_instance(
    agent: &wind_core::storage::agent::AgentStorage,
    topic_id: i64,
    agent_id: i64,
    role: AgentRole,
) -> AgentInstance {
    agent
        .create_instance(CreateInstance {
            topic_id,
            parent_id: None,
            agent_id: Some(agent_id),
            mode: Some(AgentMode::Sync),
            status: Some(AgentStatus::Idle),
            role: Some(role),
        })
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

/// AgentDefinition / AgentInstance / TopicAgentMap 的 CRUD 与查询
#[tokio::test]
async fn agent_instance_crud() {
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

    // create_instance：在 topic 下创建三个实例，分别绑定三个 definition；第一个为 Main
    let instance_main = agent
        .create_instance(CreateInstance {
            topic_id: topic.id,
            parent_id: None,
            agent_id: Some(def_a.id),
            mode: Some(AgentMode::Sync),
            status: Some(AgentStatus::Idle),
            role: Some(AgentRole::Main),
        })
        .await
        .unwrap();
    let instance_b = agent
        .create_instance(CreateInstance {
            topic_id: topic.id,
            parent_id: None,
            agent_id: Some(def_b.id),
            mode: Some(AgentMode::Sync),
            status: Some(AgentStatus::Idle),
            role: Some(AgentRole::Child),
        })
        .await
        .unwrap();
    let instance_c = agent
        .create_instance(CreateInstance {
            topic_id: topic.id,
            parent_id: None,
            agent_id: Some(def_c.id),
            mode: Some(AgentMode::Sync),
            status: Some(AgentStatus::Idle),
            role: Some(AgentRole::Child),
        })
        .await
        .unwrap();

    // get_instance / get_definition：通过 id 找回创建的实例 / definition
    let got = agent.get_instance(instance_main.id).await.unwrap().unwrap();
    assert_eq!(got.id, instance_main.id);
    assert_eq!(got.topic_id, topic.id);
    assert_eq!(got.agent_id, Some(def_a.id));
    assert_eq!(got.role, AgentRole::Main);

    let got_def = agent.get_definition(def_b.id).await.unwrap().unwrap();
    assert_eq!(got_def.id, def_b.id);
    assert_eq!(got_def.key, "agent-b");
    assert_eq!(got_def.name, "Agent B");

    // get_main_instance：按 role 取回 topic 的主实例
    let main = agent
        .get_main_instance(topic.id)
        .await
        .unwrap()
        .expect("main instance exists");
    assert_eq!(main.id, instance_main.id);

    // 能力列表来自 topic_agent_maps：按映射顺序返回全部 definition
    for agent_id in [def_a.id, def_b.id, def_c.id] {
        agent
            .create_topic_agent_map(CreateTopicAgentMap {
                topic_id: topic.id,
                agent_id,
            })
            .await
            .unwrap();
    }
    let defs = agent.list_sub_definitions_by_topic(topic.id).await.unwrap();
    let def_ids: Vec<i64> = defs.iter().map(|d| d.id).collect();
    assert_eq!(def_ids, vec![def_a.id, def_b.id, def_c.id]);

    // get_definition_by_key：通过 key 查找 definition
    let by_key = agent
        .get_definition_by_key("agent-c")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(by_key.id, def_c.id);

    // 按 agent_id 过滤实例列表可定位到绑定该 definition 的实例
    let instances = agent.list_instances_by_topic(topic.id).await.unwrap();
    let found = instances
        .iter()
        .find(|i| i.agent_id == Some(def_b.id))
        .expect("instance bound to agent-b");
    assert_eq!(found.id, instance_b.id);
    assert_eq!(found.topic_id, topic.id);

    let found_c = instances
        .iter()
        .find(|i| i.agent_id == Some(def_c.id))
        .expect("instance bound to agent-c");
    assert_eq!(found_c.id, instance_c.id);
    assert_eq!(found_c.role, AgentRole::Child);
}

/// 子话题查询：list_child_topics 只返回 parent_id 匹配的直接子话题，且不改变 list_topics 语义
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
            model_id: None,
            tool_approval_policy: None,
        })
        .await
        .unwrap();
    let child_b = topics
        .create(CreateTopic {
            parent_id: Some(root_a.id),
            label: "child-b".into(),
            icon: None,
            model_id: None,
            tool_approval_policy: None,
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

// ---------------------------------------------------------------------------
// 内建 MCP 绑定（builtin_mcp_servers）
// ---------------------------------------------------------------------------

/// agent 定义内建 MCP 绑定：内建名可正常写入并读回
#[tokio::test]
async fn agent_definition_accepts_builtin_binding_names() {
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

    let created = agent
        .create_definition(build_def(
            "with-builtin",
            vec![BuiltinMcpBinding {
                name: wind_mcp::builtin::BUILTIN_FS.name.to_string(),
                allowed_tools: vec![],
                denied_tools: vec![],
                enabled: true,
            }],
        ))
        .await
        .expect("valid builtin name should pass");

    // builtin 绑定随 data 一起持久化，读回后保持原值
    let loaded = agent
        .get_definition(created.id)
        .await
        .expect("get definition")
        .expect("definition exists");
    assert_eq!(loaded.data.builtin_mcp_servers.len(), 1);
    assert_eq!(
        loaded.data.builtin_mcp_servers[0].name,
        wind_mcp::builtin::BUILTIN_FS.name
    );
}

/// 旧数据缺 builtin_mcp_servers 字段时反序列化回退为空列表
#[test]
fn agent_definition_data_defaults_builtin_servers() {
    let mut value = serde_json::to_value(AgentDefinitionData::default()).unwrap();
    value.as_object_mut().unwrap().remove("builtin_mcp_servers");
    let data: AgentDefinitionData = serde_json::from_value(value).unwrap();
    assert!(data.builtin_mcp_servers.is_empty());
}

/// 删除 Topic 时级联清理：专属 definition、instance、message、审批记录
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

    let instance = create_agent_instance(agent, root.id, def.id, AgentRole::Main).await;

    let (u1, a1) = create_pair(msg, instance.id, model_id, "q1", "a1", false, false).await;
    let approvals = core
        .storage()
        .approval()
        .create_requests(CreateToolApprovalRequests {
            instance_id: instance.id,
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
        agent.get_instance(instance.id).await.unwrap().is_none(),
        "instance 应被删除"
    );
    assert!(msg.get(u1).await.unwrap().is_none(), "user 消息应被删除");
    assert!(
        msg.get(a1).await.unwrap().is_none(),
        "assistant 消息应被删除"
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

/// 删除 instance 时级联清理 messages 与审批记录，topic/definition 保留
#[tokio::test]
async fn instance_delete_cascades_messages_and_approvals() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();
    let msg = core.storage().message();
    let model_id = 1;

    let root = create_root_topic(topics, "instance-cascade").await;
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

    let instance = create_agent_instance(agent, root.id, def.id, AgentRole::Main).await;

    let (u1, a1) = create_pair(msg, instance.id, model_id, "q1", "a1", false, false).await;
    core.storage()
        .approval()
        .create_requests(CreateToolApprovalRequests {
            instance_id: instance.id,
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

    agent.delete_instances(&[instance.id]).await.unwrap();

    assert!(
        agent.get_instance(instance.id).await.unwrap().is_none(),
        "instance 应被删除"
    );
    assert!(msg.get(u1).await.unwrap().is_none(), "user 消息应被删除");
    assert!(
        msg.get(a1).await.unwrap().is_none(),
        "assistant 消息应被删除"
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

/// 一个 topic 下同一个 AgentDefinition 可以被多个实例使用
///
/// `agent_instances(topic_id, agent_id)` 的唯一索引已被移除：同一 Agent 可派生多个实例
#[tokio::test]
async fn multiple_instances_per_agent_allowed() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "dup-agent").await;
    let def = create_agent_def(agent, "dup", None).await;

    let first = create_agent_instance(agent, root.id, def.id, AgentRole::Child).await;
    let second = create_agent_instance(agent, root.id, def.id, AgentRole::Child).await;

    assert_ne!(first.id, second.id, "同一个 agent 的两次创建应产生两个实例");
    assert_eq!(second.agent_id, Some(def.id));
    let instances = agent.list_instances_by_topic(root.id).await.unwrap();
    assert_eq!(instances.len(), 2);
}

/// 一个 topic 下只能有一个主 Agent（partial 唯一索引兜底）
#[tokio::test]
async fn main_instance_is_unique_per_topic() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "dup-main").await;
    let def_a = create_agent_def(agent, "main-a", None).await;
    let def_b = create_agent_def(agent, "main-b", None).await;

    let main = create_agent_instance(agent, root.id, def_a.id, AgentRole::Main).await;

    // 应用层不再预校验，冲突由 `(topic_id) WHERE role = 'main'` 唯一索引兜底
    let err = agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            parent_id: None,
            agent_id: Some(def_b.id),
            mode: Some(AgentMode::Sync),
            status: Some(AgentStatus::Idle),
            role: Some(AgentRole::Main),
        })
        .await
        .expect_err("第二个主 Agent 应被拒绝");
    assert!(
        matches!(err, CoreError::Database(_)),
        "expected Database, got {err:?}"
    );

    // 被拒绝的插入没有留下任何行：库中始终只有一个主实例
    let mains: Vec<i64> = agent
        .list_instances_by_topic(root.id)
        .await
        .unwrap()
        .into_iter()
        .filter(|i| i.role == AgentRole::Main)
        .map(|i| i.id)
        .collect();
    assert_eq!(mains, vec![main.id], "被拒绝的插入不应留下主实例行");

    let current = agent
        .get_main_instance(root.id)
        .await
        .unwrap()
        .expect("main instance exists");
    assert_eq!(current.id, main.id, "原主实例应保持不变");
}

/// topic 没有任何专属 definition 时，删除也必须成功
#[tokio::test]
async fn topic_delete_succeeds_without_owned_definitions() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();
    let msg = core.storage().message();
    let model_id = 1;

    let root = create_root_topic(topics, "no-owned-def").await;
    let def = create_agent_def(agent, "global-agent", None).await;
    let instance = create_agent_instance(agent, root.id, def.id, AgentRole::Main).await;
    let (u1, a1) = create_pair(msg, instance.id, model_id, "q1", "a1", false, false).await;

    topics.delete_topics(&[root.id]).await.unwrap();

    assert!(
        topics.get_topic(root.id).await.unwrap().is_none(),
        "topic 应被删除"
    );
    assert!(
        agent.get_instance(instance.id).await.unwrap().is_none(),
        "instance 应被删除"
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

/// 删除一个 topic 不能影响其它 topic 的 definition / instance / 消息
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
    let instance_a = create_agent_instance(agent, root_a.id, def_a.id, AgentRole::Main).await;
    let instance_b = create_agent_instance(agent, root_b.id, def_b.id, AgentRole::Main).await;
    let (ua, aa) = create_pair(msg, instance_a.id, model_id, "qa", "aa", false, false).await;
    let (ub, ab) = create_pair(msg, instance_b.id, model_id, "qb", "ab", false, false).await;

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
        agent.get_instance(instance_a.id).await.unwrap().is_none(),
        "A 的 instance 应被删除"
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
        agent.get_instance(instance_b.id).await.unwrap().is_some(),
        "B 的 instance 必须保留"
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

/// 一次删除多个 topic，且不级联删除其子 topic
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
            model_id: None,
            tool_approval_policy: None,
        })
        .await
        .unwrap();

    // 两个根 topic 各带一个实例，使 delete_instances 走多元素 IN (?, ?) 分支
    let mut instances = Vec::new();
    for (topic_id, key) in [(root_a.id, "multi-a"), (root_b.id, "multi-b")] {
        let def = create_agent_def(agent, key, None).await;
        instances.push(create_agent_instance(agent, topic_id, def.id, AgentRole::Main).await);
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
    for instance in instances {
        assert!(
            agent.get_instance(instance.id).await.unwrap().is_none(),
            "instance {} 应被删除",
            instance.id
        );
    }
}

/// update_instance 变更 agent_id / status / mode
///
/// `(topic_id, agent_id)` 无唯一约束，改绑其它实例已使用的 Agent 同样允许
#[tokio::test]
async fn update_instance_switches_agent_and_mode() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "update-instance").await;
    let def_a = create_agent_def(agent, "upd-a", None).await;
    let def_b = create_agent_def(agent, "upd-b", None).await;

    let main = create_agent_instance(agent, root.id, def_a.id, AgentRole::Main).await;
    let child = create_agent_instance(agent, root.id, def_b.id, AgentRole::Child).await;

    agent
        .update_instance(
            child.id,
            UpdateInstance {
                status: Some(AgentStatus::Running),
                mode: Some(AgentMode::Fork),
                agent_id: Some(def_a.id),
            },
        )
        .await
        .unwrap();

    let updated = agent.get_instance(child.id).await.unwrap().unwrap();
    assert_eq!(updated.agent_id, Some(def_a.id));
    assert_eq!(updated.status, AgentStatus::Running);
    assert_eq!(updated.mode, Some(AgentMode::Fork));
    assert_eq!(
        updated.role,
        AgentRole::Child,
        "role 不由 update_instance 变更"
    );

    // 未被更新的实例保持原值
    let main_now = agent.get_instance(main.id).await.unwrap().unwrap();
    assert_eq!(main_now.agent_id, Some(def_a.id));
    assert_eq!(main_now.status, AgentStatus::Idle);
    assert_eq!(main_now.mode, Some(AgentMode::Sync));
}

// ---------------------------------------------------------------------------
// AgentInstance：状态默认值、父子关系与子实例查询
// ---------------------------------------------------------------------------

/// 不传 `status` 时实例落库为 Idle
#[tokio::test]
async fn instance_status_defaults_to_idle() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "status-default").await;
    let created = agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            parent_id: None,
            agent_id: None,
            mode: None,
            status: None,
            role: None,
        })
        .await
        .expect("create instance without status");

    assert_eq!(created.status, AgentStatus::Idle, "创建返回值应为 Idle");

    let loaded = agent
        .get_instance(created.id)
        .await
        .expect("get instance")
        .expect("instance exists");
    assert_eq!(loaded.status, AgentStatus::Idle, "库中记录应为 Idle");
}

/// 父子实例关系与运行模式可完整往返
#[tokio::test]
async fn instance_round_trips_parent_id_and_mode() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "instance-round-trip").await;
    let parent = agent
        .create_instance(CreateInstance::new_main(root.id, None))
        .await
        .expect("create parent");

    let child = agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            parent_id: Some(parent.id),
            agent_id: None,
            mode: Some(AgentMode::Fork),
            status: Some(AgentStatus::Idle),
            role: Some(AgentRole::Child),
        })
        .await
        .expect("create child");

    let loaded = agent
        .get_instance(child.id)
        .await
        .expect("get instance")
        .expect("instance exists");

    assert_eq!(loaded.parent_id, Some(parent.id));
    assert_eq!(loaded.mode, Some(AgentMode::Fork));
    assert_eq!(loaded.role, AgentRole::Child);
    assert_eq!(loaded.status, AgentStatus::Idle);
}

/// `list_instances_by_topic` 走 `select_instances`，其列清单必须与表结构一致
#[tokio::test]
async fn list_instances_by_topic_reads_refactored_columns() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "instance-list").await;
    let main = agent
        .create_instance(CreateInstance::new_main(root.id, None))
        .await
        .expect("create main instance");

    let child = agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            parent_id: Some(main.id),
            agent_id: None,
            mode: Some(AgentMode::Fork),
            status: Some(AgentStatus::WaitingChild),
            role: Some(AgentRole::Child),
        })
        .await
        .expect("create child instance");

    let loaded = agent
        .list_instances_by_topic(root.id)
        .await
        .expect("list instances by topic");

    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].id, main.id);
    assert_eq!(loaded[0].mode, Some(AgentMode::Sync));
    assert_eq!(loaded[1].id, child.id);
    assert_eq!(loaded[1].parent_id, Some(main.id));
    assert_eq!(loaded[1].status, AgentStatus::WaitingChild);
}

/// `list_child_instances_by_topic` 只返回 `role = Child` 的实例
#[tokio::test]
async fn list_child_instances_excludes_main() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "child-only").await;
    let main = agent
        .create_instance(CreateInstance::new_main(root.id, None))
        .await
        .expect("create main");
    let child = agent
        .create_instance(CreateInstance {
            topic_id: root.id,
            parent_id: Some(main.id),
            agent_id: None,
            mode: Some(AgentMode::Sync),
            status: Some(AgentStatus::Idle),
            role: Some(AgentRole::Child),
        })
        .await
        .expect("create child");

    let children = agent
        .list_child_instances_by_topic(root.id)
        .await
        .expect("list children");

    assert_eq!(children.len(), 1);
    assert_eq!(children[0].id, child.id);
    assert!(children.iter().all(|i| i.role == AgentRole::Child));
}

/// 主实例不绑定任何 AgentDefinition，且重复获取是幂等的
///
/// 主实例只负责调度；topic 的能力列表由 `topic_agent_maps` 表达，
/// 不影响主实例自身绑定哪个 definition
#[tokio::test]
async fn main_instance_binds_no_agent() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "main-no-agent").await;
    let planner = create_agent_def(agent, "main-no-agent-planner", None).await;

    // 即便 topic 已映射了能力，主实例也不采用它
    agent
        .create_topic_agent_map(CreateTopicAgentMap {
            topic_id: root.id,
            agent_id: planner.id,
        })
        .await
        .expect("create map");

    let main = get_or_create_main_instance(core.storage(), root.id)
        .await
        .expect("main instance");
    assert_eq!(main.role, AgentRole::Main);
    assert_eq!(main.agent_id, None);

    // 幂等：再次获取返回同一个实例
    let again = get_or_create_main_instance(core.storage(), root.id)
        .await
        .expect("main instance again");
    assert_eq!(main.id, again.id);
}

// ---------------------------------------------------------------------------
// TopicAgentMap：唯一约束与能力列表
// ---------------------------------------------------------------------------

/// 同一 topic 下同一个 AgentDefinition 只能映射一次
#[tokio::test]
async fn topic_agent_map_is_unique_per_agent() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "map-unique").await;
    let def = create_agent_def(agent, "map-unique-def", None).await;

    agent
        .create_topic_agent_map(CreateTopicAgentMap {
            topic_id: root.id,
            agent_id: def.id,
        })
        .await
        .expect("first map");

    // 应用层预校验 `(topic_id, agent_id)`，不依赖驱动抛出的 UNIQUE 冲突（那会冒泡成 500）
    let err = agent
        .create_topic_agent_map(CreateTopicAgentMap {
            topic_id: root.id,
            agent_id: def.id,
        })
        .await
        .expect_err("重复映射应被拒绝");
    assert!(
        matches!(err, CoreError::Validation(_)),
        "expected Validation, got {err:?}"
    );
    assert_eq!(
        agent
            .list_agent_maps_by_topic(root.id)
            .await
            .expect("list maps")
            .len(),
        1,
        "被拒绝的映射不应留下行"
    );
}

/// 指向不存在的 AgentDefinition 的映射必须被拒绝，否则会静默漏掉能力
#[tokio::test]
async fn topic_agent_map_rejects_unknown_agent() {
    let core = setup().await;
    let agent = core.storage().agent();
    let root = create_root_topic(core.storage().topic(), "map-unknown-agent").await;

    let err = agent
        .create_topic_agent_map(CreateTopicAgentMap {
            topic_id: root.id,
            agent_id: 424242,
        })
        .await
        .expect_err("不存在的 agent 应被拒绝");
    assert!(
        matches!(err, CoreError::Validation(_)),
        "expected Validation, got {err:?}"
    );
    assert!(
        agent
            .list_agent_maps_by_topic(root.id)
            .await
            .expect("list maps")
            .is_empty(),
        "被拒绝的映射不应留下行"
    );
}

/// TopicAgentMap 的 CRUD 与能力列表
#[tokio::test]
async fn topic_agent_map_crud_and_sub_definitions() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "agent-map").await;
    let backend = create_agent_def(agent, "map-backend", None).await;
    let frontend = create_agent_def(agent, "map-frontend", None).await;
    let planner = create_agent_def(agent, "map-planner", None).await;

    let planner_map = agent
        .create_topic_agent_map(CreateTopicAgentMap {
            topic_id: root.id,
            agent_id: planner.id,
        })
        .await
        .expect("create planner map");
    let backend_map = agent
        .create_topic_agent_map(CreateTopicAgentMap {
            topic_id: root.id,
            agent_id: backend.id,
        })
        .await
        .expect("create backend map");
    let frontend_map = agent
        .create_topic_agent_map(CreateTopicAgentMap {
            topic_id: root.id,
            agent_id: frontend.id,
        })
        .await
        .expect("create frontend map");

    // 能力列表按 definition key 升序返回全部映射，不做任何过滤
    let keys: Vec<String> = agent
        .list_sub_definitions_by_topic(root.id)
        .await
        .expect("list sub definitions")
        .into_iter()
        .map(|d| d.key)
        .collect();
    assert_eq!(keys, vec!["map-backend", "map-frontend", "map-planner"]);

    // 删除
    agent
        .delete_topic_agent_map(planner_map.id)
        .await
        .expect("delete map");
    let remaining: Vec<i64> = agent
        .list_agent_maps_by_topic(root.id)
        .await
        .expect("list maps")
        .into_iter()
        .map(|m| m.id)
        .collect();
    assert_eq!(remaining, vec![backend_map.id, frontend_map.id]);
}

/// 能力列表返回全部映射：`active = false` 的 definition 也不被过滤
#[tokio::test]
async fn list_sub_definitions_returns_all_mapped() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let root = create_root_topic(topics, "sub-defs").await;
    let disabled = create_agent_def(agent, "sub-disabled", None).await;
    let enabled = create_agent_def(agent, "sub-main", None).await;

    agent
        .update_definition(
            disabled.id,
            UpdateAgentDefinition {
                active: Some(false),
                ..Default::default()
            },
        )
        .await
        .expect("disable definition");

    for agent_id in [disabled.id, enabled.id] {
        agent
            .create_topic_agent_map(CreateTopicAgentMap {
                topic_id: root.id,
                agent_id,
            })
            .await
            .expect("create map");
    }

    let keys: Vec<String> = agent
        .list_sub_definitions_by_topic(root.id)
        .await
        .expect("list sub definitions")
        .into_iter()
        .map(|d| d.key)
        .collect();
    assert_eq!(keys, vec!["sub-disabled", "sub-main"]);

    let disabled_now = agent
        .get_definition(disabled.id)
        .await
        .expect("get definition")
        .expect("definition exists");
    assert!(!disabled_now.active, "被列出的 definition 确实处于停用状态");
}

// ---------------------------------------------------------------------------
// Topic 级联与 Model 默认值
// ---------------------------------------------------------------------------

/// 删除 topic 级联清理 instance / 专属 definition / 能力映射，且不波及子 topic
#[tokio::test]
async fn topic_delete_cascades_instances_definitions_and_maps() {
    let core = setup().await;
    let agent = core.storage().agent();
    let topics = core.storage().topic();

    let parent = create_root_topic(topics, "cascade-parent").await;
    let child = topics
        .create(CreateTopic {
            parent_id: Some(parent.id),
            label: "cascade-child".into(),
            icon: None,
            model_id: None,
            tool_approval_policy: None,
        })
        .await
        .expect("create child topic");

    let definition = create_agent_def(agent, "cascade-local", Some(child.id)).await;
    let instance = create_agent_instance(agent, child.id, definition.id, AgentRole::Child).await;
    agent
        .create_topic_agent_map(CreateTopicAgentMap {
            topic_id: child.id,
            agent_id: definition.id,
        })
        .await
        .expect("create map");

    // 只删 parent 自身，子 topic 需调用方显式传入
    topics
        .delete_topics(&[parent.id])
        .await
        .expect("delete parent topic");

    assert!(
        agent
            .get_instance(instance.id)
            .await
            .expect("get instance")
            .is_some(),
        "child topic instance must survive parent deletion"
    );
    assert!(
        agent
            .get_definition(definition.id)
            .await
            .expect("get definition")
            .is_some(),
        "child topic 专属 definition 不因 parent 删除而消失"
    );
    assert_eq!(
        agent
            .list_agent_maps_by_topic(child.id)
            .await
            .expect("list maps")
            .len(),
        1,
        "child topic 能力映射不因 parent 删除而消失"
    );

    topics
        .delete_topics(&[child.id])
        .await
        .expect("delete child topic");

    assert!(
        agent
            .get_instance(instance.id)
            .await
            .expect("get instance")
            .is_none()
    );
    assert!(
        agent
            .get_definition(definition.id)
            .await
            .expect("get definition")
            .is_none()
    );
    assert!(
        agent
            .list_agent_maps_by_topic(child.id)
            .await
            .expect("list maps")
            .is_empty()
    );
}

/// 不传 `config` 时模型的请求配置读回为 None
#[tokio::test]
async fn model_config_defaults_to_none() {
    let core = setup().await;
    let provider = core
        .storage()
        .provider()
        .create(CreateProvider {
            name: "no-config-provider".into(),
            description: None,
            base_url: "https://example.invalid".into(),
            doc: None,
            alias: None,
        })
        .await
        .expect("create provider");

    let model = core
        .storage()
        .model()
        .create(CreateModel {
            name: "no-config-model".into(),
            provider_id: provider.id,
            alias: None,
            adapter: AdapterType::OpenAICompletion,
            modalities: Some(vec![ModelType::Chat]),
            active: Some(true),
            icon: None,
            endpoint: None,
            config: None,
        })
        .await
        .expect("create model");
    assert!(model.config.is_none());

    let loaded = core
        .storage()
        .model()
        .get(model.id)
        .await
        .expect("get model")
        .expect("model exists");
    assert!(loaded.config.is_none(), "config 缺省应读回 None");
}

/// 模型的 config 可完整写入并读回
#[tokio::test]
async fn model_config_round_trip() {
    let core = setup().await;
    let provider = core
        .storage()
        .provider()
        .create(CreateProvider {
            name: "config-provider".into(),
            description: None,
            base_url: "https://example.invalid".into(),
            doc: None,
            alias: None,
        })
        .await
        .expect("create provider");

    let model = core
        .storage()
        .model()
        .create(CreateModel {
            name: "config-model".into(),
            provider_id: provider.id,
            alias: None,
            adapter: AdapterType::OpenAICompletion,
            modalities: Some(vec![ModelType::Chat]),
            active: Some(true),
            icon: None,
            endpoint: None,
            config: Some(ModelConfig {
                stream: Some(true),
                reasoning: Some(ReasonEffort::High),
            }),
        })
        .await
        .expect("create model");

    let loaded = core
        .storage()
        .model()
        .get(model.id)
        .await
        .expect("get model")
        .expect("model exists");
    let config = loaded.config.expect("config exists");
    assert_eq!(config.stream, Some(true));
    assert_eq!(config.reasoning, Some(ReasonEffort::High));
}

/// `models.config` 列可空：SQL NULL 必须读回 `None`，不能 panic
#[tokio::test]
async fn model_config_null_column_reads_as_none() {
    let pool = setup_pool().await;
    let core = WindCore::init_with_pool(pool.clone())
        .await
        .expect("init core");

    let provider = core
        .storage()
        .provider()
        .create(CreateProvider {
            name: "null-config-provider".into(),
            description: None,
            base_url: "https://example.invalid".into(),
            doc: None,
            alias: None,
        })
        .await
        .expect("create provider");

    let model = core
        .storage()
        .model()
        .create(CreateModel {
            name: "null-config-model".into(),
            provider_id: provider.id,
            alias: None,
            adapter: AdapterType::OpenAICompletion,
            modalities: Some(vec![ModelType::Chat]),
            active: Some(true),
            icon: None,
            endpoint: None,
            config: Some(ModelConfig {
                stream: Some(true),
                reasoning: None,
            }),
        })
        .await
        .expect("create model");

    // 绕过 ModelStorage 直接写 NULL，模拟外部写入与历史数据
    sqlx::query("UPDATE models SET config = NULL WHERE id = ?")
        .bind(model.id)
        .execute(&pool)
        .await
        .expect("null out config");

    let loaded = core
        .storage()
        .model()
        .get(model.id)
        .await
        .expect("get model")
        .expect("model exists");
    assert!(loaded.config.is_none(), "NULL config 应读回 None");
}

/// 更新后的 config 在 get 与 list_by_provider 两条读路径上都可见
#[tokio::test]
async fn model_config_readable_from_update_and_list() {
    let core = setup().await;
    let provider = core
        .storage()
        .provider()
        .create(CreateProvider {
            name: "config-list-provider".into(),
            description: None,
            base_url: "https://example.invalid".into(),
            doc: None,
            alias: None,
        })
        .await
        .expect("create provider");

    let model = core
        .storage()
        .model()
        .create(CreateModel {
            name: "config-list-model".into(),
            provider_id: provider.id,
            alias: None,
            adapter: AdapterType::OpenAICompletion,
            modalities: Some(vec![ModelType::Chat]),
            active: Some(true),
            icon: None,
            endpoint: None,
            config: Some(ModelConfig {
                stream: Some(false),
                reasoning: None,
            }),
        })
        .await
        .expect("create model");

    core.storage()
        .model()
        .update(
            model.id,
            UpdateModel {
                config: Some(ModelConfig {
                    stream: Some(true),
                    reasoning: Some(ReasonEffort::Low),
                }),
                ..Default::default()
            },
        )
        .await
        .expect("update model");

    let listed = core
        .storage()
        .model()
        .list_by_provider()
        .await
        .expect("list models");
    let loaded = listed
        .into_iter()
        .find(|m| m.id == model.id)
        .expect("model listed");
    let config = loaded.config.expect("config exists");
    assert_eq!(config.stream, Some(true));
    assert_eq!(config.reasoning, Some(ReasonEffort::Low));
}

/// topic 的 model_id 与 tool_approval_policy 可完整往返
#[tokio::test]
async fn topic_model_and_policy_round_trip() {
    let core = setup().await;
    let topics = core.storage().topic();

    let topic = topics
        .create(CreateTopic {
            parent_id: None,
            label: "topic-model-policy".into(),
            icon: None,
            model_id: Some(7),
            tool_approval_policy: Some(ToolApprovalPolicy::Manual),
        })
        .await
        .expect("create topic");

    let loaded = topics
        .get_topic(topic.id)
        .await
        .expect("get topic")
        .expect("topic exists");
    assert_eq!(loaded.model_id, Some(7));
    assert_eq!(
        loaded.tool_approval_policy,
        Some(ToolApprovalPolicy::Manual)
    );

    topics
        .update(
            topic.id,
            UpdateTopic {
                model_id: Some(9),
                tool_approval_policy: Some(ToolApprovalPolicy::AllowAll),
                ..Default::default()
            },
        )
        .await
        .expect("update topic");

    let loaded = topics
        .get_topic(topic.id)
        .await
        .expect("get topic")
        .expect("topic exists");
    assert_eq!(loaded.model_id, Some(9));
    assert_eq!(
        loaded.tool_approval_policy,
        Some(ToolApprovalPolicy::AllowAll)
    );
}
