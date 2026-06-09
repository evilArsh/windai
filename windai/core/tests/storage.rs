use sqlx::SqlitePool;
use std::collections::HashMap;
use wind_ai::message::{Content, Message as AiMessage, ReqConfig, Role};
use wind_ai::model::AdaptorType;
use wind_core::models::*;
use wind_core::schema::init_schema;
use wind_core::storage::mcp::McpStorage;
use wind_core::storage::message::MessageStorage;
use wind_core::storage::model::ModelStorage;
use wind_core::storage::provider::ProviderStorage;
use wind_core::storage::topic::TopicStorage;
use wind_mcp::client::TransportType;

async fn setup() -> SqlitePool {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    init_schema(&pool).await.unwrap();
    wind_core::storage::init_id_generator(0);
    pool
}

// ---- helpers ----

fn sample_provider(name: &str) -> CreateProvider {
    CreateProvider {
        name: name.into(),
        base_url: "https://api.test.com".into(),
        description: Some("test".into()),
        doc: None,
        alias: None,
    }
}

fn sample_mcp(name: &str, t: TransportType) -> CreateMcpServer {
    CreateMcpServer {
        r#type: t,
        name: name.into(),
        url: Some(format!("https://{}.test.com", name)),
        description: None,
        command: Some("node".into()),
        args: Some(vec!["server.js".into()]),
        env: None,
    }
}

fn sample_model(provider_id: i64) -> CreateModel {
    CreateModel {
        name: "gpt-4".into(),
        provider_id,
        alias: None,
        adaptor: AdaptorType::OpenAICompletion,
        modalities: Some(vec![ModelType::Chat]),
        active: Some(true),
        icon: None,
        endpoint: None,
    }
}

fn sample_topic(label: &str, parent_id: Option<i64>) -> CreateTopic {
    CreateTopic {
        parent_id,
        chat_config_id: 0,
        label: label.into(),
        icon: None,
        max_context: None,
        mcp_server_ids: None,
    }
}

fn user_msg(topic_id: i64, model_id: i64, text: &str) -> CreateMessage {
    CreateMessage {
        from_id: None,
        stream: false,
        content: vec![AiMessage::new_simple(
            Role::User,
            vec![Content::new_text(text.into())],
            None,
        )],
        model_id,
        topic_id,
        is_boundary: false,
        input_tokens: 5,
        output_tokens: 0,
        tools_allowed: None,
        tools_denied: None,
    }
}

fn asst_msg(topic_id: i64, model_id: i64, from_id: i64) -> CreateMessage {
    CreateMessage {
        from_id: Some(from_id),
        stream: false,
        content: vec![AiMessage::new_simple(
            Role::Assistant,
            vec![Content::new_text("response".into())],
            None,
        )],
        model_id,
        topic_id,
        is_boundary: false,
        input_tokens: 0,
        output_tokens: 10,
        tools_allowed: None,
        tools_denied: None,
    }
}

fn asst_msg_empty(topic_id: i64, model_id: i64, from_id: i64) -> CreateMessage {
    CreateMessage {
        from_id: Some(from_id),
        ..user_msg(topic_id, model_id, "")
    }
}

// ==================== ProviderStorage ====================

#[tokio::test]
async fn provider_crud() {
    let pool = setup().await;
    let svc = ProviderStorage::new(pool);

    // create
    let id = svc.create(sample_provider("p1")).await.unwrap();
    let p = svc.get(id).await.unwrap().unwrap();
    assert_eq!(p.name, "p1");
    assert!(p.active);

    // get_by_name
    let by_name = svc.get_by_name("p1").await.unwrap().unwrap();
    assert_eq!(by_name.id, p.id);
    assert!(svc.get_by_name("no-such").await.unwrap().is_none());

    // list_all
    assert_eq!(svc.list_all().await.unwrap().len(), 1);

    // update
    svc.update(
        id,
        UpdateProvider {
            name: Some("p1-renamed".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(svc.get(id).await.unwrap().unwrap().name, "p1-renamed");

    // delete cascades credentials & json_rules
    svc.create_credentials(CreateCredentials {
        provider_id: id,
        key: "k".into(),
    })
    .await
    .unwrap();
    svc.create_json_rule(CreateJsonRule {
        provider_id: id,
        adaptor: AdaptorType::OpenAICompletion,
        json_rule: "{}".into(),
    })
    .await
    .unwrap();
    svc.delete(id).await.unwrap();
    assert!(svc.get(id).await.unwrap().is_none());
    assert_eq!(svc.get_provider_credentials(id).await.unwrap().len(), 0);
    assert_eq!(svc.list_json_rules(id).await.unwrap().len(), 0);
}

#[tokio::test]
async fn provider_validation() {
    let svc = ProviderStorage::new(setup().await);

    // empty name on create
    let err = svc
        .create(CreateProvider {
            name: "".into(),
            base_url: "u".into(),
            description: None,
            doc: None,
            alias: None,
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("cannot be empty"));

    // duplicate name on create
    svc.create(sample_provider("dup")).await.unwrap();
    let err = svc.create(sample_provider("dup")).await.unwrap_err();
    assert!(err.to_string().contains("already exists"));

    // empty name on update
    let id = svc.create(sample_provider("u1")).await.unwrap();
    let err = svc
        .update(
            id,
            UpdateProvider {
                name: Some("".into()),
                ..Default::default()
            },
        )
        .await;
    assert!(err.unwrap_err().to_string().contains("cannot be empty"));

    // duplicate name on update
    let id2 = svc.create(sample_provider("u2")).await.unwrap();
    let err = svc
        .update(
            id2,
            UpdateProvider {
                name: Some("u1".into()),
                ..Default::default()
            },
        )
        .await;
    assert!(err.unwrap_err().to_string().contains("already exists"));
}

#[tokio::test]
async fn credentials_crud() {
    let pool = setup().await;
    let svc = ProviderStorage::new(pool);
    let pid = svc.create(sample_provider("p")).await.unwrap();

    let cid = svc
        .create_credentials(CreateCredentials {
            provider_id: pid,
            key: "sk-1".into(),
        })
        .await
        .unwrap();
    let cid2 = svc
        .create_credentials(CreateCredentials {
            provider_id: pid,
            key: "sk-2".into(),
        })
        .await
        .unwrap();

    let list = svc.get_provider_credentials(pid).await.unwrap();
    assert_eq!(list.len(), 2);

    svc.delete_credentials(cid).await.unwrap();
    assert_eq!(svc.get_provider_credentials(pid).await.unwrap().len(), 1);

    svc.delete_credentials(cid2).await.unwrap();
    assert_eq!(svc.get_provider_credentials(pid).await.unwrap().len(), 0);
}

#[tokio::test]
async fn json_rule_crud() {
    let pool = setup().await;
    let svc = ProviderStorage::new(pool);
    let pid = svc.create(sample_provider("p")).await.unwrap();

    // create
    let rid = svc
        .create_json_rule(CreateJsonRule {
            provider_id: pid,
            adaptor: AdaptorType::OpenAICompletion,
            json_rule: "{}".into(),
        })
        .await
        .unwrap();

    // get by id, get by provider+adaptor
    let r = svc.get_json_rule_by_id(rid).await.unwrap().unwrap();
    assert_eq!(r.provider_id, pid);
    let r2 = svc
        .get_json_rule(pid, AdaptorType::OpenAICompletion)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r2.id, r.id);
    assert!(
        svc.get_json_rule(pid, AdaptorType::OpenAIResponse)
            .await
            .unwrap()
            .is_none()
    );

    // list
    assert_eq!(svc.list_json_rules(pid).await.unwrap().len(), 1);

    // partial update — only json_rule, keep others
    svc.update_json_rule(
        rid,
        UpdateJsonRule {
            json_rule: Some(r#"{"v":2}"#.into()),
            active: None,
            provider_id: None,
            adaptor: None,
        },
    )
    .await
    .unwrap();
    let updated = svc.get_json_rule_by_id(rid).await.unwrap().unwrap();
    assert_eq!(updated.json_rule, r#"{"v":2}"#);
    assert_eq!(updated.adaptor, AdaptorType::OpenAICompletion);
    assert!(updated.active);

    // full update
    svc.update_json_rule(
        rid,
        UpdateJsonRule {
            json_rule: Some(r#"{"v":3}"#.into()),
            active: Some(false),
            provider_id: None,
            adaptor: None,
        },
    )
    .await
    .unwrap();
    let updated = svc.get_json_rule_by_id(rid).await.unwrap().unwrap();
    assert_eq!(updated.json_rule, r#"{"v":3}"#);
    assert!(!updated.active);

    // delete
    svc.delete_json_rule(rid).await.unwrap();
    assert!(svc.get_json_rule_by_id(rid).await.unwrap().is_none());
}

// ==================== ModelStorage ====================

#[tokio::test]
async fn model_crud() {
    let pool = setup().await;
    let p_svc = ProviderStorage::new(pool.clone());
    let m_svc = ModelStorage::new(pool);
    let pid = p_svc.create(sample_provider("p")).await.unwrap();

    // create
    let mid = m_svc.create(sample_model(pid)).await.unwrap();
    let m = m_svc.get(mid).await.unwrap().unwrap();
    assert_eq!(m.name, "gpt-4");
    assert_eq!(m.provider_id, pid);
    assert_eq!(m.modalities, Some(vec![ModelType::Chat]));

    // get non-existent
    assert!(m_svc.get(999).await.unwrap().is_none());

    // list_by_provider
    m_svc
        .create(CreateModel {
            name: "m2".into(),
            ..sample_model(pid)
        })
        .await
        .unwrap();
    assert_eq!(m_svc.list_by_provider().await.unwrap().len(), 2);

    // update
    m_svc
        .update(
            mid,
            UpdateModel {
                name: Some("gpt-4-turbo".into()),
                frequency: Some(5),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let updated = m_svc.get(mid).await.unwrap().unwrap();
    assert_eq!(updated.name, "gpt-4-turbo");
    assert_eq!(updated.frequency, Some(5));

    // delete
    m_svc.delete(mid).await.unwrap();
    assert!(m_svc.get(mid).await.unwrap().is_none());
}

#[tokio::test]
async fn model_create_empty_name() {
    let pool = setup().await;
    let p_svc = ProviderStorage::new(pool.clone());
    let m_svc = ModelStorage::new(pool);
    let pid = p_svc.create(sample_provider("p")).await.unwrap();

    let err = m_svc
        .create(CreateModel {
            name: "".into(),
            ..sample_model(pid)
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("cannot be empty"));
}

// ==================== TopicStorage ====================

#[tokio::test]
async fn topic_crud() {
    let pool = setup().await;
    let svc = TopicStorage::new(pool.clone());
    let msg_svc = MessageStorage::new(pool);

    // create root — max_context defaults to 999
    let tid = svc.create(sample_topic("root", None)).await.unwrap();
    let t = svc.get_topic(tid).await.unwrap().unwrap();
    assert_eq!(t.label, "root");
    assert!(t.index > 0);
    assert_eq!(t.max_context, Some(999));
    assert_eq!(t.tool_approval_policy, ToolApprovalPolicy::AllowAll);

    // create child
    let cid = svc.create(sample_topic("child", Some(tid))).await.unwrap();
    let c = svc.get_topic(cid).await.unwrap().unwrap();
    assert_eq!(c.parent_id, Some(tid));

    // list_topics (ordered by index)
    let list = svc.list_topics().await.unwrap();
    assert_eq!(list.len(), 2);
    assert!(list[0].index <= list[1].index);

    // update
    svc.update(
        tid,
        UpdateTopic {
            label: Some("root-renamed".into()),
            max_context: Some(500),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        svc.get_topic(tid).await.unwrap().unwrap().label,
        "root-renamed"
    );
    assert_eq!(
        svc.get_topic(tid)
            .await
            .unwrap()
            .unwrap()
            .tool_approval_policy,
        ToolApprovalPolicy::AllowAll
    );

    svc.update(
        tid,
        UpdateTopic {
            tool_approval_policy: Some(ToolApprovalPolicy::AllowList(vec!["srv0m0tool".into()])),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        svc.get_topic(tid)
            .await
            .unwrap()
            .unwrap()
            .tool_approval_policy,
        ToolApprovalPolicy::AllowList(vec!["srv0m0tool".into()])
    );

    // delete cascade
    msg_svc.create(user_msg(tid, 1, "hi")).await.unwrap();
    svc.create_chat_config(tid, ReqConfig::default())
        .await
        .unwrap();
    svc.delete_topics(&[tid, cid]).await.unwrap();
    assert!(svc.get_topic(tid).await.unwrap().is_none());
    assert!(svc.get_topic(cid).await.unwrap().is_none());
    assert_eq!(msg_svc.list_by_topic(tid).await.unwrap().len(), 0);
}

#[tokio::test]
async fn topic_chat_config() {
    let pool = setup().await;
    let svc = TopicStorage::new(pool);
    let tid = svc.create(sample_topic("t", None)).await.unwrap();

    // create
    let cfg = ReqConfig {
        temperature: Some(0.7),
        max_tokens: Some(2048),
        ..Default::default()
    };
    svc.create_chat_config(tid, cfg).await.unwrap();
    let saved = svc.get_chat_config(tid).await.unwrap().unwrap();
    assert_eq!(saved.data.temperature, Some(0.7));
    assert_eq!(saved.data.max_tokens, Some(2048));

    // update — set some, skip others (None = don't change)
    svc.update_chat_config(
        tid,
        ReqConfig {
            temperature: Some(0.2),
            top_p: Some(0.9),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let updated = svc.get_chat_config(tid).await.unwrap().unwrap();
    assert_eq!(updated.data.temperature, Some(0.2));
    assert_eq!(updated.data.top_p, Some(0.9));
    assert_eq!(updated.data.max_tokens, Some(2048)); // kept

    // get non-existent
    assert!(svc.get_chat_config(999).await.unwrap().is_none());
}

#[tokio::test]
async fn topic_mcp_servers() {
    let pool = setup().await;
    let mcp_svc = McpStorage::new(pool.clone());
    let topic_svc = TopicStorage::new(pool);
    let tid = topic_svc.create(sample_topic("t", None)).await.unwrap();

    let s1 = mcp_svc
        .create(sample_mcp("s1", TransportType::Stdio))
        .await
        .unwrap();
    let s2 = mcp_svc
        .create(sample_mcp("s2", TransportType::Streamable))
        .await
        .unwrap();

    // set
    topic_svc
        .update(
            tid,
            UpdateTopic {
                mcp_server_ids: Some(vec![s1, s2]),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // replace
    let s3 = mcp_svc
        .create(sample_mcp("s3", TransportType::Stdio))
        .await
        .unwrap();
    topic_svc
        .update(
            tid,
            UpdateTopic {
                mcp_server_ids: Some(vec![s3]),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // clear
    topic_svc
        .update(
            tid,
            UpdateTopic {
                mcp_server_ids: Some(vec![]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

// ==================== MessageStorage ====================

#[tokio::test]
async fn message_crud_and_is_excluded() {
    let pool = setup().await;
    let topic_svc = TopicStorage::new(pool.clone());
    let msg_svc = MessageStorage::new(pool);
    let tid = topic_svc.create(sample_topic("t", None)).await.unwrap();

    // user message: is_excluded = true by default
    let uid = msg_svc.create(user_msg(tid, 1, "hello")).await.unwrap();
    let u = msg_svc.get(uid).await.unwrap().unwrap();
    assert!(u.is_excluded);
    assert_eq!(u.topic_id, tid);
    assert!(u.index > 0);

    // assistant message: unexcludes user and self
    let aid = msg_svc.create(asst_msg(tid, 1, uid)).await.unwrap();
    let a = msg_svc.get(aid).await.unwrap().unwrap();
    assert!(!a.is_excluded);
    assert!(!msg_svc.get(uid).await.unwrap().unwrap().is_excluded);

    // list_by_topic (ordered by index)
    let list = msg_svc.list_by_topic(tid).await.unwrap();
    assert_eq!(list.len(), 2);
    assert!(list[0].index < list[1].index);

    // update
    msg_svc
        .update(
            aid,
            UpdateMessage {
                input_tokens: Some(10),
                ..UpdateMessage::from(a.clone())
            },
        )
        .await
        .unwrap();
    assert_eq!(msg_svc.get(aid).await.unwrap().unwrap().input_tokens, 10);
}

#[tokio::test]
async fn message_delete_assistant() {
    let pool = setup().await;
    let topic_svc = TopicStorage::new(pool.clone());
    let msg_svc = MessageStorage::new(pool);
    let tid = topic_svc.create(sample_topic("t", None)).await.unwrap();

    // --- sole child: deleting assistant excludes user ---
    let u1 = msg_svc.create(user_msg(tid, 1, "q1")).await.unwrap();
    let a1 = msg_svc.create(asst_msg(tid, 1, u1)).await.unwrap();
    msg_svc.delete(a1).await.unwrap();
    assert!(msg_svc.get(u1).await.unwrap().unwrap().is_excluded);
    assert!(msg_svc.get(a1).await.unwrap().is_none());

    // --- siblings: deleting one assistant keeps user visible ---
    let u2 = msg_svc.create(user_msg(tid, 1, "q2")).await.unwrap();
    let a2 = msg_svc.create(asst_msg(tid, 1, u2)).await.unwrap();
    let a3 = msg_svc.create(asst_msg(tid, 1, u2)).await.unwrap();
    msg_svc.delete(a2).await.unwrap();
    assert!(!msg_svc.get(u2).await.unwrap().unwrap().is_excluded);
    assert!(!msg_svc.get(a3).await.unwrap().unwrap().is_excluded);
}

#[tokio::test]
async fn message_delete_user() {
    let pool = setup().await;
    let topic_svc = TopicStorage::new(pool.clone());
    let msg_svc = MessageStorage::new(pool);
    let tid = topic_svc.create(sample_topic("t", None)).await.unwrap();

    let u = msg_svc.create(user_msg(tid, 1, "q")).await.unwrap();
    let a1 = msg_svc.create(asst_msg(tid, 1, u)).await.unwrap();
    let a2 = msg_svc.create(asst_msg(tid, 1, u)).await.unwrap();
    let a3 = msg_svc.create(asst_msg(tid, 1, u)).await.unwrap();

    msg_svc.delete(u).await.unwrap();

    // all assistants excluded, user deleted
    for id in [a1, a2, a3] {
        assert!(msg_svc.get(id).await.unwrap().unwrap().is_excluded);
    }
    assert!(msg_svc.get(u).await.unwrap().is_none());
}

#[tokio::test]
async fn message_delete_nonexistent() {
    let msg_svc = MessageStorage::new(setup().await);
    assert!(msg_svc.delete(99999).await.is_ok());
}

#[tokio::test]
async fn message_create_validation() {
    let pool = setup().await;
    let topic_svc = TopicStorage::new(pool.clone());
    let msg_svc = MessageStorage::new(pool);
    let tid = topic_svc.create(sample_topic("t", None)).await.unwrap();
    let uid = msg_svc.create(user_msg(tid, 1, "q")).await.unwrap();
    let a1 = msg_svc.create(asst_msg(tid, 1, uid)).await.unwrap();

    // from_id not found
    let err = msg_svc.create(asst_msg(tid, 1, 99999)).await.unwrap_err();
    assert!(err.to_string().contains("not found"));

    // from_id references assistant (must be user message)
    let err = msg_svc.create(asst_msg(tid, 1, a1)).await.unwrap_err();
    assert!(err.to_string().contains("user message"));
}

#[tokio::test]
async fn message_batch() {
    let pool = setup().await;
    let topic_svc = TopicStorage::new(pool.clone());
    let msg_svc = MessageStorage::new(pool);
    let tid = topic_svc.create(sample_topic("t", None)).await.unwrap();
    let uid = msg_svc.create(user_msg(tid, 1, "q")).await.unwrap();

    // --- batch_create_assistant success ---
    let ids = msg_svc
        .batch_create_assistant(vec![
            asst_msg_empty(tid, 1, uid),
            asst_msg_empty(tid, 1, uid),
            asst_msg_empty(tid, 1, uid),
        ])
        .await
        .unwrap();
    assert_eq!(ids.len(), 3);

    let msgs = msg_svc.batch_get(&ids).await.unwrap();
    assert_eq!(msgs.len(), 3);
    // from_id consistent
    for m in &msgs {
        assert_eq!(m.from_id, Some(uid));
    }
    // index incrementing sequentially
    assert_eq!(msgs[1].index, msgs[0].index + 1);
    assert_eq!(msgs[2].index, msgs[0].index + 2);
    // first is excluded (default context), rest visible
    assert!(msgs[0].is_excluded);
    assert!(!msgs[1].is_excluded);
    assert!(!msgs[2].is_excluded);
    // user unexcluded
    assert!(!msg_svc.get(uid).await.unwrap().unwrap().is_excluded);
    // list_by_topic sees all
    assert_eq!(msg_svc.list_by_topic(tid).await.unwrap().len(), 4);

    // --- batch_get partial missing ---
    let results = msg_svc.batch_get(&[ids[0], 99999, 99998]).await.unwrap();
    assert_eq!(results.len(), 1);

    // --- batch_get empty ---
    assert!(msg_svc.batch_get(&[]).await.is_err());

    // --- batch_create errors ---
    // empty
    assert!(msg_svc.batch_create_assistant(vec![]).await.is_err());
    // missing from_id
    assert!(
        msg_svc
            .batch_create_assistant(vec![user_msg(tid, 1, "x")])
            .await
            .unwrap_err()
            .to_string()
            .contains("from_id")
    );
    // mismatched from_id
    let u2 = msg_svc.create(user_msg(tid, 1, "x")).await.unwrap();
    assert!(
        msg_svc
            .batch_create_assistant(vec![
                asst_msg_empty(tid, 1, uid),
                asst_msg_empty(tid, 1, u2),
            ])
            .await
            .unwrap_err()
            .to_string()
            .contains("same from_id")
    );
    // from_id not user message
    let err = msg_svc
        .batch_create_assistant(vec![asst_msg_empty(tid, 1, ids[0])])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("user message"));
    // from_id not found
    assert!(
        msg_svc
            .batch_create_assistant(vec![asst_msg_empty(tid, 1, 99999)])
            .await
            .unwrap_err()
            .to_string()
            .contains("not found")
    );
}

// ==================== McpStorage ====================

#[tokio::test]
async fn mcp_server_crud() {
    let pool = setup().await;
    let svc = McpStorage::new(pool);

    // create
    let id = svc
        .create(sample_mcp("srv", TransportType::Stdio))
        .await
        .unwrap();
    assert!(id > 0);
    let s = svc.get(id).await.unwrap().unwrap();
    assert_eq!(s.name, "srv");
    assert_eq!(s.r#type, TransportType::Stdio);
    assert_eq!(s.args, Some(vec!["server.js".into()]));

    // get non-existent
    assert!(svc.get(999).await.unwrap().is_none());

    // list
    assert_eq!(svc.list().await.unwrap().len(), 1);

    // update — partial (keeps originals for None)
    svc.update(
        id,
        UpdateMcpServer {
            name: "renamed".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let u = svc.get(id).await.unwrap().unwrap();
    assert_eq!(u.name, "renamed");
    assert_eq!(u.r#type, TransportType::Stdio); // kept

    // update — full
    let mut env = HashMap::new();
    env.insert("PORT".into(), "3000".into());
    svc.update(
        id,
        UpdateMcpServer {
            name: "srv2".into(),
            url: Some("https://new.test.com".into()),
            args: Some(vec![]),
            env: Some(env.clone()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let u = svc.get(id).await.unwrap().unwrap();
    assert_eq!(u.name, "srv2");
    assert_eq!(u.url, Some("https://new.test.com".into()));
    assert_eq!(u.args, Some(vec![]));
    assert_eq!(u.env, Some(env));

    // delete
    svc.delete(id).await.unwrap();
    assert!(svc.get(id).await.unwrap().is_none());
}

#[tokio::test]
async fn mcp_server_validation() {
    let svc = McpStorage::new(setup().await);

    // create empty name
    let err = svc
        .create(CreateMcpServer {
            name: "".into(),
            r#type: TransportType::Stdio,
            url: None,
            description: None,
            command: None,
            args: None,
            env: None,
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("cannot be empty"));

    // update empty name
    let id = svc
        .create(sample_mcp("ok", TransportType::Stdio))
        .await
        .unwrap();
    let err = svc
        .update(
            id,
            UpdateMcpServer {
                name: "".into(),
                ..Default::default()
            },
        )
        .await;
    assert!(err.unwrap_err().to_string().contains("cannot be empty"));
}
