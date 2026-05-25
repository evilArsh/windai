use sqlx::SqlitePool;
use wind_ai::message::ReqConfig;
use wind_ai::model::AdaptorType;
use wind_core::models::*;
use wind_core::schema::init_schema;
use wind_core::storage::mcp::service::McpService;
use wind_core::storage::message::service::MessageService;
use wind_core::storage::model::service::ModelService;
use wind_core::storage::provider::service::ProviderService;
use wind_core::storage::topic::service::TopicService;
use wind_mcp::client::TransportType;

async fn setup() -> SqlitePool {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    init_schema(&pool).await.unwrap();
    pool
}

fn create_provider() -> CreateProvider {
    CreateProvider {
        name: "test-provider".into(),
        base_url: "https://api.test.com".into(),
        description: Some("test".into()),
        doc: Some("https://docs.test.com".into()),
        alias: Some("tp".into()),
        active: Some(true),
    }
}

fn create_mcp_server(name: &str, r#type: TransportType) -> CreateMcpServer {
    CreateMcpServer {
        r#type,
        name: name.into(),
        url: Some(format!("https://{}.test.com", name)),
        description: Some(format!("{} description", name)),
        command: Some("node".into()),
        args: Some(vec!["server.js".into()]),
        env: None,
    }
}

// ==================== Provider CRUD ====================

#[tokio::test]
async fn provider_crud() {
    let pool = setup().await;
    let svc = ProviderService::new(pool);

    // Create
    let p = svc.create(create_provider()).await.unwrap();
    assert_eq!(p.name, "test-provider");
    assert_eq!(p.base_url, "https://api.test.com");
    assert!(p.active);

    // Get
    let found = svc.get(p.id).await.unwrap().unwrap();
    assert_eq!(found.name, p.name);

    // List
    let list = svc.list().await.unwrap();
    assert_eq!(list.len(), 1);

    // Update
    svc.update(
        p.id,
        UpdateProvider {
            name: Some("updated-provider".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let updated = svc.get(p.id).await.unwrap().unwrap();
    assert_eq!(updated.name, "updated-provider");

    // Delete
    svc.delete(p.id).await.unwrap();
    assert!(svc.get(p.id).await.unwrap().is_none());
}

#[tokio::test]
async fn provider_create_duplicate_name() {
    let svc = ProviderService::new(setup().await);
    svc.create(create_provider()).await.unwrap();
    let err = svc.create(create_provider()).await.unwrap_err();
    assert!(err.to_string().contains("already exists"));
}

// ==================== Credentials CRUD ====================

#[tokio::test]
async fn credentials_crud() {
    let pool = setup().await;
    let svc = ProviderService::new(pool);
    let p = svc.create(create_provider()).await.unwrap();

    // Create
    let cred = svc
        .create_credentials(CreateCredentials {
            provider_id: p.id,
            key: "sk-test-key".into(),
        })
        .await
        .unwrap();
    assert_eq!(cred.key, "sk-test-key");

    // List
    let list = svc.list_credentials(p.id).await.unwrap();
    assert_eq!(list.len(), 1);

    // Delete
    svc.delete_credentials(cred.id).await.unwrap();
    assert_eq!(svc.list_credentials(p.id).await.unwrap().len(), 0);
}

// ==================== JsonRule CRUD ====================

#[tokio::test]
async fn json_rule_crud() {
    let pool = setup().await;
    let svc = ProviderService::new(pool);
    let p = svc.create(create_provider()).await.unwrap();

    // Create
    let rule = svc
        .create_json_rule(CreateJsonRule {
            provider_id: p.id,
            adaptor: AdaptorType::OpenAICompletion,
            json_rule: r#"{"rules": [{"type": "set", "path": "stream", "value": true}]}"#.into(),
            active: true,
        })
        .await
        .unwrap();
    assert_eq!(rule.provider_id, p.id);

    // Get by provider + adaptor
    let found = svc
        .get_json_rule(p.id, AdaptorType::OpenAICompletion)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.id, rule.id);

    // Get by id
    let by_id = svc.get_json_rule_by_id(rule.id).await.unwrap().unwrap();
    assert_eq!(by_id.id, rule.id);

    // List
    let list = svc.list_json_rules(p.id).await.unwrap();
    assert_eq!(list.len(), 1);

    // Update
    svc.update_json_rule(
        rule.id,
        UpdateJsonRule {
            json_rule: Some(r#"{"rules": []}"#.into()),
            active: Some(false),
            provider_id: None,
            adaptor: None,
        },
    )
    .await
    .unwrap();
    let updated = svc.get_json_rule_by_id(rule.id).await.unwrap().unwrap();
    assert_eq!(updated.json_rule, r#"{"rules": []}"#);
    assert!(!updated.active);

    // Delete
    svc.delete_json_rule(rule.id).await.unwrap();
    assert!(svc.get_json_rule_by_id(rule.id).await.unwrap().is_none());
}

#[tokio::test]
async fn json_rule_update_uses_current_when_omitted() {
    let pool = setup().await;
    let svc = ProviderService::new(pool);
    let p = svc.create(create_provider()).await.unwrap();
    let rule = svc
        .create_json_rule(CreateJsonRule {
            provider_id: p.id,
            adaptor: AdaptorType::OpenAICompletion,
            json_rule: r#"{"rules": [{"type": "set", "path": "x", "value": 1}]}"#.into(),
            active: true,
        })
        .await
        .unwrap();

    // Update without providing adaptor/provider_id — should keep originals
    svc.update_json_rule(
        rule.id,
        UpdateJsonRule {
            json_rule: Some(r#"{"rules": [{"type": "set", "path": "y", "value": 2}]}"#.into()),
            active: None,
            adaptor: None,
            provider_id: None,
        },
    )
    .await
    .unwrap();

    let updated = svc.get_json_rule_by_id(rule.id).await.unwrap().unwrap();
    assert_eq!(
        updated.json_rule,
        r#"{"rules": [{"type": "set", "path": "y", "value": 2}]}"#
    );
    assert_eq!(updated.adaptor, AdaptorType::OpenAICompletion);
    assert_eq!(updated.provider_id, p.id);
    assert!(updated.active); // kept original
}

// ==================== Model CRUD ====================

#[tokio::test]
async fn model_crud() {
    let pool = setup().await;
    let p_svc = ProviderService::new(pool.clone());
    let m_svc = ModelService::new(pool);
    let p = p_svc.create(create_provider()).await.unwrap();

    // Create
    let m = m_svc
        .create(CreateModel {
            name: "gpt-4".into(),
            provider_id: p.id,
            alias: Some("gpt4".into()),
            adaptor: AdaptorType::OpenAICompletion,
            modalities: Some(vec![ModelType::Chat]),
            active: Some(true),
            icon: Some("icon.png".into()),
            endpoint: None,
        })
        .await
        .unwrap();
    assert_eq!(m.name, "gpt-4");
    assert_eq!(m.frequency, Some(0));

    // Get
    let found = m_svc.get(m.id).await.unwrap().unwrap();
    assert_eq!(found.name, m.name);
    assert_eq!(found.modalities, Some(vec![ModelType::Chat]));

    // List by provider
    let list = m_svc.list_by_provider(p.id).await.unwrap();
    assert_eq!(list.len(), 1);

    // Update
    m_svc
        .update(
            m.id,
            UpdateModel {
                name: Some("gpt-4-turbo".into()),
                frequency: Some(5),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let updated = m_svc.get(m.id).await.unwrap().unwrap();
    assert_eq!(updated.name, "gpt-4-turbo");
    assert_eq!(updated.frequency, Some(5));

    // Delete
    m_svc.delete(m.id).await.unwrap();
    assert!(m_svc.get(m.id).await.unwrap().is_none());
}

// ==================== Topic CRUD ====================

fn create_topic(chat_config_id: i64) -> CreateTopic {
    CreateTopic {
        parent_id: None,
        chat_config_id,
        label: "test-topic".into(),
        icon: Some("topic-icon".into()),
        max_context: Some(1000),
    }
}

#[tokio::test]
async fn topic_crud() {
    let pool = setup().await;
    let svc = TopicService::new(pool);

    // Create
    let t = svc.create_topic(create_topic(0)).await.unwrap();
    assert_eq!(t.label, "test-topic");
    assert!(t.index > 0);

    // Get
    let found = svc.get_topic(t.id).await.unwrap().unwrap();
    assert_eq!(found.label, t.label);

    // List
    let list = svc.list_topics().await.unwrap();
    assert_eq!(list.len(), 1);

    // Update
    svc.update_topic(
        t.id,
        UpdateTopic {
            label: Some("updated-topic".into()),
            max_context: Some(500),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let updated = svc.get_topic(t.id).await.unwrap().unwrap();
    assert_eq!(updated.label, "updated-topic");
    assert_eq!(updated.max_context, Some(500));

    // Delete
    svc.delete_topics(&[t.id]).await.unwrap();
    assert!(svc.get_topic(t.id).await.unwrap().is_none());
}

#[tokio::test]
async fn topic_with_parent() {
    let pool = setup().await;
    let svc = TopicService::new(pool);

    let parent = svc.create_topic(create_topic(0)).await.unwrap();
    let child = svc
        .create_topic(CreateTopic {
            parent_id: Some(parent.id),
            chat_config_id: 0,
            label: "child".into(),
            icon: None,
            max_context: None,
        })
        .await
        .unwrap();
    assert_eq!(child.parent_id, Some(parent.id));
}

// ==================== Message CRUD ====================

fn create_msg_data(topic_id: i64, model_id: i64) -> CreateMessage {
    CreateMessage {
        from_id: None,
        stream: false,
        content_json: r#"[{"role":"user","content":"hello"}]"#.into(),
        model_id,
        topic_id,
        is_boundary: false,
        is_excluded: false,
        input_tokens: 5,
        output_tokens: 0,
    }
}

#[tokio::test]
async fn message_crud() {
    let pool = setup().await;
    let topic_svc = TopicService::new(pool.clone());
    let msg_svc = MessageService::new(pool);
    let t = topic_svc.create_topic(create_topic(0)).await.unwrap();

    // Create
    let msg = msg_svc.create(create_msg_data(t.id, 1)).await.unwrap();
    assert_eq!(msg.topic_id, t.id);
    assert_eq!(msg.model_id, 1);
    assert!(msg.index > 0);

    // Get
    let found = msg_svc.get(msg.id).await.unwrap().unwrap();
    assert_eq!(found.id, msg.id);

    // List by topic
    let list = msg_svc.list_by_topic(t.id).await.unwrap();
    assert_eq!(list.len(), 1);

    // Update
    msg_svc
        .update(
            msg.id,
            UpdateMessage {
                is_boundary: Some(true),
                input_tokens: Some(10),
                ..UpdateMessage::from(msg.clone())
            },
        )
        .await
        .unwrap();
    let updated = msg_svc.get(msg.id).await.unwrap().unwrap();
    assert!(updated.is_boundary);
    assert_eq!(updated.input_tokens, 10);
}

#[tokio::test]
async fn message_ordering() {
    let pool = setup().await;
    let topic_svc = TopicService::new(pool.clone());
    let msg_svc = MessageService::new(pool);
    let t = topic_svc.create_topic(create_topic(0)).await.unwrap();

    let m1 = msg_svc.create(create_msg_data(t.id, 1)).await.unwrap();
    let m2 = msg_svc
        .create(CreateMessage {
            content_json: r#"[{"role":"assistant","content":"hi"}]"#.into(),
            ..create_msg_data(t.id, 1)
        })
        .await
        .unwrap();

    assert!(m2.index > m1.index);
}

// ==================== Chat Config ====================

#[tokio::test]
async fn chat_config_upsert() {
    let pool = setup().await;
    let svc = TopicService::new(pool);
    let t = svc.create_topic(create_topic(0)).await.unwrap();

    // Create
    let cfg = ReqConfig {
        temperature: Some(0.7),
        top_p: None,
        max_tokens: Some(2048),
        stream: Some(true),
        presence_penalty: None,
        frequency_penalty: None,
        parallel_tool_calls: None,
        reasoning: None,
    };
    svc.create_chat_config(t.id, cfg).await.unwrap();

    let saved = svc.get_chat_config(t.id).await.unwrap().unwrap();
    assert_eq!(saved.data.temperature, Some(0.7));
    assert_eq!(saved.data.max_tokens, Some(2048));

    // Upsert — overwrite
    let cfg2 = ReqConfig {
        temperature: Some(0.2),
        top_p: Some(0.9),
        max_tokens: None,
        stream: None,
        presence_penalty: None,
        frequency_penalty: None,
        parallel_tool_calls: None,
        reasoning: None,
    };
    svc.create_chat_config(t.id, cfg2).await.unwrap();

    let updated = svc.get_chat_config(t.id).await.unwrap().unwrap();
    assert_eq!(updated.data.temperature, Some(0.2));
    assert_eq!(updated.data.top_p, Some(0.9));
    assert_eq!(updated.data.max_tokens, None); // overwritten
}

// ==================== MCP Server ⬌ Topic association ====================

#[tokio::test]
async fn topic_link_mcp_servers() {
    let pool = setup().await;
    let mcp_svc = McpService::new(pool.clone());
    let topic_svc = TopicService::new(pool);

    let s1 = mcp_svc
        .create(create_mcp_server("server1", TransportType::Stdio))
        .await
        .unwrap();
    let s2 = mcp_svc
        .create(create_mcp_server("server2", TransportType::Streamable))
        .await
        .unwrap();
    let t = topic_svc.create_topic(create_topic(0)).await.unwrap();

    // Link servers to topic
    topic_svc.set_mcp_servers(t.id, vec![s1, s2]).await.unwrap();
    let servers = topic_svc.list_mcp_servers(t.id).await.unwrap();
    assert_eq!(servers.len(), 2);
    assert!(servers.iter().any(|s| s.id == s1));
    assert!(servers.iter().any(|s| s.id == s2));

    // Replace
    let s3 = mcp_svc
        .create(create_mcp_server("server3", TransportType::Stdio))
        .await
        .unwrap();
    topic_svc.set_mcp_servers(t.id, vec![s3]).await.unwrap();
    let servers = topic_svc.list_mcp_servers(t.id).await.unwrap();
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0].id, s3);

    // Clear
    topic_svc.set_mcp_servers(t.id, vec![]).await.unwrap();
    assert_eq!(topic_svc.list_mcp_servers(t.id).await.unwrap().len(), 0);
}

// ==================== McpServerParam CRUD ====================

#[tokio::test]
async fn mcp_server_crud() {
    let pool = setup().await;
    let svc = McpService::new(pool);

    // Create
    let id = svc
        .create(create_mcp_server("test-mcp", TransportType::Stdio))
        .await
        .unwrap();
    assert!(id > 0);

    // Get
    let got = svc.get(id).await.unwrap();
    assert_eq!(got.name, "test-mcp");
    assert_eq!(got.r#type, TransportType::Stdio);
    assert_eq!(got.url, Some("https://test-mcp.test.com".into()));
    assert_eq!(got.args, Some(vec!["server.js".into()]));

    // List
    let list = svc.list().await.unwrap();
    assert_eq!(list.len(), 1);

    // Update — change url and clear args
    let mut new_env = std::collections::HashMap::new();
    new_env.insert("PORT".into(), "3000".into());
    svc.update(
        id,
        UpdateMcpServer {
            url: Some("https://updated.test.com".into()),
            args: Some(vec![]),
            env: Some(new_env.clone()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let updated = svc.get(id).await.unwrap();
    assert_eq!(updated.url, Some("https://updated.test.com".into()));
    assert_eq!(updated.args, Some(vec![]));
    assert_eq!(updated.env, Some(new_env));

    // Delete
    svc.delete(id).await.unwrap();
    assert!(svc.get(id).await.is_err());
}

#[tokio::test]
async fn mcp_server_update_keeps_originals() {
    let pool = setup().await;
    let svc = McpService::new(pool);
    let id = svc
        .create(create_mcp_server("keep-test", TransportType::Streamable))
        .await
        .unwrap();

    // Partial update — only change name
    svc.update(
        id,
        UpdateMcpServer {
            name: Some("renamed".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let updated = svc.get(id).await.unwrap();
    assert_eq!(updated.name, "renamed");
    assert_eq!(updated.r#type, TransportType::Streamable); // kept original
    assert_eq!(updated.url, Some("https://keep-test.test.com".into())); // kept original
}

#[tokio::test]
async fn mcp_server_create_empty_name() {
    let svc = McpService::new(setup().await);
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
}

// ==================== Cascade Delete ====================

#[tokio::test]
async fn delete_provider_cascades() {
    let pool = setup().await;
    let svc = ProviderService::new(pool);
    let p = svc.create(create_provider()).await.unwrap();

    // Create associated records
    let _cred = svc
        .create_credentials(CreateCredentials {
            provider_id: p.id,
            key: "key".into(),
        })
        .await
        .unwrap();
    let _rule = svc
        .create_json_rule(CreateJsonRule {
            provider_id: p.id,
            adaptor: AdaptorType::OpenAICompletion,
            json_rule: r#"{"rules": []}"#.into(),
            active: true,
        })
        .await
        .unwrap();

    // Delete provider
    svc.delete(p.id).await.unwrap();

    // Credentials and json_rules should be gone
    assert_eq!(svc.list_credentials(p.id).await.unwrap().len(), 0);
    assert_eq!(svc.list_json_rules(p.id).await.unwrap().len(), 0);
}

#[tokio::test]
async fn delete_topic_cascades() {
    let pool = setup().await;
    let topic_svc = TopicService::new(pool.clone());
    let msg_svc = MessageService::new(pool);
    let t = topic_svc.create_topic(create_topic(0)).await.unwrap();

    // Create associated records
    let msg = msg_svc.create(create_msg_data(t.id, 1)).await.unwrap();
    topic_svc
        .create_chat_config(
            t.id,
            ReqConfig {
                temperature: Some(0.5),
                top_p: None,
                max_tokens: None,
                stream: None,
                presence_penalty: None,
                frequency_penalty: None,
                parallel_tool_calls: None,
                reasoning: None,
            },
        )
        .await
        .unwrap();
    topic_svc.set_mcp_servers(t.id, vec![1, 2]).await.unwrap();

    // Delete topic
    topic_svc.delete_topics(&[t.id]).await.unwrap();

    // Cascaded records should be gone
    assert!(msg_svc.get(msg.id).await.unwrap().is_none());
    assert!(topic_svc.get_topic(t.id).await.unwrap().is_none());
    assert!(topic_svc.get_chat_config(t.id).await.unwrap().is_none());
    assert_eq!(topic_svc.list_mcp_servers(t.id).await.unwrap().len(), 0);
}
