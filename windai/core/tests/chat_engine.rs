use futures::StreamExt;
use sqlx::SqlitePool;
use std::sync::Arc;
use wind_ai::message::{Content, Message as AiMessage, ReqConfig, Role};
use wind_ai::model::AdaptorType;
use wind_core::chat::{ChatEngine, ChatEvent};
use wind_core::models::*;
use wind_core::schema::init_schema;
use wind_core::storage::mcp::service::McpService;
use wind_core::storage::message::service::MessageService;
use wind_core::storage::model::service::ModelService;
use wind_core::storage::provider::service::ProviderService;
use wind_core::storage::topic::service::TopicService;
use wind_js::JsEngine;
use wind_mcp::client::registry::Registry;
use wind_mcp::client::{ServerParams, StdioParams, TransportType};

#[path = "./common/lib.rs"]
mod common;

struct TestContext {
    model_id: i64,
    topic_id: i64,
    user_msg_id: i64,
    assistant_msg_id: i64,
}

async fn setup_db(pool: &SqlitePool, env: &common::Env) -> TestContext {
    let provider_svc = ProviderService::new(pool.clone());
    let provider = provider_svc
        .create(CreateProvider {
            name: "test-provider".into(),
            description: None,
            base_url: env.test_mcp_completion_base_url.clone(),
            doc: None,
            alias: None,
            active: Some(true),
        })
        .await
        .unwrap();

    provider_svc
        .create_credentials(CreateCredentials {
            provider_id: provider.id,
            key: env.test_mcp_completion_key.clone(),
        })
        .await
        .unwrap();

    let model_svc = ModelService::new(pool.clone());
    let model = model_svc
        .create(CreateModel {
            name: env.test_mcp_completion_model.clone(),
            provider_id: provider.id,
            alias: None,
            adaptor: AdaptorType::OpenAICompletion,
            modalities: Some(vec![ModelType::Chat]),
            active: Some(true),
            icon: None,
            endpoint: env.test_mcp_completion_endpoint.clone(),
        })
        .await
        .unwrap();

    let topic_svc = TopicService::new(pool.clone());
    let topic = topic_svc
        .create_topic(CreateTopic {
            parent_id: None,
            chat_config_id: 0,
            label: "test-chat-engine".into(),
            icon: None,
            max_context: Some(100),
        })
        .await
        .unwrap();

    let msg_svc = MessageService::new(pool.clone());
    let user_msg = msg_svc
        .create(CreateMessage {
            from_id: None,
            stream: false,
            content_json: serde_json::to_string(&vec![AiMessage::new_simple(
                Role::User,
                vec![Content::new_text(
                    "Hello! Please say something interesting about Rust programming.".into(),
                )],
                None,
            )])
            .unwrap(),
            model_id: model.id,
            topic_id: topic.id,
            is_boundary: false,
            is_excluded: false,
            input_tokens: 10,
            output_tokens: 0,
        })
        .await
        .unwrap();

    let assistant_msg = msg_svc
        .create(CreateMessage {
            from_id: Some(user_msg.id),
            stream: false,
            content_json: "[]".into(),
            model_id: model.id,
            topic_id: topic.id,
            is_boundary: false,
            is_excluded: false,
            input_tokens: 0,
            output_tokens: 0,
        })
        .await
        .unwrap();

    TestContext {
        model_id: model.id,
        topic_id: topic.id,
        user_msg_id: user_msg.id,
        assistant_msg_id: assistant_msg.id,
    }
}

fn make_engine(pool: SqlitePool) -> (ChatEngine, wind_mcp::client::registry::RegistryHandle) {
    let js_engine = Arc::new(JsEngine::new().unwrap());
    let registry_handle = Registry::new();
    let engine = ChatEngine::new(pool, js_engine, registry_handle.clone());
    (engine, registry_handle)
}

fn everything_params() -> ServerParams {
    ServerParams::Stdio(StdioParams {
        name: "everything".into(),
        description: None,
        command: "npx".into(),
        args: vec![
            "-y".into(),
            "@modelcontextprotocol/server-everything".into(),
        ],
        env: None,
    })
}

// ---------- non-stream, no MCP ----------

#[tokio::test]
async fn test_chat_engine_send_non_stream() {
    let env = common::load_env();

    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    init_schema(&pool).await.unwrap();
    let ctx = setup_db(&pool, &env).await;

    let topic_svc = TopicService::new(pool.clone());
    topic_svc
        .create_chat_config(
            ctx.topic_id,
            ReqConfig {
                stream: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let (engine, _handle) = make_engine(pool.clone());

    let stream = engine.send(
        ctx.topic_id,
        ctx.model_id,
        ctx.user_msg_id,
        ctx.assistant_msg_id,
    );
    let mut stream = Box::pin(stream);

    let mut seen_created = false;
    let mut seen_finish = false;
    while let Some(event) = stream.next().await {
        match event {
            ChatEvent::Created { message_id } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
                seen_created = true;
            }
            ChatEvent::Partial {
                index,
                message_id,
                delta,
            } => {
                log::debug!("[partial]\n{}", delta);
                assert_eq!(message_id, ctx.assistant_msg_id);
                println!("[partial] index={index}");
            }
            ChatEvent::Finish {
                message_id,
                message,
                error,
            } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
                assert!(error.is_none(), "Chat error: {:?}", error);
                assert!(message.is_some(), "Should have response content");
                seen_finish = true;
            }
        }
    }
    assert!(seen_created, "Should emit Created event");
    assert!(seen_finish, "Should emit Finish event");

    let msg_svc = MessageService::new(pool);
    let msg = msg_svc.get(ctx.assistant_msg_id).await.unwrap().unwrap();
    assert!(
        !msg.content.is_empty(),
        "Assistant message should have content"
    );
    println!("[persisted] {} content items", msg.content.len());
}

// ---------- stream, no MCP ----------

#[tokio::test]
async fn test_chat_engine_send_stream() {
    let env = common::load_env();

    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    init_schema(&pool).await.unwrap();
    let ctx = setup_db(&pool, &env).await;

    let topic_svc = TopicService::new(pool.clone());
    topic_svc
        .create_chat_config(
            ctx.topic_id,
            ReqConfig {
                stream: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let (engine, _handle) = make_engine(pool.clone());

    let stream = engine.send(
        ctx.topic_id,
        ctx.model_id,
        ctx.user_msg_id,
        ctx.assistant_msg_id,
    );
    let mut stream = Box::pin(stream);

    let mut partial_count = 0;
    let mut seen_created = false;
    let mut seen_finish = false;
    while let Some(event) = stream.next().await {
        match event {
            ChatEvent::Created { message_id } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
                seen_created = true;
            }
            ChatEvent::Partial {
                index,
                message_id,
                delta,
            } => {
                // log::debug!("[partial delta]\n{}", delta);
                assert_eq!(message_id, ctx.assistant_msg_id);
                println!(
                    "[partial] index={index}, content_items={}",
                    delta.content.len()
                );
                partial_count += 1;
            }
            ChatEvent::Finish {
                message_id, error, ..
            } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
                assert!(error.is_none(), "Chat error: {:?}", error);
                seen_finish = true;
            }
        }
    }
    assert!(seen_created, "Should emit Created event");
    assert!(seen_finish, "Should emit Finish event");
    assert!(partial_count > 0, "Streaming should produce partial chunks");
    println!("[stream] received {partial_count} partial chunks");

    let msg_svc = MessageService::new(pool);
    let msg = msg_svc.get(ctx.assistant_msg_id).await.unwrap().unwrap();
    assert!(
        !msg.content.is_empty(),
        "Assistant message should have content"
    );
}

// ---------- non-stream, with MCP ----------

#[tokio::test]
async fn test_chat_engine_send_mcp_non_stream() {
    let env = common::load_env();

    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    init_schema(&pool).await.unwrap();

    let mcp_svc = McpService::new(pool.clone());
    let server_id = mcp_svc
        .create(CreateMcpServer {
            r#type: TransportType::Stdio,
            name: "everything".into(),
            url: None,
            description: None,
            command: Some("npx".into()),
            args: Some(vec![
                "-y".into(),
                "@modelcontextprotocol/server-everything".into(),
            ]),
            env: None,
        })
        .await
        .unwrap();

    let ctx = setup_db(&pool, &env).await;

    let topic_svc = TopicService::new(pool.clone());
    topic_svc
        .set_mcp_servers(ctx.topic_id, vec![server_id])
        .await
        .unwrap();

    let msg_svc = MessageService::new(pool.clone());
    msg_svc
        .update(
            ctx.user_msg_id,
            UpdateMessage {
                from_id: None,
                stream: None,
                content_json: Some(
                    serde_json::to_string(&vec![AiMessage::new_simple(
                        Role::User,
                        vec![Content::new_text(
                            "Use the echo tool to echo this exact message: \"Hello from ChatEngine MCP test\""
                                .into(),
                        )],
                        None,
                    )])
                    .unwrap(),
                ),
                model_id: None,
                topic_id: None,
                is_boundary: None,
                is_excluded: None,
                input_tokens: None,
                output_tokens: None,
            },
        )
        .await
        .unwrap();

    topic_svc
        .create_chat_config(
            ctx.topic_id,
            ReqConfig {
                stream: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let (engine, handle) = make_engine(pool.clone());

    let _snapshot = handle
        .acquire("test-session-mcp-ns", everything_params())
        .await
        .unwrap();
    println!("[mcp] server acquired: {:?}", _snapshot.status);

    let stream = engine.send(
        ctx.topic_id,
        ctx.model_id,
        ctx.user_msg_id,
        ctx.assistant_msg_id,
    );
    let mut stream = Box::pin(stream);

    let mut seen_created = false;
    let mut seen_finish = false;
    let mut has_tool_call = false;
    while let Some(event) = stream.next().await {
        match event {
            ChatEvent::Created { message_id } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
                seen_created = true;
            }
            ChatEvent::Partial {
                index,
                message_id,
                delta,
            } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
                if delta.tool_calls.as_ref().is_some_and(|t| !t.is_empty()) {
                    has_tool_call = true;
                    println!("[partial] index={index}, tool_calls={:?}", delta.tool_calls);
                } else {
                    println!("[partial] index={index}, role={:?}", delta.role);
                }
            }
            ChatEvent::Finish {
                message_id,
                message,
                error,
            } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
                assert!(error.is_none(), "Chat error: {:?}", error);
                seen_finish = true;
                if let Some(ref msgs) = message {
                    for (i, msg) in msgs.iter().enumerate() {
                        if msg.tool_calls.is_some() {
                            println!("[finish] message[{i}] has tool_calls");
                        }
                    }
                }
            }
        }
    }
    assert!(seen_created, "Should emit Created event");
    assert!(seen_finish, "Should emit Finish event");
    println!("[mcp non-stream] has_tool_call={has_tool_call} (model may or may not call tools)");

    handle.shutdown().await;
}

// ---------- stream, with MCP ----------

#[tokio::test]
async fn test_chat_engine_send_mcp_stream() {
    let env = common::load_env();

    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    init_schema(&pool).await.unwrap();

    let mcp_svc = McpService::new(pool.clone());
    let server_id = mcp_svc
        .create(CreateMcpServer {
            r#type: TransportType::Stdio,
            name: "everything".into(),
            url: None,
            description: None,
            command: Some("npx".into()),
            args: Some(vec![
                "-y".into(),
                "@modelcontextprotocol/server-everything".into(),
            ]),
            env: None,
        })
        .await
        .unwrap();

    let ctx = setup_db(&pool, &env).await;

    let topic_svc = TopicService::new(pool.clone());
    topic_svc
        .set_mcp_servers(ctx.topic_id, vec![server_id])
        .await
        .unwrap();

    let msg_svc = MessageService::new(pool.clone());
    msg_svc
        .update(
            ctx.user_msg_id,
            UpdateMessage {
                from_id: None,
                stream: None,
                content_json: Some(
                    serde_json::to_string(&vec![AiMessage::new_simple(
                        Role::User,
                        vec![Content::new_text(
                            "Use the echo tool to echo this exact message: \"Hello from ChatEngine MCP test\""
                                .into(),
                        )],
                        None,
                    )])
                    .unwrap(),
                ),
                model_id: None,
                topic_id: None,
                is_boundary: None,
                is_excluded: None,
                input_tokens: None,
                output_tokens: None,
            },
        )
        .await
        .unwrap();

    topic_svc
        .create_chat_config(
            ctx.topic_id,
            ReqConfig {
                stream: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let (engine, handle) = make_engine(pool.clone());

    let _snapshot = handle
        .acquire("test-session-mcp-s", everything_params())
        .await
        .unwrap();
    println!("[mcp] server acquired: {:?}", _snapshot.status);

    let stream = engine.send(
        ctx.topic_id,
        ctx.model_id,
        ctx.user_msg_id,
        ctx.assistant_msg_id,
    );
    let mut stream = Box::pin(stream);

    let mut partial_count = 0;
    let mut seen_created = false;
    let mut seen_finish = false;
    let mut has_tool_call = false;
    while let Some(event) = stream.next().await {
        match event {
            ChatEvent::Created { message_id } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
                seen_created = true;
            }
            ChatEvent::Partial {
                index,
                message_id,
                delta,
            } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
                partial_count += 1;
                if delta.tool_calls.as_ref().is_some_and(|t| !t.is_empty()) {
                    has_tool_call = true;
                    println!("[partial] index={index}, tool_calls={:?}", delta.tool_calls);
                }
            }
            ChatEvent::Finish {
                message_id, error, ..
            } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
                assert!(error.is_none(), "Chat error: {:?}", error);
                seen_finish = true;
            }
        }
    }
    assert!(seen_created, "Should emit Created event");
    assert!(seen_finish, "Should emit Finish event");
    assert!(partial_count > 0, "Streaming should produce partial chunks");
    println!("[mcp stream] {partial_count} partial chunks, has_tool_call={has_tool_call}");

    handle.shutdown().await;
}
