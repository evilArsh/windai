// use futures::StreamExt;
// use std::sync::OnceLock;
// use wind_ai::message::{Content, Message as AiMessage, ReqConfig, Role};
// use wind_core::WindCore;
// use wind_core::chat::ChatEvent;
// use wind_core::models::*;
// use wind_mcp::client::registry::{Registry, RegistryHandle};

// #[path = "./common/lib.rs"]
// mod common;

// use common::init_test_core_with_registry;

// // ---------------------------------------------------------------------------
// // 全局 registry — 纯 chat 流程不启动 MCP 服务，只复用空 registry
// // ---------------------------------------------------------------------------

// static CHAT_REGISTRY: OnceLock<RegistryHandle> = OnceLock::new();

// fn shared_chat_registry() -> RegistryHandle {
//     CHAT_REGISTRY
//         .get_or_init(|| {
//             let (tx, rx) = std::sync::mpsc::sync_channel(1);
//             std::thread::Builder::new()
//                 .name("chat-test-registry".to_string())
//                 .spawn(move || {
//                     let rt = tokio::runtime::Builder::new_multi_thread()
//                         .enable_all()
//                         .thread_name("chat-test-registry-worker")
//                         .build()
//                         .expect("failed to build chat test runtime");
//                     let registry = rt.block_on(async { Registry::new() });
//                     tx.send(registry)
//                         .expect("failed to publish chat test registry");
//                     rt.block_on(std::future::pending::<()>());
//                 })
//                 .expect("failed to spawn chat test registry thread");
//             rx.recv().expect("chat test registry thread stopped")
//         })
//         .clone()
// }

// async fn test_core() -> WindCore {
//     init_test_core_with_registry(shared_chat_registry()).await
// }

// struct TestContext {
//     provider_id: i64,
//     model_id: i64,
//     topic_id: i64,
//     user_msg_id: i64,
//     assistant_msg_id: i64,
// }

// /// 为非 MCP 测试创建完整的 provider + credential + model + topic + messages。
// async fn seed_chat_data(core: &WindCore, label: &str) -> TestContext {
//     let env = common::load_env();
//     let provider_name = format!("chat-{}", label);
//     let provider_id = match core
//         .storage()
//         .provider()
//         .get_by_name(&provider_name)
//         .await
//         .unwrap()
//     {
//         Some(p) => p.id,
//         None => core
//             .storage()
//             .provider()
//             .create(CreateProvider {
//                 name: provider_name,
//                 description: None,
//                 base_url: env.test_base_url.clone(),
//                 doc: None,
//                 alias: None,
//             })
//             .await
//             .unwrap(),
//     };

//     core.storage()
//         .provider()
//         .create_credentials(CreateCredentials {
//             provider_id,
//             key: env.test_key.clone(),
//         })
//         .await
//         .unwrap();

//     let model_id = core
//         .storage()
//         .model()
//         .create(CreateModel {
//             name: env.test_model.clone(),
//             provider_id,
//             alias: None,
//             adapter: env.test_adapter,
//             modalities: Some(vec![ModelType::Chat]),
//             active: Some(true),
//             icon: None,
//             endpoint: env.test_endpoint.clone(),
//         })
//         .await
//         .unwrap();

//     let topic_id = core
//         .storage()
//         .topic()
//         .create(CreateTopic {
//             parent_id: None,
//             chat_config_id: 0,
//             label: format!("test-chat-{}", label),
//             icon: None,
//             max_context: Some(100),
//             mcp_server_ids: None,
//         })
//         .await
//         .unwrap();

//     let user_msg_id = core
//         .storage()
//         .message()
//         .create(CreateMessage {
//             from_id: None,
//             stream: false,
//             content: vec![AiMessage::new_simple(
//                 Role::User,
//                 vec![Content::new_text(
//                     "Hello! Reply in one short sentence.".into(),
//                 )],
//                 None,
//             )],
//             model_id,
//             topic_id,
//             is_boundary: false,
//             input_tokens: 0,
//             output_tokens: 0,
//             tools_allowed: None,
//             tools_denied: None,
//         })
//         .await
//         .unwrap();

//     let assistant_msg_id = core
//         .storage()
//         .message()
//         .create(CreateMessage {
//             from_id: Some(user_msg_id),
//             stream: false,
//             content: vec![],
//             model_id,
//             topic_id,
//             is_boundary: false,
//             input_tokens: 0,
//             output_tokens: 0,
//             tools_allowed: None,
//             tools_denied: None,
//         })
//         .await
//         .unwrap();

//     TestContext {
//         provider_id,
//         model_id,
//         topic_id,
//         user_msg_id,
//         assistant_msg_id,
//     }
// }

// // ===========================================================================
// // 基础对话
// // ===========================================================================

// #[tokio::test]
// #[ignore = "need to complete .env config file"]
// async fn test_chat_non_stream() {
//     let core = test_core().await;
//     let ctx = seed_chat_data(&core, "non-stream").await;

//     core.storage()
//         .topic()
//         .create_chat_config(
//             ctx.topic_id,
//             ReqConfig {
//                 stream: Some(false),
//                 ..Default::default()
//             },
//         )
//         .await
//         .unwrap();

//     let engine = core.chat();
//     let mut stream = Box::pin(engine.start(ctx.topic_id, ctx.user_msg_id, ctx.assistant_msg_id));

//     let mut seen_created = false;
//     let mut seen_finish = false;
//     while let Some(event) = stream.next().await {
//         match event {
//             ChatEvent::Created { message_id } => {
//                 assert_eq!(message_id, ctx.assistant_msg_id);
//                 seen_created = true;
//             }
//             ChatEvent::Partial { message_id, .. } => {
//                 assert_eq!(message_id, ctx.assistant_msg_id);
//             }
//             ChatEvent::AwaitToolCall { .. } => {}
//             ChatEvent::Finish {
//                 message_id,
//                 message,
//                 error,
//             } => {
//                 assert_eq!(message_id, ctx.assistant_msg_id);
//                 assert!(error.is_none(), "Chat error: {:?}", error);
//                 assert!(message.is_some(), "Should have response content");
//                 seen_finish = true;
//             }
//         }
//     }
//     assert!(seen_created, "Should emit Created event");
//     assert!(seen_finish, "Should emit Finish event");

//     let msg = core
//         .storage()
//         .message()
//         .get(ctx.assistant_msg_id)
//         .await
//         .unwrap()
//         .unwrap();
//     assert!(!msg.content.is_empty(), "Response should be persisted");
// }

// #[tokio::test]
// #[ignore = "need to complete .env config file"]
// async fn test_chat_stream() {
//     let core = test_core().await;
//     let ctx = seed_chat_data(&core, "stream").await;

//     core.storage()
//         .topic()
//         .create_chat_config(
//             ctx.topic_id,
//             ReqConfig {
//                 stream: Some(true),
//                 ..Default::default()
//             },
//         )
//         .await
//         .unwrap();

//     let engine = core.chat();
//     let mut stream = Box::pin(engine.start(ctx.topic_id, ctx.user_msg_id, ctx.assistant_msg_id));

//     let mut partial_count = 0u32;
//     let mut seen_created = false;
//     let mut seen_finish = false;
//     while let Some(event) = stream.next().await {
//         match event {
//             ChatEvent::Created { message_id } => {
//                 assert_eq!(message_id, ctx.assistant_msg_id);
//                 seen_created = true;
//             }
//             ChatEvent::Partial { message_id, .. } => {
//                 assert_eq!(message_id, ctx.assistant_msg_id);
//                 partial_count += 1;
//             }
//             ChatEvent::AwaitToolCall { .. } => {}
//             ChatEvent::Finish {
//                 message_id, error, ..
//             } => {
//                 assert_eq!(message_id, ctx.assistant_msg_id);
//                 assert!(error.is_none(), "Chat error: {:?}", error);
//                 seen_finish = true;
//             }
//         }
//     }
//     assert!(seen_created, "Should emit Created event");
//     assert!(seen_finish, "Should emit Finish event");
//     assert!(partial_count > 0, "Stream should produce partial chunks");

//     let msg = core
//         .storage()
//         .message()
//         .get(ctx.assistant_msg_id)
//         .await
//         .unwrap()
//         .unwrap();
//     assert!(!msg.content.is_empty(), "Response should be persisted");
// }

// // ===========================================================================
// // 消息历史 — 多轮对话 from_id 链
// // ===========================================================================

// #[tokio::test]
// #[ignore = "need to complete .env config file"]
// async fn test_message_history_chain() {
//     let core = test_core().await;
//     let ctx = seed_chat_data(&core, "history").await;

//     core.storage()
//         .topic()
//         .create_chat_config(
//             ctx.topic_id,
//             ReqConfig {
//                 stream: Some(false),
//                 ..Default::default()
//             },
//         )
//         .await
//         .unwrap();

//     // 第一轮对话
//     let (user2_id, assistant2_id) = {
//         let engine = core.chat();
//         let mut stream =
//             Box::pin(engine.start(ctx.topic_id, ctx.user_msg_id, ctx.assistant_msg_id));

//         let mut seen_finish = false;
//         while let Some(event) = stream.next().await {
//             if let ChatEvent::Finish { error, .. } = event {
//                 assert!(error.is_none(), "Chat error: {:?}", error);
//                 seen_finish = true;
//             }
//         }
//         assert!(seen_finish);

//         let msg = core
//             .storage()
//             .message()
//             .get(ctx.assistant_msg_id)
//             .await
//             .unwrap()
//             .unwrap();
//         assert!(!msg.content.is_empty());

//         let user2 = core
//             .storage()
//             .message()
//             .create(CreateMessage {
//                 from_id: None,
//                 stream: false,
//                 content: vec![AiMessage::new_simple(
//                     Role::User,
//                     vec![Content::new_text("What did I just ask you?".into())],
//                     None,
//                 )],
//                 model_id: ctx.model_id,
//                 topic_id: ctx.topic_id,
//                 is_boundary: false,
//                 input_tokens: 0,
//                 output_tokens: 0,
//                 tools_allowed: None,
//                 tools_denied: None,
//             })
//             .await
//             .unwrap();

//         let assistant2 = core
//             .storage()
//             .message()
//             .create(CreateMessage {
//                 from_id: Some(user2),
//                 stream: false,
//                 content: vec![],
//                 model_id: ctx.model_id,
//                 topic_id: ctx.topic_id,
//                 is_boundary: false,
//                 input_tokens: 0,
//                 output_tokens: 0,
//                 tools_allowed: None,
//                 tools_denied: None,
//             })
//             .await
//             .unwrap();

//         (user2, assistant2)
//     };

//     // 第二轮对话
//     let engine = core.chat();
//     let mut stream = Box::pin(engine.start(ctx.topic_id, user2_id, assistant2_id));

//     let mut seen_finish = false;
//     while let Some(event) = stream.next().await {
//         if let ChatEvent::Finish { error, .. } = event {
//             assert!(error.is_none(), "Second round error: {:?}", error);
//             seen_finish = true;
//         }
//     }
//     assert!(seen_finish);

//     // 验证链：至少 4 条消息
//     let all_msgs = core
//         .storage()
//         .message()
//         .list_by_topic(ctx.topic_id)
//         .await
//         .unwrap();
//     assert!(
//         all_msgs.len() >= 4,
//         "Should have at least 4 messages in chain"
//     );

//     let persisted = core
//         .storage()
//         .message()
//         .get(assistant2_id)
//         .await
//         .unwrap()
//         .unwrap();
//     assert!(
//         !persisted.content.is_empty(),
//         "Second response should be persisted"
//     );
// }

// // ===========================================================================
// // 错误处理
// // ===========================================================================

// #[tokio::test]
// #[ignore = "need to complete .env config file"]
// async fn test_missing_provider() {
//     common::load_env();

//     let core = test_core().await;
//     let engine = core.chat();
//     let mut stream = Box::pin(engine.start(999, 999, 999));

//     let mut error_seen = false;
//     while let Some(event) = stream.next().await {
//         if let ChatEvent::Finish { error, .. } = event {
//             assert!(error.is_some(), "Should have error for missing data");
//             error_seen = true;
//         }
//     }
//     assert!(error_seen);
// }

// #[tokio::test]
// #[ignore = "need to complete .env config file"]
// async fn test_missing_credentials() {
//     let env = common::load_env();

//     let core = test_core().await;

//     let provider_id = core
//         .storage()
//         .provider()
//         .create(CreateProvider {
//             name: "no-creds".into(),
//             description: None,
//             base_url: env.test_base_url.clone(),
//             doc: None,
//             alias: None,
//         })
//         .await
//         .unwrap();

//     let model_id = core
//         .storage()
//         .model()
//         .create(CreateModel {
//             name: env.test_model.clone(),
//             provider_id,
//             alias: None,
//             adapter: env.test_adapter,
//             modalities: Some(vec![ModelType::Chat]),
//             active: Some(true),
//             icon: None,
//             endpoint: env.test_endpoint.clone(),
//         })
//         .await
//         .unwrap();

//     let topic_id = core
//         .storage()
//         .topic()
//         .create(CreateTopic {
//             parent_id: None,
//             chat_config_id: 0,
//             label: "no-creds".into(),
//             icon: None,
//             max_context: Some(100),
//             mcp_server_ids: None,
//         })
//         .await
//         .unwrap();

//     let user_msg_id = core
//         .storage()
//         .message()
//         .create(CreateMessage {
//             from_id: None,
//             stream: false,
//             content: vec![AiMessage::new_simple(
//                 Role::User,
//                 vec![Content::new_text("Hello".into())],
//                 None,
//             )],
//             model_id,
//             topic_id,
//             is_boundary: false,
//             input_tokens: 0,
//             output_tokens: 0,
//             tools_allowed: None,
//             tools_denied: None,
//         })
//         .await
//         .unwrap();

//     let assistant_msg_id = core
//         .storage()
//         .message()
//         .create(CreateMessage {
//             from_id: Some(user_msg_id),
//             stream: false,
//             content: vec![],
//             model_id,
//             topic_id,
//             is_boundary: false,
//             input_tokens: 0,
//             output_tokens: 0,
//             tools_allowed: None,
//             tools_denied: None,
//         })
//         .await
//         .unwrap();

//     let engine = core.chat();
//     let mut stream = Box::pin(engine.start(topic_id, user_msg_id, assistant_msg_id));

//     let mut error_seen = false;
//     while let Some(event) = stream.next().await {
//         if let ChatEvent::Finish { error, .. } = event {
//             assert!(error.is_some(), "Should error for missing credentials");
//             error_seen = true;
//         }
//     }
//     assert!(error_seen);
// }

// // ===========================================================================
// // JSON 规则 — 请求体转换
// // ===========================================================================

// const REASONING_RULE: &str = r#"{
//     "rules": [{
//         "type": "map_value",
//         "path": "reasoning_effort",
//         "mappings": {
//             "medium": {"thinking": {"type": "enabled"}},
//             "high": {"thinking": {"type": "enabled"}}
//         },
//         "default": {"thinking": {"type": "disabled"}},
//         "remove_source": true
//     }]
// }"#;

// #[tokio::test]
// #[ignore = "need to complete .env config file"]
// async fn test_json_rule_reasoning_enabled() {
//     let core = test_core().await;
//     let ctx = seed_chat_data(&core, "rule-on").await;

//     core.storage()
//         .provider()
//         .create_json_rule(CreateJsonRule {
//             provider_id: ctx.provider_id,
//             adapter: common::load_env().test_adapter,
//             json_rule: REASONING_RULE.into(),
//         })
//         .await
//         .unwrap();

//     let rules = core
//         .storage()
//         .provider()
//         .list_json_rules(ctx.provider_id)
//         .await
//         .unwrap();
//     assert_eq!(rules.len(), 1);
//     assert!(rules[0].active);

//     core.storage()
//         .topic()
//         .create_chat_config(
//             ctx.topic_id,
//             ReqConfig {
//                 stream: Some(false),
//                 reasoning: Some(true),
//                 ..Default::default()
//             },
//         )
//         .await
//         .unwrap();

//     let engine = core.chat();
//     let mut stream = Box::pin(engine.start(ctx.topic_id, ctx.user_msg_id, ctx.assistant_msg_id));

//     let mut seen_finish = false;
//     while let Some(event) = stream.next().await {
//         if let ChatEvent::Finish { error, message, .. } = event {
//             assert!(error.is_none(), "Chat error with json_rule: {:?}", error);
//             assert!(message.is_some(), "Should have response with rule applied");
//             assert!(
//                 message
//                     .unwrap()
//                     .iter()
//                     .any(|m| m.reasoning_content.is_some()),
//                 "Should contain reasoning_content"
//             );
//             seen_finish = true;
//         }
//     }
//     assert!(seen_finish);

//     let msg = core
//         .storage()
//         .message()
//         .get(ctx.assistant_msg_id)
//         .await
//         .unwrap()
//         .unwrap();
//     assert!(!msg.content.is_empty());
// }

// #[tokio::test]
// #[ignore = "need to complete .env config file"]
// async fn test_json_rule_reasoning_disabled() {
//     let core = test_core().await;
//     let ctx = seed_chat_data(&core, "rule-off").await;

//     core.storage()
//         .provider()
//         .create_json_rule(CreateJsonRule {
//             provider_id: ctx.provider_id,
//             adapter: common::load_env().test_adapter,
//             json_rule: REASONING_RULE.into(),
//         })
//         .await
//         .unwrap();

//     core.storage()
//         .topic()
//         .create_chat_config(
//             ctx.topic_id,
//             ReqConfig {
//                 stream: Some(false),
//                 reasoning: Some(false),
//                 ..Default::default()
//             },
//         )
//         .await
//         .unwrap();

//     let engine = core.chat();
//     let mut stream = Box::pin(engine.start(ctx.topic_id, ctx.user_msg_id, ctx.assistant_msg_id));

//     let mut seen_finish = false;
//     while let Some(event) = stream.next().await {
//         if let ChatEvent::Finish { error, message, .. } = event {
//             assert!(error.is_none(), "Chat error: {:?}", error);
//             assert!(message.is_some());
//             assert!(
//                 !message
//                     .unwrap()
//                     .iter()
//                     .any(|m| m.reasoning_content.is_some()),
//                 "Should not contain reasoning_content"
//             );
//             seen_finish = true;
//         }
//     }
//     assert!(seen_finish);
// }

// // ===========================================================================
// // 文件 DB 持久化（独立 WindCore，不使用全局内存 Core）
// // ===========================================================================

// #[tokio::test]
// #[ignore = "need to complete .env config file"]
// async fn test_data_persistence() {
//     common::load_env();

//     let dir = std::env::temp_dir().join("windai_core_test");
//     std::fs::create_dir_all(&dir).unwrap();
//     let db_path = dir.join("persist.db");
//     let db_path_str = db_path.to_string_lossy().to_string();

//     // 第一轮：创建、对话、关闭
//     let (topic_id, model_id, aid) = {
//         let wc = WindCore::init_local(Some(&db_path_str)).await.unwrap();
//         let ctx = seed_chat_data(&wc, "persist").await;

//         wc.storage()
//             .topic()
//             .create_chat_config(
//                 ctx.topic_id,
//                 ReqConfig {
//                     stream: Some(false),
//                     ..Default::default()
//                 },
//             )
//             .await
//             .unwrap();

//         let engine = wc.chat();
//         let mut stream =
//             Box::pin(engine.start(ctx.topic_id, ctx.user_msg_id, ctx.assistant_msg_id));

//         let mut seen_finish = false;
//         while let Some(event) = stream.next().await {
//             if let ChatEvent::Finish { error, .. } = event {
//                 assert!(error.is_none(), "Chat error: {:?}", error);
//                 seen_finish = true;
//             }
//         }
//         assert!(seen_finish);

//         let msg = wc
//             .storage()
//             .message()
//             .get(ctx.assistant_msg_id)
//             .await
//             .unwrap()
//             .unwrap();
//         assert!(!msg.content.is_empty());

//         wc.shutdown().await;
//         (ctx.topic_id, ctx.model_id, ctx.assistant_msg_id)
//     }; // WindCore drop

//     // 第二轮：重新打开，验证数据
//     let wc = WindCore::init_local(Some(&db_path_str)).await.unwrap();

//     let providers = wc.storage().provider().list_all().await.unwrap();
//     assert!(!providers.is_empty());

//     let topic = wc
//         .storage()
//         .topic()
//         .get_topic(topic_id)
//         .await
//         .unwrap()
//         .unwrap();
//     assert!(topic.label.contains("persist"));

//     let msg = wc.storage().message().get(aid).await.unwrap().unwrap();
//     assert!(!msg.content.is_empty(), "Content should survive reopen");

//     // 第二轮对话在新打开的 DB 上正常工作
//     let user2_id = wc
//         .storage()
//         .message()
//         .create(CreateMessage {
//             from_id: None,
//             stream: false,
//             content: vec![AiMessage::new_simple(
//                 Role::User,
//                 vec![Content::new_text("Tell me more.".into())],
//                 None,
//             )],
//             model_id,
//             topic_id,
//             is_boundary: false,
//             input_tokens: 0,
//             output_tokens: 0,
//             tools_allowed: None,
//             tools_denied: None,
//         })
//         .await
//         .unwrap();

//     let assistant2_id = wc
//         .storage()
//         .message()
//         .create(CreateMessage {
//             from_id: Some(user2_id),
//             stream: false,
//             content: vec![],
//             model_id,
//             topic_id,
//             is_boundary: false,
//             input_tokens: 0,
//             output_tokens: 0,
//             tools_allowed: None,
//             tools_denied: None,
//         })
//         .await
//         .unwrap();

//     let engine = wc.chat();
//     let mut stream = Box::pin(engine.start(topic_id, user2_id, assistant2_id));

//     let mut seen_finish = false;
//     while let Some(event) = stream.next().await {
//         if let ChatEvent::Finish { error, .. } = event {
//             assert!(error.is_none(), "Chat error on reopened DB: {:?}", error);
//             seen_finish = true;
//         }
//     }
//     assert!(seen_finish);

//     wc.shutdown().await;
//     let _ = std::fs::remove_dir_all(&dir);
// }
