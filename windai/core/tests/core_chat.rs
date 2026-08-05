use std::sync::OnceLock;
use wind_ai::message::{Content, ReqConfig};
use wind_core::WindCore;
use wind_core::agent::event::TopicEvent;
use wind_core::models::*;
use wind_mcp::client::registry::{Registry, RegistryHandle};

#[path = "./common/lib.rs"]
mod common;

use common::init_test_core_with_registry;

// ---------------------------------------------------------------------------
// 全局 registry — 纯 chat 流程不启动 MCP 服务，只复用空 registry
// ---------------------------------------------------------------------------

static CHAT_REGISTRY: OnceLock<RegistryHandle> = OnceLock::new();

fn shared_chat_registry() -> RegistryHandle {
    CHAT_REGISTRY
        .get_or_init(|| {
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            std::thread::Builder::new()
                .name("chat-test-registry".to_string())
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .thread_name("chat-test-registry-worker")
                        .build()
                        .expect("failed to build chat test runtime");
                    let registry = rt.block_on(async { Registry::new() });
                    tx.send(registry)
                        .expect("failed to publish chat test registry");
                    rt.block_on(std::future::pending::<()>());
                })
                .expect("failed to spawn chat test registry thread");
            rx.recv().expect("chat test registry thread stopped")
        })
        .clone()
}

async fn test_core() -> WindCore {
    init_test_core_with_registry(shared_chat_registry()).await
}

struct TestContext {
    provider: Provider,
    model: Model,
    topic: Topic,
}

/// 为非 MCP 测试创建完整的 provider + credential + model + topic
async fn seed_chat_data(core: &WindCore, label: &str) -> TestContext {
    let env = common::load_env();
    let provider_name = format!("chat-{}", label);
    let provider = match core
        .storage()
        .provider()
        .get_by_name(&provider_name)
        .await
        .unwrap()
    {
        Some(p) => p,
        None => core
            .storage()
            .provider()
            .create(CreateProvider {
                name: provider_name,
                description: None,
                base_url: env.test_base_url.clone(),
                doc: None,
                alias: None,
            })
            .await
            .unwrap(),
    };

    core.storage()
        .provider()
        .create_credentials(CreateCredentials {
            provider_id: provider.id,
            key: env.test_key.clone(),
        })
        .await
        .unwrap();

    let model = core
        .storage()
        .model()
        .create(CreateModel {
            name: env.test_model.clone(),
            provider_id: provider.id,
            alias: None,
            adapter: env.test_adapter,
            modalities: Some(vec![ModelType::Chat]),
            active: Some(true),
            icon: None,
            endpoint: env.test_endpoint.clone(),
        })
        .await
        .unwrap();

    let topic = core
        .storage()
        .topic()
        .create(CreateTopic {
            binding_id: None,
            parent_id: None,
            label: format!("test-chat-{}", label),
            icon: None,
        })
        .await
        .unwrap();

    TestContext {
        provider,
        model,
        topic,
    }
}

// ===========================================================================
// 基础对话
// ===========================================================================
#[tokio::test]
#[ignore = "need to complete .env config file"]
async fn test_agent_chat() {
    let mut wc = test_core().await;
    let ctx = seed_chat_data(&wc, "agent-chat").await;
    let user_input = vec![Content::new_text(
        "Hello! Reply in one short sentence.Response in Chinese".into(),
    )];

    let agent_def = wc
        .storage()
        .agent()
        .create_definition(CreateAgentDefinition {
            name: "test-agent-chat".into(),
            key: "text-main-agent".into(),
            description: "A simple agent that responds to user queries.".into(),
            scope: AgentScope::Global,
            owner_topic_id: None,
            cloned_from_agent_id: None,
            active: Some(true),
            data: AgentDefinitionData::default(),
        })
        .await
        .unwrap();

    wc.storage()
        .agent()
        .create_binding(CreateAgentBinding {
            parent_topic_id: ctx.topic.id,
            agent_id: agent_def.id,
            role: AgentRole::Main,
            model_id: Some(ctx.model.id),
            chat_config_id: None,
            enabled: Some(true),
        })
        .await
        .unwrap();

    wc.storage()
        .topic()
        .create_chat_config(
            ctx.topic.id,
            ReqConfig {
                stream: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let engine = wc.fetch_topic(ctx.topic.id);
    let mut event_rx = engine.subscribe();
    let hdl = tokio::spawn(async move {
        loop {
            tokio::select! {
                result = event_rx.recv() => {
                    match result {
                        Ok(msg) => {
                            log::debug!("[event]\n{:?}", msg);
                            if let TopicEvent::TaskStatusChanged { status, .. } = &msg {
                                if matches!(
                                    status,
                                    AgentStatus::Finished
                                        | AgentStatus::Failed
                                        | AgentStatus::Cancelled|AgentStatus::WaitingApproval
                                ) {
                                    break;
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });
    engine.create_chat(user_input).await.unwrap();
    hdl.await.unwrap();
    let _ = engine.shutdown().await;
}

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

// ===========================================================================
// 文件 DB 持久化
// ===========================================================================
// #[tokio::test]
// #[ignore = "need to complete .env config file"]
// async fn test_data_persistence() {
//     let dir = std::env::temp_dir().join("windai_core_test");
//     std::fs::create_dir_all(&dir).unwrap();
//     let db_path = dir.join("persist.db");
//     let db_path_str = db_path.to_string_lossy().to_string();
//     let mut wc = WindCore::init_local(Some(&db_path_str)).await.unwrap();
//     let ctx = seed_chat_data(&wc, "persist").await;

//     let user_input = vec![Content::new_text(
//         "Hello! Reply in one short sentence.".into(),
//     )];

//     wc.storage()
//         .topic()
//         .create_chat_config(
//             ctx.topic.id,
//             ReqConfig {
//                 stream: Some(false),
//                 ..Default::default()
//             },
//         )
//         .await
//         .unwrap();

//     // 第一轮：创建、对话、关闭
//     {
//         let engine = wc.fetch_topic(ctx.topic.id);
//         let mut event_rx = engine.subscribe();
//         let hdl = tokio::spawn(async move {
//             loop {
//                 tokio::select! {
//                     Ok(msg) = event_rx.recv() => {
//                         log::debug!("[event]\n{:?}", msg);
//                     }
//                     else => {
//                         break;
//                     }
//                 }
//             }
//         });
//         engine.create_chat(user_input).await.unwrap();
//         hdl.await.unwrap();
//         let _ = engine.shutdown().await;
//     };

//     // 第二轮：重新打开，验证数据
//     let wc = WindCore::init_local(Some(&db_path_str)).await.unwrap();

//     let providers = wc.storage().provider().list_all().await.unwrap();
//     assert!(!providers.is_empty());

//     let topic = wc
//         .storage()
//         .topic()
//         .get_topic(ctx.topic.id)
//         .await
//         .unwrap()
//         .unwrap();
//     assert!(topic.label.contains("persist"));

//     let _ = std::fs::remove_dir_all(&dir);
// }
