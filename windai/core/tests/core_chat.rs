use futures::StreamExt;
use wind_ai::message::{Content, Message as AiMessage, ReqConfig, Role};
use wind_core::WindCore;
use wind_core::chat::ChatEvent;
use wind_core::models::*;

#[path = "./common/lib.rs"]
mod common;

struct TestContext {
    provider_id: i64,
    model_id: i64,
    topic_id: i64,
    user_msg_id: i64,
    assistant_msg_id: i64,
}

/// 在 WindCore 中插入完整的测试数据：provider + credential + model + topic + messages。
async fn seed_data(wc: &WindCore, env: &common::Env) -> TestContext {
    let provider_name = "test-provider";
    let provider = match wc.provider().get_by_name(provider_name).await.unwrap() {
        Some(p) => p,
        None => wc
            .provider()
            .create(CreateProvider {
                name: provider_name.into(),
                description: None,
                base_url: env.test_base_url.clone(),
                doc: None,
                alias: None,
                active: Some(true),
            })
            .await
            .unwrap(),
    };

    wc.provider()
        .create_credentials(CreateCredentials {
            provider_id: provider.id,
            key: env.test_key.clone(),
        })
        .await
        .unwrap();

    let model = wc
        .model()
        .create(CreateModel {
            name: env.test_model.clone(),
            provider_id: provider.id,
            alias: None,
            adaptor: env.test_adaptor,
            modalities: Some(vec![ModelType::Chat]),
            active: Some(true),
            icon: None,
            endpoint: env.test_endpoint.clone(),
        })
        .await
        .unwrap();

    let topic = wc
        .topic()
        .create_topic(CreateTopic {
            parent_id: None,
            chat_config_id: 0,
            label: "test-core-chat".into(),
            icon: None,
            max_context: Some(100),
        })
        .await
        .unwrap();

    let user_msg = wc
        .message()
        .create(CreateMessage {
            from_id: None,
            stream: false,
            content_json: serde_json::to_string(&vec![AiMessage::new_simple(
                Role::User,
                vec![Content::new_text(
                    "Hello! Reply in one short sentence.".into(),
                )],
                None,
            )])
            .unwrap(),
            model_id: model.id,
            topic_id: topic.id,
            is_boundary: false,
            is_excluded: false,
            input_tokens: 0,
            output_tokens: 0,
        })
        .await
        .unwrap();

    let assistant_msg = wc
        .message()
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
        provider_id: provider.id,
        model_id: model.id,
        topic_id: topic.id,
        user_msg_id: user_msg.id,
        assistant_msg_id: assistant_msg.id,
    }
}

fn temp_db_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("windai_core_test");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------------------------------------------------------------------------
// non-stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_core_chat_non_stream() {
    let env = common::load_env();

    let wc = WindCore::init_memory().await.unwrap();
    let ctx = seed_data(&wc, &env).await;

    wc.topic()
        .create_chat_config(
            ctx.topic_id,
            ReqConfig {
                stream: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let engine = wc.chat();
    let mut stream = Box::pin(engine.send(
        ctx.topic_id,
        ctx.model_id,
        ctx.user_msg_id,
        ctx.assistant_msg_id,
    ));

    let mut seen_created = false;
    let mut seen_finish = false;
    while let Some(event) = stream.next().await {
        match event {
            ChatEvent::Created { message_id } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
                seen_created = true;
            }
            ChatEvent::Partial { message_id, .. } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
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
    assert!(seen_created);
    assert!(seen_finish);

    // 验证消息落库
    let msg = wc
        .message()
        .get(ctx.assistant_msg_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !msg.content.is_empty(),
        "Assistant message should be persisted"
    );
}

// ---------------------------------------------------------------------------
// stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_core_chat_stream() {
    let env = common::load_env();

    let wc = WindCore::init_memory().await.unwrap();
    let ctx = seed_data(&wc, &env).await;

    wc.topic()
        .create_chat_config(
            ctx.topic_id,
            ReqConfig {
                stream: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let engine = wc.chat();
    let mut stream = Box::pin(engine.send(
        ctx.topic_id,
        ctx.model_id,
        ctx.user_msg_id,
        ctx.assistant_msg_id,
    ));

    let mut partial_count = 0;
    let mut seen_created = false;
    let mut seen_finish = false;
    while let Some(event) = stream.next().await {
        match event {
            ChatEvent::Created { message_id } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
                seen_created = true;
            }
            ChatEvent::Partial { message_id, .. } => {
                assert_eq!(message_id, ctx.assistant_msg_id);
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
    assert!(seen_created);
    assert!(seen_finish);
    assert!(partial_count > 0, "Stream should produce partial chunks");

    let msg = wc
        .message()
        .get(ctx.assistant_msg_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !msg.content.is_empty(),
        "Persisted content should not be empty"
    );
}

// ---------------------------------------------------------------------------
// JS Hook: DeepSeek reasoning → thinking type conversion
//
// DeepSeek 要求 reasoning 字段转换为 {"thinking": {"type": "enabled"}}
// 或 {"thinking": {"type": "disabled"}}。
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_js_hook_deepseek_reasoning_to_thinking() {
    let env = common::load_env();

    let wc = WindCore::init_memory().await.unwrap();
    let ctx = seed_data(&wc, &env).await;

    // 插入 JS hook：将 reasoning 转为 DeepSeek thinking 参数
    let hook_code = r#"
function transform(body, context) {
    let type = (!body.reasoning_effort || body.reasoning_effort === "none") ? "disabled" : "enabled";
    body.thinking = { type };
    delete body.reasoning_effort;
    return body;
}
"#;
    wc.provider()
        .create_js_hook_code(CreateJsHookCode {
            provider_id: ctx.provider_id,
            adaptor: env.test_adaptor,
            js_code: hook_code.into(),
            active: true,
        })
        .await
        .unwrap();

    // 验证 hook 已存储
    let hooks = wc
        .provider()
        .list_js_hook_codes(ctx.provider_id)
        .await
        .unwrap();
    assert_eq!(hooks.len(), 1);
    assert!(hooks[0].active);
    assert_eq!(hooks[0].adaptor, env.test_adaptor);

    // 启用 reasoning
    wc.topic()
        .create_chat_config(
            ctx.topic_id,
            ReqConfig {
                stream: Some(false),
                reasoning: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let engine = wc.chat();
    let mut stream = Box::pin(engine.send(
        ctx.topic_id,
        ctx.model_id,
        ctx.user_msg_id,
        ctx.assistant_msg_id,
    ));

    let mut seen_finish = false;
    while let Some(event) = stream.next().await {
        if let ChatEvent::Finish { error, message, .. } = event {
            assert!(error.is_none(), "Chat error with JS hook: {:?}", error);
            assert!(message.is_some(), "Should have response with hook applied");
            // 存在推理消息
            assert!(
                message
                    .unwrap()
                    .iter()
                    .any(|m| m.reasoning_content.is_some())
            );
            seen_finish = true;
        }
    }
    assert!(seen_finish);

    // 验证消息落库
    let msg = wc
        .message()
        .get(ctx.assistant_msg_id)
        .await
        .unwrap()
        .unwrap();
    assert!(!msg.content.is_empty());
}

// ---------------------------------------------------------------------------
// JS Hook: reasoning=false → thinking.type = "disabled"
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_js_hook_disabled_reasoning() {
    let env = common::load_env();

    let wc = WindCore::init_memory().await.unwrap();
    let ctx = seed_data(&wc, &env).await;

    let hook_code = r#"
function transform(body, context) {
    let type = (!body.reasoning_effort || body.reasoning_effort === "none") ? "disabled" : "enabled";
    body.thinking = { type };
    delete body.reasoning_effort;
    return body;
}
"#;
    wc.provider()
        .create_js_hook_code(CreateJsHookCode {
            provider_id: ctx.provider_id,
            adaptor: env.test_adaptor,
            js_code: hook_code.into(),
            active: true,
        })
        .await
        .unwrap();

    // reasoning = false → 不发送 reasoning 字段 → hook 应不触发 thinking
    wc.topic()
        .create_chat_config(
            ctx.topic_id,
            ReqConfig {
                stream: Some(false),
                reasoning: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let engine = wc.chat();
    let mut stream = Box::pin(engine.send(
        ctx.topic_id,
        ctx.model_id,
        ctx.user_msg_id,
        ctx.assistant_msg_id,
    ));

    let mut seen_finish = false;
    while let Some(event) = stream.next().await {
        if let ChatEvent::Finish { error, message, .. } = event {
            assert!(error.is_none(), "Chat error: {:?}", error);
            assert!(message.is_some());
            // 不存在推理消息
            assert!(
                !message
                    .unwrap()
                    .iter()
                    .any(|m| m.reasoning_content.is_some())
            );
            seen_finish = true;
        }
    }
    assert!(seen_finish);
}

// ---------------------------------------------------------------------------
// file DB 持久化：关闭后重新打开，数据仍在
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_data_persistence_file_db() {
    let env = common::load_env();

    let dir = temp_db_dir();
    let db_path = dir.join("persist.db");
    let db_path_str = db_path.to_string_lossy().to_string();

    // 第一轮：初始化，写入数据，跑一次对话
    let (provider_id, topic_id, model_id, aid) = {
        let wc = WindCore::init_local(Some(&db_path_str)).await.unwrap();
        let ctx = seed_data(&wc, &env).await;

        wc.topic()
            .create_chat_config(
                ctx.topic_id,
                ReqConfig {
                    stream: Some(false),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let engine = wc.chat();
        let mut stream = Box::pin(engine.send(
            ctx.topic_id,
            ctx.model_id,
            ctx.user_msg_id,
            ctx.assistant_msg_id,
        ));

        let mut seen_finish = false;
        while let Some(event) = stream.next().await {
            if let ChatEvent::Finish { error, .. } = event {
                assert!(error.is_none(), "Chat error: {:?}", error);
                seen_finish = true;
            }
        }
        assert!(seen_finish);

        let msg = wc
            .message()
            .get(ctx.assistant_msg_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!msg.content.is_empty());

        wc.shutdown().await;
        let provider_id = ctx.provider_id;
        let topic_id = ctx.topic_id;
        let model_id = ctx.model_id;
        let aid = ctx.assistant_msg_id;
        (provider_id, topic_id, model_id, aid)
    }; // WindCore 在此处 drop

    // 第二轮：从同一个文件重新打开，验证历史数据
    let wc = WindCore::init_local(Some(&db_path_str)).await.unwrap();

    // Provider 仍在
    let providers = wc.provider().list().await.unwrap();
    assert!(!providers.is_empty());
    assert_eq!(providers[0].name, "test-provider");

    // Model 仍在
    let models = wc.model().list_by_provider(provider_id).await.unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].name, env.test_model);

    // Topic 仍在
    let topic = wc.topic().get_topic(topic_id).await.unwrap().unwrap();
    assert_eq!(topic.label, "test-core-chat");

    // 消息仍在且内容非空
    let msg = wc.message().get(aid).await.unwrap().unwrap();
    assert!(
        !msg.content.is_empty(),
        "Message content persisted after reopen"
    );

    // 第二轮对话在新打开的 DB 上仍可正常工作
    let user2 = wc
        .message()
        .create(CreateMessage {
            from_id: None,
            stream: false,
            content_json: serde_json::to_string(&vec![AiMessage::new_simple(
                Role::User,
                vec![Content::new_text("Tell me more.".into())],
                None,
            )])
            .unwrap(),
            model_id,
            topic_id,
            is_boundary: false,
            is_excluded: false,
            input_tokens: 0,
            output_tokens: 0,
        })
        .await
        .unwrap();

    let assistant_msg2 = wc
        .message()
        .create(CreateMessage {
            from_id: Some(user2.id),
            stream: false,
            content_json: "[]".into(),
            model_id,
            topic_id,
            is_boundary: false,
            is_excluded: false,
            input_tokens: 0,
            output_tokens: 0,
        })
        .await
        .unwrap();

    let engine = wc.chat();
    let mut stream = Box::pin(engine.send(topic_id, model_id, user2.id, assistant_msg2.id));

    let mut seen_finish = false;
    while let Some(event) = stream.next().await {
        if let ChatEvent::Finish { error, .. } = event {
            assert!(error.is_none(), "Chat error on reopened DB: {:?}", error);
            seen_finish = true;
        }
    }
    assert!(seen_finish);

    wc.shutdown().await;

    // 清理
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 数据校验：缺少 provider 时的错误
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_chat_missing_provider() {
    let _env = common::load_env();

    let wc = WindCore::init_memory().await.unwrap();

    // 直接发送，没有任何数据 → 应返回错误
    let engine = wc.chat();
    let mut stream = Box::pin(engine.send(999, 999, 999, 999));

    let mut error_seen = false;
    while let Some(event) = stream.next().await {
        if let ChatEvent::Finish { error, .. } = event {
            assert!(error.is_some(), "Should have error for missing data");
            error_seen = true;
        }
    }
    assert!(error_seen);
}

// ---------------------------------------------------------------------------
// 数据校验：缺少 credentials 时的错误
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_chat_missing_credentials() {
    let env = common::load_env();

    let wc = WindCore::init_memory().await.unwrap();

    let provider = wc
        .provider()
        .create(CreateProvider {
            name: "no-creds-provider".into(),
            description: None,
            base_url: env.test_base_url.clone(),
            doc: None,
            alias: None,
            active: Some(true),
        })
        .await
        .unwrap();

    let model = wc
        .model()
        .create(CreateModel {
            name: env.test_model.clone(),
            provider_id: provider.id,
            alias: None,
            adaptor: env.test_adaptor,
            modalities: Some(vec![ModelType::Chat]),
            active: Some(true),
            icon: None,
            endpoint: env.test_endpoint.clone(),
        })
        .await
        .unwrap();

    let topic = wc
        .topic()
        .create_topic(CreateTopic {
            parent_id: None,
            chat_config_id: 0,
            label: "no-creds".into(),
            icon: None,
            max_context: Some(100),
        })
        .await
        .unwrap();

    let user_msg = wc
        .message()
        .create(CreateMessage {
            from_id: None,
            stream: false,
            content_json: serde_json::to_string(&vec![AiMessage::new_simple(
                Role::User,
                vec![Content::new_text("Hello".into())],
                None,
            )])
            .unwrap(),
            model_id: model.id,
            topic_id: topic.id,
            is_boundary: false,
            is_excluded: false,
            input_tokens: 0,
            output_tokens: 0,
        })
        .await
        .unwrap();

    let assistant_msg = wc
        .message()
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

    let engine = wc.chat();
    let mut stream = Box::pin(engine.send(topic.id, model.id, user_msg.id, assistant_msg.id));

    let mut error_seen = false;
    while let Some(event) = stream.next().await {
        if let ChatEvent::Finish { error, .. } = event {
            assert!(error.is_some(), "Should error for missing credentials");
            error_seen = true;
        }
    }
    assert!(error_seen);
}

// ---------------------------------------------------------------------------
// 数据校验：re-chat 后消息持久化正确（历史消息 id 可追溯）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_message_history_chain() {
    let env = common::load_env();

    let wc = WindCore::init_memory().await.unwrap();
    let ctx = seed_data(&wc, &env).await;

    wc.topic()
        .create_chat_config(
            ctx.topic_id,
            ReqConfig {
                stream: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // 第一轮对话
    let (user2_id, assistant2_id) = {
        let engine = wc.chat();
        let mut stream = Box::pin(engine.send(
            ctx.topic_id,
            ctx.model_id,
            ctx.user_msg_id,
            ctx.assistant_msg_id,
        ));

        let mut seen_finish = false;
        while let Some(event) = stream.next().await {
            if let ChatEvent::Finish { error, .. } = event {
                assert!(error.is_none(), "Chat error: {:?}", error);
                seen_finish = true;
            }
        }
        assert!(seen_finish);

        let assistant_msg = wc
            .message()
            .get(ctx.assistant_msg_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!assistant_msg.content.is_empty());

        // 第二轮：用户的追问消息（user 消息的 from_id 为 None）
        let user2 = wc
            .message()
            .create(CreateMessage {
                from_id: None,
                stream: false,
                content_json: serde_json::to_string(&vec![AiMessage::new_simple(
                    Role::User,
                    vec![Content::new_text("What did I just ask you?".into())],
                    None,
                )])
                .unwrap(),
                model_id: ctx.model_id,
                topic_id: ctx.topic_id,
                is_boundary: false,
                is_excluded: false,
                input_tokens: 0,
                output_tokens: 0,
            })
            .await
            .unwrap();

        // 空占位 assistant 消息，from_id 指向 user2
        let assistant2 = wc
            .message()
            .create(CreateMessage {
                from_id: Some(user2.id),
                stream: false,
                content_json: "[]".into(),
                model_id: ctx.model_id,
                topic_id: ctx.topic_id,
                is_boundary: false,
                is_excluded: false,
                input_tokens: 0,
                output_tokens: 0,
            })
            .await
            .unwrap();

        (user2.id, assistant2.id)
    };

    // 第二轮对话：from_message_id = user2.id, message_id = assistant2.id
    let engine = wc.chat();
    let mut stream = Box::pin(engine.send(ctx.topic_id, ctx.model_id, user2_id, assistant2_id));

    let mut seen_finish = false;
    while let Some(event) = stream.next().await {
        if let ChatEvent::Finish { error, .. } = event {
            assert!(error.is_none(), "Second round error: {:?}", error);
            seen_finish = true;
        }
    }
    assert!(seen_finish);

    // 验证历史链：user_msg → assistant_msg → user2 → assistant2
    let all_msgs = wc.message().list_by_topic(ctx.topic_id).await.unwrap();
    assert!(
        all_msgs.len() >= 4,
        "Should have at least 4 messages in chain"
    );

    let persisted = wc.message().get(assistant2_id).await.unwrap().unwrap();
    assert!(
        !persisted.content.is_empty(),
        "Second response should be persisted"
    );
}
