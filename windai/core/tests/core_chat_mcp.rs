use futures::StreamExt;
use std::sync::OnceLock;
use wind_ai::message::{Content, Message as AiMessage, ReqConfig, Role};
use wind_core::WindCore;
use wind_core::chat::ChatEvent;
use wind_core::models::*;
use wind_mcp::client::ClientStatus;
use wind_mcp::client::registry::{Registry, RegistryHandle};

#[path = "./common/lib.rs"]
mod common;

use common::{
    McpTestEnv, create_everything_server_params, everything_params, init_test_core_with_registry,
    mcp_completion_env, mcp_responses_env,
};

// ---------------------------------------------------------------------------
// 全局 MCP registry — 在独立 runtime 中仅初始化一次
// ---------------------------------------------------------------------------

static MCP_REGISTRY: OnceLock<RegistryHandle> = OnceLock::new();

fn shared_mcp_registry() -> RegistryHandle {
    MCP_REGISTRY
        .get_or_init(|| {
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            std::thread::Builder::new()
                .name("mcp-test-registry".to_string())
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .thread_name("mcp-test-registry-worker")
                        .build()
                        .expect("failed to build MCP test runtime");
                    let registry = rt.block_on(async {
                        let registry = Registry::new();
                        let snapshot = registry
                            .acquire("mcp-global-session", everything_params())
                            .await
                            .expect("failed to acquire everything MCP server");
                        assert_eq!(
                            snapshot.status,
                            ClientStatus::Connected,
                            "MCP everything server must be connected"
                        );
                        registry
                    });
                    tx.send(registry)
                        .expect("failed to publish MCP test registry");
                    rt.block_on(std::future::pending::<()>());
                })
                .expect("failed to spawn MCP test registry thread");
            rx.recv().expect("MCP test registry thread stopped")
        })
        .clone()
}

async fn create_mcp_server_record(core: &WindCore) -> i64 {
    let id = match core
        .storage()
        .mcp()
        .get_by_name("everything")
        .await
        .unwrap()
    {
        Some(p) => p.id,
        None => core
            .storage()
            .mcp()
            .create(create_everything_server_params())
            .await
            .unwrap(),
    };
    id
}

// ---------------------------------------------------------------------------
// 测试上下文 & 数据播种
// ---------------------------------------------------------------------------

struct TestContext {
    _provider_id: i64,
    _model_id: i64,
    topic_id: i64,
    user_msg_id: i64,
    assistant_msg_id: i64,
}

async fn seed_mcp_data(
    core: &WindCore,
    env: &McpTestEnv,
    mcp_server_id: i64,
    label: &str,
    prompt: &str,
) -> TestContext {
    let provider_name = format!("mcp-{}", label);
    let provider_id = match core
        .storage()
        .provider()
        .get_by_name(&provider_name)
        .await
        .unwrap()
    {
        Some(p) => p.id,
        None => core
            .storage()
            .provider()
            .create(CreateProvider {
                name: provider_name,
                description: None,
                base_url: env.base_url.clone(),
                doc: None,
                alias: None,
            })
            .await
            .unwrap(),
    };

    core.storage()
        .provider()
        .create_credentials(CreateCredentials {
            provider_id,
            key: env.key.clone(),
        })
        .await
        .unwrap();

    let model_id = core
        .storage()
        .model()
        .create(CreateModel {
            name: env.model.clone(),
            provider_id,
            alias: None,
            adaptor: env.adaptor,
            modalities: Some(vec![ModelType::Chat]),
            active: Some(true),
            icon: None,
            endpoint: env.endpoint.clone(),
        })
        .await
        .unwrap();

    let topic_id = core
        .storage()
        .topic()
        .create(CreateTopic {
            parent_id: None,
            chat_config_id: 0,
            label: format!("test-mcp-{}", label),
            icon: None,
            max_context: Some(100),
            mcp_server_ids: Some(vec![mcp_server_id]),
        })
        .await
        .unwrap();

    let user_msg_id = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: None,
            stream: false,
            content: vec![AiMessage::new_simple(
                Role::User,
                vec![Content::new_text(prompt.into())],
                None,
            )],
            model_id,
            topic_id,
            is_boundary: false,
            input_tokens: 10,
            output_tokens: 0,
            tools_allowed: None,
            tools_denied: None,
        })
        .await
        .unwrap();

    let assistant_msg_id = core
        .storage()
        .message()
        .create(CreateMessage {
            from_id: Some(user_msg_id),
            stream: false,
            content: vec![],
            model_id,
            topic_id,
            is_boundary: false,
            input_tokens: 0,
            output_tokens: 0,
            tools_allowed: None,
            tools_denied: None,
        })
        .await
        .unwrap();

    TestContext {
        _provider_id: provider_id,
        _model_id: model_id,
        topic_id,
        user_msg_id,
        assistant_msg_id,
    }
}

// ---------------------------------------------------------------------------
// 核心测试函数
// ---------------------------------------------------------------------------

/// MCP 工具拒绝 — 模型调用 echo 工具，用户拒绝执行
async fn test_mcp_tool_reject(env: &McpTestEnv) {
    let core = init_test_core_with_registry(shared_mcp_registry()).await;
    let mcp_server_id = create_mcp_server_record(&core).await;
    let ctx = seed_mcp_data(
        &core,
        env,
        mcp_server_id,
        "reject",
        "Use the echo tool to echo: APPROVE_TEST;calculate 10000 plus 90000",
    )
    .await;

    core.storage()
        .topic()
        .update(
            ctx.topic_id,
            UpdateTopic {
                tool_approval_policy: Some(ToolApprovalPolicy::Manual),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    core.storage()
        .topic()
        .create_chat_config(
            ctx.topic_id,
            ReqConfig {
                stream: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut seen_await = false;
    let mut captured_tool_ids: Vec<String> = vec![];
    let mut seen_finish = false;
    let mut finish_error: Option<String> = None;
    let mut current_message_id = ctx.assistant_msg_id;
    let mut round = 0u32;
    let max_rounds = 5u32;

    loop {
        round += 1;
        if round > max_rounds {
            eprintln!(
                "[reject] max_rounds ({max_rounds}) reached, seen_await={seen_await}, seen_finish={seen_finish}, finish_error={finish_error:?}"
            );
            break;
        }
        let engine = core.chat();
        let mut stream = Box::pin(engine.start(ctx.topic_id, ctx.user_msg_id, current_message_id));
        let mut saw_await_this_round = false;

        while let Some(event) = stream.next().await {
            match event {
                ChatEvent::AwaitToolCall { message_id, tools } => {
                    saw_await_this_round = true;
                    captured_tool_ids = tools.iter().map(|t| t.id.clone()).collect();
                    if !seen_await {
                        seen_await = true;
                        eprintln!(
                            "[reject] round {round}: {} tools captured",
                            captured_tool_ids.len()
                        );
                    }
                    current_message_id = message_id;
                }
                ChatEvent::Finish { message, error, .. } => {
                    finish_error = error.clone();
                    if error.is_none() {
                        seen_finish = true;
                    } else {
                        eprintln!("[reject] round {round}: Finish with error: {error:?}");
                    }
                    if let Some(ref msgs) = message {
                        let has_denied = msgs.iter().any(|m| {
                            m.content.iter().any(|c| {
                                if let Content::FunctionCall { data } = c {
                                    data.content.get("error").and_then(|v| v.as_str())
                                        == Some("User denied this tool call")
                                } else {
                                    false
                                }
                            })
                        });
                        // 拒绝路径下可能出现拒绝标记
                        _ = has_denied;
                    }
                    break;
                }
                _ => {}
            }
        }

        if saw_await_this_round {
            core.storage()
                .message()
                .update(
                    current_message_id,
                    UpdateMessage {
                        tools_denied: Some(captured_tool_ids.clone()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            continue;
        }
        break;
    }

    if !captured_tool_ids.is_empty() {
        assert!(seen_await, "Should emit AwaitToolCall");
        assert!(
            seen_finish,
            "Should emit Finish after rejection (finish_error={finish_error:?}, round={round})"
        );

        let msg = core
            .storage()
            .message()
            .get(ctx.assistant_msg_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!msg.content.is_empty());
        assert!(msg.content.iter().any(|m| m.is_tool_request()));
        assert!(msg.content.iter().any(|m| m.is_tool_result()));
        assert!(msg.tools_allowed.unwrap_or_default().is_empty());
        assert!(msg.tools_denied.unwrap_or_default().is_empty());
    }
}

/// MCP 工具批准 — 模型调用 echo 工具，用户批准执行
async fn test_mcp_tool_approve(env: &McpTestEnv) {
    let core = init_test_core_with_registry(shared_mcp_registry()).await;
    let mcp_server_id = create_mcp_server_record(&core).await;
    let ctx: TestContext = seed_mcp_data(
        &core,
        env,
        mcp_server_id,
        "approve",
        "Use the echo tool to echo: APPROVE_TEST;calculate 10000 plus 90000",
    )
    .await;

    core.storage()
        .topic()
        .update(
            ctx.topic_id,
            UpdateTopic {
                tool_approval_policy: Some(ToolApprovalPolicy::Manual),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    core.storage()
        .topic()
        .create_chat_config(
            ctx.topic_id,
            ReqConfig {
                stream: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut seen_await = false;
    let mut captured_tool_ids: Vec<String> = vec![];
    let mut seen_finish = false;
    let mut finish_error: Option<String> = None;
    let mut current_message_id = ctx.assistant_msg_id;
    let mut round = 0u32;
    let max_rounds = 5u32;

    loop {
        round += 1;
        if round > max_rounds {
            eprintln!(
                "[approve] max_rounds ({max_rounds}) reached, seen_await={seen_await}, seen_finish={seen_finish}, finish_error={finish_error:?}"
            );
            break;
        }
        let engine = core.chat();
        let mut stream = Box::pin(engine.start(ctx.topic_id, ctx.user_msg_id, current_message_id));
        let mut saw_await_this_round = false;
        let mut round_tool_ids: Vec<String> = vec![];

        while let Some(event) = stream.next().await {
            match event {
                ChatEvent::AwaitToolCall { message_id, tools } => {
                    saw_await_this_round = true;
                    if captured_tool_ids.is_empty() {
                        seen_await = true;
                        captured_tool_ids = tools.iter().map(|t| t.id.clone()).collect();
                        eprintln!(
                            "[approve] round {round}: {} tools captured, ids={:?}",
                            captured_tool_ids.len(),
                            captured_tool_ids
                        );
                    } else {
                        round_tool_ids = tools.iter().map(|t| t.id.clone()).collect();
                        eprintln!(
                            "[approve] round {round}: {} additional tools, ids={:?}",
                            round_tool_ids.len(),
                            round_tool_ids
                        );
                    }
                    current_message_id = message_id;
                }
                ChatEvent::Partial { delta, .. } => {
                    let has_denied = delta.content.iter().any(|c| {
                        if let Content::FunctionCall { data } = c {
                            data.content.get("error").and_then(|v| v.as_str())
                                == Some("User denied this tool call")
                        } else {
                            false
                        }
                    });
                    assert!(
                        !has_denied,
                        "Approved tools must not contain denial markers"
                    );
                }
                ChatEvent::Finish { message, error, .. } => {
                    finish_error = error.clone();
                    if error.is_none() {
                        seen_finish = true;
                        eprintln!("[approve] round {round}: Finish (ok)");
                    } else {
                        eprintln!("[approve] round {round}: Finish with error: {error:?}");
                    }
                    if let Some(ref msgs) = message {
                        let has_denied = msgs.iter().any(|m| {
                            m.content.iter().any(|c| {
                                if let Content::FunctionCall { data } = c {
                                    data.content.get("error").and_then(|v| v.as_str())
                                        == Some("User denied this tool call")
                                } else {
                                    false
                                }
                            })
                        });
                        assert!(!has_denied, "Approved path must not show denial markers");
                    }
                    break;
                }
                _ => {}
            }
        }

        if saw_await_this_round {
            let ids = if round_tool_ids.is_empty() {
                &captured_tool_ids
            } else {
                &round_tool_ids
            };
            eprintln!("[approve] round {round}: approving {} tool ids", ids.len());
            core.storage()
                .message()
                .update(
                    current_message_id,
                    UpdateMessage {
                        tools_allowed: Some(ids.clone()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            continue;
        }
        break;
    }

    if !captured_tool_ids.is_empty() {
        assert!(seen_await, "Should emit AwaitToolCall");
        assert!(
            seen_finish,
            "Should emit Finish after approval (finish_error={finish_error:?}, round={round})"
        );

        let msg = core
            .storage()
            .message()
            .get(ctx.assistant_msg_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!msg.content.is_empty());
        assert!(msg.content.iter().any(|m| m.is_tool_request()));
        assert!(msg.content.iter().any(|m| m.is_tool_result()));
        assert!(msg.tools_allowed.unwrap_or_default().is_empty());
        assert!(msg.tools_denied.unwrap_or_default().is_empty());
    }
}

/// MCP 工具审批 — 未审批直接 resume 应失败，而不是隐式拒绝
async fn test_mcp_tool_requires_explicit_review(env: &McpTestEnv) {
    let core = init_test_core_with_registry(shared_mcp_registry()).await;
    let mcp_server_id = create_mcp_server_record(&core).await;
    let ctx = seed_mcp_data(
        &core,
        env,
        mcp_server_id,
        "requires-review",
        "Use the echo tool to echo exactly: REQUIRES_REVIEW_TEST. Do not answer without calling the tool.",
    )
    .await;

    core.storage()
        .topic()
        .update(
            ctx.topic_id,
            UpdateTopic {
                tool_approval_policy: Some(ToolApprovalPolicy::Manual),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    core.storage()
        .topic()
        .create_chat_config(
            ctx.topic_id,
            ReqConfig {
                stream: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut current_message_id = ctx.assistant_msg_id;
    let mut seen_await = false;
    let engine = core.chat();
    let mut stream = Box::pin(engine.start(ctx.topic_id, ctx.user_msg_id, current_message_id));
    while let Some(event) = stream.next().await {
        if let ChatEvent::AwaitToolCall { message_id, tools } = event {
            seen_await = true;
            assert!(!tools.is_empty());
            current_message_id = message_id;
            break;
        } else if let ChatEvent::Finish { error, .. } = event {
            log::debug!("finish with error: {error:?}");
        }
    }
    drop(stream);

    assert!(seen_await, "Should emit AwaitToolCall before resume");

    let mut stream = Box::pin(engine.start(ctx.topic_id, ctx.user_msg_id, current_message_id));
    let mut finish_error: Option<String> = None;
    while let Some(event) = stream.next().await {
        if let ChatEvent::Finish { error, .. } = event {
            finish_error = error;
            break;
        }
    }

    let error = finish_error.expect("resume without approval should finish with error");
    assert!(
        error.contains("approval"),
        "error should mention approval, got: {error}"
    );
}

// ---------------------------------------------------------------------------
// 提供商变体入口
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "need to complete .env config file"]
async fn test_mcp_tool_reject_completion() {
    let env = mcp_completion_env();
    test_mcp_tool_reject(&env).await;
}

#[tokio::test]
#[ignore = "need to complete .env config file"]
async fn test_mcp_tool_reject_responses() {
    let env = mcp_responses_env();
    test_mcp_tool_reject(&env).await;
}

#[tokio::test]
#[ignore = "need to complete .env config file"]
async fn test_mcp_tool_approve_completion() {
    let env = mcp_completion_env();
    test_mcp_tool_approve(&env).await;
}

#[tokio::test]
#[ignore = "need to complete .env config file"]
async fn test_mcp_tool_approve_responses() {
    let env = mcp_responses_env();
    test_mcp_tool_approve(&env).await;
}

#[tokio::test]
#[ignore = "need to complete .env config file"]
async fn test_mcp_tool_requires_explicit_review_completion() {
    let env = mcp_completion_env();
    test_mcp_tool_requires_explicit_review(&env).await;
}

#[tokio::test]
#[ignore = "need to complete .env config file"]
async fn test_mcp_tool_requires_explicit_review_responses() {
    let env = mcp_responses_env();
    test_mcp_tool_requires_explicit_review(&env).await;
}
