use std::sync::OnceLock;
use wind_ai::message::Content;
use wind_core::WindCore;
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

fn test_agent_group1() -> Vec<CreateAgentDefinition> {
    vec![
        CreateAgentDefinition {
            name: "test-main-agent".into(),
            key: "test-main-agent".into(),
            description: "专业的项目/产品经理，善于将用户需求拆分并将任务分配给团队".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: Some(true),
            data: AgentDefinitionData::default(),
        },
        CreateAgentDefinition {
            name: "test-frontend-agent".into(),
            key: "test-frontend-agent".into(),
            description:
                "一个专业的前端vue/react开发工程师,擅长前端开发和架构设计，以及各种疑难杂症解决"
                    .into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: Some(true),
            data: AgentDefinitionData::default(),
        },
        CreateAgentDefinition {
            name: "test-law-agent".into(),
            key: "test-law-agent".into(),
            description: "专业的计算机领域的律师，善于分析并规避项目中法律有关的问题".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: Some(true),
            data: AgentDefinitionData::default(),
        },
        CreateAgentDefinition {
            name: "test-backend-agent".into(),
            key: "test-backend-agent".into(),
            description:
                "一个专业的rust后端开发工程师,擅长后端开发和架构设计，以及解决各种疑难杂症".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: Some(true),
            data: AgentDefinitionData::default(),
        },
    ]
}

#[allow(dead_code)]
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
        .expect("get provider by name")
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
            .expect("create provider"),
    };

    core.storage()
        .provider()
        .create_credentials(CreateCredentials {
            provider_id: provider.id,
            key: env.test_key.clone(),
        })
        .await
        .expect("create credentials");

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
            config: Some(ModelConfig {
                stream: Some(false),
                reasoning: None,
            }),
        })
        .await
        .expect("create model");

    let topic = core
        .storage()
        .topic()
        .create(CreateTopic {
            parent_id: None,
            label: format!("test-chat-{}", label),
            icon: None,
            model_id: Some(model.id),
            tool_approval_policy: Some(ToolApprovalPolicy::AllowAll),
        })
        .await
        .expect("create topic");

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
    let wc = test_core().await;
    let ctx = seed_chat_data(&wc, "agent-chat").await;
    let user_input = vec![Content::new_text(
        "我要设计一个基于Rust的IM实时聊天项目，并且利用网络开源工具，以及UI设计元素和网络LOGO;利用你的专业团队，为我推荐后端和前端框架和技术选型以及要规避的法律风险".into(),
    )];

    let mut agents = vec![];
    for a in test_agent_group1() {
        agents.push(
            wc.storage()
                .agent()
                .create_definition(a)
                .await
                .expect("create definition"),
        );
    }

    // 全部 agent 进入 topic 能力映射
    for agent in agents.iter() {
        wc.storage()
            .agent()
            .create_topic_agent_map(CreateTopicAgentMap {
                topic_id: ctx.topic.id,
                agent_id: agent.id,
            })
            .await
            .expect("create agent map");
    }

    let defs = wc
        .storage()
        .agent()
        .list_sub_definitions_by_topic(ctx.topic.id)
        .await
        .expect("list sub definitions");
    // 不过滤任何 agent：主 Agent 偏好与禁用项同样返回
    assert_eq!(defs.len(), agents.len());

    let engine = wc.fetch_topic(ctx.topic.id);
    let mut event_rx = engine.subscribe().await.expect("subscribe topic events");
    let hdl = tokio::spawn(async move {
        loop {
            match event_rx.recv().await {
                Ok(msg) => log::debug!("[event]\n{:?}", msg),
                _ => break,
            }
        }
    });
    engine.create_task(user_input).await.expect("create task");
    hdl.await.expect("event logger task");
    let _ = engine.shutdown().await;
}
