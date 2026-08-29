mod common;

use wind_ai::message::ReqConfig;
use wind_ai::model::AdapterType;
use wind_core::models::agent::{AgentDefinitionData, AgentRole, AgentScope};
use wind_core::models::{
    CreateAgentBinding, CreateAgentDefinition, CreateCredentials, CreateMcpServer, CreateModel,
    CreatePromptModule, CreateProvider, CreateTopic,
};
use wind_http::facade::storage::agent::AgentStorageFacade;
use wind_http::facade::storage::approval::ToolApprovalFacade;
use wind_http::facade::storage::mcp::McpStorageFacade;
use wind_http::facade::storage::model::ModelStorageFacade;
use wind_http::facade::storage::prompt::PromptStorageFacade;
use wind_http::facade::storage::provider::ProviderStorageFacade;
use wind_http::facade::system::SystemFacade;
use wind_http::facade::topic::TopicFacade;
use wind_mcp::client::TransportType;

#[tokio::test]
async fn health_reports_ok() {
    let core = common::test_core().await;
    let facade = SystemFacade::new(core, 1234567890);
    let r = facade.health();
    assert_eq!(r.code, 200);
    let data = r.data.unwrap();
    assert_eq!(data["status"], "ok");
}

#[tokio::test]
async fn create_topic_roundtrips() {
    let core = common::test_core().await;
    let facade = TopicFacade::new(core);
    let created = facade
        .create_topic(CreateTopic {
            parent_id: None,
            binding_id: None,
            label: "hello".into(),
            icon: None,
        })
        .await;
    assert_eq!(created.code, 200);
    let topic = created.data.unwrap();
    assert_eq!(topic.label, "hello");

    let got = facade.get_topic(topic.id).await;
    assert_eq!(got.code, 200);
    assert_eq!(got.data.unwrap().label, "hello");

    let missing = facade.get_topic(999_999).await;
    assert_eq!(missing.code, 404);
}

#[tokio::test]
async fn delete_topic_missing_returns_404() {
    let core = common::test_core().await;
    let facade = TopicFacade::new(core);
    let r = facade.delete_topic(999_999).await;
    assert_eq!(r.code, 404);
    assert!(r.data.is_none());
}

#[tokio::test]
async fn cancel_task_missing_topic_returns_404() {
    let core = common::test_core().await;
    let facade = TopicFacade::new(core);
    let r = facade.cancel_task(999_999, 1).await;
    assert_eq!(r.code, 404);
}

#[tokio::test]
async fn create_chat_config_missing_binding_returns_404_without_insert() {
    let (core, pool) = common::test_core_with_pool().await;
    let facade = TopicFacade::new(core);
    let r = facade
        .create_chat_config(999_999, ReqConfig::default())
        .await;
    assert_eq!(r.code, 404);

    // 预检查应阻止插入，chat_configs 无孤儿行。
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chat_configs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn provider_crud_roundtrips() {
    let core = common::test_core().await;
    let f = ProviderStorageFacade::new(core);
    let created = f
        .create_provider(CreateProvider {
            name: "openai".into(),
            description: None,
            base_url: "https://x".into(),
            doc: None,
            alias: None,
        })
        .await;
    assert_eq!(created.code, 200);
    let id = created.data.unwrap().id;

    let got = f.get_provider(id).await;
    assert_eq!(got.code, 200);
    assert_eq!(got.data.unwrap().name, "openai");

    let cred = f
        .create_credentials(CreateCredentials {
            provider_id: id,
            key: "sk-secret".into(),
        })
        .await;
    assert_eq!(cred.code, 200);
    assert_eq!(cred.data.unwrap().key, "sk-secret");
}

#[tokio::test]
async fn model_crud_roundtrips() {
    let core = common::test_core().await;
    let f = ModelStorageFacade::new(core);
    let created = f
        .create_model(CreateModel {
            name: "gpt-4o".into(),
            provider_id: 1,
            alias: None,
            adapter: AdapterType::OpenAICompletion,
            modalities: None,
            active: None,
            icon: None,
            endpoint: None,
        })
        .await;
    assert_eq!(created.code, 200);
    let id = created.data.unwrap().id;

    let got = f.get_model(id).await;
    assert_eq!(got.code, 200);
    assert_eq!(got.data.unwrap().name, "gpt-4o");

    assert_eq!(f.delete_model(id).await.code, 200);
    assert_eq!(f.get_model(id).await.code, 404);
}

#[tokio::test]
async fn mcp_crud_roundtrips() {
    let core = common::test_core().await;
    let f = McpStorageFacade::new(core);
    let created = f
        .create_mcp_server(CreateMcpServer {
            r#type: TransportType::Stdio,
            name: "everything".into(),
            url: None,
            description: None,
            command: Some("npx".into()),
            args: None,
            env: None,
        })
        .await;
    assert_eq!(created.code, 200);
    let id = created.data.unwrap().id;

    let got = f.get_mcp_server(id).await;
    assert_eq!(got.code, 200);
    assert_eq!(got.data.unwrap().name, "everything");

    let by_name = f.get_mcp_server_by_name("everything".into()).await;
    assert_eq!(by_name.code, 200);
    assert_eq!(by_name.data.unwrap().id, id);
}

#[tokio::test]
async fn prompt_crud_roundtrips() {
    let core = common::test_core().await;
    let f = PromptStorageFacade::new(core);
    let created = f
        .create_prompt_module(CreatePromptModule {
            key: "sys".into(),
            alias: "System".into(),
            description: "base".into(),
            content: "you are helpful".into(),
            active: None,
        })
        .await;
    assert_eq!(created.code, 200);
    let id = created.data.unwrap().id;

    let got = f.get_prompt_module(id).await;
    assert_eq!(got.code, 200);
    assert_eq!(got.data.unwrap().key, "sys");

    let by_key = f.get_prompt_module_by_key("sys".into()).await;
    assert_eq!(by_key.code, 200);
    assert_eq!(by_key.data.unwrap().id, id);
}

#[tokio::test]
async fn agent_crud_roundtrips() {
    let core = common::test_core().await;
    let f = AgentStorageFacade::new(core);

    let created = f
        .create_agent_definition(CreateAgentDefinition {
            key: "main".into(),
            name: "Main".into(),
            description: "main agent".into(),
            scope: AgentScope::Global,
            owner_topic_id: None,
            cloned_from_agent_id: None,
            active: None,
            data: AgentDefinitionData::default(),
        })
        .await;
    assert_eq!(created.code, 200);
    let agent_id = created.data.unwrap().id;

    assert_eq!(f.get_agent_definition(agent_id).await.code, 200);
    assert_eq!(f.get_agent_definition_by_key("main".into()).await.code, 200);

    // clone 到 topic 42，产生 TopicLocal 副本
    let cloned = f.clone_agent_definition(agent_id, 42).await;
    assert_eq!(cloned.code, 200);
    assert_eq!(cloned.data.unwrap().owner_topic_id, Some(42));

    // binding
    let binding = f
        .create_agent_binding(CreateAgentBinding {
            parent_topic_id: 42,
            agent_id,
            role: AgentRole::Main,
            model_id: None,
            chat_config_id: None,
            enabled: None,
        })
        .await;
    assert_eq!(binding.code, 200);
    let binding_id = binding.data.unwrap().id;

    assert_eq!(f.get_agent_binding(binding_id).await.code, 200);
    assert_eq!(f.get_agent_binding_by_agent(agent_id, 42).await.code, 200);
    assert_eq!(f.get_main_binding(42).await.code, 200);
    assert_eq!(f.list_agent_bindings_by_topic(42).await.code, 200);
    assert_eq!(f.list_agent_definitions_by_topic(42).await.code, 200);
}

#[tokio::test]
async fn delete_provider_missing_returns_404() {
    let core = common::test_core().await;
    let f = ProviderStorageFacade::new(core);
    let r = f.delete_provider(999_999).await;
    assert_eq!(r.code, 404);
    assert!(r.data.is_none());
}

#[tokio::test]
async fn delete_prompt_module_missing_returns_404() {
    let core = common::test_core().await;
    let f = PromptStorageFacade::new(core);
    let r = f.delete_prompt_module(999_999).await;
    assert_eq!(r.code, 404);
    assert!(r.data.is_none());
}

#[tokio::test]
async fn delete_agent_binding_missing_returns_404() {
    let core = common::test_core().await;
    let f = AgentStorageFacade::new(core);
    let r = f.delete_agent_binding(999_999).await;
    assert_eq!(r.code, 404);
    assert!(r.data.is_none());
}

#[tokio::test]
async fn approval_lists_return_empty() {
    let core = common::test_core().await;
    let f = ToolApprovalFacade::new(core);
    assert_eq!(f.list_by_message(1).await.code, 200);
    assert_eq!(f.list_pending_by_topic(1).await.code, 200);
    assert_eq!(f.list_pending_by_binding(1).await.code, 200);
}
