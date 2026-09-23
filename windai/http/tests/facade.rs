mod common;

use std::sync::Arc;
use wind_ai::model::AdapterType;
use wind_core::WindCore;
use wind_core::agent::helper::{create_child_instance, get_or_create_main_instance};
use wind_core::models::agent::{AgentDefinitionData, AgentMode, AgentRole, BuiltinMcpBinding};
use wind_core::models::{
    CreateAgentDefinition, CreateCredentials, CreateMcpServer, CreateModel, CreatePromptModule,
    CreateProvider, CreateTopic, CreateTopicAgentMap,
};
use wind_http::facade::storage::agent::AgentStorageFacade;
use wind_http::facade::storage::approval::ToolApprovalFacade;
use wind_http::facade::storage::mcp::McpStorageFacade;
use wind_http::facade::storage::model::ModelStorageFacade;
use wind_http::facade::storage::prompt::PromptStorageFacade;
use wind_http::facade::storage::provider::ProviderStorageFacade;
use wind_http::facade::topic::TopicFacade;
use wind_http::facade::topic_map::TopicMapFacade;
use wind_mcp::client::TransportType;

/// 建一个话题并返回 id，供需要落库依赖的用例复用
async fn create_topic(core: &Arc<WindCore>, label: &str) -> i64 {
    TopicFacade::new(core.clone())
        .create_topic(CreateTopic {
            parent_id: None,
            label: label.into(),
            icon: None,
            model_id: None,
            agent_id: None,
            tool_approval_policy: None,
        })
        .await
        .data
        .expect("创建话题应返回数据")
        .id
}

#[tokio::test]
async fn create_topic_roundtrips() {
    let core = common::test_core().await;
    let facade = TopicFacade::new(core);
    let created = facade
        .create_topic(CreateTopic {
            parent_id: None,
            label: "hello".into(),
            icon: None,
            model_id: None,
            agent_id: None,
            tool_approval_policy: None,
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
            config: None,
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
}

#[tokio::test]
async fn agent_crud_roundtrips() {
    let core = common::test_core().await;
    let f = AgentStorageFacade::new(core);

    let created = f
        .create_agent_definition(CreateAgentDefinition {
            name: "Main".into(),
            description: "main agent".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: None,
            data: AgentDefinitionData::default(),
        })
        .await;
    assert_eq!(created.code, 200);
    let agent_key = created.data.as_ref().unwrap().key.clone();
    let agent_id = created.data.unwrap().id;

    assert_eq!(f.get_agent_definition(agent_id).await.code, 200);
    assert_eq!(f.get_agent_definition_by_key(agent_key).await.code, 200);

    // clone 到 topic 42，产生 owner_topic_id 指向 42 的副本
    let cloned = f.clone_agent_definition(agent_id, 42).await;
    assert_eq!(cloned.code, 200);
    assert_eq!(cloned.data.unwrap().owner_topic_id, Some(42));

    // 话题下的定义列表：42 尚无能力映射，返回空列表而非 404
    assert_eq!(f.list_agent_definitions_by_topic(42).await.code, 200);
}

#[tokio::test]
async fn instance_facade_semantics() {
    let core = common::test_core().await;
    let topic_id = create_topic(&core, "instances").await;
    let main = get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("获取主实例");
    let child = create_child_instance(core.storage(), topic_id, main.id, 1, AgentMode::Sync)
        .await
        .expect("创建子实例");
    let f = AgentStorageFacade::new(core);

    let read = f.get_instance(child.id).await;
    assert_eq!(read.code, 200);
    assert_eq!(
        read.data.expect("读取实例应返回数据").role,
        AgentRole::Child
    );

    // 主实例与子实例同等对待，不做任何隐藏
    let main_read = f.get_instance(main.id).await;
    assert_eq!(main_read.code, 200);
    assert_eq!(
        main_read.data.expect("读取主实例应返回数据").role,
        AgentRole::Main
    );

    let listed = f.list_instances_by_topic(topic_id).await;
    assert_eq!(listed.code, 200);
    assert_eq!(
        listed.data.expect("列表应返回数据").len(),
        2,
        "列表含主实例与子实例"
    );
}

#[tokio::test]
async fn agent_map_facade_semantics() {
    let core = common::test_core().await;
    let topic_id = create_topic(&core, "maps").await;
    let agent_id = AgentStorageFacade::new(core.clone())
        .create_agent_definition(CreateAgentDefinition {
            name: "Map Agent".into(),
            description: "for map facade test".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: None,
            data: AgentDefinitionData::default(),
        })
        .await
        .data
        .expect("创建 Agent 定义应返回数据")
        .id;
    let f = TopicMapFacade::new(core);

    let created = f.create(CreateTopicAgentMap { topic_id, agent_id }).await;
    assert_eq!(created.code, 200);
    let map_id = created.data.expect("创建映射应返回数据").id;

    let listed = f.list(topic_id).await;
    assert_eq!(listed.code, 200);
    assert_eq!(listed.data.expect("列表应返回数据").len(), 1);

    assert_eq!(f.delete(map_id).await.code, 200);
    let listed_after = f.list(topic_id).await;
    assert_eq!(listed_after.code, 200);
    assert!(listed_after.data.expect("列表应返回数据").is_empty());
}

#[tokio::test]
async fn agent_map_unknown_id_returns_404() {
    let core = common::test_core().await;
    let f = TopicMapFacade::new(core);

    let deleted = f.delete(999_999).await;
    assert_eq!(deleted.code, 404);
    assert!(deleted.data.is_none());
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
async fn get_instance_missing_returns_404() {
    let core = common::test_core().await;
    let f = AgentStorageFacade::new(core);
    let r = f.get_instance(999_999).await;
    assert_eq!(r.code, 404);
    assert!(r.data.is_none());
}

#[tokio::test]
async fn list_instance_messages_returns_404_for_unknown_instance() {
    let core = common::test_core().await;
    let f = TopicFacade::new(core);
    let r = f.list_instance_messages(999_999).await;
    assert_eq!(r.code, 404);
    assert!(r.data.is_none());
}

#[tokio::test]
async fn approval_lists_return_empty() {
    let core = common::test_core().await;
    let f = ToolApprovalFacade::new(core);
    assert_eq!(f.list_by_message(1).await.code, 200);
    assert_eq!(f.list_pending_by_topic(1).await.code, 200);
    assert_eq!(f.list_pending_by_instance(1).await.code, 200);
}

#[tokio::test]
async fn agent_definition_accepts_valid_builtin_mcp_name() {
    let core = common::test_core().await;
    let f = AgentStorageFacade::new(core);
    let r = f
        .create_agent_definition(CreateAgentDefinition {
            name: "WithBuiltin".into(),
            description: "x".into(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: None,
            data: AgentDefinitionData {
                builtin_mcp_servers: vec![BuiltinMcpBinding {
                    name: wind_mcp::builtin::BUILTIN_FS.name.to_string(),
                    allowed_tools: vec![],
                    denied_tools: vec![],
                    enabled: true,
                }],
                ..AgentDefinitionData::default()
            },
        })
        .await;
    assert_eq!(r.code, 200, "got: {r:?}");
}
