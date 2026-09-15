use axum::Json;
use serde_json::Value;
use std::sync::OnceLock;
use utoipa::OpenApi;
use wind_core::agent::event::TopicEvent;
use wind_core::models::{
    AgentDefinition, AgentInstance, AgentRole, CreateAgentDefinition, CreateCredentials,
    CreateJsonRule, CreateMcpServer, CreateModel, CreatePromptModule, CreateProvider, CreateTopic,
    CreateTopicAgentMap, Credentials, JsonRule, McpServerParam, Message, Model, PromptModule,
    Provider, ToolApprovalRequest, Topic, TopicAgentMap, UpdateAgentDefinition, UpdateJsonRule,
    UpdateMcpServer, UpdateMessage, UpdateModel, UpdatePromptModule, UpdateProvider, UpdateTopic,
};

use crate::dto::{ApiResponse, ApproveToolCallsRequest, CreateChatRequest};
use wind_mcp::client::{
    ClientEvent, ClientSnapshot, ClientStatus, Prompt, PromptArgument, Resource, Tool,
};

/// 聚合 wind-http 全部公开路由与 schema 的 OpenAPI 文档
#[derive(OpenApi)]
#[openapi(
    info(title = "wind-http API", version = "0.1.0"),
    paths(
        // topic
        crate::routes::topic::list_topics,
        crate::routes::topic::create_topic,
        crate::routes::topic::list_child_topics,
        crate::routes::topic::get_topic,
        crate::routes::topic::update_topic,
        crate::routes::topic::delete_topic,
        // topic agent map
        crate::routes::topic_map::list_agent_maps,
        crate::routes::topic_map::create_agent_map,
        crate::routes::topic_map::delete_agent_map,
        // chat / message / SSE
        crate::routes::chat::list_topic_messages,
        crate::routes::chat::list_instance_messages,
        crate::routes::chat::create_chat,
        crate::routes::chat::get_message,
        crate::routes::chat::update_message,
        crate::routes::chat::cancel_task,
        crate::routes::chat::approve_tool_calls,
        crate::routes::chat::subscribe_events,
        // provider
        crate::routes::provider::list_providers,
        crate::routes::provider::create_provider,
        crate::routes::provider::get_provider,
        crate::routes::provider::update_provider,
        crate::routes::provider::delete_provider,
        crate::routes::provider::get_provider_by_name,
        crate::routes::provider::list_credentials,
        crate::routes::provider::create_credentials,
        crate::routes::provider::delete_credentials,
        crate::routes::provider::list_json_rules,
        crate::routes::provider::create_json_rule,
        crate::routes::provider::get_json_rule_by_adapter,
        crate::routes::provider::get_json_rule,
        crate::routes::provider::update_json_rule,
        crate::routes::provider::delete_json_rule,
        // model
        crate::routes::model::list_models,
        crate::routes::model::create_model,
        crate::routes::model::get_model,
        crate::routes::model::update_model,
        crate::routes::model::delete_model,
        // mcp
        crate::routes::mcp::list_mcp_servers,
        crate::routes::mcp::create_mcp_server,
        crate::routes::mcp::get_mcp_server,
        crate::routes::mcp::update_mcp_server,
        crate::routes::mcp::delete_mcp_server,
        crate::routes::mcp::get_mcp_server_by_name,
        crate::routes::mcp::start_mcp_server,
        crate::routes::mcp::stop_mcp_server,
        crate::routes::mcp::attach_mcp_server,
        crate::routes::mcp::list_mcp_clients,
        crate::routes::mcp::get_mcp_client,
        crate::routes::mcp::list_mcp_client_tools,
        crate::routes::mcp::list_mcp_client_prompts,
        crate::routes::mcp::list_mcp_client_resources,
        crate::routes::mcp::list_all_mcp_tools,
        crate::routes::mcp::list_mcp_tools_by_names,
        crate::routes::mcp::subscribe_mcp_events,
        // prompt
        crate::routes::prompt::list_prompt_modules,
        crate::routes::prompt::create_prompt_module,
        crate::routes::prompt::get_prompt_module,
        crate::routes::prompt::update_prompt_module,
        crate::routes::prompt::delete_prompt_module,
        // agent
        crate::routes::agent::list_definitions,
        crate::routes::agent::create_definition,
        crate::routes::agent::get_definition,
        crate::routes::agent::update_definition,
        crate::routes::agent::delete_definition,
        crate::routes::agent::get_definition_by_key,
        crate::routes::agent::list_definitions_by_topic,
        crate::routes::agent::clone_definition,
        crate::routes::agent::get_instance,
        crate::routes::agent::list_pending_by_instance,
        crate::routes::agent::list_instances_by_topic,
        crate::routes::agent::list_approvals_by_message,
        crate::routes::agent::list_pending_by_topic,
    ),
    components(schemas(
        ApiResponse<Value>,
        // 命令 DTO
        CreateChatRequest,
        ApproveToolCallsRequest,
        CreateTopicAgentMap,
        // topic
        Topic,
        CreateTopic,
        UpdateTopic,
        // topic agent map
        TopicAgentMap,
        Provider,
        CreateProvider,
        UpdateProvider,
        Credentials,
        CreateCredentials,
        JsonRule,
        CreateJsonRule,
        UpdateJsonRule,
        // model
        Model,
        CreateModel,
        UpdateModel,
        // mcp
        McpServerParam,
        CreateMcpServer,
        UpdateMcpServer,
        ClientStatus,
        ClientSnapshot,
        ClientEvent,
        Tool,
        Prompt,
        PromptArgument,
        Resource,
        // prompt
        PromptModule,
        CreatePromptModule,
        UpdatePromptModule,
        // message
        Message,
        UpdateMessage,
        // agent
        AgentDefinition,
        CreateAgentDefinition,
        UpdateAgentDefinition,
        AgentInstance,
        AgentRole,
        ToolApprovalRequest,
        // SSE 事件
        TopicEvent,
    ))
)]
pub struct ApiDoc;

fn openapi_doc() -> &'static Value {
    static DOC: OnceLock<Value> = OnceLock::new();
    DOC.get_or_init(|| serde_json::to_value(ApiDoc::openapi()).expect("OpenAPI 文档序列化失败"))
}

pub async fn serve_openapi_json() -> Json<&'static Value> {
    Json(openapi_doc())
}
