use serde_json::Value;
use utoipa::OpenApi;
use wind_ai::message::ReqConfig;
use wind_core::agent::event::TopicEvent;
use wind_core::models::{
    AgentBinding, AgentDefinition, ChatConfig, CreateAgentBinding, CreateAgentDefinition,
    CreateCredentials, CreateJsonRule, CreateMcpServer, CreateModel, CreatePromptModule,
    CreateProvider, CreateTopic, Credentials, JsonRule, McpServerParam, Message, Model,
    PromptModule, Provider, ToolApprovalRequest, Topic, UpdateAgentBinding, UpdateAgentDefinition,
    UpdateJsonRule, UpdateMcpServer, UpdateMessage, UpdateModel, UpdatePromptModule,
    UpdateProvider, UpdateTopic,
};

use crate::dto::agent::CloneAgentDefinitionRequest;
use crate::dto::approval::ApproveToolCallsRequest;
use crate::dto::envelope::ApiResponse;
use crate::dto::message::{CreateChatRequest, SubmitChatResponse};

/// 聚合 wind-http 全部公开路由与 schema 的 OpenAPI 文档。
#[derive(OpenApi)]
#[openapi(
    info(title = "wind-http API", version = "0.1.0"),
    paths(
        crate::routes::health::health,
        // topic
        crate::routes::topic::list_topics,
        crate::routes::topic::create_topic,
        crate::routes::topic::get_topic_by_binding,
        crate::routes::topic::get_topic,
        crate::routes::topic::update_topic,
        crate::routes::topic::delete_topic,
        // chat / message / SSE
        crate::routes::chat::list_messages,
        crate::routes::chat::create_chat,
        crate::routes::chat::list_context,
        crate::routes::chat::get_message,
        crate::routes::chat::update_message,
        crate::routes::chat::get_message_from_message,
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
        // prompt
        crate::routes::prompt::list_prompt_modules,
        crate::routes::prompt::create_prompt_module,
        crate::routes::prompt::get_prompt_module,
        crate::routes::prompt::update_prompt_module,
        crate::routes::prompt::delete_prompt_module,
        crate::routes::prompt::get_prompt_module_by_key,
        // agent
        crate::routes::agent::list_definitions,
        crate::routes::agent::create_definition,
        crate::routes::agent::get_definition,
        crate::routes::agent::update_definition,
        crate::routes::agent::delete_definition,
        crate::routes::agent::get_definition_by_key,
        crate::routes::agent::list_definitions_by_topic,
        crate::routes::agent::clone_definition,
        crate::routes::agent::create_binding,
        crate::routes::agent::get_binding_by_agent,
        crate::routes::agent::get_binding,
        crate::routes::agent::update_binding,
        crate::routes::agent::delete_binding,
        crate::routes::agent::get_chat_config,
        crate::routes::agent::create_chat_config,
        crate::routes::agent::update_chat_config,
        crate::routes::agent::list_pending_by_binding,
        crate::routes::agent::list_bindings_by_topic,
        crate::routes::agent::get_main_binding,
        crate::routes::agent::list_approvals_by_message,
        crate::routes::agent::list_pending_by_topic,
    ),
    components(schemas(
        ApiResponse<Value>,
        // 命令 DTO
        CreateChatRequest,
        SubmitChatResponse,
        ApproveToolCallsRequest,
        CloneAgentDefinitionRequest,
        // topic / chat config
        Topic,
        CreateTopic,
        UpdateTopic,
        ChatConfig,
        ReqConfig,
        // provider / credentials / json rule
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
        AgentBinding,
        CreateAgentBinding,
        UpdateAgentBinding,
        ToolApprovalRequest,
        // SSE 事件
        TopicEvent,
    ))
)]
pub struct ApiDoc;
