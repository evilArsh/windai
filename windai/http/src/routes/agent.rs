use serde_json::Value;
use std::sync::Arc;

use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use wind_core::WindCore;

use crate::dto::agent::CloneAgentDefinitionRequest;
use crate::dto::envelope::ApiResponse;
use crate::extractor::{ApiPath, ApiQuery, json_body};
use crate::facade::storage::agent::AgentStorageFacade;
use crate::facade::storage::approval::ToolApprovalFacade;
use crate::facade::topic::TopicFacade;
use crate::state::AppState;
use wind_ai::message::ReqConfig;
use wind_core::models::{
    AgentBinding, AgentDefinition, ChatConfig, CreateAgentBinding, CreateAgentDefinition,
    ToolApprovalRequest, UpdateAgentBinding, UpdateAgentDefinition,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/agent-definitions",
            get(list_definitions).post(create_definition),
        )
        .route(
            "/api/v1/agent-definitions/by-key/{key}",
            get(get_definition_by_key),
        )
        .route(
            "/api/v1/agent-definitions/{agent_definition_id}",
            get(get_definition)
                .put(update_definition)
                .delete(delete_definition),
        )
        .route(
            "/api/v1/topics/{topic_id}/agent-definitions",
            get(list_definitions_by_topic),
        )
        .route(
            "/api/v1/topics/{topic_id}/agent-definitions/clone",
            post(clone_definition),
        )
        .route("/api/v1/agent-bindings", post(create_binding))
        .route(
            "/api/v1/agent-bindings/by-agent/{agent_id}",
            get(get_binding_by_agent),
        )
        .route(
            "/api/v1/agent-bindings/{binding_id}",
            get(get_binding).put(update_binding).delete(delete_binding),
        )
        .route(
            "/api/v1/agent-bindings/{binding_id}/chat-config",
            get(get_chat_config)
                .post(create_chat_config)
                .put(update_chat_config),
        )
        .route(
            "/api/v1/agent-bindings/{binding_id}/tool-approvals/pending",
            get(list_pending_by_binding),
        )
        .route(
            "/api/v1/topics/{topic_id}/agent-bindings",
            get(list_bindings_by_topic),
        )
        .route(
            "/api/v1/topics/{topic_id}/agent-bindings/main",
            get(get_main_binding),
        )
        .route(
            "/api/v1/messages/{message_id}/tool-approvals",
            get(list_approvals_by_message),
        )
        .route(
            "/api/v1/topics/{topic_id}/tool-approvals/pending",
            get(list_pending_by_topic),
        )
}

// ---- agent definitions ----

#[utoipa::path(
    get,
    summary = "获取 Agent 定义列表",
    path = "/api/v1/agent-definitions",
    responses(
        (status = 200, description = "获取 Agent 定义列表", body = ApiResponse<Vec<AgentDefinition>>)
    )
)]
pub(crate) async fn list_definitions(
    State(core): State<Arc<WindCore>>,
) -> Json<ApiResponse<Vec<AgentDefinition>>> {
    Json(AgentStorageFacade::new(core).list_agent_definitions().await)
}

#[utoipa::path(
    post,
    summary = "创建 Agent 定义",
    path = "/api/v1/agent-definitions",
    responses(
        (status = 200, description = "创建 Agent 定义", body = ApiResponse<AgentDefinition>)
    )
)]
pub(crate) async fn create_definition(
    State(core): State<Arc<WindCore>>,
    body: Result<Json<CreateAgentDefinition>, JsonRejection>,
) -> Result<Json<ApiResponse<AgentDefinition>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        AgentStorageFacade::new(core)
            .create_agent_definition(input)
            .await,
    ))
}

#[utoipa::path(
    get,
    summary = "获取 Agent 定义",
    path = "/api/v1/agent-definitions/{agent_definition_id}",
    params(
        ("agent_definition_id", Path, description = "Agent 定义 ID"),
    ),
    responses(
        (status = 200, description = "获取 Agent 定义", body = ApiResponse<AgentDefinition>)
    )
)]
pub(crate) async fn get_definition(
    State(core): State<Arc<WindCore>>,
    ApiPath(agent_definition_id): ApiPath<i64>,
) -> Json<ApiResponse<AgentDefinition>> {
    Json(
        AgentStorageFacade::new(core)
            .get_agent_definition(agent_definition_id)
            .await,
    )
}

#[utoipa::path(
    put,
    summary = "更新 Agent 定义",
    path = "/api/v1/agent-definitions/{agent_definition_id}",
    params(
        ("agent_definition_id", Path, description = "Agent 定义 ID"),
    ),
    responses(
        (status = 200, description = "更新 Agent 定义", body = ApiResponse<AgentDefinition>)
    )
)]
pub(crate) async fn update_definition(
    State(core): State<Arc<WindCore>>,
    ApiPath(agent_definition_id): ApiPath<i64>,
    body: Result<Json<UpdateAgentDefinition>, JsonRejection>,
) -> Result<Json<ApiResponse<AgentDefinition>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        AgentStorageFacade::new(core)
            .update_agent_definition(agent_definition_id, input)
            .await,
    ))
}

#[utoipa::path(
    delete,
    summary = "删除 Agent 定义",
    path = "/api/v1/agent-definitions/{agent_definition_id}",
    params(
        ("agent_definition_id", Path, description = "Agent 定义 ID"),
    ),
    responses(
        (status = 200, description = "删除 Agent 定义", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn delete_definition(
    State(core): State<Arc<WindCore>>,
    ApiPath(agent_definition_id): ApiPath<i64>,
) -> Json<ApiResponse<()>> {
    Json(
        AgentStorageFacade::new(core)
            .delete_agent_definition(agent_definition_id)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "按 key 获取 Agent 定义",
    path = "/api/v1/agent-definitions/by-key/{key}",
    params(
        ("key", Path, description = "Agent 定义 key"),
    ),
    responses(
        (status = 200, description = "按 key 获取 Agent 定义", body = ApiResponse<AgentDefinition>)
    )
)]
pub(crate) async fn get_definition_by_key(
    State(core): State<Arc<WindCore>>,
    ApiPath(key): ApiPath<String>,
) -> Json<ApiResponse<AgentDefinition>> {
    Json(
        AgentStorageFacade::new(core)
            .get_agent_definition_by_key(key)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "获取话题下的 Agent 定义列表",
    path = "/api/v1/topics/{topic_id}/agent-definitions",
    params(
        ("topic_id", Path, description = "话题 ID"),
    ),
    responses(
        (status = 200, description = "获取话题下的 Agent 定义列表", body = ApiResponse<Vec<AgentDefinition>>)
    )
)]
pub(crate) async fn list_definitions_by_topic(
    State(core): State<Arc<WindCore>>,
    ApiPath(topic_id): ApiPath<i64>,
) -> Json<ApiResponse<Vec<AgentDefinition>>> {
    Json(
        AgentStorageFacade::new(core)
            .list_agent_definitions_by_topic(topic_id)
            .await,
    )
}

#[utoipa::path(
    post,
    summary = "克隆 Agent 定义",
    path = "/api/v1/topics/{topic_id}/agent-definitions/clone",
    params(
        ("topic_id", Path, description = "话题 ID"),
    ),
    responses(
        (status = 200, description = "克隆 Agent 定义", body = ApiResponse<AgentDefinition>)
    )
)]
pub(crate) async fn clone_definition(
    State(core): State<Arc<WindCore>>,
    ApiPath(topic_id): ApiPath<i64>,
    body: Result<Json<CloneAgentDefinitionRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<AgentDefinition>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        AgentStorageFacade::new(core)
            .clone_agent_definition(input.agent_id, topic_id)
            .await,
    ))
}

// ---- agent bindings ----

#[utoipa::path(
    post,
    summary = "创建 Agent 绑定",
    path = "/api/v1/agent-bindings",
    responses(
        (status = 200, description = "创建 Agent 绑定", body = ApiResponse<AgentBinding>)
    )
)]
pub(crate) async fn create_binding(
    State(core): State<Arc<WindCore>>,
    body: Result<Json<CreateAgentBinding>, JsonRejection>,
) -> Result<Json<ApiResponse<AgentBinding>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        AgentStorageFacade::new(core)
            .create_agent_binding(input)
            .await,
    ))
}

#[utoipa::path(
    get,
    summary = "获取 Agent 绑定",
    path = "/api/v1/agent-bindings/{binding_id}",
    params(
        ("binding_id", Path, description = "Agent 绑定 ID"),
    ),
    responses(
        (status = 200, description = "获取 Agent 绑定", body = ApiResponse<AgentBinding>)
    )
)]
pub(crate) async fn get_binding(
    State(core): State<Arc<WindCore>>,
    ApiPath(binding_id): ApiPath<i64>,
) -> Json<ApiResponse<AgentBinding>> {
    Json(
        AgentStorageFacade::new(core)
            .get_agent_binding(binding_id)
            .await,
    )
}

#[utoipa::path(
    put,
    summary = "更新 Agent 绑定",
    path = "/api/v1/agent-bindings/{binding_id}",
    params(
        ("binding_id", Path, description = "Agent 绑定 ID"),
    ),
    responses(
        (status = 200, description = "更新 Agent 绑定", body = ApiResponse<AgentBinding>)
    )
)]
pub(crate) async fn update_binding(
    State(core): State<Arc<WindCore>>,
    ApiPath(binding_id): ApiPath<i64>,
    body: Result<Json<UpdateAgentBinding>, JsonRejection>,
) -> Result<Json<ApiResponse<AgentBinding>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        AgentStorageFacade::new(core)
            .update_agent_binding(binding_id, input)
            .await,
    ))
}

#[utoipa::path(
    delete,
    summary = "删除 Agent 绑定",
    path = "/api/v1/agent-bindings/{binding_id}",
    params(
        ("binding_id", Path, description = "Agent 绑定 ID"),
    ),
    responses(
        (status = 200, description = "删除 Agent 绑定", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn delete_binding(
    State(core): State<Arc<WindCore>>,
    ApiPath(binding_id): ApiPath<i64>,
) -> Json<ApiResponse<()>> {
    Json(
        AgentStorageFacade::new(core)
            .delete_agent_binding(binding_id)
            .await,
    )
}

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ByAgentQuery {
    /// 父 Topic id（必填，用于定位 binding 所属话题）
    parent_topic_id: i64,
}

#[utoipa::path(
    get,
    summary = "按 Agent 获取绑定",
    path = "/api/v1/agent-bindings/by-agent/{agent_id}",
    params(
        ("agent_id", Path, description = "Agent ID"),
        ByAgentQuery,
    ),
    responses(
        (status = 200, description = "按 Agent 获取绑定", body = ApiResponse<AgentBinding>)
    )
)]
pub(crate) async fn get_binding_by_agent(
    State(core): State<Arc<WindCore>>,
    ApiPath(agent_id): ApiPath<i64>,
    ApiQuery(q): ApiQuery<ByAgentQuery>,
) -> Json<ApiResponse<AgentBinding>> {
    Json(
        AgentStorageFacade::new(core)
            .get_agent_binding_by_agent(agent_id, q.parent_topic_id)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "获取话题下的 Agent 绑定列表",
    path = "/api/v1/topics/{topic_id}/agent-bindings",
    params(
        ("topic_id", Path, description = "话题 ID"),
    ),
    responses(
        (status = 200, description = "获取话题下的 Agent 绑定列表", body = ApiResponse<Vec<AgentBinding>>)
    )
)]
pub(crate) async fn list_bindings_by_topic(
    State(core): State<Arc<WindCore>>,
    ApiPath(topic_id): ApiPath<i64>,
) -> Json<ApiResponse<Vec<AgentBinding>>> {
    Json(
        AgentStorageFacade::new(core)
            .list_agent_bindings_by_topic(topic_id)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "获取主 Agent 绑定",
    path = "/api/v1/topics/{topic_id}/agent-bindings/main",
    params(
        ("topic_id", Path, description = "话题 ID"),
    ),
    responses(
        (status = 200, description = "获取主 Agent 绑定", body = ApiResponse<AgentBinding>)
    )
)]
pub(crate) async fn get_main_binding(
    State(core): State<Arc<WindCore>>,
    ApiPath(topic_id): ApiPath<i64>,
) -> Json<ApiResponse<AgentBinding>> {
    Json(
        AgentStorageFacade::new(core)
            .get_main_binding(topic_id)
            .await,
    )
}

// ---- chat config (TopicFacade) ----

#[utoipa::path(
    get,
    summary = "获取对话配置",
    path = "/api/v1/agent-bindings/{binding_id}/chat-config",
    params(
        ("binding_id", Path, description = "Agent 绑定 ID"),
    ),
    responses(
        (status = 200, description = "获取对话配置", body = ApiResponse<ChatConfig>)
    )
)]
pub(crate) async fn get_chat_config(
    State(core): State<Arc<WindCore>>,
    ApiPath(binding_id): ApiPath<i64>,
) -> Json<ApiResponse<ChatConfig>> {
    Json(TopicFacade::new(core).get_chat_config(binding_id).await)
}

#[utoipa::path(
    post,
    summary = "创建对话配置",
    path = "/api/v1/agent-bindings/{binding_id}/chat-config",
    params(
        ("binding_id", Path, description = "Agent 绑定 ID"),
    ),
    responses(
        (status = 200, description = "创建对话配置", body = ApiResponse<ChatConfig>)
    )
)]
pub(crate) async fn create_chat_config(
    State(core): State<Arc<WindCore>>,
    ApiPath(binding_id): ApiPath<i64>,
    body: Result<Json<ReqConfig>, JsonRejection>,
) -> Result<Json<ApiResponse<ChatConfig>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        TopicFacade::new(core)
            .create_chat_config(binding_id, input)
            .await,
    ))
}

#[utoipa::path(
    put,
    summary = "更新对话配置",
    path = "/api/v1/agent-bindings/{binding_id}/chat-config",
    params(
        ("binding_id", Path, description = "Agent 绑定 ID"),
    ),
    responses(
        (status = 200, description = "更新对话配置", body = ApiResponse<ChatConfig>)
    )
)]
pub(crate) async fn update_chat_config(
    State(core): State<Arc<WindCore>>,
    ApiPath(binding_id): ApiPath<i64>,
    body: Result<Json<ReqConfig>, JsonRejection>,
) -> Result<Json<ApiResponse<ChatConfig>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        TopicFacade::new(core)
            .update_chat_config(binding_id, input)
            .await,
    ))
}

// ---- tool approvals (read-only) ----

#[utoipa::path(
    get,
    summary = "获取消息的工具审批列表",
    path = "/api/v1/messages/{message_id}/tool-approvals",
    params(
        ("message_id", Path, description = "消息 ID"),
    ),
    responses(
        (status = 200, description = "获取消息的工具审批列表", body = ApiResponse<Vec<ToolApprovalRequest>>)
    )
)]
pub(crate) async fn list_approvals_by_message(
    State(core): State<Arc<WindCore>>,
    ApiPath(message_id): ApiPath<i64>,
) -> Json<ApiResponse<Vec<ToolApprovalRequest>>> {
    Json(
        ToolApprovalFacade::new(core)
            .list_by_message(message_id)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "获取话题待审批列表",
    path = "/api/v1/topics/{topic_id}/tool-approvals/pending",
    params(
        ("topic_id", Path, description = "话题 ID"),
    ),
    responses(
        (status = 200, description = "获取话题待审批列表", body = ApiResponse<Vec<ToolApprovalRequest>>)
    )
)]
pub(crate) async fn list_pending_by_topic(
    State(core): State<Arc<WindCore>>,
    ApiPath(topic_id): ApiPath<i64>,
) -> Json<ApiResponse<Vec<ToolApprovalRequest>>> {
    Json(
        ToolApprovalFacade::new(core)
            .list_pending_by_topic(topic_id)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "获取绑定待审批列表",
    path = "/api/v1/agent-bindings/{binding_id}/tool-approvals/pending",
    params(
        ("binding_id", Path, description = "Agent 绑定 ID"),
    ),
    responses(
        (status = 200, description = "获取绑定待审批列表", body = ApiResponse<Vec<ToolApprovalRequest>>)
    )
)]
pub(crate) async fn list_pending_by_binding(
    State(core): State<Arc<WindCore>>,
    ApiPath(binding_id): ApiPath<i64>,
) -> Json<ApiResponse<Vec<ToolApprovalRequest>>> {
    Json(
        ToolApprovalFacade::new(core)
            .list_pending_by_binding(binding_id)
            .await,
    )
}
