use crate::dto::ApiResponse;
use crate::extractor::{ApiPath, json_body};
use crate::facade::storage::agent::AgentStorageFacade;
use crate::facade::storage::approval::ToolApprovalFacade;
use crate::state::AppState;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::Value;
use std::sync::Arc;
use wind_core::WindCore;
use wind_core::models::{
    AgentDefinition, AgentInstance, CreateAgentDefinition, ToolApprovalRequest,
    UpdateAgentDefinition,
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
            "/api/v1/agent-definitions/topics/{topic_id}",
            get(list_definitions_by_topic),
        )
        .route(
            "/api/v1/agent-definitions/topics/{topic_id}/clone/{agent_definition_id}",
            post(clone_definition),
        )
        .route("/api/v1/agent-instances/{instance_id}", get(get_instance))
        .route(
            "/api/v1/agent-instances/{instance_id}/tool-approvals/pending",
            get(list_pending_by_instance),
        )
        .route(
            "/api/v1/topics/{topic_id}/agent-instances",
            get(list_instances_by_topic),
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
    path = "/api/v1/agent-definitions/topics/{topic_id}",
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
    path = "/api/v1/agent-definitions/topics/{topic_id}/clone/{agent_definition_id}",
    params(
        ("topic_id", Path, description = "话题 ID"),
        ("agent_definition_id", Path, description = "被克隆的 Agent 定义 ID"),
    ),
    responses(
        (status = 200, description = "克隆 Agent 定义", body = ApiResponse<AgentDefinition>),
        (status = 404, description = "源 Agent 定义不存在", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn clone_definition(
    State(core): State<Arc<WindCore>>,
    ApiPath((topic_id, agent_definition_id)): ApiPath<(i64, i64)>,
) -> Json<ApiResponse<AgentDefinition>> {
    Json(
        AgentStorageFacade::new(core)
            .clone_agent_definition(agent_definition_id, topic_id)
            .await,
    )
}

// ---- agent instances (core 托管，只读) ----

#[utoipa::path(
    get,
    summary = "获取 Agent 实例",
    path = "/api/v1/agent-instances/{instance_id}",
    params(
        ("instance_id", Path, description = "Agent 实例 ID"),
    ),
    responses(
        (status = 200, description = "获取 Agent 实例", body = ApiResponse<AgentInstance>),
        (status = 404, description = "实例不存在", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn get_instance(
    State(core): State<Arc<WindCore>>,
    ApiPath(instance_id): ApiPath<i64>,
) -> Json<ApiResponse<AgentInstance>> {
    Json(
        AgentStorageFacade::new(core)
            .get_instance(instance_id)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "获取话题下的所有 Agent 实例列表",
    path = "/api/v1/topics/{topic_id}/agent-instances",
    params(
        ("topic_id", Path, description = "话题 ID"),
    ),
    responses(
        (status = 200, description = "获取话题下的所有 Agent 实例列表", body = ApiResponse<Vec<AgentInstance>>)
    )
)]
pub(crate) async fn list_instances_by_topic(
    State(core): State<Arc<WindCore>>,
    ApiPath(topic_id): ApiPath<i64>,
) -> Json<ApiResponse<Vec<AgentInstance>>> {
    Json(
        AgentStorageFacade::new(core)
            .list_instances_by_topic(topic_id)
            .await,
    )
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
    summary = "获取实例待审批列表",
    path = "/api/v1/agent-instances/{instance_id}/tool-approvals/pending",
    params(
        ("instance_id", Path, description = "Agent 实例 ID"),
    ),
    responses(
        (status = 200, description = "获取实例待审批列表", body = ApiResponse<Vec<ToolApprovalRequest>>)
    )
)]
pub(crate) async fn list_pending_by_instance(
    State(core): State<Arc<WindCore>>,
    ApiPath(instance_id): ApiPath<i64>,
) -> Json<ApiResponse<Vec<ToolApprovalRequest>>> {
    Json(
        ToolApprovalFacade::new(core)
            .list_pending_by_instance(instance_id)
            .await,
    )
}
