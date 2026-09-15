use crate::dto::ApiResponse;
use crate::extractor::{ApiPath, json_body};
use crate::facade::topic_map::TopicMapFacade;
use crate::state::AppState;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::Value;
use std::sync::Arc;
use wind_core::WindCore;
use wind_core::models::{CreateTopicAgentMap, TopicAgentMap};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/v1/topics/{topic_id}/agent-maps", get(list_agent_maps))
        .route("/api/v1/agent-maps", post(create_agent_map))
        .route("/api/v1/agent-maps/{map_id}", delete(delete_agent_map))
}

#[utoipa::path(
    get,
    summary = "获取话题的 Agent 能力映射列表",
    path = "/api/v1/topics/{topic_id}/agent-maps",
    params(
        ("topic_id", Path, description = "话题 ID"),
    ),
    responses(
        (status = 200, description = "获取话题的 Agent 能力映射列表", body = ApiResponse<Vec<TopicAgentMap>>),
        (status = 404, description = "话题不存在", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn list_agent_maps(
    State(core): State<Arc<WindCore>>,
    ApiPath(topic_id): ApiPath<i64>,
) -> Json<ApiResponse<Vec<TopicAgentMap>>> {
    Json(TopicMapFacade::new(core).list(topic_id).await)
}

#[utoipa::path(
    post,
    summary = "为话题新增 Agent 能力映射",
    path = "/api/v1/agent-maps",
    request_body = CreateTopicAgentMap,
    responses(
        (status = 200, description = "为话题新增 Agent 能力映射", body = ApiResponse<TopicAgentMap>),
        (status = 404, description = "话题不存在", body = ApiResponse<Value>),
        (status = 400, description = "参数校验失败", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn create_agent_map(
    State(core): State<Arc<WindCore>>,
    body: Result<Json<CreateTopicAgentMap>, JsonRejection>,
) -> Result<Json<ApiResponse<TopicAgentMap>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(TopicMapFacade::new(core).create(input).await))
}

#[utoipa::path(
    delete,
    summary = "删除话题 Agent 能力映射",
    path = "/api/v1/agent-maps/{map_id}",
    params(
        ("map_id", Path, description = "能力映射 ID"),
    ),
    responses(
        (status = 200, description = "删除话题 Agent 能力映射", body = ApiResponse<Value>),
        (status = 404, description = "能力映射不存在", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn delete_agent_map(
    State(core): State<Arc<WindCore>>,
    ApiPath(map_id): ApiPath<i64>,
) -> Json<ApiResponse<()>> {
    Json(TopicMapFacade::new(core).delete(map_id).await)
}
