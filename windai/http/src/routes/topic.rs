use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

use crate::dto::envelope::ApiResponse;
use crate::facade::topic::TopicFacade;
use crate::state::AppState;
use wind_core::WindCore;
use wind_core::models::{CreateTopic, Topic, UpdateTopic};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/v1/topics", get(list_topics).post(create_topic))
        .route(
            "/api/v1/topics/by-binding/{binding_id}",
            get(get_topic_by_binding),
        )
        .route(
            "/api/v1/topics/{topic_id}",
            get(get_topic).put(update_topic).delete(delete_topic),
        )
}

#[utoipa::path(
    get,
    summary = "获取话题列表",
    path = "/api/v1/topics",
    responses(
        (status = 200, description = "获取话题列表", body = ApiResponse<Vec<Topic>>)
    )
)]
pub(crate) async fn list_topics(
    State(core): State<Arc<WindCore>>,
) -> Json<ApiResponse<Vec<Topic>>> {
    Json(TopicFacade::new(core).list_topics().await)
}

#[utoipa::path(
    post,
    summary = "创建话题",
    path = "/api/v1/topics",
    responses(
        (status = 200, description = "创建话题", body = ApiResponse<Topic>)
    )
)]
pub(crate) async fn create_topic(
    State(core): State<Arc<WindCore>>,
    Json(input): Json<CreateTopic>,
) -> Json<ApiResponse<Topic>> {
    Json(TopicFacade::new(core).create_topic(input).await)
}

#[utoipa::path(
    get,
    summary = "获取话题",
    path = "/api/v1/topics/{topic_id}",
    responses(
        (status = 200, description = "获取话题", body = ApiResponse<Topic>)
    )
)]
pub(crate) async fn get_topic(
    State(core): State<Arc<WindCore>>,
    Path(topic_id): Path<i64>,
) -> Json<ApiResponse<Topic>> {
    Json(TopicFacade::new(core).get_topic(topic_id).await)
}

#[utoipa::path(
    put,
    summary = "更新话题",
    path = "/api/v1/topics/{topic_id}",
    responses(
        (status = 200, description = "更新话题", body = ApiResponse<Topic>)
    )
)]
pub(crate) async fn update_topic(
    State(core): State<Arc<WindCore>>,
    Path(topic_id): Path<i64>,
    Json(input): Json<UpdateTopic>,
) -> Json<ApiResponse<Topic>> {
    Json(TopicFacade::new(core).update_topic(topic_id, input).await)
}

#[utoipa::path(
    delete,
    summary = "删除话题",
    path = "/api/v1/topics/{topic_id}",
    responses(
        (status = 200, description = "删除话题", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn delete_topic(
    State(core): State<Arc<WindCore>>,
    Path(topic_id): Path<i64>,
) -> Json<ApiResponse<()>> {
    Json(TopicFacade::new(core).delete_topic(topic_id).await)
}

#[derive(Deserialize, utoipa::IntoParams)]
pub(crate) struct ByBindingQuery {
    /// 父 Topic id（必填，用于定位 binding 所属话题）
    parent_topic_id: i64,
}

#[utoipa::path(
    get,
    summary = "按 binding 获取话题",
    path = "/api/v1/topics/by-binding/{binding_id}",
    params(ByBindingQuery),
    responses(
        (status = 200, description = "按 binding 获取话题", body = ApiResponse<Topic>)
    )
)]
pub(crate) async fn get_topic_by_binding(
    State(core): State<Arc<WindCore>>,
    Path(binding_id): Path<i64>,
    Query(q): Query<ByBindingQuery>,
) -> Json<ApiResponse<Topic>> {
    Json(
        TopicFacade::new(core)
            .get_topic_by_binding(binding_id, q.parent_topic_id)
            .await,
    )
}
