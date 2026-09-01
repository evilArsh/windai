use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::sse::{KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use wind_core::WindCore;
use wind_core::agent::event::TopicEvent;

use crate::dto::approval::ApproveToolCallsRequest;
use crate::dto::envelope::{ApiResponse, map_core_error};
use crate::dto::message::{CreateChatRequest, SubmitChatResponse};
use crate::extractor::{ApiPath, json_body};
use crate::facade::topic::TopicFacade;
use crate::sse::event_stream;
use crate::state::AppState;
use wind_core::models::{Message, UpdateMessage};

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/topics/{topic_id}/messages",
            get(list_messages).post(create_chat),
        )
        .route(
            "/api/v1/topics/{topic_id}/messages/context",
            get(list_context),
        )
        .route(
            "/api/v1/messages/{message_id}",
            get(get_message).put(update_message),
        )
        .route(
            "/api/v1/messages/{message_id}/from-message",
            get(get_message_from_message),
        )
        .route(
            "/api/v1/topics/{topic_id}/bindings/{binding_id}/cancel",
            post(cancel_task),
        )
        .route(
            "/api/v1/topics/{topic_id}/tool-approvals/{message_id}/approve",
            post(approve_tool_calls),
        )
}

/// SSE 单独成 router，不套 TimeoutLayer。
pub fn sse_router() -> Router<AppState> {
    Router::new().route("/api/v1/topics/{topic_id}/events", get(subscribe_events))
}

#[utoipa::path(
    get,
    summary = "获取话题消息列表",
    path = "/api/v1/topics/{topic_id}/messages",
    params(
        ("topic_id", Path, description = "话题 ID"),
    ),
    responses(
        (status = 200, description = "获取话题消息列表", body = ApiResponse<Vec<Message>>)
    )
)]
pub(crate) async fn list_messages(
    State(core): State<Arc<WindCore>>,
    ApiPath(topic_id): ApiPath<i64>,
) -> Json<ApiResponse<Vec<Message>>> {
    Json(TopicFacade::new(core).list_topic_messages(topic_id).await)
}

#[utoipa::path(
    post,
    summary = "提交对话消息",
    path = "/api/v1/topics/{topic_id}/messages",
    params(
        ("topic_id", Path, description = "话题 ID"),
    ),
    responses(
        (status = 200, description = "提交对话消息", body = ApiResponse<SubmitChatResponse>)
    )
)]
pub(crate) async fn create_chat(
    State(core): State<Arc<WindCore>>,
    ApiPath(topic_id): ApiPath<i64>,
    body: Result<Json<CreateChatRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<SubmitChatResponse>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        TopicFacade::new(core).create_chat(topic_id, input).await,
    ))
}

#[utoipa::path(
    get,
    summary = "获取消息上下文",
    path = "/api/v1/topics/{topic_id}/messages/context",
    params(
        ("topic_id", Path, description = "话题 ID"),
    ),
    responses(
        (status = 200, description = "获取消息上下文", body = ApiResponse<Vec<Message>>)
    )
)]
pub(crate) async fn list_context(
    State(core): State<Arc<WindCore>>,
    ApiPath(topic_id): ApiPath<i64>,
) -> Json<ApiResponse<Vec<Message>>> {
    Json(TopicFacade::new(core).list_message_context(topic_id).await)
}

#[utoipa::path(
    get,
    summary = "获取消息",
    path = "/api/v1/messages/{message_id}",
    params(
        ("message_id", Path, description = "消息 ID"),
    ),
    responses(
        (status = 200, description = "获取消息", body = ApiResponse<Message>)
    )
)]
pub(crate) async fn get_message(
    State(core): State<Arc<WindCore>>,
    ApiPath(message_id): ApiPath<i64>,
) -> Json<ApiResponse<Message>> {
    Json(TopicFacade::new(core).get_message(message_id).await)
}

#[utoipa::path(
    put,
    summary = "更新消息",
    path = "/api/v1/messages/{message_id}",
    params(
        ("message_id", Path, description = "消息 ID"),
    ),
    responses(
        (status = 200, description = "更新消息", body = ApiResponse<Message>)
    )
)]
pub(crate) async fn update_message(
    State(core): State<Arc<WindCore>>,
    ApiPath(message_id): ApiPath<i64>,
    body: Result<Json<UpdateMessage>, JsonRejection>,
) -> Result<Json<ApiResponse<Message>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        TopicFacade::new(core)
            .update_message(message_id, input)
            .await,
    ))
}

#[utoipa::path(
    get,
    summary = "获取消息对应的源消息",
    path = "/api/v1/messages/{message_id}/from-message",
    params(
        ("message_id", Path, description = "消息 ID"),
    ),
    responses(
        (status = 200, description = "获取消息对应的源消息", body = ApiResponse<Message>)
    )
)]
pub(crate) async fn get_message_from_message(
    State(core): State<Arc<WindCore>>,
    ApiPath(message_id): ApiPath<i64>,
) -> Json<ApiResponse<Message>> {
    Json(
        TopicFacade::new(core)
            .get_message_from_message(message_id)
            .await,
    )
}

#[utoipa::path(
    post,
    summary = "取消 Agent 任务",
    path = "/api/v1/topics/{topic_id}/bindings/{binding_id}/cancel",
    params(
        ("topic_id", Path, description = "话题 ID"),
        ("binding_id", Path, description = "Agent 绑定 ID"),
    ),
    responses(
        (status = 200, description = "取消 Agent 任务", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn cancel_task(
    State(core): State<Arc<WindCore>>,
    ApiPath((topic_id, binding_id)): ApiPath<(i64, i64)>,
) -> Json<ApiResponse<()>> {
    Json(
        TopicFacade::new(core)
            .cancel_task(topic_id, binding_id)
            .await,
    )
}

#[utoipa::path(
    post,
    summary = "审批工具调用",
    path = "/api/v1/topics/{topic_id}/tool-approvals/{message_id}/approve",
    params(
        ("topic_id", Path, description = "话题 ID"),
        ("message_id", Path, description = "消息 ID"),
    ),
    responses(
        (status = 200, description = "审批工具调用", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn approve_tool_calls(
    State(core): State<Arc<WindCore>>,
    ApiPath((topic_id, message_id)): ApiPath<(i64, i64)>,
    body: Result<Json<ApproveToolCallsRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        TopicFacade::new(core)
            .approve_tool_calls(topic_id, message_id, input)
            .await,
    ))
}

#[utoipa::path(
    get,
    summary = "订阅话题事件流(SSE)",
    path = "/api/v1/topics/{topic_id}/events",
    params(
        ("topic_id", Path, description = "话题 ID"),
    ),
    responses(
        (status = 200, description = "订阅话题事件流(SSE): 每条事件帧格式为 `event: <变体名>` / `id: <递增序号>` / `data: <TopicEvent JSON>`，帧间空行分隔。`data` 字段即 TopicEvent 结构", content(
            (TopicEvent = "text/event-stream"),
            (TopicEvent = "application/json"),
        )),
        (status = 404, description = "话题不存在", body = ApiResponse<Value>),
        (status = 500, description = "内部错误", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn subscribe_events(
    State(state): State<AppState>,
    ApiPath(topic_id): ApiPath<i64>,
) -> impl IntoResponse {
    let core = &state.core;
    let cancel = state.cancel.clone();
    match core.storage().topic().get_topic(topic_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ApiResponse::<()>::not_found("topic not found")),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(map_core_error::<()>(e)),
            )
                .into_response();
        }
    }
    let handle = core.fetch_topic(topic_id);
    match handle.subscribe().await {
        Ok(rx) => Sse::new(event_stream(rx, cancel))
            .keep_alive(
                KeepAlive::new()
                    .interval(Duration::from_secs(15))
                    .text("keep-alive"),
            )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(map_core_error::<()>(e)),
        )
            .into_response(),
    }
}
