use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::response::IntoResponse;
use axum::response::sse::{KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use wind_core::WindCore;
use wind_mcp::client::ClientEvent;

use crate::dto::envelope::ApiResponse;
use crate::dto::mcp::{McpServerStatusDto, StartMcpServerResult};
use crate::extractor::{ApiPath, json_body};
use crate::facade::mcp_runtime::McpRuntimeFacade;
use crate::facade::storage::mcp::McpStorageFacade;
use crate::sse::event_stream;
use crate::state::AppState;
use wind_core::models::{CreateMcpServer, McpServerParam, UpdateMcpServer};

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/mcp-servers",
            get(list_mcp_servers).post(create_mcp_server),
        )
        .route(
            "/api/v1/mcp-servers/by-name/{name}",
            get(get_mcp_server_by_name),
        )
        .route(
            "/api/v1/mcp-servers/{mcp_server_id}",
            get(get_mcp_server)
                .put(update_mcp_server)
                .delete(delete_mcp_server),
        )
        .route(
            "/api/v1/mcp-servers/{mcp_server_id}/status",
            get(get_mcp_server_status),
        )
        .route(
            "/api/v1/topics/{topic_id}/mcp-servers/{mcp_server_id}/start",
            post(start_mcp_server),
        )
        .route(
            "/api/v1/topics/{topic_id}/mcp-servers/{mcp_server_id}/stop",
            post(stop_mcp_server),
        )
}

/// SSE 单独成 router，不套 TimeoutLayer（与 chat::sse_router 一致）。
pub fn sse_router() -> Router<AppState> {
    Router::new().route("/api/v1/mcp-servers/events", get(subscribe_mcp_events))
}

#[utoipa::path(
    get,
    summary = "获取 MCP 服务列表",
    path = "/api/v1/mcp-servers",
    responses(
        (status = 200, description = "获取 MCP 服务列表", body = ApiResponse<Vec<McpServerParam>>)
    )
)]
pub(crate) async fn list_mcp_servers(
    State(core): State<Arc<WindCore>>,
) -> Json<ApiResponse<Vec<McpServerParam>>> {
    Json(McpStorageFacade::new(core).list_mcp_servers().await)
}

#[utoipa::path(
    post,
    summary = "创建 MCP 服务",
    path = "/api/v1/mcp-servers",
    responses(
        (status = 200, description = "创建 MCP 服务", body = ApiResponse<McpServerParam>)
    )
)]
pub(crate) async fn create_mcp_server(
    State(core): State<Arc<WindCore>>,
    body: Result<Json<CreateMcpServer>, JsonRejection>,
) -> Result<Json<ApiResponse<McpServerParam>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        McpStorageFacade::new(core).create_mcp_server(input).await,
    ))
}

#[utoipa::path(
    get,
    summary = "获取 MCP 服务",
    path = "/api/v1/mcp-servers/{mcp_server_id}",
    params(
        ("mcp_server_id", Path, description = "MCP 服务 ID"),
    ),
    responses(
        (status = 200, description = "获取 MCP 服务", body = ApiResponse<McpServerParam>)
    )
)]
pub(crate) async fn get_mcp_server(
    State(core): State<Arc<WindCore>>,
    ApiPath(mcp_server_id): ApiPath<i64>,
) -> Json<ApiResponse<McpServerParam>> {
    Json(
        McpStorageFacade::new(core)
            .get_mcp_server(mcp_server_id)
            .await,
    )
}

#[utoipa::path(
    put,
    summary = "更新 MCP 服务",
    path = "/api/v1/mcp-servers/{mcp_server_id}",
    params(
        ("mcp_server_id", Path, description = "MCP 服务 ID"),
    ),
    responses(
        (status = 200, description = "更新 MCP 服务", body = ApiResponse<McpServerParam>)
    )
)]
pub(crate) async fn update_mcp_server(
    State(core): State<Arc<WindCore>>,
    ApiPath(mcp_server_id): ApiPath<i64>,
    body: Result<Json<UpdateMcpServer>, JsonRejection>,
) -> Result<Json<ApiResponse<McpServerParam>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        McpStorageFacade::new(core)
            .update_mcp_server(mcp_server_id, input)
            .await,
    ))
}

#[utoipa::path(
    delete,
    summary = "删除 MCP 服务",
    path = "/api/v1/mcp-servers/{mcp_server_id}",
    params(
        ("mcp_server_id", Path, description = "MCP 服务 ID"),
    ),
    responses(
        (status = 200, description = "删除 MCP 服务", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn delete_mcp_server(
    State(core): State<Arc<WindCore>>,
    ApiPath(mcp_server_id): ApiPath<i64>,
) -> Json<ApiResponse<()>> {
    Json(
        McpStorageFacade::new(core)
            .delete_mcp_server(mcp_server_id)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "按名称获取 MCP 服务",
    path = "/api/v1/mcp-servers/by-name/{name}",
    params(
        ("name", Path, description = "MCP 服务名称"),
    ),
    responses(
        (status = 200, description = "按名称获取 MCP 服务", body = ApiResponse<McpServerParam>)
    )
)]
pub(crate) async fn get_mcp_server_by_name(
    State(core): State<Arc<WindCore>>,
    ApiPath(name): ApiPath<String>,
) -> Json<ApiResponse<McpServerParam>> {
    Json(
        McpStorageFacade::new(core)
            .get_mcp_server_by_name(name)
            .await,
    )
}

#[utoipa::path(
    post,
    summary = "启动 MCP 服务（供 topic 使用，后台连接）",
    path = "/api/v1/topics/{topic_id}/mcp-servers/{mcp_server_id}/start",
    params(
        ("topic_id", Path, description = "话题 ID，即 acquire 的 session_id"),
        ("mcp_server_id", Path, description = "MCP 服务 ID"),
    ),
    responses(
        (status = 200, description = "已受理，连接在后台进行；结果经 `GET /events` 的 Connecting/Connected/Error 事件推送", body = ApiResponse<StartMcpServerResult>),
        (status = 400, description = "参数校验失败", body = ApiResponse<Value>),
        (status = 404, description = "话题或服务不存在", body = ApiResponse<Value>),
        (status = 500, description = "内部错误", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn start_mcp_server(
    State(core): State<Arc<WindCore>>,
    ApiPath((topic_id, mcp_server_id)): ApiPath<(i64, i64)>,
) -> Json<ApiResponse<StartMcpServerResult>> {
    Json(
        McpRuntimeFacade::new(core)
            .start_server(topic_id, mcp_server_id)
            .await,
    )
}

#[utoipa::path(
    post,
    summary = "停止 MCP 服务（释放该 topic 的引用）",
    path = "/api/v1/topics/{topic_id}/mcp-servers/{mcp_server_id}/stop",
    params(
        ("topic_id", Path, description = "话题 ID，即 release 的 session_id"),
        ("mcp_server_id", Path, description = "MCP 服务 ID"),
    ),
    responses(
        (status = 200, description = "已释放该 topic 的引用（最后一个引用释放时服务停止）", body = ApiResponse<McpServerStatusDto>),
        (status = 404, description = "话题不存在、服务不存在或未被该 topic 引用", body = ApiResponse<Value>),
        (status = 500, description = "内部错误", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn stop_mcp_server(
    State(core): State<Arc<WindCore>>,
    ApiPath((topic_id, mcp_server_id)): ApiPath<(i64, i64)>,
) -> Json<ApiResponse<McpServerStatusDto>> {
    Json(
        McpRuntimeFacade::new(core)
            .stop_server(topic_id, mcp_server_id)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "查询 MCP 服务运行期状态",
    path = "/api/v1/mcp-servers/{mcp_server_id}/status",
    params(
        ("mcp_server_id", Path, description = "MCP 服务 ID"),
    ),
    responses(
        (status = 200, description = "运行期状态；未运行时 `running=false`", body = ApiResponse<McpServerStatusDto>),
        (status = 404, description = "服务不存在", body = ApiResponse<Value>),
        (status = 500, description = "内部错误", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn get_mcp_server_status(
    State(core): State<Arc<WindCore>>,
    ApiPath(mcp_server_id): ApiPath<i64>,
) -> Json<ApiResponse<McpServerStatusDto>> {
    Json(
        McpRuntimeFacade::new(core)
            .server_status(mcp_server_id)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "订阅 MCP 服务状态事件流(SSE)",
    path = "/api/v1/mcp-servers/events",
    responses(
        (status = 200, description = "订阅 MCP 服务状态事件流(SSE): 每条事件帧格式为 `event: <变体名>` / `id: <递增序号>` / `data: <ClientEvent JSON>`，帧间空行分隔。`data` 字段即 ClientEvent 结构。客户端断开连接即自动取消订阅", content(
            (ClientEvent = "text/event-stream"),
            (ClientEvent = "application/json"),
        )),
    )
)]
pub(crate) async fn subscribe_mcp_events(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let rx = state.core.registry().subscribe();
    Sse::new(event_stream(rx, state.cancel.clone()))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}
