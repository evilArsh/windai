use serde_json::Value;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use wind_core::WindCore;

use crate::dto::envelope::ApiResponse;
use crate::facade::storage::mcp::McpStorageFacade;
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
    Json(input): Json<CreateMcpServer>,
) -> Json<ApiResponse<McpServerParam>> {
    Json(McpStorageFacade::new(core).create_mcp_server(input).await)
}

#[utoipa::path(
    get,
    summary = "获取 MCP 服务",
    path = "/api/v1/mcp-servers/{mcp_server_id}",
    responses(
        (status = 200, description = "获取 MCP 服务", body = ApiResponse<McpServerParam>)
    )
)]
pub(crate) async fn get_mcp_server(
    State(core): State<Arc<WindCore>>,
    Path(mcp_server_id): Path<i64>,
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
    responses(
        (status = 200, description = "更新 MCP 服务", body = ApiResponse<McpServerParam>)
    )
)]
pub(crate) async fn update_mcp_server(
    State(core): State<Arc<WindCore>>,
    Path(mcp_server_id): Path<i64>,
    Json(input): Json<UpdateMcpServer>,
) -> Json<ApiResponse<McpServerParam>> {
    Json(
        McpStorageFacade::new(core)
            .update_mcp_server(mcp_server_id, input)
            .await,
    )
}

#[utoipa::path(
    delete,
    summary = "删除 MCP 服务",
    path = "/api/v1/mcp-servers/{mcp_server_id}",
    responses(
        (status = 200, description = "删除 MCP 服务", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn delete_mcp_server(
    State(core): State<Arc<WindCore>>,
    Path(mcp_server_id): Path<i64>,
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
    responses(
        (status = 200, description = "按名称获取 MCP 服务", body = ApiResponse<McpServerParam>)
    )
)]
pub(crate) async fn get_mcp_server_by_name(
    State(core): State<Arc<WindCore>>,
    Path(name): Path<String>,
) -> Json<ApiResponse<McpServerParam>> {
    Json(
        McpStorageFacade::new(core)
            .get_mcp_server_by_name(name)
            .await,
    )
}
