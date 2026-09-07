use serde::Deserialize;
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
use wind_core::models::{CreateMcpServer, McpServerParam, UpdateMcpServer};
use wind_mcp::client::{ClientEvent, ClientSnapshot, Prompt, Resource, Tool};

use crate::dto::envelope::ApiResponse;
use crate::dto::mcp::{McpServerStatusDto, StartMcpServerResult};
use crate::extractor::{ApiPath, ApiQuery, json_body};
use crate::facade::mcp_runtime::McpRuntimeFacade;
use crate::facade::storage::mcp::McpStorageFacade;
use crate::sse::event_stream;
use crate::state::AppState;

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
        .route(
            "/api/v1/topics/{topic_id}/mcp-servers/attach",
            post(attach_mcp_server),
        )
        // 运行时发现：registry 按 server name 操作，与上面的配置 CRUD（按 DB id）区分
        .route("/api/v1/mcp-servers/clients", get(list_mcp_clients))
        .route("/api/v1/mcp-servers/clients/{name}", get(get_mcp_client))
        .route(
            "/api/v1/mcp-servers/clients/{name}/tools",
            get(list_mcp_client_tools),
        )
        .route(
            "/api/v1/mcp-servers/clients/{name}/prompts",
            get(list_mcp_client_prompts),
        )
        .route(
            "/api/v1/mcp-servers/clients/{name}/resources",
            get(list_mcp_client_resources),
        )
        .route("/api/v1/mcp-servers/tools", get(list_all_mcp_tools))
        .route(
            "/api/v1/mcp-servers/tools-by-names",
            get(list_mcp_tools_by_names),
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

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct AttachMcpQuery {
    /// 已运行的 MCP 服务名（内建即此场景）
    name: String,
}

#[utoipa::path(
    post,
    summary = "按名称让话题引用运行中的 MCP 客户端（内建等已运行服务）",
    path = "/api/v1/topics/{topic_id}/mcp-servers/attach",
    params(
        ("topic_id", Path, description = "话题 ID，即 session_id"),
        AttachMcpQuery,
    ),
    responses(
        (status = 200, description = "已引用：只增加该 topic 的 ref_sessions，不发起连接", body = ApiResponse<McpServerStatusDto>),
        (status = 404, description = "话题不存在或服务未运行", body = ApiResponse<Value>),
        (status = 500, description = "内部错误", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn attach_mcp_server(
    State(core): State<Arc<WindCore>>,
    ApiPath(topic_id): ApiPath<i64>,
    ApiQuery(q): ApiQuery<AttachMcpQuery>,
) -> Json<ApiResponse<McpServerStatusDto>> {
    Json(
        McpRuntimeFacade::new(core)
            .attach_server(topic_id, &q.name)
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

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ToolsByNamesQuery {
    /// 逗号分隔的 MCP 服务名
    names: String,
}

#[utoipa::path(
    get,
    summary = "列出所有运行中的 MCP 客户端",
    path = "/api/v1/mcp-servers/clients",
    responses(
        (status = 200, description = "运行中的 MCP 客户端列表", body = ApiResponse<Vec<ClientSnapshot>>)
    )
)]
pub(crate) async fn list_mcp_clients(
    State(core): State<Arc<WindCore>>,
) -> Json<ApiResponse<Vec<ClientSnapshot>>> {
    Json(McpRuntimeFacade::new(core).list_clients().await)
}

#[utoipa::path(
    get,
    summary = "按名称获取运行中的 MCP 客户端",
    path = "/api/v1/mcp-servers/clients/{name}",
    params(
        ("name", Path, description = "运行中的 MCP 服务名"),
    ),
    responses(
        (status = 200, description = "客户端快照", body = ApiResponse<ClientSnapshot>),
        (status = 404, description = "服务未运行", body = ApiResponse<Value>),
        (status = 500, description = "内部错误", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn get_mcp_client(
    State(core): State<Arc<WindCore>>,
    ApiPath(name): ApiPath<String>,
) -> Json<ApiResponse<ClientSnapshot>> {
    Json(McpRuntimeFacade::new(core).get_client(&name).await)
}

#[utoipa::path(
    get,
    summary = "列出指定 MCP 客户端的工具",
    path = "/api/v1/mcp-servers/clients/{name}/tools",
    params(
        ("name", Path, description = "运行中的 MCP 服务名"),
    ),
    responses(
        (status = 200, description = "工具列表", body = ApiResponse<Vec<Tool>>),
        (status = 404, description = "服务未运行", body = ApiResponse<Value>),
        (status = 500, description = "内部错误", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn list_mcp_client_tools(
    State(core): State<Arc<WindCore>>,
    ApiPath(name): ApiPath<String>,
) -> Json<ApiResponse<Vec<Tool>>> {
    Json(McpRuntimeFacade::new(core).list_tools(&name).await)
}

#[utoipa::path(
    get,
    summary = "列出指定 MCP 客户端的提示词",
    path = "/api/v1/mcp-servers/clients/{name}/prompts",
    params(
        ("name", Path, description = "运行中的 MCP 服务名"),
    ),
    responses(
        (status = 200, description = "提示词列表", body = ApiResponse<Vec<Prompt>>),
        (status = 404, description = "服务未运行", body = ApiResponse<Value>),
        (status = 500, description = "内部错误", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn list_mcp_client_prompts(
    State(core): State<Arc<WindCore>>,
    ApiPath(name): ApiPath<String>,
) -> Json<ApiResponse<Vec<Prompt>>> {
    Json(McpRuntimeFacade::new(core).list_prompts(&name).await)
}

#[utoipa::path(
    get,
    summary = "列出指定 MCP 客户端的资源",
    path = "/api/v1/mcp-servers/clients/{name}/resources",
    params(
        ("name", Path, description = "运行中的 MCP 服务名"),
    ),
    responses(
        (status = 200, description = "资源列表", body = ApiResponse<Vec<Resource>>),
        (status = 404, description = "服务未运行", body = ApiResponse<Value>),
        (status = 500, description = "内部错误", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn list_mcp_client_resources(
    State(core): State<Arc<WindCore>>,
    ApiPath(name): ApiPath<String>,
) -> Json<ApiResponse<Vec<Resource>>> {
    Json(McpRuntimeFacade::new(core).list_resources(&name).await)
}

#[utoipa::path(
    get,
    summary = "列出所有运行中 MCP 客户端的工具",
    path = "/api/v1/mcp-servers/tools",
    responses(
        (status = 200, description = "全部工具列表", body = ApiResponse<Vec<Tool>>),
        (status = 500, description = "内部错误", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn list_all_mcp_tools(
    State(core): State<Arc<WindCore>>,
) -> Json<ApiResponse<Vec<Tool>>> {
    Json(McpRuntimeFacade::new(core).list_all_tools().await)
}

#[utoipa::path(
    get,
    summary = "按服务名批量列出工具",
    path = "/api/v1/mcp-servers/tools-by-names",
    params(
        ToolsByNamesQuery,
    ),
    responses(
        (status = 200, description = "匹配服务名的工具列表", body = ApiResponse<Vec<Tool>>),
        (status = 500, description = "内部错误", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn list_mcp_tools_by_names(
    State(core): State<Arc<WindCore>>,
    ApiQuery(q): ApiQuery<ToolsByNamesQuery>,
) -> Json<ApiResponse<Vec<Tool>>> {
    let names: Vec<String> = q
        .names
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    Json(McpRuntimeFacade::new(core).list_tools_by_names(names).await)
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
pub(crate) async fn subscribe_mcp_events(State(state): State<AppState>) -> impl IntoResponse {
    let rx = state.core.registry().subscribe();
    Sse::new(event_stream(rx, state.cancel.clone()))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}
