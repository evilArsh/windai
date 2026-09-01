use axum::Json;
use axum::Router;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use tower_http::timeout::TimeoutLayer;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::dto::envelope::ApiResponse;
use crate::middleware::request_id::request_id_layers;
use crate::middleware::timeout::CRUD_TIMEOUT;
use crate::middleware::trace::trace_layer;
use crate::openapi::ApiDoc;
use crate::routes::{agent, chat, health, mcp, model, prompt, provider, topic};
use crate::state::AppState;

/// 构建未绑定 state 的路由树，子路由组合后统一 `.with_state`。
pub fn build_router() -> Router<AppState> {
    let (set_id, propagate) = request_id_layers();

    // CRUD 路由统一套 timeout + trace + request-id。
    //
    // tower-http 0.7 的 `TimeoutLayer` 不接受错误处理闭包，超时只会返回指定
    // 状态码 + 空 body（无法塞 JSON envelope），故用 `with_status_code` 指定 408。
    let api = Router::new()
        .merge(topic::router())
        .merge(chat::router())
        .merge(provider::router())
        .merge(model::router())
        .merge(mcp::router())
        .merge(prompt::router())
        .merge(agent::router())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            CRUD_TIMEOUT,
        ))
        // axum layer 自底向上执行，后 add 的在外层。顺序：timeout 最内（只包 handler），
        // propagate 读 header、set_id 生成 header（须在最外层），trace 最外记录耗时。
        .layer(propagate)
        .layer(set_id)
        .layer(trace_layer());

    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .merge(health::router())
        .merge(chat::sse_router()) // SSE 不套 timeout
        .merge(mcp::sse_router()) // MCP 状态 SSE
        .merge(api)
        .fallback(fallback_404)
}

async fn fallback_404(_req: Request) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ApiResponse::<()>::not_found("route not found")),
    )
        .into_response()
}

/// 组装最终 `Router<()>`，供 `axum::serve` 或 `tower::ServiceExt::oneshot` 测试。
pub fn app(state: AppState) -> Router {
    build_router().with_state(state)
}
