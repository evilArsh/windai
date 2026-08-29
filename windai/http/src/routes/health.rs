use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::routing::get;
use serde_json::Value;

use crate::dto::envelope::ApiResponse;
use crate::facade::system::SystemFacade;
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/healthz", get(health))
}

#[utoipa::path(
    get,
    summary = "服务健康检查",
    path = "/healthz",
    responses(
        (status = 200, description = "服务健康检查", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn health(State(state): State<AppState>) -> Json<ApiResponse<Value>> {
    let started_at = state.started_at;
    let facade = SystemFacade::new(state.core, started_at);
    Json(facade.health())
}
