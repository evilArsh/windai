use serde_json::Value;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use wind_core::WindCore;

use crate::dto::envelope::ApiResponse;
use crate::facade::storage::prompt::PromptStorageFacade;
use crate::state::AppState;
use wind_core::models::{CreatePromptModule, PromptModule, UpdatePromptModule};

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/prompt-modules",
            get(list_prompt_modules).post(create_prompt_module),
        )
        .route(
            "/api/v1/prompt-modules/by-key/{key}",
            get(get_prompt_module_by_key),
        )
        .route(
            "/api/v1/prompt-modules/{prompt_module_id}",
            get(get_prompt_module)
                .put(update_prompt_module)
                .delete(delete_prompt_module),
        )
}

#[utoipa::path(
    get,
    summary = "获取 Prompt 模块列表",
    path = "/api/v1/prompt-modules",
    responses(
        (status = 200, description = "获取 Prompt 模块列表", body = ApiResponse<Vec<PromptModule>>)
    )
)]
pub(crate) async fn list_prompt_modules(
    State(core): State<Arc<WindCore>>,
) -> Json<ApiResponse<Vec<PromptModule>>> {
    Json(PromptStorageFacade::new(core).list_prompt_modules().await)
}

#[utoipa::path(
    post,
    summary = "创建 Prompt 模块",
    path = "/api/v1/prompt-modules",
    responses(
        (status = 200, description = "创建 Prompt 模块", body = ApiResponse<PromptModule>)
    )
)]
pub(crate) async fn create_prompt_module(
    State(core): State<Arc<WindCore>>,
    Json(input): Json<CreatePromptModule>,
) -> Json<ApiResponse<PromptModule>> {
    Json(
        PromptStorageFacade::new(core)
            .create_prompt_module(input)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "获取 Prompt 模块",
    path = "/api/v1/prompt-modules/{prompt_module_id}",
    responses(
        (status = 200, description = "获取 Prompt 模块", body = ApiResponse<PromptModule>)
    )
)]
pub(crate) async fn get_prompt_module(
    State(core): State<Arc<WindCore>>,
    Path(prompt_module_id): Path<i64>,
) -> Json<ApiResponse<PromptModule>> {
    Json(
        PromptStorageFacade::new(core)
            .get_prompt_module(prompt_module_id)
            .await,
    )
}

#[utoipa::path(
    put,
    summary = "更新 Prompt 模块",
    path = "/api/v1/prompt-modules/{prompt_module_id}",
    responses(
        (status = 200, description = "更新 Prompt 模块", body = ApiResponse<PromptModule>)
    )
)]
pub(crate) async fn update_prompt_module(
    State(core): State<Arc<WindCore>>,
    Path(prompt_module_id): Path<i64>,
    Json(input): Json<UpdatePromptModule>,
) -> Json<ApiResponse<PromptModule>> {
    Json(
        PromptStorageFacade::new(core)
            .update_prompt_module(prompt_module_id, input)
            .await,
    )
}

#[utoipa::path(
    delete,
    summary = "删除 Prompt 模块",
    path = "/api/v1/prompt-modules/{prompt_module_id}",
    responses(
        (status = 200, description = "删除 Prompt 模块", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn delete_prompt_module(
    State(core): State<Arc<WindCore>>,
    Path(prompt_module_id): Path<i64>,
) -> Json<ApiResponse<()>> {
    Json(
        PromptStorageFacade::new(core)
            .delete_prompt_module(prompt_module_id)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "按 key 获取 Prompt 模块",
    path = "/api/v1/prompt-modules/by-key/{key}",
    responses(
        (status = 200, description = "按 key 获取 Prompt 模块", body = ApiResponse<PromptModule>)
    )
)]
pub(crate) async fn get_prompt_module_by_key(
    State(core): State<Arc<WindCore>>,
    Path(key): Path<String>,
) -> Json<ApiResponse<PromptModule>> {
    Json(
        PromptStorageFacade::new(core)
            .get_prompt_module_by_key(key)
            .await,
    )
}
