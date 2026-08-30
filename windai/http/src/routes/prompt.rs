use serde_json::Value;
use std::sync::Arc;

use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::routing::get;
use axum::{Json, Router};
use wind_core::WindCore;

use crate::dto::envelope::ApiResponse;
use crate::extractor::{ApiPath, json_body};
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
    body: Result<Json<CreatePromptModule>, JsonRejection>,
) -> Result<Json<ApiResponse<PromptModule>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        PromptStorageFacade::new(core)
            .create_prompt_module(input)
            .await,
    ))
}

#[utoipa::path(
    get,
    summary = "获取 Prompt 模块",
    path = "/api/v1/prompt-modules/{prompt_module_id}",
    params(
        ("prompt_module_id", Path, description = "Prompt 模块 ID"),
    ),
    responses(
        (status = 200, description = "获取 Prompt 模块", body = ApiResponse<PromptModule>)
    )
)]
pub(crate) async fn get_prompt_module(
    State(core): State<Arc<WindCore>>,
    ApiPath(prompt_module_id): ApiPath<i64>,
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
    params(
        ("prompt_module_id", Path, description = "Prompt 模块 ID"),
    ),
    responses(
        (status = 200, description = "更新 Prompt 模块", body = ApiResponse<PromptModule>)
    )
)]
pub(crate) async fn update_prompt_module(
    State(core): State<Arc<WindCore>>,
    ApiPath(prompt_module_id): ApiPath<i64>,
    body: Result<Json<UpdatePromptModule>, JsonRejection>,
) -> Result<Json<ApiResponse<PromptModule>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        PromptStorageFacade::new(core)
            .update_prompt_module(prompt_module_id, input)
            .await,
    ))
}

#[utoipa::path(
    delete,
    summary = "删除 Prompt 模块",
    path = "/api/v1/prompt-modules/{prompt_module_id}",
    params(
        ("prompt_module_id", Path, description = "Prompt 模块 ID"),
    ),
    responses(
        (status = 200, description = "删除 Prompt 模块", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn delete_prompt_module(
    State(core): State<Arc<WindCore>>,
    ApiPath(prompt_module_id): ApiPath<i64>,
) -> Json<ApiResponse<()>> {
    Json(
        PromptStorageFacade::new(core)
            .delete_prompt_module(prompt_module_id)
            .await,
    )
}
