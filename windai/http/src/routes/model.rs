use serde_json::Value;
use std::sync::Arc;

use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::routing::get;
use axum::{Json, Router};
use wind_core::WindCore;

use crate::dto::envelope::ApiResponse;
use crate::extractor::{ApiPath, json_body};
use crate::facade::storage::model::ModelStorageFacade;
use crate::state::AppState;
use wind_core::models::{CreateModel, Model, UpdateModel};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/v1/models", get(list_models).post(create_model))
        .route(
            "/api/v1/models/{model_id}",
            get(get_model).put(update_model).delete(delete_model),
        )
}

#[utoipa::path(
    get,
    summary = "获取模型列表",
    path = "/api/v1/models",
    responses(
        (status = 200, description = "获取模型列表", body = ApiResponse<Vec<Model>>)
    )
)]
pub(crate) async fn list_models(
    State(core): State<Arc<WindCore>>,
) -> Json<ApiResponse<Vec<Model>>> {
    Json(ModelStorageFacade::new(core).list_models().await)
}

#[utoipa::path(
    post,
    summary = "创建模型",
    path = "/api/v1/models",
    responses(
        (status = 200, description = "创建模型", body = ApiResponse<Model>)
    )
)]
pub(crate) async fn create_model(
    State(core): State<Arc<WindCore>>,
    body: Result<Json<CreateModel>, JsonRejection>,
) -> Result<Json<ApiResponse<Model>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        ModelStorageFacade::new(core).create_model(input).await,
    ))
}

#[utoipa::path(
    get,
    summary = "获取模型",
    path = "/api/v1/models/{model_id}",
    params(
        ("model_id", Path, description = "模型 ID"),
    ),
    responses(
        (status = 200, description = "获取模型", body = ApiResponse<Model>)
    )
)]
pub(crate) async fn get_model(
    State(core): State<Arc<WindCore>>,
    ApiPath(model_id): ApiPath<i64>,
) -> Json<ApiResponse<Model>> {
    Json(ModelStorageFacade::new(core).get_model(model_id).await)
}

#[utoipa::path(
    put,
    summary = "更新模型",
    path = "/api/v1/models/{model_id}",
    params(
        ("model_id", Path, description = "模型 ID"),
    ),
    responses(
        (status = 200, description = "更新模型", body = ApiResponse<Model>)
    )
)]
pub(crate) async fn update_model(
    State(core): State<Arc<WindCore>>,
    ApiPath(model_id): ApiPath<i64>,
    body: Result<Json<UpdateModel>, JsonRejection>,
) -> Result<Json<ApiResponse<Model>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        ModelStorageFacade::new(core)
            .update_model(model_id, input)
            .await,
    ))
}

#[utoipa::path(
    delete,
    summary = "删除模型",
    path = "/api/v1/models/{model_id}",
    params(
        ("model_id", Path, description = "模型 ID"),
    ),
    responses(
        (status = 200, description = "删除模型", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn delete_model(
    State(core): State<Arc<WindCore>>,
    ApiPath(model_id): ApiPath<i64>,
) -> Json<ApiResponse<()>> {
    Json(ModelStorageFacade::new(core).delete_model(model_id).await)
}
