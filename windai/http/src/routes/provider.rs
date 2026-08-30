use serde_json::Value;
use std::sync::Arc;

use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::routing::{delete, get};
use axum::{Json, Router};
use serde::Deserialize;
use wind_ai::model::AdapterType;
use wind_core::WindCore;
use wind_core::models::{
    CreateCredentials, CreateJsonRule, CreateProvider, Credentials, JsonRule, Provider,
    UpdateJsonRule, UpdateProvider,
};

use crate::dto::envelope::ApiResponse;
use crate::extractor::{ApiPath, ApiQuery, json_body};
use crate::facade::storage::provider::ProviderStorageFacade;
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/providers",
            get(list_providers).post(create_provider),
        )
        .route(
            "/api/v1/providers/by-name/{name}",
            get(get_provider_by_name),
        )
        .route(
            "/api/v1/providers/{provider_id}",
            get(get_provider)
                .put(update_provider)
                .delete(delete_provider),
        )
        .route(
            "/api/v1/credentials",
            get(list_credentials).post(create_credentials),
        )
        .route(
            "/api/v1/credentials/{credential_id}",
            delete(delete_credentials),
        )
        .route(
            "/api/v1/json-rules",
            get(list_json_rules).post(create_json_rule),
        )
        .route(
            "/api/v1/json-rules/by-adapter",
            get(get_json_rule_by_adapter),
        )
        .route(
            "/api/v1/json-rules/{json_rule_id}",
            get(get_json_rule)
                .put(update_json_rule)
                .delete(delete_json_rule),
        )
}

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ProviderIdQuery {
    /// 提供商 id（必填）
    provider_id: i64,
}

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ByAdapterQuery {
    /// 提供商 id（必填）
    provider_id: i64,
    /// 适配器类型
    adapter: AdapterType,
}

#[utoipa::path(
    get,
    summary = "获取提供商列表",
    path = "/api/v1/providers",
    responses(
        (status = 200, description = "获取提供商列表", body = ApiResponse<Vec<Provider>>)
    )
)]
pub(crate) async fn list_providers(
    State(core): State<Arc<WindCore>>,
) -> Json<ApiResponse<Vec<Provider>>> {
    Json(ProviderStorageFacade::new(core).list_providers().await)
}

#[utoipa::path(
    post,
    summary = "创建提供商",
    path = "/api/v1/providers",
    responses(
        (status = 200, description = "创建提供商", body = ApiResponse<Provider>)
    )
)]
pub(crate) async fn create_provider(
    State(core): State<Arc<WindCore>>,
    body: Result<Json<CreateProvider>, JsonRejection>,
) -> Result<Json<ApiResponse<Provider>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        ProviderStorageFacade::new(core)
            .create_provider(input)
            .await,
    ))
}

#[utoipa::path(
    get,
    summary = "获取提供商",
    path = "/api/v1/providers/{provider_id}",
    params(
        ("provider_id", Path, description = "提供商 ID"),
    ),
    responses(
        (status = 200, description = "获取提供商", body = ApiResponse<Provider>)
    )
)]
pub(crate) async fn get_provider(
    State(core): State<Arc<WindCore>>,
    ApiPath(provider_id): ApiPath<i64>,
) -> Json<ApiResponse<Provider>> {
    Json(
        ProviderStorageFacade::new(core)
            .get_provider(provider_id)
            .await,
    )
}

#[utoipa::path(
    put,
    summary = "更新提供商",
    path = "/api/v1/providers/{provider_id}",
    params(
        ("provider_id", Path, description = "提供商 ID"),
    ),
    responses(
        (status = 200, description = "更新提供商", body = ApiResponse<Provider>)
    )
)]
pub(crate) async fn update_provider(
    State(core): State<Arc<WindCore>>,
    ApiPath(provider_id): ApiPath<i64>,
    body: Result<Json<UpdateProvider>, JsonRejection>,
) -> Result<Json<ApiResponse<Provider>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        ProviderStorageFacade::new(core)
            .update_provider(provider_id, input)
            .await,
    ))
}

#[utoipa::path(
    delete,
    summary = "删除提供商",
    path = "/api/v1/providers/{provider_id}",
    params(
        ("provider_id", Path, description = "提供商 ID"),
    ),
    responses(
        (status = 200, description = "删除提供商", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn delete_provider(
    State(core): State<Arc<WindCore>>,
    ApiPath(provider_id): ApiPath<i64>,
) -> Json<ApiResponse<()>> {
    Json(
        ProviderStorageFacade::new(core)
            .delete_provider(provider_id)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "按名称获取提供商",
    path = "/api/v1/providers/by-name/{name}",
    params(
        ("name", Path, description = "提供商名称"),
    ),
    responses(
        (status = 200, description = "按名称获取提供商", body = ApiResponse<Provider>)
    )
)]
pub(crate) async fn get_provider_by_name(
    State(core): State<Arc<WindCore>>,
    ApiPath(name): ApiPath<String>,
) -> Json<ApiResponse<Provider>> {
    Json(
        ProviderStorageFacade::new(core)
            .get_provider_by_name(name)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "获取凭证列表",
    path = "/api/v1/credentials",
    params(ProviderIdQuery),
    responses(
        (status = 200, description = "获取凭证列表", body = ApiResponse<Vec<Credentials>>)
    )
)]
pub(crate) async fn list_credentials(
    State(core): State<Arc<WindCore>>,
    ApiQuery(q): ApiQuery<ProviderIdQuery>,
) -> Json<ApiResponse<Vec<Credentials>>> {
    Json(
        ProviderStorageFacade::new(core)
            .list_credentials(q.provider_id)
            .await,
    )
}

#[utoipa::path(
    post,
    summary = "创建凭证",
    path = "/api/v1/credentials",
    responses(
        (status = 200, description = "创建凭证", body = ApiResponse<Credentials>)
    )
)]
pub(crate) async fn create_credentials(
    State(core): State<Arc<WindCore>>,
    body: Result<Json<CreateCredentials>, JsonRejection>,
) -> Result<Json<ApiResponse<Credentials>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        ProviderStorageFacade::new(core)
            .create_credentials(input)
            .await,
    ))
}

#[utoipa::path(
    delete,
    summary = "删除凭证",
    path = "/api/v1/credentials/{credential_id}",
    params(
        ("credential_id", Path, description = "凭证 ID"),
    ),
    responses(
        (status = 200, description = "删除凭证", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn delete_credentials(
    State(core): State<Arc<WindCore>>,
    ApiPath(credential_id): ApiPath<i64>,
) -> Json<ApiResponse<()>> {
    Json(
        ProviderStorageFacade::new(core)
            .delete_credentials(credential_id)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "获取 JSON 规则列表",
    path = "/api/v1/json-rules",
    params(ProviderIdQuery),
    responses(
        (status = 200, description = "获取 JSON 规则列表", body = ApiResponse<Vec<JsonRule>>)
    )
)]
pub(crate) async fn list_json_rules(
    State(core): State<Arc<WindCore>>,
    ApiQuery(q): ApiQuery<ProviderIdQuery>,
) -> Json<ApiResponse<Vec<JsonRule>>> {
    Json(
        ProviderStorageFacade::new(core)
            .list_json_rules(q.provider_id)
            .await,
    )
}

#[utoipa::path(
    post,
    summary = "创建 JSON 规则",
    path = "/api/v1/json-rules",
    responses(
        (status = 200, description = "创建 JSON 规则", body = ApiResponse<JsonRule>)
    )
)]
pub(crate) async fn create_json_rule(
    State(core): State<Arc<WindCore>>,
    body: Result<Json<CreateJsonRule>, JsonRejection>,
) -> Result<Json<ApiResponse<JsonRule>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        ProviderStorageFacade::new(core)
            .create_json_rule(input)
            .await,
    ))
}

#[utoipa::path(
    get,
    summary = "获取 JSON 规则",
    path = "/api/v1/json-rules/{json_rule_id}",
    params(
        ("json_rule_id", Path, description = "JSON 规则 ID"),
    ),
    responses(
        (status = 200, description = "获取 JSON 规则", body = ApiResponse<JsonRule>)
    )
)]
pub(crate) async fn get_json_rule(
    State(core): State<Arc<WindCore>>,
    ApiPath(json_rule_id): ApiPath<i64>,
) -> Json<ApiResponse<JsonRule>> {
    Json(
        ProviderStorageFacade::new(core)
            .get_json_rule(json_rule_id)
            .await,
    )
}

#[utoipa::path(
    put,
    summary = "更新 JSON 规则",
    path = "/api/v1/json-rules/{json_rule_id}",
    params(
        ("json_rule_id", Path, description = "JSON 规则 ID"),
    ),
    responses(
        (status = 200, description = "更新 JSON 规则", body = ApiResponse<JsonRule>)
    )
)]
pub(crate) async fn update_json_rule(
    State(core): State<Arc<WindCore>>,
    ApiPath(json_rule_id): ApiPath<i64>,
    body: Result<Json<UpdateJsonRule>, JsonRejection>,
) -> Result<Json<ApiResponse<JsonRule>>, Json<ApiResponse<()>>> {
    let input = json_body(body)?;
    Ok(Json(
        ProviderStorageFacade::new(core)
            .update_json_rule(json_rule_id, input)
            .await,
    ))
}

#[utoipa::path(
    delete,
    summary = "删除 JSON 规则",
    path = "/api/v1/json-rules/{json_rule_id}",
    params(
        ("json_rule_id", Path, description = "JSON 规则 ID"),
    ),
    responses(
        (status = 200, description = "删除 JSON 规则", body = ApiResponse<Value>)
    )
)]
pub(crate) async fn delete_json_rule(
    State(core): State<Arc<WindCore>>,
    ApiPath(json_rule_id): ApiPath<i64>,
) -> Json<ApiResponse<()>> {
    Json(
        ProviderStorageFacade::new(core)
            .delete_json_rule(json_rule_id)
            .await,
    )
}

#[utoipa::path(
    get,
    summary = "按适配器获取 JSON 规则",
    path = "/api/v1/json-rules/by-adapter",
    params(ByAdapterQuery),
    responses(
        (status = 200, description = "按适配器获取 JSON 规则", body = ApiResponse<JsonRule>)
    )
)]
pub(crate) async fn get_json_rule_by_adapter(
    State(core): State<Arc<WindCore>>,
    ApiQuery(q): ApiQuery<ByAdapterQuery>,
) -> Json<ApiResponse<JsonRule>> {
    Json(
        ProviderStorageFacade::new(core)
            .get_json_rule_by_adapter(q.provider_id, q.adapter)
            .await,
    )
}
