use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, FromRequestParts, Json, Path, Query, Request};
use serde::de::DeserializeOwned;

use crate::dto::envelope::ApiResponse;
use crate::error::ApiError;

/// 带统一错误封装的 `Query` 提取器。
///
/// 反序列化失败时返回 `ApiError`（统一封装为 `ApiResponse`，code=500），
/// 而非 axum 默认的纯文本 400 响应。仅实现 `FromRequestParts`：
/// axum_core 提供了 `FromRequestParts → FromRequest` 的 blanket impl，
/// 因此可作为 handler 的末位或非末位参数使用。
pub struct ApiQuery<T>(pub T);

impl<S, T> FromRequestParts<S> for ApiQuery<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let Query(value) = Query::<T>::from_request_parts(parts, state).await?;
        Ok(Self(value))
    }
}

/// 带统一错误封装的 `Path` 提取器。
pub struct ApiPath<T>(pub T);

impl<S, T> FromRequestParts<S> for ApiPath<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let Path(value) = Path::<T>::from_request_parts(parts, state).await?;
        Ok(Self(value))
    }
}

/// 带统一错误封装的 `Json` 提取器。
///
/// 注意：`ApiJson` 会让 utoipa 无法从 handler 签名识别 requestBody（自定义类型
/// 对 utoipa 宏不透明）。需要生成 OpenAPI 文档的 body 参数时，请改用原生
/// `Result<Json<T>, JsonRejection>` 提取器参数配合 [`json_body`]。
pub struct ApiJson<T>(pub T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(value) = Json::<T>::from_request(req, state).await?;
        Ok(Self(value))
    }
}

/// 从 axum 原生 `Json` 提取器的 `Result<_, JsonRejection>` 中取出 body 值。
///
/// handler 参数保持 `Result<Json<T>, JsonRejection>`（axum 官方「Handling
/// extractor rejections」模式），以便 utoipa 从签名识别 requestBody；这里只把
/// 失败分支统一收口为 `ApiResponse`（code=500）。返回值可直接用 `?` 短路：
///
/// ```ignore
/// let input = json_body(body)?;
/// ```
pub(crate) fn json_body<T>(
    body: Result<Json<T>, JsonRejection>,
) -> Result<T, Json<ApiResponse<()>>> {
    body.map(|Json(value)| value)
        .map_err(|rejection| Json(ApiResponse::internal(rejection.body_text())))
}
