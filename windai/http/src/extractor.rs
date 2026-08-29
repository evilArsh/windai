use axum::extract::{FromRequest, FromRequestParts, Json, Path, Query, Request};
use serde::de::DeserializeOwned;

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
