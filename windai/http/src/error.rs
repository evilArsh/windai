use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::dto::envelope::ApiResponse;

/// HTTP 层错误：extractor 校验失败统一封装为 `ApiResponse`（code=500）。
///
/// axum 0.8 的 handler 不会把 extractor rejection 转成 handler 错误类型，
/// 而是直接调用 rejection 的 `IntoResponse`。因此这里的错误由 `ApiQuery`/
/// `ApiPath`/`ApiJson` 包装提取器产生（见 `crate::extractor`）。
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    Query(#[from] QueryRejection),
    #[error("{0}")]
    Path(#[from] PathRejection),
    #[error("{0}")]
    Json(#[from] JsonRejection),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let msg = match &self {
            ApiError::Query(e) => e.body_text(),
            ApiError::Path(e) => e.body_text(),
            ApiError::Json(e) => e.body_text(),
        };
        // 业务层约定：所有 ApiResponse 均以 HTTP 200 承载，code 表达业务结果。
        let body = ApiResponse::<()> {
            code: 500,
            data: None,
            msg,
        };
        (StatusCode::OK, Json(body)).into_response()
    }
}
