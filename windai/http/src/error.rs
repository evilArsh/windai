use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use tower::timeout::error::Elapsed;

use crate::dto::envelope::ApiResponse;

/// HTTP 层错误：协议层错误走真实 HTTP status，业务错误由 facade 收口成 `ApiResponse`。
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error(transparent)]
    JsonRejection(#[from] JsonRejection),
    #[error("not found")]
    NotFound,
    #[error(transparent)]
    Timeout(#[from] Elapsed),
    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            ApiError::JsonRejection(e) => (e.status(), e.body_text()),
            ApiError::NotFound => (StatusCode::NOT_FOUND, "not found".into()),
            ApiError::Timeout(_) => (StatusCode::REQUEST_TIMEOUT, "request timeout".into()),
            ApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
        };
        let body = ApiResponse::<()> {
            code: status.as_u16(),
            data: None,
            msg,
        };
        (status, Json(body)).into_response()
    }
}
