use axum::http::HeaderName;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};

pub const X_REQUEST_ID: &str = "x-request-id";

/// 返回 (set_id, propagate)。set_id 生成 UUID，propagate 回传请求头。
pub fn request_id_layers() -> (SetRequestIdLayer<MakeRequestUuid>, PropagateRequestIdLayer) {
    let header = HeaderName::from_static(X_REQUEST_ID);
    (
        SetRequestIdLayer::new(header.clone(), MakeRequestUuid),
        PropagateRequestIdLayer::new(header),
    )
}
