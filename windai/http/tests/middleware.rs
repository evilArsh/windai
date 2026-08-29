use axum::{Router, body::Body, http::Request};
use tower::ServiceExt;
use wind_http::middleware::{request_id::request_id_layers, trace::trace_layer};

#[tokio::test]
async fn request_id_propagates() {
    let (set_id, propagate) = request_id_layers();
    // axum 链式 `.layer()` 自底向上执行，最后添加的层在最外层；
    // 必须让 `set_id` 先执行（最外层）以生成请求头，`propagate` 随后读取并回传。
    let app = Router::new()
        .route("/x", axum::routing::get(|| async { "ok" }))
        .layer(propagate)
        .layer(set_id);
    let res = app
        .oneshot(Request::get("/x").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert!(res.headers().contains_key("x-request-id"));
}

#[tokio::test]
async fn trace_layer_smoke() {
    let app = Router::new()
        .route("/x", axum::routing::get(|| async { "ok" }))
        .layer(trace_layer());
    let res = app
        .oneshot(Request::get("/x").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
}
