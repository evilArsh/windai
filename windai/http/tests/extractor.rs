mod common;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;
use wind_http::config::AppConfig;
use wind_http::routes::{agent, chat, health, mcp, model, prompt, provider, topic};
use wind_http::state::AppState;

async fn test_router() -> Router {
    Router::<AppState>::new()
        .merge(health::router())
        .merge(topic::router())
        .merge(chat::router())
        .merge(provider::router())
        .merge(model::router())
        .merge(mcp::router())
        .merge(prompt::router())
        .merge(agent::router())
        .with_state(AppState::new(
            AppConfig::default(),
            common::test_core().await,
            0,
        ))
}

/// 发送请求并解析统一信封响应（HTTP 200 + code=500）。
async fn assert_unified_error(router: &Router, req: Request<Body>, msg_contains: &str) {
    let res = router.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["code"], 500);
    assert!(body["data"].is_null());
    assert!(
        body["msg"].as_str().unwrap().contains(msg_contains),
        "msg {:?} should contain {:?}",
        body["msg"],
        msg_contains
    );
}

#[tokio::test]
async fn query_param_validation_errors_are_unified() {
    let router = test_router().await;

    // provider_id 是字符串
    assert_unified_error(
        &router,
        Request::builder()
            .uri("/api/v1/credentials?provider_id=abc")
            .body(Body::empty())
            .unwrap(),
        "invalid digit",
    )
    .await;

    // provider_id 数字过长
    assert_unified_error(
        &router,
        Request::builder()
            .uri("/api/v1/credentials?provider_id=99999999999999999999999")
            .body(Body::empty())
            .unwrap(),
        "number too large",
    )
    .await;
}

#[tokio::test]
async fn path_param_validation_errors_are_unified() {
    let router = test_router().await;

    assert_unified_error(
        &router,
        Request::builder()
            .uri("/api/v1/topics/abc")
            .body(Body::empty())
            .unwrap(),
        "Cannot parse `abc`",
    )
    .await;
}

#[tokio::test]
async fn json_body_errors_are_unified() {
    let router = test_router().await;

    assert_unified_error(
        &router,
        Request::builder()
            .method("POST")
            .uri("/api/v1/topics")
            .header("content-type", "application/json")
            .body(Body::from("not json"))
            .unwrap(),
        "Failed to parse the request body as JSON",
    )
    .await;
}
