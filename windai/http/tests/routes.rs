mod common;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use wind_http::app::app;
use wind_http::config::AppConfig;
use wind_http::routes::{agent, chat, health, mcp, model, prompt, provider, topic};
use wind_http::state::AppState;

async fn test_state() -> AppState {
    AppState::new(AppConfig::default(), common::test_core().await, 0)
}

/// 直接拼装已存在的路由（本任务尚未有 `app()`——它在 Task 10 才建）。
/// 后续 Task 8/9 在本函数里追加 `.merge(...)` 各自的 router。
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
        .with_state(test_state().await)
}

#[tokio::test]
async fn healthz_returns_200() {
    let app = test_router().await;
    let res = app
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn create_topic_via_http() {
    let app = test_router().await;
    let res = app
        .oneshot(
            Request::post("/api/v1/topics")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"label":"demo"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body["code"], 200);
    assert_eq!(body["data"]["label"], "demo");
}

#[tokio::test]
async fn provider_via_http() {
    let app = test_router().await;
    let res = app
        .oneshot(
            Request::post("/api/v1/providers")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"openai","base_url":"https://x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body["code"], 200);
    assert_eq!(body["data"]["name"], "openai");
}

#[tokio::test]
async fn mcp_server_via_http() {
    let app = test_router().await;
    let res = app.oneshot(Request::post("/api/v1/mcp-servers")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"type":"stdio","name":"srv","command":"npx","args":["-y","@mcp/everything"]}"#)).unwrap()).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body["code"], 200);
    assert_eq!(body["data"]["name"], "srv");
}

#[tokio::test]
async fn unknown_route_returns_404() {
    let app = app(test_state().await);
    let res = app
        .oneshot(Request::get("/nope").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body["code"], 404);
    assert_eq!(body["msg"], "route not found");
}
