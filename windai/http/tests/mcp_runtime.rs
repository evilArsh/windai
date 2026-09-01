//! MCP 服务运行时端点测试（start/stop/status + SSE）。无 .env。
//!
//! session 语义：`topic_id` 即 `acquire`/`release` 的 `session_id`，
//! 同一 mcp server 可被多个 topic 共享引用。
mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use wind_core::WindCore;
use wind_core::models::{CreateMcpServer, CreateTopic, McpServerParam, Topic};
use wind_http::config::AppConfig;
use wind_http::routes::mcp;
use wind_http::state::AppState;
use wind_mcp::client::TransportType;

fn test_router(core: Arc<WindCore>) -> Router {
    let state = AppState::new(AppConfig::default(), core, 0);
    Router::<AppState>::new()
        .merge(mcp::sse_router())
        .merge(mcp::router())
        .with_state(state)
}

async fn create_topic(core: &Arc<WindCore>) -> Topic {
    core.storage()
        .topic()
        .create(CreateTopic {
            parent_id: None,
            binding_id: None,
            label: "mcp-runtime-test".to_string(),
            icon: None,
        })
        .await
        .unwrap()
}

async fn create_stdio_server(
    core: &Arc<WindCore>,
    name: &str,
    command: Option<&str>,
) -> McpServerParam {
    core.storage()
        .mcp()
        .create(CreateMcpServer {
            r#type: TransportType::Stdio,
            name: name.to_string(),
            url: None,
            description: None,
            command: command.map(|s| s.to_string()),
            args: Some(vec![]),
            env: None,
        })
        .await
        .unwrap()
}

async fn read_json<T: serde::de::DeserializeOwned>(
    res: axum::response::Response,
) -> serde_json::Value {
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn start_unknown_topic_returns_404() {
    let core = common::test_core().await;
    let app = test_router(core);
    let res = app
        .oneshot(
            Request::post("/api/v1/topics/999999/mcp-servers/999999/start")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = read_json::<()>(res).await;
    assert_eq!(body["code"], 404);
}

#[tokio::test]
async fn start_unknown_server_returns_404() {
    let core = common::test_core().await;
    let topic = create_topic(&core).await;
    let app = test_router(core);
    let res = app
        .oneshot(
            Request::post(format!(
                "/api/v1/topics/{}/mcp-servers/999999/start",
                topic.id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    let body = read_json::<()>(res).await;
    assert_eq!(body["code"], 404);
}

#[tokio::test]
async fn start_valid_server_returns_accepted() {
    let core = common::test_core().await;
    let topic = create_topic(&core).await;
    let srv = create_stdio_server(&core, "start-ok", Some("/nonexistent-cmd")).await;
    let app = test_router(core);
    let res = app
        .oneshot(
            Request::post(format!(
                "/api/v1/topics/{}/mcp-servers/{}/start",
                topic.id, srv.id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = read_json::<()>(res).await;
    assert_eq!(body["code"], 200);
    assert_eq!(body["data"]["accepted"], true);
    assert_eq!(body["data"]["name"], "start-ok");
}

#[tokio::test]
async fn start_stdio_missing_command_returns_400() {
    let core = common::test_core().await;
    let topic = create_topic(&core).await;
    let srv = create_stdio_server(&core, "no-cmd", None).await;
    let app = test_router(core);
    let res = app
        .oneshot(
            Request::post(format!(
                "/api/v1/topics/{}/mcp-servers/{}/start",
                topic.id, srv.id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    let body = read_json::<()>(res).await;
    assert_eq!(body["code"], 400);
}

#[tokio::test]
async fn stop_unknown_topic_returns_404() {
    let core = common::test_core().await;
    let app = test_router(core);
    let res = app
        .oneshot(
            Request::post("/api/v1/topics/999999/mcp-servers/999999/stop")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = read_json::<()>(res).await;
    assert_eq!(body["code"], 404);
}

#[tokio::test]
async fn stop_not_running_returns_not_found() {
    let core = common::test_core().await;
    let topic = create_topic(&core).await;
    let srv = create_stdio_server(&core, "never-started", Some("/nonexistent-cmd")).await;
    let app = test_router(core);
    let res = app
        .oneshot(
            Request::post(format!(
                "/api/v1/topics/{}/mcp-servers/{}/stop",
                topic.id, srv.id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    let body = read_json::<()>(res).await;
    assert_eq!(body["code"], 404);
    assert!(
        body["msg"].as_str().unwrap().contains("not running"),
        "msg: {}",
        body["msg"]
    );
}

#[tokio::test]
async fn status_unknown_id_returns_404() {
    let core = common::test_core().await;
    let app = test_router(core);
    let res = app
        .oneshot(
            Request::get("/api/v1/mcp-servers/999999/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = read_json::<()>(res).await;
    assert_eq!(body["code"], 404);
}

#[tokio::test]
async fn status_not_running_reports_running_false() {
    let core = common::test_core().await;
    let srv = create_stdio_server(&core, "status-idle", Some("/nonexistent-cmd")).await;
    let app = test_router(core);
    let res = app
        .oneshot(
            Request::get(format!("/api/v1/mcp-servers/{}/status", srv.id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = read_json::<()>(res).await;
    assert_eq!(body["code"], 200);
    assert_eq!(body["data"]["running"], false);
    assert_eq!(body["data"]["name"], "status-idle");
}

#[tokio::test]
async fn sse_stream_ends_when_cancel_token_fired() {
    let core = common::test_core().await;
    let cancel = CancellationToken::new();
    let state = AppState::with_cancel(AppConfig::default(), core, 0, cancel.clone());
    cancel.cancel();
    let app = Router::<AppState>::new()
        .merge(mcp::sse_router())
        .with_state(state);

    let res = app
        .oneshot(
            Request::get("/api/v1/mcp-servers/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 已取消 token：流应立即结束，body 可被完整读回（无限流会挂起）。
    let read = tokio::time::timeout(
        Duration::from_secs(2),
        axum::body::to_bytes(res.into_body(), 1024),
    )
    .await;
    assert!(read.is_ok(), "cancel token 触发后 SSE 流必须终止");
}

#[tokio::test]
async fn sse_endpoint_returns_event_stream_content_type() {
    let core = common::test_core().await;
    let app = test_router(core);
    let res = app
        .oneshot(
            Request::get("/api/v1/mcp-servers/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let content_type = res.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(
        content_type.starts_with("text/event-stream"),
        "content-type: {content_type}"
    );
}

#[tokio::test]
async fn sse_streams_connecting_and_error_for_failed_start() {
    let core = common::test_core().await;
    let topic = create_topic(&core).await;
    let srv = create_stdio_server(&core, "sse-bad", Some("/nonexistent-cmd")).await;
    let app = test_router(core);

    // 先订阅，再触发事件，避免错过广播。
    let res = app
        .clone()
        .oneshot(
            Request::get("/api/v1/mcp-servers/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let mut body = res.into_body().into_data_stream();

    // start 失败的命令：registry 会广播 Connecting 后紧跟 Error。
    let start = app
        .oneshot(
            Request::post(format!(
                "/api/v1/topics/{}/mcp-servers/{}/start",
                topic.id, srv.id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(start.status(), StatusCode::OK);

    let mut buf = String::new();
    let read = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(chunk) = body.next().await {
            let chunk = chunk.expect("body chunk");
            buf.push_str(&String::from_utf8_lossy(&chunk));
            if buf.contains("connecting") && buf.contains("error") {
                break;
            }
        }
    })
    .await;

    assert!(
        read.is_ok(),
        "SSE stream did not emit connecting/error: {buf}"
    );
    assert!(buf.contains("connecting"), "buf: {buf}");
    assert!(buf.contains("error"), "buf: {buf}");
}
