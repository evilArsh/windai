//! MCP 运行时发现端点测试（clients / tools / prompts / resources）。无 .env。
//!
//! registry 按 server name 操作。测试覆盖空 registry 与「服务未运行」的语义；
//! 需要真实 MCP 服务的完整路径用 `#[ignore]`（需 npx），仓库惯例与 core 测试一致。
mod common;

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;
use wind_core::WindCore;
use wind_core::models::CreateTopic;
use wind_http::config::AppConfig;
use wind_http::routes::mcp;
use wind_http::state::AppState;

fn test_router(core: Arc<WindCore>) -> Router {
    let state = AppState::new(AppConfig::default(), core, 0);
    Router::<AppState>::new()
        .merge(mcp::router())
        .with_state(state)
}

async fn get(app: &Router, path: &str) -> serde_json::Value {
    let res = app
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn post(app: &Router, path: &str) -> serde_json::Value {
    let res = app
        .clone()
        .oneshot(Request::post(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn create_topic(core: &Arc<WindCore>, label: &str) -> i64 {
    core.storage()
        .topic()
        .create(CreateTopic {
            parent_id: None,
            binding_id: None,
            label: label.to_string(),
            icon: None,
        })
        .await
        .unwrap()
        .id
}

#[tokio::test]
async fn list_clients_returns_registered_builtin_clients() {
    let core = common::test_core().await;
    let app = test_router(core);
    let body = get(&app, "/api/v1/mcp-servers/clients").await;
    assert_eq!(body["code"], 200, "body: {body}");
    let names: Vec<String> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap().to_string())
        .collect();
    // WindCore 初始化会注册 fs / skills 两个内建 client
    assert!(
        names.contains(&"wind-mcp-skills".to_string()),
        "names: {names:?}"
    );
    assert!(
        names.contains(&"wind-mcp-fs".to_string()),
        "names: {names:?}"
    );
}

#[tokio::test]
async fn get_client_running_builtin_returns_200() {
    let core = common::test_core().await;
    let app = test_router(core);
    let body = get(&app, "/api/v1/mcp-servers/clients/wind-mcp-skills").await;
    assert_eq!(body["code"], 200, "body: {body}");
    assert_eq!(body["data"]["name"], "wind-mcp-skills");
}

#[tokio::test]
async fn get_client_not_running_returns_404() {
    let core = common::test_core().await;
    let app = test_router(core);
    let body = get(&app, "/api/v1/mcp-servers/clients/nonexistent").await;
    assert_eq!(body["code"], 404);
    assert!(body["msg"].as_str().unwrap().contains("not running"));
}

#[tokio::test]
async fn list_tools_not_running_returns_404() {
    let core = common::test_core().await;
    let app = test_router(core);
    let body = get(&app, "/api/v1/mcp-servers/clients/nonexistent/tools").await;
    assert_eq!(body["code"], 404);
}

#[tokio::test]
async fn list_prompts_not_running_returns_404() {
    let core = common::test_core().await;
    let app = test_router(core);
    let body = get(&app, "/api/v1/mcp-servers/clients/nonexistent/prompts").await;
    assert_eq!(body["code"], 404);
}

#[tokio::test]
async fn list_resources_not_running_returns_404() {
    let core = common::test_core().await;
    let app = test_router(core);
    let body = get(&app, "/api/v1/mcp-servers/clients/nonexistent/resources").await;
    assert_eq!(body["code"], 404);
}

#[tokio::test]
async fn list_all_tools_includes_builtin_tools() {
    let core = common::test_core().await;
    let app = test_router(core);
    let body = get(&app, "/api/v1/mcp-servers/tools").await;
    assert_eq!(body["code"], 200, "body: {body}");
    assert!(!body["data"].as_array().unwrap().is_empty(), "body: {body}");
}

#[tokio::test]
async fn list_client_tools_includes_builtin_tools() {
    let core = common::test_core().await;
    let app = test_router(core);
    let body = get(&app, "/api/v1/mcp-servers/clients/wind-mcp-skills/tools").await;
    assert_eq!(body["code"], 200, "body: {body}");
    assert!(!body["data"].as_array().unwrap().is_empty(), "body: {body}");
}

#[tokio::test]
async fn list_tools_by_names_unknown_returns_empty() {
    let core = common::test_core().await;
    let app = test_router(core);
    let body = get(
        &app,
        "/api/v1/mcp-servers/tools-by-names?names=nope,missing",
    )
    .await;
    assert_eq!(body["code"], 200, "body: {body}");
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
}

// ---------------------------------------------------------------------------
// attach：按 name 让 topic 引用已运行的 client（内建等）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn attach_to_unknown_topic_returns_404() {
    let core = common::test_core().await;
    let app = test_router(core);
    let body = post(
        &app,
        "/api/v1/topics/999999/mcp-servers/attach?name=wind-mcp-skills",
    )
    .await;
    assert_eq!(body["code"], 404, "body: {body}");
}

#[tokio::test]
async fn attach_unknown_server_returns_404() {
    let core = common::test_core().await;
    let topic = create_topic(&core, "attach").await;
    let app = test_router(core);
    let body = post(
        &app,
        &format!("/api/v1/topics/{topic}/mcp-servers/attach?name=nope"),
    )
    .await;
    assert_eq!(body["code"], 404, "body: {body}");
    assert!(body["msg"].as_str().unwrap().contains("not running"));
}

#[tokio::test]
async fn attach_builtin_adds_topic_ref() {
    let core = common::test_core().await;
    let topic = create_topic(&core, "attach-builtin").await;
    let app = test_router(core);
    let body = post(
        &app,
        &format!("/api/v1/topics/{topic}/mcp-servers/attach?name=wind-mcp-skills"),
    )
    .await;
    assert_eq!(body["code"], 200, "body: {body}");
    assert_eq!(body["data"]["running"], true);
    assert_eq!(body["data"]["name"], "wind-mcp-skills");
    let refs = body["data"]["ref_sessions"].as_array().unwrap();
    let topic_str = topic.to_string();
    assert!(
        refs.iter().any(|s| s.as_str() == Some(topic_str.as_str())),
        "refs: {refs:?}"
    );
}

#[tokio::test]
async fn attach_builtin_twice_is_idempotent() {
    let core = common::test_core().await;
    let topic = create_topic(&core, "attach-idem").await;
    let app = test_router(core);
    let path = format!("/api/v1/topics/{topic}/mcp-servers/attach?name=wind-mcp-skills");
    let first = post(&app, &path).await;
    assert_eq!(first["code"], 200, "body: {first}");
    let count_after_first = first["data"]["ref_sessions"].as_array().unwrap().len();
    let second = post(&app, &path).await;
    assert_eq!(second["code"], 200, "body: {second}");
    let count_after_second = second["data"]["ref_sessions"].as_array().unwrap().len();
    assert_eq!(count_after_first, count_after_second);
}
