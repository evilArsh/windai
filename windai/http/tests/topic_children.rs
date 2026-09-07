//! 子话题查询端点测试（GET /api/v1/topics/{topic_id}/children）。无 .env。
//!
//! 语义：返回 `parent_id == {topic_id}` 的直接子话题；父话题不存在返回 404；
//! 现有 `GET /api/v1/topics`（根列表）保持不变。
mod common;

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;
use wind_core::WindCore;
use wind_core::models::CreateTopic;
use wind_http::config::AppConfig;
use wind_http::routes::topic;
use wind_http::state::AppState;

fn test_router(core: Arc<WindCore>) -> Router {
    let state = AppState::new(AppConfig::default(), core, 0);
    Router::<AppState>::new()
        .merge(topic::router())
        .with_state(state)
}

async fn create_topic(core: &Arc<WindCore>, parent_id: Option<i64>, label: &str) -> i64 {
    core.storage()
        .topic()
        .create(CreateTopic {
            parent_id,
            binding_id: None,
            label: label.to_string(),
            icon: None,
        })
        .await
        .unwrap()
        .id
}

async fn read_json(res: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn children_of_unknown_topic_returns_404() {
    let core = common::test_core().await;
    let app = test_router(core);
    let res = app
        .oneshot(
            Request::get("/api/v1/topics/999999/children")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = read_json(res).await;
    assert_eq!(body["code"], 404);
}

#[tokio::test]
async fn children_returns_only_direct_children() {
    let core = common::test_core().await;
    let root_a = create_topic(&core, None, "root-a").await;
    let root_b = create_topic(&core, None, "root-b").await;
    let child_1 = create_topic(&core, Some(root_a), "child-1").await;
    let child_2 = create_topic(&core, Some(root_a), "child-2").await;
    // 属于 root_b 的子话题，不应出现在 root_a 的 children 中
    let _other = create_topic(&core, Some(root_b), "child-b").await;

    let app = test_router(core);
    let res = app
        .oneshot(
            Request::get(format!("/api/v1/topics/{root_a}/children"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = read_json(res).await;
    assert_eq!(body["code"], 200);
    let data = body["data"].as_array().unwrap();
    let ids: Vec<i64> = data.iter().map(|t| t["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![child_1, child_2]);
    assert!(
        data.iter().all(|t| t["parent_id"] == root_a),
        "children must share the queried parent"
    );
}

#[tokio::test]
async fn children_of_topic_without_children_returns_empty() {
    let core = common::test_core().await;
    let root = create_topic(&core, None, "root").await;
    let app = test_router(core);
    let res = app
        .oneshot(
            Request::get(format!("/api/v1/topics/{root}/children"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = read_json(res).await;
    assert_eq!(body["code"], 200);
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
}
