//! Agent 实例 / 话题能力映射端点测试。无 .env
//!
//! 语义：消息按实例归属，`/agent-instances` 不区分主实例与子实例 ——
//! 两者都按 id 查询实例本身与其消息；
//! `/agent-maps` 是话题与 AgentDefinition 的能力映射，一个定义在一个话题下至多映射一次
mod common;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;
use wind_ai::message::{Content, Message as AiMessage, Role};
use wind_core::WindCore;
use wind_core::agent::helper::{create_child_instance, get_or_create_main_instance};
use wind_core::models::{
    AgentDefinitionData, AgentMode, CreateAgentDefinition, CreateMessage, CreateTopic,
};
use wind_http::app::app;
use wind_http::config::AppConfig;
use wind_http::state::AppState;

fn test_router(core: Arc<WindCore>) -> Router {
    app(AppState::new(AppConfig::default(), core))
}

/// 发一次请求并解析响应 envelope，`body` 为 `None` 时带空 body
async fn call(core: &Arc<WindCore>, method: &str, uri: &str, body: Option<&str>) -> Value {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(json) => {
            builder = builder.header("content-type", "application/json");
            Body::from(json.to_string())
        }
        None => Body::empty(),
    };
    let res = test_router(core.clone())
        .oneshot(builder.body(body).expect("构造请求"))
        .await
        .expect("请求执行");
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .expect("读取响应体");
    serde_json::from_slice(&bytes).expect("解析响应 JSON")
}

/// 发一次请求只取 HTTP 状态码，供断言协议层错误（业务错误一律 200）
async fn status_of(core: &Arc<WindCore>, method: &str, uri: &str) -> StatusCode {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .expect("构造请求");
    test_router(core.clone())
        .oneshot(request)
        .await
        .expect("请求执行")
        .status()
}

/// 取 `data` 数组各元素的 id
fn ids_of(body: &Value) -> Vec<i64> {
    body["data"]
        .as_array()
        .expect("data 应为数组")
        .iter()
        .map(|row| row["id"].as_i64().expect("id 应为整数"))
        .collect()
}

async fn create_topic(core: &Arc<WindCore>, label: &str) -> i64 {
    core.storage()
        .topic()
        .create(CreateTopic {
            parent_id: None,
            label: label.to_string(),
            icon: None,
            model_id: None,
            tool_approval_policy: None,
        })
        .await
        .expect("创建话题")
        .id
}

async fn create_definition(core: &Arc<WindCore>, label: &str) -> i64 {
    core.storage()
        .agent()
        .create_definition(CreateAgentDefinition {
            name: label.to_string(),
            description: "for http test".to_string(),
            owner_topic_id: None,
            cloned_from_id: None,
            active: None,
            data: AgentDefinitionData::default(),
        })
        .await
        .expect("创建 Agent 定义")
        .id
}

async fn create_message(
    core: &Arc<WindCore>,
    instance_id: i64,
    role: Role,
    text: &str,
    from_id: Option<i64>,
) -> i64 {
    core.storage()
        .message()
        .create(CreateMessage {
            from_id,
            content: vec![AiMessage::new_simple(
                role,
                vec![Content::new_text(text.to_string())],
                None,
            )],
            model_id: 1,
            instance_id,
            is_boundary: false,
            is_excluded: false,
            input_tokens: 1,
            output_tokens: 0,
        })
        .await
        .expect("创建消息")
        .id
}

#[tokio::test]
async fn instance_messages_returns_full_history() {
    let core = common::test_core().await;
    let topic_id = create_topic(&core, "main-messages").await;
    let main = get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("获取主实例");

    let user_msg = create_message(&core, main.id, Role::User, "hello", None).await;
    let reply_msg = create_message(&core, main.id, Role::Assistant, "hi", Some(user_msg)).await;

    let list = call(
        &core,
        "GET",
        &format!("/api/v1/agent-instances/{}/messages", main.id),
        None,
    )
    .await;
    assert_eq!(list["code"], 200);
    assert_eq!(ids_of(&list), vec![user_msg, reply_msg], "问答对完整返回");
}

#[tokio::test]
async fn instance_messages_are_scoped_to_their_instance() {
    let core = common::test_core().await;
    let topic_id = create_topic(&core, "chat").await;
    let main = get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("获取主实例");
    let child = create_child_instance(core.storage(), topic_id, main.id, 1, AgentMode::Sync)
        .await
        .expect("创建子实例");

    let main_msg = create_message(&core, main.id, Role::User, "hello", None).await;
    let child_msg = create_message(&core, child.id, Role::User, "from child", None).await;

    let main_list = call(
        &core,
        "GET",
        &format!("/api/v1/agent-instances/{}/messages", main.id),
        None,
    )
    .await;
    assert_eq!(main_list["code"], 200);
    assert_eq!(ids_of(&main_list), vec![main_msg], "主实例只返回自己的消息");
    assert!(!ids_of(&main_list).contains(&child_msg));

    let child_list = call(
        &core,
        "GET",
        &format!("/api/v1/agent-instances/{}/messages", child.id),
        None,
    )
    .await;
    assert_eq!(
        ids_of(&child_list),
        vec![child_msg],
        "子实例只返回自己的消息"
    );
}

/// 话题级消息 GET 已删除，对话输入仍由 POST 受理
#[tokio::test]
async fn topic_messages_get_is_gone() {
    let core = common::test_core().await;
    let topic_id = create_topic(&core, "no-topic-get").await;

    let status = status_of(&core, "GET", &format!("/api/v1/topics/{topic_id}/messages")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn instance_messages_unknown_id_returns_not_found() {
    let core = common::test_core().await;
    let read = call(
        &core,
        "GET",
        "/api/v1/agent-instances/999999/messages",
        None,
    )
    .await;
    assert_eq!(read["code"], 404);
    assert!(read["data"].is_null());
}

#[tokio::test]
async fn instance_list_includes_main() {
    let core = common::test_core().await;
    let topic_id = create_topic(&core, "instances").await;
    let main = get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("获取主实例");
    let child = create_child_instance(core.storage(), topic_id, main.id, 1, AgentMode::Sync)
        .await
        .expect("创建子实例");

    let list = call(
        &core,
        "GET",
        &format!("/api/v1/topics/{topic_id}/agent-instances"),
        None,
    )
    .await;
    assert_eq!(list["code"], 200);
    assert_eq!(
        ids_of(&list),
        vec![main.id, child.id],
        "话题实例列表含主实例与子实例"
    );
}

#[tokio::test]
async fn get_main_instance_via_http_returns_instance() {
    let core = common::test_core().await;
    let topic_id = create_topic(&core, "main-readable").await;
    let main = get_or_create_main_instance(core.storage(), topic_id)
        .await
        .expect("获取主实例");
    let child = create_child_instance(core.storage(), topic_id, main.id, 1, AgentMode::Sync)
        .await
        .expect("创建子实例");

    // 主实例与子实例同等对待，都按 id 直接可查
    let main_read = call(
        &core,
        "GET",
        &format!("/api/v1/agent-instances/{}", main.id),
        None,
    )
    .await;
    assert_eq!(main_read["code"], 200);
    assert_eq!(main_read["data"]["role"], "main");
    assert_eq!(main_read["data"]["id"], main.id);

    let child_read = call(
        &core,
        "GET",
        &format!("/api/v1/agent-instances/{}", child.id),
        None,
    )
    .await;
    assert_eq!(child_read["code"], 200);
    assert_eq!(child_read["data"]["role"], "child");
}

#[tokio::test]
async fn instance_unknown_id_returns_not_found() {
    let core = common::test_core().await;
    let read = call(&core, "GET", "/api/v1/agent-instances/999999", None).await;
    assert_eq!(read["code"], 404);
    assert!(read["data"].is_null());
}

#[tokio::test]
async fn agent_map_crud_round_trip() {
    let core = common::test_core().await;
    let topic_id = create_topic(&core, "maps").await;
    let agent_id = create_definition(&core, "helper").await;

    let created = call(
        &core,
        "POST",
        "/api/v1/agent-maps",
        Some(&serde_json::json!({ "topic_id": topic_id, "agent_id": agent_id }).to_string()),
    )
    .await;
    assert_eq!(created["code"], 200);
    let map_id = created["data"]["id"].as_i64().expect("映射 id");
    assert_eq!(created["data"]["agent_id"], agent_id);
    assert_eq!(created["data"]["topic_id"], topic_id);

    let maps_uri = format!("/api/v1/topics/{topic_id}/agent-maps");
    let listed = call(&core, "GET", &maps_uri, None).await;
    assert_eq!(listed["code"], 200);
    assert_eq!(ids_of(&listed), vec![map_id]);

    let deleted = call(
        &core,
        "DELETE",
        &format!("/api/v1/agent-maps/{map_id}"),
        None,
    )
    .await;
    assert_eq!(deleted["code"], 200);

    let listed_after = call(&core, "GET", &maps_uri, None).await;
    assert_eq!(listed_after["code"], 200);
    assert!(ids_of(&listed_after).is_empty(), "删除后列表为空");
}

#[tokio::test]
async fn agent_map_unknown_id_returns_not_found() {
    let core = common::test_core().await;

    let deleted = call(&core, "DELETE", "/api/v1/agent-maps/999999", None).await;
    assert_eq!(deleted["code"], 404);
    assert!(deleted["data"].is_null());
}

#[tokio::test]
async fn agent_map_requires_existing_topic() {
    let (core, pool) = common::test_core_with_pool().await;
    let missing = 999_999;

    let listed = call(
        &core,
        "GET",
        &format!("/api/v1/topics/{missing}/agent-maps"),
        None,
    )
    .await;
    assert_eq!(listed["code"], 404, "不存在的话题应 404 而非空列表");

    let created = call(
        &core,
        "POST",
        "/api/v1/agent-maps",
        Some(&serde_json::json!({ "topic_id": missing, "agent_id": 1 }).to_string()),
    )
    .await;
    assert_eq!(created["code"], 404);

    // 预检查应阻止插入，该话题在映射表无孤儿行
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM topic_agent_maps WHERE topic_id = ?")
        .bind(missing)
        .fetch_one(&pool)
        .await
        .expect("统计映射行");
    assert_eq!(count, 0);
}

/// 未配置模型的话题不能受理对话：必须在返回 `accepted` 之前拦下，
/// 且首次失败不能把话题毒化成后续请求一律 500
#[tokio::test]
async fn chat_without_model_returns_business_error() {
    let core = common::test_core().await;
    let topic_id = create_topic(&core, "no-model-chat").await;
    let chat_uri = format!("/api/v1/topics/{topic_id}/messages");
    let body = serde_json::json!({ "content": [{ "type": "text", "data": "ping" }] }).to_string();

    let first = call(&core, "POST", &chat_uri, Some(&body)).await;
    assert_eq!(
        first["code"], 400,
        "话题无模型应返回业务错误而非 500: {first}"
    );
    assert!(
        first["msg"]
            .as_str()
            .expect("msg 应为字符串")
            .contains("model"),
        "错误信息应指明缺少模型: {first}"
    );
    assert!(first["data"].is_null(), "失败响应不应带 data");

    // 第二次请求必须得到同样的业务错误，而不是被上一次的失败毒化成 500
    let second = call(&core, "POST", &chat_uri, Some(&body)).await;
    assert_eq!(second["code"], 400, "话题不应被首次请求毒化: {second}");
}

/// 同一个 AgentDefinition 在同一话题下只能映射一次，重复 POST 是业务错误而非 500
#[tokio::test]
async fn agent_map_duplicate_returns_bad_request() {
    let core = common::test_core().await;
    let topic_id = create_topic(&core, "map-duplicate").await;
    let agent_id = create_definition(&core, "map-duplicate-def").await;
    let maps_uri = format!("/api/v1/topics/{topic_id}/agent-maps");
    let body = serde_json::json!({ "topic_id": topic_id, "agent_id": agent_id }).to_string();

    let first = call(&core, "POST", "/api/v1/agent-maps", Some(&body)).await;
    assert_eq!(first["code"], 200);

    let second = call(&core, "POST", "/api/v1/agent-maps", Some(&body)).await;
    assert_eq!(second["code"], 400, "重复映射应为业务错误: {second}");

    let listed = call(&core, "GET", &maps_uri, None).await;
    assert_eq!(ids_of(&listed).len(), 1, "被拒绝的映射不应留下行");
}

/// 映射指向不存在的 AgentDefinition 时必须被拒绝，避免悬空映射静默失效
#[tokio::test]
async fn agent_map_unknown_agent_returns_bad_request() {
    let core = common::test_core().await;
    let topic_id = create_topic(&core, "map-unknown-agent").await;

    let created = call(
        &core,
        "POST",
        "/api/v1/agent-maps",
        Some(&serde_json::json!({ "topic_id": topic_id, "agent_id": 424242 }).to_string()),
    )
    .await;
    assert_eq!(created["code"], 400, "未知 agent 应为业务错误: {created}");

    let listed = call(
        &core,
        "GET",
        &format!("/api/v1/topics/{topic_id}/agent-maps"),
        None,
    )
    .await;
    assert!(ids_of(&listed).is_empty(), "被拒绝的映射不应留下行");
}

/// 话题不存在时实例列表与 `agent-maps` 一样返回 404，而不是空列表
#[tokio::test]
async fn instance_list_requires_existing_topic() {
    let core = common::test_core().await;

    let listed = call(
        &core,
        "GET",
        &format!("/api/v1/topics/{}/agent-instances", 999_999),
        None,
    )
    .await;
    assert_eq!(listed["code"], 404, "不存在的话题应 404 而非空列表");
    assert!(listed["data"].is_null());
}
