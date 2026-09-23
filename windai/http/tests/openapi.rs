mod common;

use axum::body::Body;
use axum::http::Request;
use serde_json::Value;
use tower::ServiceExt;
use utoipa::OpenApi;
use wind_http::app::app;
use wind_http::config::AppConfig;
use wind_http::openapi::ApiDoc;
use wind_http::state::AppState;

fn doc_json() -> Value {
    serde_json::to_value(ApiDoc::openapi()).expect("序列化 OpenAPI 文档")
}

#[test]
fn openapi_documents_request_bodies() {
    let json = doc_json();
    for path in ["/api/v1/agent-definitions", "/api/v1/topics"] {
        let post = &json["paths"][path]["post"];
        assert!(
            post.get("requestBody").is_some(),
            "POST {path} must document requestBody, got: {post}"
        );
    }
}

#[test]
fn openapi_query_params_are_query() {
    let json = doc_json();
    // 路径模板占位符必须是 path;`IntoParams` 结构体字段必须是 query
    let params =
        json["paths"]["/api/v1/topics/{topic_id}/mcp-servers/attach"]["post"]["parameters"]
            .as_array()
            .expect("attach 路由应有 parameters");
    let topic_id = params
        .iter()
        .find(|p| p["name"] == "topic_id")
        .expect("应有 topic_id 参数");
    assert_eq!(topic_id["in"], "path", "topic_id 应为 path");
    let name = params
        .iter()
        .find(|p| p["name"] == "name")
        .expect("应有 name 参数");
    assert_eq!(name["in"], "query", "name 应为 query");

    // 话题能力映射：唯一的参数来自路径模板
    let params = json["paths"]["/api/v1/topics/{topic_id}/agent-maps"]["get"]["parameters"]
        .as_array()
        .expect("agent-maps 路由应有 parameters");
    let topic_id = params
        .iter()
        .find(|p| p["name"] == "topic_id")
        .expect("应有 topic_id 参数");
    assert_eq!(topic_id["in"], "path", "topic_id 应为 path");
}

#[tokio::test]
async fn openapi_schema_has_no_removed_types() {
    let state = AppState::new(AppConfig::default(), common::test_core().await);
    let app = app(state);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api-docs/openapi.json")
                .body(Body::empty())
                .expect("构造请求"),
        )
        .await
        .expect("请求执行");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("读取响应体");
    let value: Value = serde_json::from_slice(&body).expect("解析响应 JSON");
    // 正向对照：文档结构必须真的存在，否则下面的「不存在」断言在 404 兜底响应上也会通过
    let schemas = value["components"]["schemas"]
        .as_object()
        .expect("响应应为 OpenAPI 文档，含 components.schemas");
    assert!(
        schemas.contains_key("AgentInstance"),
        "文档结构变化：components.schemas 缺少 AgentInstance"
    );
    // 绑定 / 聊天配置两代模型已从 core 移除，不得再从 OpenAPI 泄漏
    assert!(!schemas.contains_key("ChatConfig"));
    assert!(!schemas.contains_key("ReqConfig"));
}

#[test]
fn openapi_generates_without_panic() {
    let doc = ApiDoc::openapi();
    let json = doc.to_pretty_json().unwrap();
    assert!(json.contains("/api/v1/topics"));
    assert!(json.contains("ApiResponse"));
}

/// 已删除的路由不得残留在文档里 —— 文档描述不存在的端点比缺少文档更糟
#[test]
fn openapi_omits_removed_routes() {
    let json = doc_json();
    let paths = json["paths"].as_object().expect("paths 应为对象");
    for removed in [
        "/healthz",
        "/api/v1/topics/by-instance/{instance_id}",
        "/api/v1/topics/{topic_id}/messages/context",
        "/api/v1/agent-instances/{instance_id}/messages/context",
        "/api/v1/messages/{message_id}/from-message",
        "/api/v1/mcp-servers/{mcp_server_id}/status",
    ] {
        assert!(
            !paths.contains_key(removed),
            "已删除的路由仍出现在 OpenAPI paths 中: {removed}"
        );
    }
    // PUT 已随 UpdateTopicAgentMap 一同删除，能力映射只保留增删
    assert!(
        json["paths"]["/api/v1/agent-maps/{map_id}"]
            .get("put")
            .is_none(),
        "能力映射不应再有 PUT"
    );
    // 话题级消息 GET 已删除，读某一实例的对话一律走 `/agent-instances/{id}/messages`
    assert!(
        json["paths"]["/api/v1/topics/{topic_id}/messages"]
            .get("get")
            .is_none(),
        "话题级消息不应再有 GET"
    );
}

#[test]
fn openapi_does_not_leak_internal_models() {
    let schemas = doc_json()["components"]["schemas"]
        .as_object()
        .unwrap()
        .clone();
    for leaked in [
        "CreateMessage",
        "CreateToolApprovalRequests",
        "CreateToolApprovalCall",
        "ApprovalRecord",
    ] {
        assert!(
            !schemas.contains_key(leaked),
            "internal model leaked into OpenAPI schemas: {leaked}"
        );
    }
}

#[test]
fn openapi_sse_and_credential_shapes() {
    let json = doc_json();
    // SSE route must document `text/event-stream` on its 200 response, referencing TopicEvent.
    let sse_200 = &json["paths"]["/api/v1/topics/{topic_id}/events"]["get"]["responses"]["200"];
    assert!(
        sse_200["content"].get("text/event-stream").is_some(),
        "SSE 200 response must document text/event-stream"
    );
    assert_eq!(
        sse_200["content"]["text/event-stream"]["schema"]["$ref"],
        "#/components/schemas/TopicEvent"
    );
    // 评审取消脱敏：Credentials 直接暴露 key
    let props = json["components"]["schemas"]["Credentials"]["properties"]
        .as_object()
        .unwrap();
    assert!(props.contains_key("key"));
}
