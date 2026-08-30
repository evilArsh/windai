use serde_json::Value;
use utoipa::OpenApi;
use wind_http::openapi::ApiDoc;

fn doc_json() -> Value {
    serde_json::to_value(ApiDoc::openapi()).unwrap()
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
    // 路径模板占位符必须是 path;`IntoParams` 结构体字段必须是 query。
    let params = json["paths"]["/api/v1/agent-bindings/by-agent/{agent_id}"]["get"]["parameters"]
        .as_array()
        .unwrap();
    let agent_id = params.iter().find(|p| p["name"] == "agent_id").unwrap();
    assert_eq!(agent_id["in"], "path", "agent_id 应为 path");
    let parent = params
        .iter()
        .find(|p| p["name"] == "parent_topic_id")
        .unwrap();
    assert_eq!(parent["in"], "query", "parent_topic_id 应为 query");
}

#[test]
fn openapi_generates_without_panic() {
    let doc = ApiDoc::openapi();
    let json = doc.to_pretty_json().unwrap();
    assert!(json.contains("/api/v1/topics"));
    assert!(json.contains("/healthz"));
    assert!(json.contains("ApiResponse"));
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
    // 评审取消脱敏：Credentials 直接暴露 key。
    let props = json["components"]["schemas"]["Credentials"]["properties"]
        .as_object()
        .unwrap();
    assert!(props.contains_key("key"));
}
