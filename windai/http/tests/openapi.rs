use serde_json::Value;
use utoipa::OpenApi;
use wind_http::openapi::ApiDoc;

fn doc_json() -> Value {
    serde_json::to_value(ApiDoc::openapi()).unwrap()
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
