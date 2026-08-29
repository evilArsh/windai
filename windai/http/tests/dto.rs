use serde_json::json;
use wind_ai::message::Content;
use wind_http::dto::agent::CloneAgentDefinitionRequest;
use wind_http::dto::approval::ApproveToolCallsRequest;
use wind_http::dto::message::{CreateChatRequest, SubmitChatResponse};

#[test]
fn create_chat_request_round_trips() {
    let req = CreateChatRequest {
        content: vec![Content::new_text("hi".into())],
    };
    let v = serde_json::to_value(&req).unwrap();
    assert_eq!(v["content"][0]["type"], "text");

    let back: CreateChatRequest = serde_json::from_value(v).unwrap();
    assert_eq!(back.content.len(), 1);
}

#[test]
fn approve_tool_calls_request_round_trips() {
    let req = ApproveToolCallsRequest {
        allow_ids: vec![1, 2],
        deny_ids: vec![3],
    };
    let v = serde_json::to_value(&req).unwrap();
    assert_eq!(v["allow_ids"], json!([1, 2]));
    assert_eq!(v["deny_ids"], json!([3]));

    let back: ApproveToolCallsRequest = serde_json::from_value(v).unwrap();
    assert_eq!(back.allow_ids, vec![1, 2]);
    assert_eq!(back.deny_ids, vec![3]);
}

#[test]
fn clone_agent_definition_request_round_trips() {
    let req = CloneAgentDefinitionRequest { agent_id: 7 };
    let v = serde_json::to_value(&req).unwrap();
    assert_eq!(v["agent_id"], 7);

    let back: CloneAgentDefinitionRequest = serde_json::from_value(v).unwrap();
    assert_eq!(back.agent_id, 7);
}

#[test]
fn submit_chat_response_serializes() {
    let v = serde_json::to_value(SubmitChatResponse { accepted: true }).unwrap();
    assert_eq!(v["accepted"], true);
}
