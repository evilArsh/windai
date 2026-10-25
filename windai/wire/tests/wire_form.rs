//! 序列化结果的对照断言
//!
//! 这些值是搬运时的形状快照，与原类型逐字段对应。改动可空性、字段名、默认值
//! 或大小写，都会让这里失败。

use wind_wire::{
    AudioContent, Content, FunctionCall, FunctionCallOutput, FunctionTool, Message, Role, Tools,
};

#[test]
fn a_role_is_a_lowercase_string() {
    for (role, expected) in [
        (Role::System, "system"),
        (Role::User, "user"),
        (Role::Assistant, "assistant"),
        (Role::Tool, "tool"),
        (Role::Developer, "developer"),
    ] {
        assert_eq!(
            serde_json::to_value(role).expect("serialize"),
            serde_json::json!(expected)
        );
    }
}

#[test]
fn a_text_item_is_tagged_by_type() {
    assert_eq!(
        serde_json::to_value(Content::Text { data: "x".into() }).expect("serialize"),
        serde_json::json!({ "type": "text", "data": "x" })
    );
}

#[test]
fn a_function_call_result_nests_the_output_shape() {
    assert_eq!(
        serde_json::to_value(Content::FunctionCall {
            data: FunctionCallOutput {
                id: "c".into(),
                content: serde_json::json!(1)
            }
        })
        .expect("serialize"),
        serde_json::json!({ "type": "function_call", "data": { "id": "c", "content": 1 } })
    );
}

#[test]
fn an_audio_item_nests_format_and_content() {
    assert_eq!(
        serde_json::to_value(Content::Audio {
            data: AudioContent {
                format: "mp3".into(),
                content: "AAAA".into()
            }
        })
        .expect("serialize"),
        serde_json::json!({
            "type": "audio",
            "data": { "format": "mp3", "content": "AAAA" }
        })
    );
}

#[test]
fn a_message_exposes_all_seven_fields() {
    let message = Message {
        role: Role::User,
        content: vec![Content::Text { data: "x".into() }],
        reasoning_content: Some("r".into()),
        created_at: 7,
        input_tokens: 1,
        output_tokens: 2,
        tool_calls: Some(vec![FunctionCall {
            id: "c".into(),
            name: "n".into(),
            arguments: "{}".into(),
        }]),
    };
    let value = serde_json::to_value(&message).expect("serialize");
    let object = value.as_object().expect("the value is an object");
    assert_eq!(object.len(), 7, "not one field more, not one less");
    for key in [
        "role",
        "content",
        "reasoning_content",
        "created_at",
        "input_tokens",
        "output_tokens",
        "tool_calls",
    ] {
        assert!(object.contains_key(key), "missing field {key}");
    }
}

#[test]
fn a_tool_serializes_without_its_enum_wrapper() {
    let value = serde_json::to_value(Tools::Function(FunctionTool {
        name: "n".into(),
        description: None,
        parameters: None,
        strict: None,
    }))
    .expect("serialize");
    assert_eq!(value, serde_json::json!({ "name": "n" }));
}

#[test]
fn the_default_message_is_deterministic() {
    assert_eq!(Message::default().created_at, 0);
    assert_eq!(Message::default().role, Role::Assistant);
}
