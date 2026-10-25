use super::tool::{FunctionCall, FunctionCallOutput};
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Clone,
    Copy,
    strum::AsRefStr,
    strum::EnumString,
    strum::Display,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
    Developer,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct AudioContent {
    /// 编码，例如 mp3、wav
    pub format: String,
    pub content: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Content {
    Text { data: String },
    Image { data: String },
    File { data: String },
    Audio { data: AudioContent },
    FunctionCall { data: FunctionCallOutput },
}

impl Content {
    #[inline]
    pub fn new_text(text: String) -> Self {
        Self::Text { data: text }
    }

    #[inline]
    pub fn new_image(image: String) -> Self {
        Self::Image { data: image }
    }

    #[inline]
    pub fn new_file(file: String) -> Self {
        Self::File { data: file }
    }

    #[inline]
    pub fn new_audio(content: String, format: String) -> Self {
        Self::Audio {
            data: AudioContent { content, format },
        }
    }

    #[inline]
    pub fn new_function_call(call_id: String, value: serde_json::Value) -> Self {
        Self::FunctionCall {
            data: FunctionCallOutput {
                id: call_id,
                content: value,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Message {
    pub role: Role,
    pub content: Vec<Content>,
    pub reasoning_content: Option<String>,
    pub created_at: i64,
    pub input_tokens: i32,
    pub output_tokens: i32,
    pub tool_calls: Option<Vec<FunctionCall>>,
}

impl Default for Message {
    fn default() -> Self {
        Self {
            role: Role::Assistant,
            content: vec![],
            reasoning_content: None,
            created_at: 0,
            input_tokens: 0,
            output_tokens: 0,
            tool_calls: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_use_lowercase_literals() {
        for (role, literal) in [
            (Role::System, "system"),
            (Role::User, "user"),
            (Role::Assistant, "assistant"),
            (Role::Tool, "tool"),
            (Role::Developer, "developer"),
        ] {
            let text = serde_json::to_string(&role).expect("serialize");
            assert_eq!(text, format!("\"{literal}\""));
            let back: Role = serde_json::from_str(&text).expect("deserialize");
            assert_eq!(back, role);
        }
    }

    #[test]
    fn an_unknown_role_is_rejected() {
        let parsed = serde_json::from_str::<Role>("\"nobody\"");
        assert!(
            parsed.is_err(),
            "an unknown role must be rejected, not silently downgraded"
        );
    }

    #[test]
    fn the_name_accessor_matches_the_serialized_literal() {
        assert_eq!(Role::Developer.to_string(), "developer");
    }

    #[test]
    fn content_items_are_tagged_by_type() {
        let text = Content::Text {
            data: "hello".into(),
        };
        assert_eq!(
            serde_json::to_value(&text).expect("serialize"),
            serde_json::json!({ "type": "text", "data": "hello" })
        );

        let image = Content::Image {
            data: "AAAA".into(),
        };
        assert_eq!(
            serde_json::to_value(&image).expect("serialize"),
            serde_json::json!({ "type": "image", "data": "AAAA" })
        );
    }

    #[test]
    fn an_audio_item_nests_its_own_shape() {
        let audio = Content::Audio {
            data: AudioContent {
                format: "mp3".into(),
                content: "AAAA".into(),
            },
        };
        assert_eq!(
            serde_json::to_value(&audio).expect("serialize"),
            serde_json::json!({
                "type": "audio",
                "data": { "format": "mp3", "content": "AAAA" }
            })
        );
    }

    #[test]
    fn a_function_call_result_carries_a_structured_payload() {
        let result = Content::FunctionCall {
            data: FunctionCallOutput {
                id: "call_abc".into(),
                content: serde_json::json!({ "temp": 22 }),
            },
        };
        assert_eq!(
            serde_json::to_value(&result).expect("serialize"),
            serde_json::json!({
                "type": "function_call",
                "data": { "id": "call_abc", "content": { "temp": 22 } }
            })
        );
    }

    #[test]
    fn an_unknown_content_kind_is_rejected() {
        let parsed = serde_json::from_str::<Content>(r#"{"type":"video","data":"x"}"#);
        assert!(parsed.is_err(), "an unknown content type must be rejected");
    }

    #[test]
    fn the_content_constructors_build_the_documented_variants() {
        let cases: [(Content, serde_json::Value); 5] = [
            (
                Content::new_text("hi".into()),
                serde_json::json!({ "type": "text", "data": "hi" }),
            ),
            (
                Content::new_image("AAAA".into()),
                serde_json::json!({ "type": "image", "data": "AAAA" }),
            ),
            (
                Content::new_file("AAAA".into()),
                serde_json::json!({ "type": "file", "data": "AAAA" }),
            ),
            (
                Content::new_audio("AAAA".into(), "mp3".into()),
                serde_json::json!({
                    "type": "audio",
                    "data": { "format": "mp3", "content": "AAAA" }
                }),
            ),
            (
                Content::new_function_call("call_abc".into(), serde_json::json!(1)),
                serde_json::json!({
                    "type": "function_call",
                    "data": { "id": "call_abc", "content": 1 }
                }),
            ),
        ];
        for (item, expected) in cases {
            assert_eq!(serde_json::to_value(&item).expect("serialize"), expected);
        }
    }

    #[test]
    fn the_default_is_deterministic_and_has_an_assistant_role() {
        // 归档实现的 `Default` 把 `created_at` 设为当前时刻，两次调用因此不相等。
        // 本 crate 固定为 0，既确定，也去掉了对 chrono 的依赖。
        let first = Message::default();
        let second = Message::default();
        assert_eq!(first.role, Role::Assistant);
        assert_eq!(first.created_at, 0);
        assert_eq!(
            serde_json::to_value(&first).expect("serialize"),
            serde_json::to_value(&second).expect("serialize"),
            "two calls to default must produce the same value"
        );
    }

    #[test]
    fn the_default_has_no_content_and_no_tool_calls() {
        let message = Message::default();
        assert!(message.content.is_empty());
        assert!(message.reasoning_content.is_none());
        assert!(message.tool_calls.is_none());
        assert_eq!(message.input_tokens, 0);
        assert_eq!(message.output_tokens, 0);
    }

    #[test]
    fn a_round_trip_keeps_every_field() {
        let message = Message {
            role: Role::Assistant,
            content: vec![Content::new_text("answer".into())],
            reasoning_content: Some("thought".into()),
            created_at: 1_760_000_000_000,
            input_tokens: 120,
            output_tokens: 45,
            tool_calls: Some(vec![FunctionCall {
                id: "call_abc".into(),
                name: "get_weather".into(),
                arguments: "{\"city\":\"SF\"}".into(),
            }]),
        };
        let text = serde_json::to_string(&message).expect("serialize");
        let back: Message = serde_json::from_str(&text).expect("deserialize");
        assert_eq!(back.role, Role::Assistant);
        assert_eq!(back.reasoning_content.as_deref(), Some("thought"));
        assert_eq!(back.created_at, 1_760_000_000_000);
        assert_eq!(back.input_tokens, 120);
        assert_eq!(back.output_tokens, 45);
        let calls = back.tool_calls.expect("the tool calls survive");
        assert_eq!(calls[0].name, "get_weather");
        assert_eq!(calls[0].arguments, "{\"city\":\"SF\"}");
    }

    #[test]
    fn absent_optional_fields_serialize_as_null() {
        // 归档实现的 `Message` 没有给任何字段加 `skip_serializing_if`，因此 `None`
        // 写成 null。这是既有行为，须照原样保持：加上 `skip_serializing_if` 会改变
        // 序列化结果，而下游解析可能依赖 null 的存在。
        let message = Message {
            reasoning_content: None,
            tool_calls: None,
            ..Message::default()
        };
        let value = serde_json::to_value(&message).expect("serialize");
        assert_eq!(
            value.get("reasoning_content"),
            Some(&serde_json::Value::Null),
            "an absent nullable field serializes as null, not omitted"
        );
        assert_eq!(value.get("tool_calls"), Some(&serde_json::Value::Null));
    }
}
