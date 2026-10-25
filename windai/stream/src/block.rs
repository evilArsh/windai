/// 块的角色就是词汇层那一个，不做裁剪、不做映射
pub use wind_wire::Role;

use super::id::{BlockId, MessageId};
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::AsRefStr,
    strum::EnumString,
    strum::Display,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum BlockKind {
    Text,
    Reasoning,
    ToolCall,
    ToolResult,
    Media,
}

/// 媒体块载荷里图片的种类标记
pub const MEDIA_IMAGE: &str = "image";

/// 媒体块载荷里文件的种类标记
pub const MEDIA_FILE: &str = "file";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Block {
    pub id: BlockId,
    pub of_message: MessageId,
    pub role: Role,
    pub kind: BlockKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    /// 被改到第几版，每次追加递增，等于已追加的片段数
    pub revision: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    pub finished: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_block_role_is_the_wire_role_unchanged() {
        let role: Role = wind_wire::Role::Developer;
        assert_eq!(role, Role::Developer);
    }

    #[test]
    fn block_kinds_use_their_documented_literals() {
        for (kind, literal) in [
            (BlockKind::Text, "text"),
            (BlockKind::Reasoning, "reasoning"),
            (BlockKind::ToolCall, "tool_call"),
            (BlockKind::ToolResult, "tool_result"),
            (BlockKind::Media, "media"),
        ] {
            let text = serde_json::to_string(&kind).expect("serialize");
            assert_eq!(text, format!("\"{literal}\""));
        }
    }

    #[test]
    fn an_unknown_kind_is_rejected() {
        let parsed = serde_json::from_str::<BlockKind>("\"video\"");
        assert!(
            parsed.is_err(),
            "an unknown block kind must be rejected, not silently downgraded"
        );
    }

    #[test]
    fn the_name_accessor_matches_the_serialized_literal() {
        assert_eq!(Role::Assistant.as_ref(), "assistant");
        assert_eq!(BlockKind::ToolResult.as_ref(), "tool_result");
    }

    #[test]
    fn optional_fields_are_omitted_from_the_wire_form() {
        let block = Block {
            id: BlockId(1),
            of_message: MessageId(2),
            role: Role::Assistant,
            kind: BlockKind::Text,
            call_id: None,
            revision: 0,
            content: String::new(),
            data: None,
            finished: false,
        };
        let text = serde_json::to_string(&block).expect("serialize");
        assert!(
            !text.contains("call_id"),
            "the field must be absent when there is no call id"
        );
        assert!(!text.contains("content"), "empty content must be absent");
        assert!(
            !text.contains("data"),
            "the field must be absent when there is no payload"
        );
        assert!(
            text.contains("\"of_message\":2"),
            "the owning message uses snake_case"
        );
    }

    #[test]
    fn a_tool_call_block_carries_its_pairing_key_as_a_scalar() {
        let block = Block {
            id: BlockId(3),
            of_message: MessageId(2),
            role: Role::Assistant,
            kind: BlockKind::ToolCall,
            call_id: Some("abc".into()),
            revision: 0,
            content: String::new(),
            data: Some(serde_json::json!({ "name": "get_weather" })),
            finished: false,
        };
        let value = serde_json::to_value(&block).expect("serialize");
        assert_eq!(value["call_id"], serde_json::json!("abc"));
        assert_eq!(value["kind"], serde_json::json!("tool_call"));
    }

    #[test]
    fn a_block_without_content_still_parses() {
        let block = Block {
            id: BlockId(1),
            of_message: MessageId(2),
            role: Role::Tool,
            kind: BlockKind::ToolResult,
            call_id: Some("abc".into()),
            revision: 1,
            content: String::new(),
            data: Some(serde_json::json!({ "temp": 22 })),
            finished: false,
        };
        let value = serde_json::to_value(&block).expect("serialize");
        assert!(
            value.get("content").is_none(),
            "empty content does not appear in the serialized result"
        );
        let back: Block = serde_json::from_value(value).expect("deserialize");
        assert_eq!(back, block);
    }

    #[test]
    fn a_block_round_trips_through_json() {
        let block = Block {
            id: BlockId(9007199254740993),
            of_message: MessageId(4),
            role: Role::Tool,
            kind: BlockKind::ToolResult,
            call_id: Some("abc".into()),
            revision: 7,
            content: "22 degrees".into(),
            data: Some(serde_json::json!({ "temp": 22 })),
            finished: true,
        };
        let text = serde_json::to_string(&block).expect("serialize");
        let back: Block = serde_json::from_str(&text).expect("deserialize");
        assert_eq!(back, block);
    }
}
