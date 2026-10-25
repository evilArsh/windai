use super::block::{Block, BlockKind, Role};
use super::id::{BlockId, MessageId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockOpened {
    pub block_id: BlockId,
    pub of_message: MessageId,
    pub role: Role,
    pub kind: BlockKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockDelta {
    pub block_id: BlockId,
    pub revision: u64,
    pub fragment: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockCommit {
    pub block_id: BlockId,
    pub kind: BlockKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockCommitted {
    #[serde(flatten)]
    pub block: BlockCommit,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub blocks: Vec<Block>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::{BlockKind, Role};

    #[test]
    fn an_open_payload_names_the_block_and_its_owner() {
        let payload = BlockOpened {
            block_id: BlockId(1),
            of_message: MessageId(2),
            role: Role::Assistant,
            kind: BlockKind::ToolCall,
            call_id: Some("abc".into()),
        };
        let value = serde_json::to_value(&payload).expect("serialize");
        assert_eq!(value["block_id"], serde_json::json!("1"));
        assert_eq!(value["of_message"], serde_json::json!(2));
        assert_eq!(value["call_id"], serde_json::json!("abc"));
    }

    #[test]
    fn a_delta_payload_carries_the_revision_that_orders_it() {
        let payload = BlockDelta {
            block_id: BlockId(1),
            revision: 17,
            fragment: "!".into(),
        };
        let value = serde_json::to_value(&payload).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({ "block_id": "1", "revision": 17, "fragment": "!" })
        );
    }

    #[test]
    fn a_commit_payload_omits_what_the_block_does_not_have() {
        let payload = BlockCommit {
            block_id: BlockId(1),
            kind: BlockKind::Text,
            call_id: None,
            content: "hello".into(),
            data: None,
        };
        let value = serde_json::to_value(&payload).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({ "block_id": "1", "kind": "text", "content": "hello" })
        );
    }

    #[test]
    fn a_commit_payload_parses_even_when_it_omits_content() {
        let parsed: BlockCommit = serde_json::from_value(serde_json::json!({
            "block_id": "7",
            "kind": "tool_call",
            "call_id": "c1",
            "data": { "name": "get_weather" },
        }))
        .expect("parse a commit payload without content");
        assert_eq!(parsed.content, "");
        assert!(parsed.data.is_some());
    }

    #[test]
    fn a_committed_payload_is_a_commit_plus_its_revision() {
        let payload = BlockCommitted {
            block: BlockCommit {
                block_id: BlockId(1),
                kind: BlockKind::Text,
                call_id: None,
                content: "hello".into(),
                data: None,
            },
            revision: 3,
        };
        let value = serde_json::to_value(&payload).expect("serialize");
        assert_eq!(value["block_id"], serde_json::json!("1"));
        assert_eq!(value["revision"], serde_json::json!(3));
        assert_eq!(value["content"], serde_json::json!("hello"));
    }

    #[test]
    fn a_snapshot_holds_a_list_of_blocks() {
        let payload = Snapshot { blocks: vec![] };
        assert_eq!(
            serde_json::to_value(&payload).expect("serialize"),
            serde_json::json!({ "blocks": [] })
        );
    }
}
