use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// 块标识，全局唯一、单调递增
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockId(pub u64);

impl Serialize for BlockId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for BlockId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse::<u64>()
            .map(BlockId)
            .map_err(serde::de::Error::custom)
    }
}

/// 消息标识，一次模型 API 往返
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MessageId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TurnId(pub i64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_block_id_is_a_string_on_the_wire() {
        let text = serde_json::to_string(&BlockId(9007199254740993)).expect("serialize");
        assert_eq!(
            text, "\"9007199254740993\"",
            "a block id exceeds the JavaScript safe integer range and must be a string, or the frontend silently loses precision"
        );
    }

    #[test]
    fn a_block_id_round_trips_above_the_safe_integer_range() {
        let id = BlockId(u64::MAX);
        let text = serde_json::to_string(&id).expect("serialize");
        let back: BlockId = serde_json::from_str(&text).expect("deserialize");
        assert_eq!(back, id);
    }

    #[test]
    fn a_block_id_rejects_a_non_numeric_string() {
        let parsed = serde_json::from_str::<BlockId>("\"abc\"");
        assert!(parsed.is_err(), "a non-numeric string must be rejected");
    }

    #[test]
    fn message_and_turn_ids_are_numbers_on_the_wire() {
        assert_eq!(
            serde_json::to_string(&MessageId(7)).expect("serialize"),
            "7"
        );
        assert_eq!(serde_json::to_string(&TurnId(7)).expect("serialize"), "7");
    }
}
