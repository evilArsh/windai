use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// 事件的全局单调递增序号
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventSeq(pub u64);

impl std::fmt::Display for EventSeq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 触发操作的作用域
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Scope {
    Global,
    Topic { topic_id: i64 },
}

impl Scope {
    pub fn topic_id(&self) -> Option<i64> {
        match self {
            Scope::Global => None,
            Scope::Topic { topic_id } => Some(*topic_id),
        }
    }
}

/// 操作从哪条路径来
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Origin {
    Agent,
    Ui,
    Internal,
}

/// 事件由谁发出
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Source {
    Core { subsystem: String },
    Mod { name: String, version: u32 },
}

impl Source {
    pub fn core(subsystem: impl Into<String>) -> Self {
        Source::Core {
            subsystem: subsystem.into(),
        }
    }

    pub fn mod_(name: impl Into<String>, version: u32) -> Self {
        Source::Mod {
            name: name.into(),
            version,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope<P> {
    pub seq: EventSeq,
    /// 产生时刻，epoch 毫秒
    pub time: i64,
    pub name: String,
    pub scope: Scope,
    pub origin: Origin,
    pub source: Source,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<EventSeq>,
    pub payload: P,
}

impl<P> EventEnvelope<P> {
    pub fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> EventEnvelope<serde_json::Value> {
        EventEnvelope {
            seq: EventSeq(42),
            time: 1_760_000_000_000,
            name: "core/mcp.tool.call".into(),
            scope: Scope::Topic { topic_id: 7 },
            origin: Origin::Agent,
            source: Source::core("mcp"),
            cause: Some(EventSeq(41)),
            payload: serde_json::json!({ "tool": "fetch" }),
        }
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let text = serde_json::to_string(&sample()).expect("serialize the envelope");
        let back: EventEnvelope<serde_json::Value> =
            serde_json::from_str(&text).expect("deserialize the envelope");
        assert_eq!(back.seq, EventSeq(42));
        assert_eq!(back.time, 1_760_000_000_000);
        assert_eq!(back.name, "core/mcp.tool.call");
        assert_eq!(back.scope, Scope::Topic { topic_id: 7 });
        assert_eq!(back.origin, Origin::Agent);
        assert_eq!(back.source, Source::core("mcp"));
        assert_eq!(back.cause, Some(EventSeq(41)));
    }

    #[test]
    fn scope_and_source_carry_their_discriminants() {
        let global = serde_json::to_value(Scope::Global).expect("serialize the scope");
        assert_eq!(global, serde_json::json!({ "kind": "global" }));

        let topic =
            serde_json::to_value(Scope::Topic { topic_id: 3 }).expect("serialize the scope");
        assert_eq!(topic, serde_json::json!({ "kind": "topic", "topic_id": 3 }));

        let source =
            serde_json::to_value(Source::mod_("research-kit", 1)).expect("serialize the source");
        assert_eq!(
            source,
            serde_json::json!({ "kind": "mod", "name": "research-kit", "version": 1 })
        );
    }

    #[test]
    fn a_top_level_event_omits_cause_from_the_wire_form() {
        let mut env = sample();
        env.cause = None;
        let text = serde_json::to_string(&env).expect("serialize the envelope");
        assert!(!text.contains("cause"));
    }

    #[test]
    fn seq_serializes_as_a_bare_number() {
        let value = serde_json::to_value(EventSeq(9)).expect("serialize the sequence");
        assert_eq!(value, serde_json::json!(9));
    }

    #[test]
    fn topic_id_is_absent_only_for_global() {
        assert_eq!(Scope::Global.topic_id(), None);
        assert_eq!(Scope::Topic { topic_id: 4 }.topic_id(), Some(4));
    }

    #[test]
    fn now_ms_is_a_positive_millisecond_stamp() {
        assert!(EventEnvelope::<()>::now_ms() > 1_600_000_000_000);
    }
}
