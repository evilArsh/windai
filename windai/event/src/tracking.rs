use super::envelope::EventSeq;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum DeliveryTarget {
    Mod { name: String },
    Observer { name: String },
}

impl DeliveryTarget {
    pub fn mod_(name: impl Into<String>) -> Self {
        DeliveryTarget::Mod { name: name.into() }
    }

    pub fn observer(name: impl Into<String>) -> Self {
        DeliveryTarget::Observer { name: name.into() }
    }

    pub fn name(&self) -> &str {
        match self {
            DeliveryTarget::Mod { name } | DeliveryTarget::Observer { name } => name,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum DeliveryOutcome {
    Pass,
    Rewrite,
    Observed,
    Skipped,
    Timeout,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryRecord {
    pub event_seq: EventSeq,
    pub target: DeliveryTarget,
    pub outcome: DeliveryOutcome,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub fn should_record(event: &str) -> bool {
    !event.starts_with("core/dispatch.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_whole_dispatch_namespace_is_excluded_from_tracking() {
        assert!(!should_record("core/dispatch.delivered"));
        assert!(!should_record("core/dispatch.anything_else"));
        assert!(should_record("core/mcp.tool.call"));
    }

    #[test]
    fn the_prefix_must_include_the_dot() {
        assert!(should_record("core/dispatchx.foo"));
    }

    #[test]
    fn delivery_targets_expose_their_name() {
        assert_eq!(DeliveryTarget::mod_("a").name(), "a");
        assert_eq!(DeliveryTarget::observer("sse").name(), "sse");
    }
}
