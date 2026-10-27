use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr, schemars::JsonSchema,
)]
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
    pub target: DeliveryTarget,
    pub outcome: DeliveryOutcome,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub fn should_record(event: &str) -> bool {
    !event.starts_with("core/dispatch.")
}
