use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::AsRefStr,
    strum::EnumString,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentRole {
    Main,
    Child,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::AsRefStr,
    strum::EnumString,
    strum::Display,
    strum::EnumIter,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentMode {
    Sync,
    Background,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr, strum::EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum PendingKind {
    User,
    BackgroundResult,
}
