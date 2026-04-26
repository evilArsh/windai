use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Hash,
    Copy,
    PartialEq,
    Eq,
    Clone,
    strum::EnumString,
    strum::Display,
)]
pub enum AdaptorType {
    OpenAICompletion,
    OpenAIResponse,
}
