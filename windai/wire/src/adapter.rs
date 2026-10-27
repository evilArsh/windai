use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
#[error("{name} is not a known adapter")]
pub struct AdapterTypeError {
    pub name: String,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr, schemars::JsonSchema,
)]
pub enum AdapterType {
    #[serde(rename = "openai_completion")]
    #[strum(serialize = "openai_completion")]
    OpenAICompletion,
    #[serde(rename = "openai_response")]
    #[strum(serialize = "openai_response")]
    OpenAIResponse,
}

impl std::fmt::Display for AdapterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl AdapterType {
    pub fn parse(name: &str) -> Result<Self, AdapterTypeError> {
        match name {
            "openai_completion" => Ok(AdapterType::OpenAICompletion),
            "openai_response" => Ok(AdapterType::OpenAIResponse),
            other => Err(AdapterTypeError {
                name: other.to_string(),
            }),
        }
    }
}
