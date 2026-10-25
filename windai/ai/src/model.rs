use serde::{Deserialize, Serialize};

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

#[derive(Debug, thiserror::Error)]
#[error("{name} is not a known adapter")]
pub struct AdapterTypeError {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ChatModel {
    pub name: String,
    pub adapter: AdapterType,
    /// 专属端点，缺省时用适配器的默认端点
    pub endpoint: Option<String>,
    pub config: Option<serde_json::Value>,
}

/// 一次请求的地址与凭据
#[derive(Debug, Clone)]
pub struct Target {
    pub base_url: String,
    pub api_key: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_name_accessor_matches_the_parsed_and_serialized_literal() {
        // 自动的 snake_case 会把 OpenAICompletion 写成 open_ai_completion，
        // 而解析用的名字是 openai_completion，两个名字必须一致
        for (kind, literal) in [
            (AdapterType::OpenAICompletion, "openai_completion"),
            (AdapterType::OpenAIResponse, "openai_response"),
        ] {
            assert_eq!(kind.as_ref(), literal);
            assert_eq!(
                serde_json::to_string(&kind).expect("serialize"),
                format!("\"{literal}\"")
            );
            assert_eq!(AdapterType::parse(kind.as_ref()).expect("parse"), kind);
        }
    }
}
