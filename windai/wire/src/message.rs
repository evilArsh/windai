use super::tool::{FunctionCall, FunctionCallOutput};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Usage {
    pub input_tokens: i32,
    pub output_tokens: i32,
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Clone,
    Copy,
    strum::AsRefStr,
    strum::EnumString,
    strum::Display,
    schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
    Developer,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, schemars::JsonSchema)]
pub struct AudioContent {
    /// mp3、wav
    pub format: String,
    pub content: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Content {
    Text { data: String },
    Image { data: String },
    File { data: String },
    Audio { data: AudioContent },
    FunctionCall { data: FunctionCallOutput },
}

impl Content {
    #[inline]
    pub fn new_text(text: String) -> Self {
        Self::Text { data: text }
    }

    #[inline]
    pub fn new_image(image: String) -> Self {
        Self::Image { data: image }
    }

    #[inline]
    pub fn new_file(file: String) -> Self {
        Self::File { data: file }
    }

    #[inline]
    pub fn new_audio(content: String, format: String) -> Self {
        Self::Audio {
            data: AudioContent { content, format },
        }
    }

    #[inline]
    pub fn new_function_call(call_id: String, value: serde_json::Value) -> Self {
        Self::FunctionCall {
            data: FunctionCallOutput {
                id: call_id,
                content: value,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, schemars::JsonSchema)]
pub struct Message {
    pub role: Role,
    pub content: Vec<Content>,
    pub reasoning_content: Option<String>,
    pub created_at: i64,
    pub input_tokens: i32,
    pub output_tokens: i32,
    pub tool_calls: Option<Vec<FunctionCall>>,
}

impl Default for Message {
    fn default() -> Self {
        Self {
            role: Role::Assistant,
            content: vec![],
            reasoning_content: None,
            created_at: 0,
            input_tokens: 0,
            output_tokens: 0,
            tool_calls: None,
        }
    }
}
