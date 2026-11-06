use super::block::{Block, Body};
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
    /// 用于流式消息中的 delta 配对
    pub item_id: Option<String>,
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
    pub fn new_audio(content: String, format: String, item_id: Option<String>) -> Self {
        Self::Audio {
            data: AudioContent {
                item_id,
                format,
                content,
            },
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
    /// 推理项目的标识
    pub reasoning_item_id: Option<String>,
    pub created_at: i64,
    pub input_tokens: i32,
    pub output_tokens: i32,
    pub tool_calls: Option<Vec<FunctionCall>>,
}

impl Message {
    pub fn push_block(&mut self, block: &Block) {
        match &block.body {
            Body::Reasoning { item_id, content } => {
                if let Some(item_id) = item_id {
                    self.reasoning_item_id
                        .get_or_insert_with(|| item_id.clone());
                }
                if !content.is_empty() {
                    self.reasoning_content
                        .get_or_insert_with(String::new)
                        .push_str(content);
                }
            }
            Body::ToolCall {
                item_id,
                call_id,
                call_name,
                call_arguments,
            } => {
                self.tool_calls
                    .get_or_insert_with(Vec::new)
                    .push(FunctionCall {
                        item_id: item_id.clone(),
                        call_id: call_id.clone(),
                        name: call_name.clone(),
                        arguments: call_arguments.clone(),
                    });
            }
            Body::Text { content } => self.content.push(Content::Text {
                data: content.clone(),
            }),
            Body::Image { content } => self.content.push(Content::Image {
                data: content.clone(),
            }),
            Body::File { content } => self.content.push(Content::File {
                data: content.clone(),
            }),
            Body::Audio {
                item_id,
                format,
                content,
            } => self.content.push(Content::Audio {
                data: AudioContent {
                    item_id: item_id.clone(),
                    format: format.clone(),
                    content: content.clone(),
                },
            }),
            Body::ToolResult { call_id, call_res } => {
                self.content.push(Content::FunctionCall {
                    data: FunctionCallOutput {
                        id: call_id.clone(),
                        content: call_res.clone(),
                    },
                });
            }
        }
    }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            role: Role::Assistant,
            content: vec![],
            reasoning_content: None,
            reasoning_item_id: None,
            created_at: 0,
            input_tokens: 0,
            output_tokens: 0,
            tool_calls: None,
        }
    }
}
