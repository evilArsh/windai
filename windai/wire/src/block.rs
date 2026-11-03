use super::tool::FunctionCall;
use crate::Role;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 块载荷，变体即块类型
#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize, strum::AsRefStr, schemars::JsonSchema,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Body {
    Reasoning {
        content: String,
    },
    Text {
        content: String,
    },
    Image {
        content: String,
    },
    File {
        content: String,
    },
    Audio {
        format: String,
        content: String,
    },
    ToolCall {
        call_id: String,
        call_name: String,
        call_arguments: String,
    },
    ToolResult {
        call_id: String,
        #[serde(default)]
        call_res: Value,
    },
}
/// 一个 Message 被拆分成多个独立的 Block
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Block {
    /// 块所属消息的角色
    pub role: Role,

    /// 块在消息内的位置，从 0 开始按吸收顺序递增
    pub seq: i64,

    /// 被改到第几版，每次追加递增
    pub revision: i64,

    #[serde(flatten)]
    pub body: Body,
}

impl Block {
    pub fn new(role: Role, seq: i64, revision: i64, body: Body) -> Self {
        Self {
            role,
            seq,
            revision,
            body,
        }
    }

    pub fn new_reasoning(role: Role, seq: i64) -> Self {
        Self::new(
            role,
            seq,
            0,
            Body::Reasoning {
                content: String::new(),
            },
        )
    }

    pub fn new_text(role: Role, seq: i64) -> Self {
        Self::new(
            role,
            seq,
            0,
            Body::Text {
                content: String::new(),
            },
        )
    }

    pub fn new_image(role: Role, seq: i64, data: String) -> Self {
        Self::new(role, seq, 1, Body::Image { content: data })
    }

    pub fn new_file(role: Role, seq: i64, data: String) -> Self {
        Self::new(role, seq, 1, Body::File { content: data })
    }

    pub fn new_audio(role: Role, seq: i64, format: String, content: String) -> Self {
        Self::new(role, seq, 1, Body::Audio { format, content })
    }

    pub fn new_tool_call(role: Role, seq: i64, call_id: String, call_name: String) -> Self {
        Self::new(
            role,
            seq,
            0,
            Body::ToolCall {
                call_id,
                call_name,
                call_arguments: String::new(),
            },
        )
    }

    pub fn new_tool_result(role: Role, seq: i64, call_id: String, content: Value) -> Self {
        Self::new(
            role,
            seq,
            1,
            Body::ToolResult {
                call_id,
                call_res: content,
            },
        )
    }

    pub fn tag(&self) -> &str {
        self.body.as_ref()
    }

    pub fn tool_call(&self) -> Option<FunctionCall> {
        match &self.body {
            Body::ToolCall {
                call_id,
                call_name,
                call_arguments,
            } => Some(FunctionCall {
                id: call_id.clone(),
                name: call_name.clone(),
                arguments: call_arguments.clone(),
            }),
            _ => None,
        }
    }

    pub fn append_text(&mut self, fragment: &str) {
        if fragment.is_empty() {
            return;
        }
        match &mut self.body {
            Body::Reasoning { content } | Body::Text { content } => content.push_str(fragment),
            _ => return,
        }
        self.revision += 1;
    }

    pub fn append_arguments(&mut self, fragment: &str) {
        if fragment.is_empty() {
            return;
        }
        if let Body::ToolCall { call_arguments, .. } = &mut self.body {
            call_arguments.push_str(fragment);
            self.revision += 1;
        }
    }

    pub fn fill_call(&mut self, call_id: &str, call_name: &str) {
        let Body::ToolCall {
            call_id: current_id,
            call_name: current_name,
            ..
        } = &mut self.body
        else {
            return;
        };
        if current_id.is_empty() && !call_id.is_empty() {
            *current_id = call_id.to_owned();
        }
        if current_name.is_empty() && !call_name.is_empty() {
            *current_name = call_name.to_owned();
        }
    }
}
