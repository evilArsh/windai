use super::tool::{FunctionCall, FunctionCallOutput};
use crate::{AudioContent, Content, Role};
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::AsRefStr,
    strum::EnumString,
    strum::Display,
    schemars::JsonSchema,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum BlockKind {
    Reasoning,
    Text,
    Image,
    File,
    Audio,
    ToolCall,
    ToolResult,
}

/// 一个 Message 被拆分成多个独立的 Block
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Block {
    /// 块类型
    kind: BlockKind,

    /// 块所属消息的角色
    role: Role,

    /// 块在消息内的位置，从 0 开始按吸收顺序递增
    seq: i64,

    /// 被改到第几版，每次追加递增
    revision: i64,

    /// reasoning/text/image/audio
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,

    /// 音频格式
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<String>,

    /// 工具调用请求/响应 id
    #[serde(skip_serializing_if = "Option::is_none")]
    call_id: Option<String>,

    /// 工具调用名字
    #[serde(skip_serializing_if = "Option::is_none")]
    call_name: Option<String>,

    /// 工具调用参数
    #[serde(skip_serializing_if = "Option::is_none")]
    call_arguments: Option<String>,

    /// 工具调用结果
    #[serde(skip_serializing_if = "Option::is_none")]
    call_res: Option<serde_json::Value>,
}

impl Block {
    pub fn new_reasoning(role: Role, seq: i64) -> Self {
        Self::blank(role, seq, BlockKind::Reasoning)
    }

    pub fn new_text(role: Role, seq: i64) -> Self {
        Self::blank(role, seq, BlockKind::Text)
    }

    pub fn new_image(role: Role, seq: i64, data: String) -> Self {
        Self {
            content: Some(data),
            revision: 1,
            ..Self::blank(role, seq, BlockKind::Image)
        }
    }

    pub fn new_file(role: Role, seq: i64, data: String) -> Self {
        Self {
            content: Some(data),
            revision: 1,
            ..Self::blank(role, seq, BlockKind::File)
        }
    }

    pub fn new_audio(role: Role, seq: i64, format: String, content: String) -> Self {
        Self {
            format: Some(format),
            content: Some(content),
            revision: 1,
            ..Self::blank(role, seq, BlockKind::Audio)
        }
    }

    pub fn new_tool_call(
        role: Role,
        seq: i64,
        call_id: String,
        name: String,
        arguments: Option<String>,
    ) -> Self {
        Self {
            call_id: (!call_id.is_empty()).then_some(call_id),
            call_name: (!name.is_empty()).then_some(name),
            call_arguments: arguments,
            ..Self::blank(role, seq, BlockKind::ToolCall)
        }
    }

    pub fn new_tool_result(
        role: Role,
        seq: i64,
        call_id: String,
        content: serde_json::Value,
    ) -> Self {
        Self {
            call_id: Some(call_id),
            call_res: Some(content),
            revision: 1,
            ..Self::blank(role, seq, BlockKind::ToolResult)
        }
    }

    pub fn reasoning(&self) -> Option<&str> {
        match self.kind {
            BlockKind::Reasoning => Some(self.content.as_deref().unwrap_or_default()),
            _ => None,
        }
    }

    pub fn text(&self) -> Option<&str> {
        match self.kind {
            BlockKind::Text => Some(self.content.as_deref().unwrap_or_default()),
            _ => None,
        }
    }

    pub fn media(&self) -> Option<&str> {
        match self.kind {
            BlockKind::Image | BlockKind::File | BlockKind::Audio => {
                Some(self.content.as_deref().unwrap_or_default())
            }
            _ => None,
        }
    }

    pub fn kind(&self) -> BlockKind {
        self.kind
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn seq(&self) -> i64 {
        self.seq
    }

    pub fn is(&self, kind: BlockKind) -> bool {
        self.kind == kind
    }

    pub fn revision(&self) -> i64 {
        self.revision
    }

    pub fn call_id(&self) -> Option<&str> {
        self.call_id.as_deref()
    }

    /// Text、Image、File、Audio、ToolResult 各自的 Message::content
    pub fn content(&self) -> Option<Content> {
        let data = self.content.as_deref().unwrap_or_default();
        match self.kind {
            BlockKind::Text => Some(Content::Text {
                data: data.to_owned(),
            }),
            BlockKind::Image => Some(Content::Image {
                data: data.to_owned(),
            }),
            BlockKind::File => Some(Content::File {
                data: data.to_owned(),
            }),
            BlockKind::Audio => Some(Content::Audio {
                data: self.audio()?,
            }),
            BlockKind::ToolResult => Some(Content::FunctionCall {
                data: self.call_output()?,
            }),
            BlockKind::Reasoning | BlockKind::ToolCall => None,
        }
    }

    pub fn audio(&self) -> Option<AudioContent> {
        match self.kind {
            BlockKind::Audio => Some(AudioContent {
                format: self.format.clone().unwrap_or_default(),
                content: self.content.clone().unwrap_or_default(),
            }),
            _ => None,
        }
    }

    pub fn tool_call(&self) -> Option<FunctionCall> {
        match self.kind {
            BlockKind::ToolCall => Some(FunctionCall {
                id: self.call_id.clone().unwrap_or_default(),
                name: self.call_name.clone().unwrap_or_default(),
                arguments: self.call_arguments.clone().unwrap_or_default(),
            }),
            _ => None,
        }
    }

    pub fn call_output(&self) -> Option<FunctionCallOutput> {
        match self.kind {
            BlockKind::ToolResult => Some(FunctionCallOutput {
                id: self.call_id.clone().unwrap_or_default(),
                content: self.call_res.clone().unwrap_or(serde_json::Value::Null),
            }),
            _ => None,
        }
    }

    pub fn append_text(&mut self, fragment: &str) {
        if fragment.is_empty() {
            return;
        }
        self.content
            .get_or_insert_with(String::new)
            .push_str(fragment);
        self.revision += 1;
    }

    pub fn append_arguments(&mut self, fragment: &str) {
        if fragment.is_empty() {
            return;
        }
        self.call_arguments
            .get_or_insert_with(String::new)
            .push_str(fragment);
        self.revision += 1;
    }

    pub fn fill_call(&mut self, call_id: &str, name: &str) {
        if self.call_id.is_none() && !call_id.is_empty() {
            self.call_id = Some(call_id.to_owned());
        }
        if self.call_name.is_none() && !name.is_empty() {
            self.call_name = Some(name.to_owned());
        }
    }

    pub fn set_result(&mut self, content: serde_json::Value) {
        self.call_res = Some(content);
        self.revision = 1;
    }

    fn blank(role: Role, seq: i64, kind: BlockKind) -> Self {
        Self {
            kind,
            role,
            seq,
            revision: 0,
            content: None,
            format: None,
            call_id: None,
            call_name: None,
            call_arguments: None,
            call_res: None,
        }
    }
}
