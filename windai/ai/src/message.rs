use super::tool::{FunctionCall, FunctionCallOutput};
use chrono::Utc;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

#[derive(
    Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy, strum::EnumString, strum::Display,
)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
    Developer,
}

/// 音频消息内容
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AudioContent {
    /// 编码, eg: mp3, wav
    pub format: String,
    /// 语音消息 base64 编码
    pub content: String,
}

/// 消息内容
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "camelCase")]
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
    pub fn new_function_call(call_id: String, value: Value) -> Self {
        Self::FunctionCall {
            data: FunctionCallOutput {
                id: call_id,
                content: value,
            },
        }
    }
}

/// 模型请求或响应信息
#[derive(Debug, Serialize, Deserialize, Clone, Builder)]
#[builder(setter(strip_option, into))]
pub struct Message {
    #[builder(default = "Role::Assistant")]
    pub role: Role,
    /// 响应和请求消息，涵盖以下类型：
    ///
    /// # 请求消息
    ///
    /// 1. 用户可能同时输入多种模态的消息：文本，图片，语音
    /// 2. 本地/远程 tool_calls 调用结果
    ///
    /// # 响应消息
    ///
    /// 模型可能响应文本，图片，语音数据。
    ///
    /// 模型为用户选择的 tool_calls 信息包含在单独的 [Self::tool_calls] 中。
    #[builder(default)]
    pub content: Vec<Content>,
    /// 推理消息
    #[builder(default)]
    pub reasoning_content: Option<String>,
    /// 创建时间
    #[builder(default)]
    pub created_at: i64,
    /// 用户输入的token数
    #[builder(default)]
    pub input_tokens: i32,
    /// 模型输出的token数
    #[builder(default)]
    pub output_tokens: i32,
    /// 工具调用信息
    /// - 模型为用户选择的工具调用信息，用户根据返回的信息调用MCP工具
    #[builder(default)]
    pub tool_calls: Option<Vec<FunctionCall>>,
}

impl fmt::Display for Content {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Content::Text { data } => write!(f, "Text({})", data),
            Content::Image { data } => write!(f, "Image({})", data),
            Content::File { data } => write!(f, "File({})", data),
            Content::Audio { data } => write!(
                f,
                "Audio(format={}, len={})",
                data.format,
                data.content.len()
            ),
            Content::FunctionCall { data } => {
                write!(f, "FunctionCall(id={}, content={})", data.id, data.content)
            }
        }
    }
}

impl Message {
    /// 追加流式数据
    pub fn append_chunk(&mut self, partial: Message) {
        if let Some(content) = partial.content.into_iter().next() {
            if let Some(self_content) = self.content.last_mut() {
                match (self_content, content) {
                    (Content::Text { data }, Content::Text { data: data_new }) => {
                        data.push_str(&data_new)
                    }
                    (Content::Image { data }, Content::Image { data: data_new }) => {
                        data.push_str(&data_new)
                    }
                    (Content::File { data }, Content::File { data: data_new }) => {
                        data.push_str(&data_new)
                    }
                    (Content::Audio { data }, Content::Audio { data: data_new }) => {
                        data.content.push_str(&data_new.content)
                    }
                    _ => {}
                }
            } else {
                self.content.push(content);
            }
        }

        if let Some(new_reasoning) = partial.reasoning_content
            && !new_reasoning.is_empty()
        {
            match self.reasoning_content.as_mut() {
                Some(self_reasoning) => *self_reasoning += &new_reasoning,
                None => self.reasoning_content = Some(new_reasoning),
            }
        }

        if let Some(new_tool_calls) = partial.tool_calls {
            for new_tool_call in new_tool_calls {
                if !new_tool_call.id.is_empty() {
                    match self.tool_calls.as_mut() {
                        Some(self_tool_calls) => {
                            self_tool_calls.push(new_tool_call);
                        }
                        None => self.tool_calls = Some(vec![new_tool_call]),
                    }
                } else {
                    match self.tool_calls.as_mut() {
                        Some(self_tool_calls) => {
                            if let Some(last) = self_tool_calls.last_mut() {
                                last.arguments += &new_tool_call.arguments;
                            }
                        }
                        // drop
                        _ => {}
                    }
                }
            }
        }

        if partial.created_at != 0 {
            self.created_at = partial.created_at;
        }
        self.input_tokens += partial.input_tokens;
        self.output_tokens += partial.output_tokens;
    }

    /// 简单消息，不包含任何 tool_call
    #[inline]
    pub fn is_simple(&self) -> bool {
        self.tool_calls.is_none() && (self.role == Role::User || self.role == Role::Assistant)
    }

    /// 是否为 tool_call 调用结果
    #[inline]
    pub fn is_tool_result(&self) -> bool {
        self.role == Role::Tool
    }

    /// 判断条件为：角色为 Assistant 并且 tool_calls 有值
    #[inline]
    pub fn is_tool_request(&self) -> bool {
        self.role == Role::Assistant && self.tool_calls.as_ref().is_some_and(|c| !c.is_empty())
    }

    /// 构建一个简单的上下文，用于直接放入文本数据
    #[inline]
    pub fn new_simple(
        role: Role,
        content: Vec<Content>,
        reasoning_content: Option<String>,
    ) -> Self {
        Self {
            role,
            content,
            reasoning_content,
            created_at: 0,
            input_tokens: 0,
            output_tokens: 0,
            tool_calls: None,
        }
    }

    /// 构建一个工具调用结果上下文，放入本地调用结果
    #[inline]
    pub fn new_tool_result(value: Vec<FunctionCallOutput>) -> Self {
        Self {
            role: Role::Tool,
            content: value
                .into_iter()
                .map(|v| Content::new_function_call(v.id, v.content))
                .collect(),
            reasoning_content: None,
            tool_calls: None,
            created_at: 0,
            input_tokens: 0,
            output_tokens: 0,
        }
    }

    /// 构建一个模型返回的工具调用选择上下文
    /// - reasoning_content: 模型推理消息，一些模型可能需要该消息(DeepSeek)
    #[inline]
    pub fn new_tool_request(
        call_res: Vec<FunctionCall>,
        reasoning_content: Option<String>,
    ) -> Self {
        Self {
            role: Role::Assistant,
            content: vec![],
            reasoning_content,
            tool_calls: Some(call_res),
            created_at: 0,
            input_tokens: 0,
            output_tokens: 0,
        }
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Message {{")?;
        writeln!(f, "  role: {}", self.role)?;
        if !self.content.is_empty() {
            writeln!(f, "  content: [")?;
            for c in &self.content {
                writeln!(f, "    {},", c)?;
            }
            write!(f, "  ]")?;
        }
        if let Some(reasoning) = &self.reasoning_content {
            writeln!(f, ",\n  reasoning: {}", reasoning)?;
        }
        if let Some(tool_calls) = &self.tool_calls {
            writeln!(f, ",\n  tool_calls: [")?;
            for tc in tool_calls {
                writeln!(f, "    {},", tc)?;
            }
            write!(f, "  ]")?;
        }
        write!(f, "\n}}")
    }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            role: Role::Assistant,
            content: vec![],
            reasoning_content: None,
            created_at: Utc::now().timestamp_millis(),
            input_tokens: 0,
            output_tokens: 0,
            tool_calls: None,
        }
    }
}

/// 对话请求参数
#[derive(Debug, Serialize, Clone, Default)]
pub struct ReqConfig {
    /// 采样温度，范围 0~2。较高值使输出更随机，较低值使输出更聚焦。
    /// 通常建议只调 temperature 或 top_p 之一。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    /// 核采样阈值。模型只考虑累积概率达到 top_p 的候选 token。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    /// 最大输出 token 数。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<i32>,
    /// 是否启用流式输出
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    /// 存在性惩罚，-2.0 ~ 2.0。正值增加模型讨论新话题的可能性。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f64>,
    /// 频率惩罚，-2.0 ~ 2.0。正值降低模型逐字重复的可能性。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f64>,
    /// 是否在工具调用期间启用并行工具调用。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    /// 是否开启推理模式。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<bool>,
}
