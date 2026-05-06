mod client;
mod error;

use std::fmt::Display;

use crate::{ChatError, adaptor::AdaptorError, client::ClientError};
use async_stream::stream;
use chrono::Utc;
use futures::stream::Stream;
use log;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

pub mod adaptor;
pub use error::*;

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

#[derive(Debug, Serialize, Clone)]
pub struct Model {
    /// 提供商提供的模型名称
    pub name: String,
    /// 当前模型的适配器类型。
    /// 该类型决定了模型请求和响应结果的处理方式
    pub adaptor: AdaptorType,
    /// 模型专属端点地址
    ///
    /// 默认使用与 [AdaptorType] 关联的提供商的默认端点。
    pub endpoint: Option<String>,
}

/// 文本消息类型，当前文本消息细分为以下类型
///
/// - Text: 文本消息（纯文本对话）
/// - Image: 图片消息（分析图像并将其用作生成文本或音频的输入）
/// - Audio: 音频消息（音频和文本的输入与输出）
/// - File: 文件消息
#[derive(Debug, Serialize, PartialEq, Copy, Eq, Clone, strum::EnumString, strum::Display)]
#[serde(rename_all = "lowercase")]
pub enum ContentType {
    Text,
    Image,
    Audio,
    File,
}

/// 消息内容
#[derive(Debug, Serialize, Clone)]
pub struct Content {
    /// 消息内容
    /// - 纯文本消息
    /// - 图片消息
    /// - 音频消息
    /// - 文件消息
    pub content: String,
    /// 消息类型
    /// - 标识文本，语音，图片，文件消息
    pub content_type: ContentType,
}
impl Content {
    pub fn new(content_type: ContentType, content: String) -> Self {
        Self {
            content,
            content_type,
        }
    }
}
/// 工具调用请求参数
#[derive(Debug, Serialize, Clone)]
pub struct ToolCallSchema {
    /// 要调用的函数名称
    pub name: String,
    /// 函数描述。模型根据此描述决定是否调用该函数。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 描述函数参数的 JSON schema 对象
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    /// 是否强制执行严格的参数验证
    /// - 一些中间厂商可能不支持该参数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

#[derive(Debug, Serialize, Clone)]
/// 工具调用信息
pub struct ToolCallInfo {
    /// 工具调用 ID
    pub call_id: String,
    /// 函数名称
    pub name: String,
    /// 模型生成的工具调用参数
    pub arguments: String,
}

/// 模型响应消息
#[derive(Debug, Serialize, Clone)]
pub struct Message {
    pub role: Role,
    /// 提供商返回的原始消息内容
    pub raw_content: Option<String>,
    /// 模型响应消息
    /// - 包含多种模态的消息；列如：文本，图片，语音
    pub content: Option<Content>,
    /// 模型推理消息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    /// 语音转文字结果
    pub transcript: Option<String>,
    /// 创建时间
    pub created_at: i64,
    /// 用户输入的token数
    pub input_tokens: i32,
    /// 模型输出的token数
    pub output_tokens: i32,
    /// 模型返回的工具调用列表
    pub tool_calls: Option<Vec<ToolCallInfo>>,
}
impl Message {
    pub fn default_assistant() -> Self {
        Self {
            role: Role::Assistant,
            raw_content: None,
            content: Some(Content::new(ContentType::Text, String::new())),
            reasoning_content: None,
            created_at: Utc::now().timestamp(),
            transcript: None,
            input_tokens: 0,
            output_tokens: 0,
            tool_calls: None,
        }
    }
    /// 追加流式数据
    pub fn append_partial(&mut self, partial: Message) {
        if let Some(content) = partial.content {
            if let Some(self_content) = self.content.as_mut() {
                self_content.content_type = content.content_type;
                self_content.content += &content.content;
            } else {
                self.content = Some(content);
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

        if let Some(new_transcript) = partial.transcript
            && !new_transcript.is_empty()
        {
            match self.transcript.as_mut() {
                Some(self_transcript) => *self_transcript += &new_transcript,
                None => self.transcript = Some(new_transcript),
            }
        }

        if let Some(new_tool_calls) = partial.tool_calls {
            for new_tool_call in new_tool_calls {
                if !new_tool_call.call_id.is_empty() {
                    match self.tool_calls.as_mut() {
                        Some(self_tool_calls) => {
                            self_tool_calls.push(new_tool_call);
                        }
                        None => self.tool_calls = Some(vec![new_tool_call]),
                    }
                } else {
                    match self.tool_calls.as_mut() {
                        Some(self_tool_calls) => {
                            // call_id 为空表示延续上一个 tool_call，追加 arguments 和补充 name
                            if let Some(last) = self_tool_calls.last_mut() {
                                if !new_tool_call.name.is_empty() {
                                    last.name = new_tool_call.name;
                                }
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
        self.raw_content = partial.raw_content;
        self.input_tokens += partial.input_tokens;
        self.output_tokens += partial.output_tokens;
    }
}

/// 消息上下文结构
#[derive(Debug, Serialize, Clone)]
pub struct Context {
    /// 角色
    pub role: Role,
    /// 解析之后的文本数据
    /// - TODO: 如果将音频数据放入上下文，放入返回的字节数据还是翻译后的 `transcript`
    pub content: Vec<Content>,
    /// 模型推理消息，一些模型可能需要该消息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    /// 工具调用上下文
    pub tool_calls: Option<Vec<ToolCallInfo>>,
    /// 工具调用 ID, 该值存在时，本地调用结果会放入 content 中。
    /// 此时该上下文为工具调用结果
    pub tool_call_id: Option<String>,
}
impl Context {
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
            tool_calls: None,
            tool_call_id: None,
        }
    }
    /// 构建一个工具调用结果上下文，放入本地调用结果
    #[inline]
    pub fn new_tool_result(tool_call_id: String, call_value: String) -> Self {
        Self {
            role: Role::Tool,
            content: vec![Content::new(ContentType::Text, call_value)],
            reasoning_content: None,
            tool_call_id: Some(tool_call_id),
            tool_calls: None,
        }
    }

    /// 构建一个模型返回的工具调用选择上下文
    /// - reasoning_content: 模型推理消息，一些模型可能需要该消息(DeepSeek)
    #[inline]
    pub fn new_tool_request(
        call_res: Vec<ToolCallInfo>,
        reasoning_content: Option<String>,
    ) -> Self {
        Self {
            role: Role::Assistant,
            content: vec![],
            reasoning_content,
            tool_calls: Some(call_res),
            tool_call_id: None,
        }
    }
}

/// 对话请求参数
#[derive(Debug, Serialize, Clone)]
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
    /// 工具调用列表
    /// - TODO: responses 风格中包含更多类型的工具调用
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ToolCallSchema>>,
}

/// 聊天统一响应事件
#[derive(Debug, PartialEq, Eq, strum::Display)]
pub enum ResEventStatus {
    /// 用于流式消息
    Partial,
    /// 流式/非流式 数据接收完毕
    Finish,
    /// 发生错误后，终止请求并且返回具体错误
    Error,
}
#[derive(Debug)]
pub struct ResEvent {
    pub status: ResEventStatus,
    pub data: Option<Message>,
    pub error: Option<ChatError>,
}
impl ResEvent {
    #[inline]
    pub fn new_partial(data: Message) -> Self {
        Self {
            status: ResEventStatus::Partial,
            data: Some(data),
            error: None,
        }
    }
    #[inline]
    pub fn new_finish(data: Message) -> Self {
        Self {
            status: ResEventStatus::Finish,
            data: Some(data),
            error: None,
        }
    }
    #[inline]
    pub fn new_error(error: ChatError) -> Self {
        Self {
            status: ResEventStatus::Error,
            data: None,
            error: Some(error),
        }
    }
}

impl Display for ResEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "status: {}, error: {:?}, data:\n{:?}",
            self.status, self.error, self.data
        )
    }
}
impl From<ClientError> for ResEvent {
    fn from(value: ClientError) -> Self {
        ResEvent {
            status: ResEventStatus::Error,
            data: None,
            error: Some(value.into()),
        }
    }
}
impl From<AdaptorError> for ResEvent {
    fn from(value: AdaptorError) -> Self {
        ResEvent {
            status: ResEventStatus::Error,
            data: None,
            error: Some(value.into()),
        }
    }
}
impl From<url::ParseError> for ResEvent {
    fn from(value: url::ParseError) -> Self {
        ResEvent {
            status: ResEventStatus::Error,
            data: None,
            error: Some(value.into()),
        }
    }
}

/// 发送一次对话请求
pub fn handle_chat(
    contexts: Vec<Context>,
    config: ReqConfig,
    model: Model,
    api_base: &str,
    api_key: &str,
) -> impl Stream<Item = ResEvent> {
    stream! {
        let chat_adaptor = adaptor::get_chat_adaptor(model.adaptor);
        let endpoint = model
            .endpoint
            .unwrap_or_else(|| adaptor::get_default_endpoint(model.adaptor));

        let url = match Url::parse(&format!(
            "{}/{}",
            api_base.trim_end_matches("/"),
            endpoint.trim_start_matches("/")
        )) {
            Ok(u) => u,
            Err(e) => {
                yield e.into();
                return;
            }
        };
        let is_stream = config.stream;
        let req_body = match chat_adaptor.build_request(&model.name, config, contexts) {
            Ok(body) => body,
            Err(e) => {
                yield e.into();
                return;
            }
        };
        log::debug!(
            "[request body]\n{}",
            serde_json::to_string_pretty(&req_body).unwrap_or_default()
        );
        match is_stream {
            Some(true) => {
                let response = match client::request_sse(url.as_str(), Method::POST, |req| {
                    req.json(&req_body).bearer_auth(api_key)
                })
                .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        yield e.into();
                        return;
                    }
                };
                let stream = client::handle_stream(response);
                let mut msg = Message::default_assistant();
                for await result in stream {
                    match result {
                        Ok(bytes) => {
                            let chunks = match chat_adaptor.parse_stream_chunk(bytes) {
                                Ok(c) => c,
                                Err(e) => {
                                    log::error!("[parse_stream_chunk error]\n{}", e.to_string());
                                    yield e.into();
                                    return;
                                }
                            };
                            for chunk in chunks {
                                msg.append_partial(chunk);
                                yield ResEvent::new_partial(msg.clone());
                            }
                        }
                        Err(err) => {
                            yield ResEvent::new_error(err.into());
                        }
                    };
                }
                yield ResEvent::new_finish(msg.clone());
            }
            _ => {
                let response = match client::request(url.as_str(), Method::POST, |req| {
                    req.json(&req_body).bearer_auth(api_key)
                })
                .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("[response error] status:{}, text: {}", e.code, e.msg);
                        yield e.into();
                        return;
                    }
                };
                let res = match client::handle_response(response).await {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("[handle_response error]\n{}", e);
                        yield e.into();
                        return;
                    }
                };
                let response = match chat_adaptor.parse_response(res) {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("[parse response error]\n{}", e);
                        yield e.into();
                        return;
                    }
                };
                yield ResEvent::new_finish(response);
            }
        }
    }
}
