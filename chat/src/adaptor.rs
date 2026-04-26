use crate::api::request::{ChatConfig, ChatMessageContext};
use crate::api::response::ChatMessageBase;
use bytes::Bytes;
use serde_json::Value;
use thiserror::Error;
use windai_domain::adaptor::AdaptorType;

pub mod openai;
pub mod openai_completion;
pub mod openai_response;
pub mod sse;

pub(crate) fn is_none_or_empty_vec<T>(opt: &Option<Vec<T>>) -> bool {
    opt.as_ref().map(|v| v.is_empty()).unwrap_or(true)
}

#[derive(Error, Debug)]
pub enum AdaptorError {
    #[error("transfer error: {0}")]
    Transfer(String),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("parse error: {0}")]
    ParseError(#[from] strum::ParseError),

    #[error("invalid content type: {0}")]
    InvalidContentType(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// 获取适配器默认的 endpoint
pub fn get_default_endpoint(adaptor: AdaptorType) -> String {
    match adaptor {
        AdaptorType::OpenAICompletion => "/chat/completions".to_string(),
        AdaptorType::OpenAIResponse => "/responses".to_string(),
    }
}

pub trait Adaptor {
    fn get_type(&self) -> AdaptorType;
}

pub trait ChatAdaptor: Adaptor {
    /// 将统一请求配置和消息列表转换为提供商标准请求格式
    fn build_request(
        &self,
        model_name: &str,
        config: &ChatConfig,
        contexts: &Vec<ChatMessageContext>,
    ) -> Result<Value, AdaptorError>;
    /// 将原始响应字节解析为统一格式消息
    fn parse_response(&self, data: Bytes) -> Result<ChatMessageBase, AdaptorError>;
    /// 将原始流式响应单块字节解析为统一格式消息
    fn parse_stream_chunk(&self, data: Bytes) -> Result<Vec<ChatMessageBase>, AdaptorError>;
}

/// 根据 AdaptorType 获取对应的对话适配器实例
pub fn get_chat_adaptor(adaptor: AdaptorType) -> Box<dyn ChatAdaptor> {
    match adaptor {
        AdaptorType::OpenAICompletion => Box::new(openai::OpenAICompletionAdaptor),
        AdaptorType::OpenAIResponse => Box::new(openai::OpenAIResponseAdaptor),
    }
}
