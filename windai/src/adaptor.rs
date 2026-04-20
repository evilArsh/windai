use super::{
    dto::chat::{MessageCommon, MessageResponse, RequestConfig},
    models::AdaptorType,
    proxy::error::ProxyError,
};
use bytes::Bytes;
use futures::stream::{Stream, StreamExt};
use serde_json::Value;
use thiserror::Error;

pub mod openai;
pub mod openai_completion;
pub mod openai_response;

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

    #[error(transparent)]
    Proxy(#[from] ProxyError),
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
    // fn include(&self, adaptor_type: AdaptorType) -> bool;
}
pub trait ChatAdaptor: Adaptor {
    /// 将统一请求配置和消息列表转换为提供商标准请求格式
    fn build_request(
        &self,
        model_name: &str,
        config: &RequestConfig,
        contexts: &Vec<MessageResponse>,
    ) -> Result<Value, AdaptorError>;

    /// 将提供商原始响应字节解析为统一格式消息
    fn parse_response(&self, data: Bytes) -> Result<MessageCommon, AdaptorError>;
    /// 将提供商原始流式响应字节解析为统一格式消息
    fn parse_stream_response(
        &self,
        stream: impl Stream<Item = Result<Bytes, ProxyError>>,
    ) -> impl Stream<Item = Result<MessageCommon, AdaptorError>>;
}

/// 根据 AdaptorType 获取对应的对话适配器实例
pub fn get_chat_adaptor(adaptor: AdaptorType) -> Box<dyn ChatAdaptor> {
    match adaptor {
        AdaptorType::OpenAICompletion => Box::new(openai::OpenAICompletionAdaptor),
        AdaptorType::OpenAIResponse => Box::new(openai::OpenAIResponseAdaptor),
    }
}
