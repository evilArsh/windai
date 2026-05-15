use crate::{
    message::{Message, ReqConfig},
    model::AdaptorType,
    tool::Tools,
};
use serde_json::Value;
use thiserror::Error;

mod openai_completion;
mod openai_responses;
mod schema;

#[derive(Error, Debug)]
pub enum AdaptorError {
    #[error("Transfer error: {0}")]
    Transfer(String),

    #[error("Json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Parse error: {0}")]
    ParseError(#[from] strum::ParseError),

    #[error("Invalid content type: {0}")]
    InvalidContentType(String),

    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
}

/// 获取适配器默认的 endpoint
pub fn get_default_endpoint(adaptor: AdaptorType) -> String {
    match adaptor {
        AdaptorType::OpenAICompletion => String::from("/chat/completions"),
        AdaptorType::OpenAIResponse => String::from("/responses"),
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
        config: &ReqConfig,
        contexts: &Vec<Message>,
        tools: Option<&Vec<Tools>>,
    ) -> Result<Value, AdaptorError>;
    /// 将原始响应字节解析为统一格式消息
    fn parse_response(&self, data: &[u8]) -> Result<Message, AdaptorError>;
    /// 将原始流式响应单块字节解析为统一格式消息
    fn parse_stream_chunk(&self, data: &[u8]) -> Result<Vec<Message>, AdaptorError>;
}

/// 根据 AdaptorType 获取对应的对话适配器实例
pub fn get_chat_adaptor(adaptor: AdaptorType) -> Box<dyn ChatAdaptor> {
    match adaptor {
        AdaptorType::OpenAICompletion => Box::new(openai_completion::OpenAICompletionAdaptor),
        AdaptorType::OpenAIResponse => Box::new(openai_responses::OpenAIResponseAdaptor),
    }
}
