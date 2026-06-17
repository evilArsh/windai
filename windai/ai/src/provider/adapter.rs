use crate::{
    message::{Message, ReqConfig},
    model::AdapterType,
    tool::Tools,
};
use serde_json::Value;
use thiserror::Error;

mod openai_completion;
mod openai_responses;
mod schema;

#[derive(Error, Debug)]
pub enum AdapterError {
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
pub fn get_default_endpoint(adapter: AdapterType) -> String {
    match adapter {
        AdapterType::OpenAICompletion => String::from("/chat/completions"),
        AdapterType::OpenAIResponse => String::from("/responses"),
    }
}

pub trait Adapter {
    fn get_type(&self) -> AdapterType;
}

/// 文本对话适配器
pub trait ChatAdapter: Adapter + Send + Sync {
    /// 将统一请求配置和消息列表转换为提供商标准请求格式
    fn build_request(
        &self,
        model_name: &str,
        config: &ReqConfig,
        contexts: &[Message],
        tools: Option<&[Tools]>,
    ) -> Result<Value, AdapterError>;
    /// 将原始响应字节解析为统一格式消息
    fn parse_response(&self, data: &[u8]) -> Result<Message, AdapterError>;
    /// 将原始流式响应单块字节解析为统一格式消息
    fn parse_stream_chunk(&self, data: &[u8]) -> Result<Vec<Message>, AdapterError>;
}

/// 根据 AdapterType 获取对应的对话适配器实例
pub fn get_chat_adapter(adapter: AdapterType) -> Box<dyn ChatAdapter + Send + Sync> {
    match adapter {
        AdapterType::OpenAICompletion => Box::new(openai_completion::OpenAICompletionAdapter),
        AdapterType::OpenAIResponse => Box::new(openai_responses::OpenAIResponseAdapter),
    }
}
