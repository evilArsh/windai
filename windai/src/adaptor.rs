use serde::{Deserialize, Serialize};
// use serde_json::Value;

pub mod openai_chat;
pub mod openai_response;
pub mod sse;

pub trait AdaptorHandler {
    // fn serialize_request(&self) -> Value;
}
pub fn is_none_or_empty_vec<T>(opt: &Option<Vec<T>>) -> bool {
    opt.as_ref().map(|v| v.is_empty()).unwrap_or(true)
}

#[derive(Serialize, Deserialize, Clone)]
pub enum AdaptorType {
    OpenAICompletion,
    OpenAIResponse,
}
