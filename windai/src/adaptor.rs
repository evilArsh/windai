pub mod openai_chat;
pub mod openai_response;
pub mod sse;

pub fn is_none_or_empty_vec<T>(opt: &Option<Vec<T>>) -> bool {
    opt.as_ref().map(|v| v.is_empty()).unwrap_or(true)
}
