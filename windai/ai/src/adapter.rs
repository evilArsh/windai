use super::error::AdapterError;
use super::eventsource::Event;
use super::model::{AdapterType, ChatModel};
use wind_wire::message::Message;
use wind_wire::tool::Tools;

pub trait ChatAdapter: Send + Sync {
    fn path(&self) -> &'static str;

    fn build_request(
        &self,
        model: &ChatModel,
        contexts: &[Message],
        tools: Option<&[Tools]>,
    ) -> Result<serde_json::Value, AdapterError>;

    fn parse_response(&self, data: &[u8]) -> Result<Message, AdapterError>;

    fn parse_stream_chunk(&self, event: &Event) -> Result<Option<Message>, AdapterError>;
}

pub fn adapter_of(kind: AdapterType) -> Box<dyn ChatAdapter> {
    match kind {
        AdapterType::OpenAICompletion => Box::new(super::openai_completion::OpenAICompletion),
        AdapterType::OpenAIResponse => Box::new(super::openai_responses::OpenAIResponse),
    }
}
