use crate::provider::adapter::AdapterError;
use serde_json::Value;

pub type JsonObject<F = Value> = serde_json::Map<String, F>;

#[derive(thiserror::Error, Debug)]
pub enum ProviderError {
    #[error(transparent)]
    Client(#[from] client::ClientError),

    #[error(transparent)]
    Adapter(#[from] AdapterError),

    #[error("Url parse error: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("SSE error: {0}")]
    SSE(String),
}

pub mod chat;
pub(crate) mod client;
mod eventsource;
pub mod message;
pub mod model;
pub mod provider;
pub mod tool;
