pub mod adapter;
pub mod chat;
pub mod client;
pub mod error;
pub mod eventsource;
pub mod model;
mod openai_completion;
mod openai_responses;
pub mod schema;

pub use adapter::{ChatAdapter, adapter_of};
pub use error::AdapterError;
pub use model::{ChatModel, Target};
