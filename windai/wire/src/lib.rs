//! 提供商中立的消息词汇

pub mod message;
pub mod tool;

pub use message::{AudioContent, Content, Message, Role};
pub use tool::{FunctionCall, FunctionCallOutput, FunctionTool, Tools};
