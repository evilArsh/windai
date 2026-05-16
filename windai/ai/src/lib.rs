use crate::provider::{adaptor::AdaptorError, client::ClientError};

#[derive(thiserror::Error, Debug)]
pub enum ProviderError {
    #[error(transparent)]
    Client(#[from] ClientError),

    #[error(transparent)]
    Adaptor(#[from] AdaptorError),

    #[error("Url parse error: {0}")]
    UrlParse(#[from] url::ParseError),
}

pub mod chat;
pub mod message;
pub mod model;
pub mod provider;
pub mod tool;
