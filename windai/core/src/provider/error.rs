use crate::provider::{adaptor::AdaptorError, client::ClientError};

#[derive(thiserror::Error, Debug)]
pub enum ProviderError {
    #[error(transparent)]
    Client(#[from] ClientError),

    #[error(transparent)]
    Adaptor(#[from] AdaptorError),

    #[error("url parse error: {0}")]
    UrlParse(#[from] url::ParseError),
}
