use wind_wire::AdapterTypeError;

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("the provider response does not match its declared shape: {0}")]
    Shape(#[from] serde_json::Error),
    #[error("unknown content type: {0}")]
    ContentType(String),
    #[error(transparent)]
    Kind(#[from] AdapterTypeError),
    #[error("transfer failed: {0}")]
    Transfer(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
