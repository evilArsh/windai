#[derive(thiserror::Error, Debug)]
pub enum CoreError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    #[error("row not found: {0}")]
    NotFound(String),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("chat error: {0}")]
    Chat(String),

    #[error(transparent)]
    Ai(#[from] wind_ai::ProviderError),

    #[error(transparent)]
    Mcp(#[from] wind_mcp::client::McpError),

    #[error(transparent)]
    UrlParse(#[from] url::ParseError),

    #[error(transparent)]
    StrumParse(#[from] strum::ParseError),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    JsonRule(#[from] wind_rule::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, CoreError>;
