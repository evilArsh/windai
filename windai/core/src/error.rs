#[derive(thiserror::Error, Debug)]
pub enum CoreError {
    #[error("database error. {0}")]
    Database(#[from] sqlx::Error),

    #[error("row not found. {0}")]
    NotFound(String),

    #[error("validation error. {0}")]
    Validation(String),

    #[error("chat error. {0}")]
    Chat(String),

    #[error("AI provider error. {0}")]
    Ai(#[from] wind_ai::ProviderError),

    #[error("MCP error. {0}")]
    Mcp(#[from] wind_mcp::client::McpError),

    #[error("JS error. {0}")]
    Js(String),

    #[error("url parse error. {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("string parse error. {0}")]
    StrumParse(#[from] strum::ParseError),

    #[error("JSON error. {0}")]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    JsonRule(#[from] wind_rule::Error),

    #[error("Internal error. {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, CoreError>;
