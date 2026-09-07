use std::result::Result as StdResult;

/// 能力层错误码。
#[derive(thiserror::Error, Debug, strum::AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum FsError {
    #[error("path not found: {0}")]
    NotFound(String),

    #[error("operation not allowed: {0}")]
    NotAllowed(String),

    #[error("is a directory: {0}")]
    IsDir(String),

    #[error("is a file: {0}")]
    IsFile(String),

    #[error("binary file: {0}")]
    Binary(String),

    #[error("content too large: {0}")]
    TooLarge(String),

    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("command timed out")]
    Timeout,

    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub type Result<T> = StdResult<T, FsError>;
