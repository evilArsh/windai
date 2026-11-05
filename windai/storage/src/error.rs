/// TODO: 删除多余的
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("expected to affect rows, but none were affected")]
    NoRowsAffected,
    #[error("expected at most one row, but found {found}")]
    TooManyRows { found: usize },
    #[error("column {column} does not hold the shape it declares: {detail}")]
    MalformedColumn { column: String, detail: String },
    #[error("text does not parse as the expected type: {0}")]
    Parse(#[from] strum::ParseError),
    #[error("event bus error: {0}")]
    Bus(String),
    #[error("denied: {0}")]
    Denied(String),
    #[error("event {epoch}:{seq} already exists with different content")]
    IdentityConflict { epoch: i64, seq: i64 },
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, StorageError>;
