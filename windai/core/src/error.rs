use wind_ai::AdapterError;
use wind_ai::chat::ProviderError;
use wind_event::dispatch::BusError;
use wind_mod::error::ModError;
use wind_storage::error::StorageError;

// TODO: 定稿后删除未使用的
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("database error: {0}")]
    Database(String),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Adapter(#[from] AdapterError),

    #[error(transparent)]
    Provider(#[from] ProviderError),

    #[error(transparent)]
    Mod(#[from] ModError),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("expected to affect rows, but none were affected")]
    NoRowsAffected,

    #[error("expected at most one row, but found {found}")]
    TooManyRows { found: usize },

    #[error("column {column} does not hold the shape it declares: {detail}")]
    MalformedColumn { column: String, detail: String },

    #[error(transparent)]
    Parse(#[from] strum::ParseError),

    #[error("event bus error: {0}")]
    Bus(String),

    #[error("denied: {0}")]
    Denied(String),

    #[error("agent is not running")]
    Closed,
}

impl From<sqlx::Error> for CoreError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error.to_string())
    }
}

impl From<serde_json::Error> for CoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Database(error.to_string())
    }
}

impl From<BusError> for CoreError {
    fn from(error: BusError) -> Self {
        Self::Bus(error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, CoreError>;
