use wind_storage::error::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum FeedError {
    #[error("the feed is closed")]
    Closed,
    #[error("the subscription fell behind")]
    Lagged,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("event identity conflict")]
    IdentityConflict,
    #[error("the event sequence is exhausted")]
    SequenceExhausted,
}
