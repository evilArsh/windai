use wind_core::error::CoreError;
use wind_core::feed::FeedError;
use wind_event::BusError;
use wind_script::ScriptsError;
use wind_storage::error::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("the runtime has stopped")]
    Stopped,
    #[error("denied: {0}")]
    Denied(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Feed(#[from] FeedError),
    #[error(transparent)]
    Scripts(#[from] ScriptsError),
    #[error(transparent)]
    Bus(#[from] BusError),
}
