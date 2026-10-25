use wind_core::error::CoreError;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("denied: {0}")]
    Denied(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("the runtime has stopped")]
    Stopped,
    #[error(transparent)]
    Core(#[from] CoreError),
}
