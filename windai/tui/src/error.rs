use wind_core::error::CoreError;

#[derive(thiserror::Error, Debug)]
pub enum TuiError {
    #[error(transparent)]
    Core(#[from] CoreError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, TuiError>;
