use std::path::PathBuf;

use wind_storage::error::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum ModError {
    #[error("cannot read the manifest at {path}: {source}")]
    ManifestIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("manifest does not match its declared shape: {0}")]
    ManifestShape(String),

    #[error("field {field} is invalid: {reason}")]
    ManifestField { field: String, reason: String },

    #[error("event {name} is not in the directory")]
    ManifestUnknownEvent { name: String },

    #[error("the manifest at {path} does not declare a usable identity: {reason}")]
    Identity { path: PathBuf, reason: String },

    #[error("the package at {path} cannot be accepted: {reason}")]
    Mismatch { path: String, reason: String },

    #[error("{name} is already installed")]
    AlreadyInstalled { name: String },

    #[error("{name} is not installed")]
    NotInstalled { name: String },

    #[error("the package at {path} was refused: {reason}")]
    Rejected { path: String, reason: String },

    #[error("event bus error: {0}")]
    Bus(String),

    #[error("topic servers refused: {0}")]
    Servers(String),

    #[error(transparent)]
    Storage(#[from] StorageError),
}

pub type Result<T> = std::result::Result<T, ModError>;
