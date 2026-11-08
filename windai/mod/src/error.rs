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

    #[error("the package at {path} cannot be accepted: {reason}")]
    Mismatch { path: String, reason: String },

    #[error("{name} is already installed")]
    AlreadyInstalled { name: String },

    #[error("{name} is not installed")]
    NotInstalled { name: String },

    #[error("mod {name} version {version} is not published")]
    VersionNotPublished { name: String, version: u32 },

    #[error("mod {name} version {version} needs a script engine")]
    ScriptsUnavailable { name: String, version: u32 },

    #[error("the operation on {name} conflicts with the current state: {reason}")]
    Conflict { name: String, reason: String },

    #[error("the publish task ended unexpectedly: {0}")]
    Background(String),

    #[error("the event payload cannot be encoded: {0}")]
    Encode(String),

    #[error("mod {name} version {version} cannot provide resources at {path}: {reason}")]
    Resource {
        name: String,
        version: u32,
        path: PathBuf,
        reason: String,
    },

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
