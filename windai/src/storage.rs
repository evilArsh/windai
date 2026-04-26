use crate::env;
use rusqlite::Connection;
use std::sync::{Mutex, OnceLock};
use thiserror::Error;

mod message;
mod model;
mod provider;
mod schema;
mod topic;

static GLOBAL_DATABASE: OnceLock<Storage> = OnceLock::new();

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("internal error: {0}")]
    Internal(String),

    #[error("database error: ${0}")]
    Database(#[from] rusqlite::Error),

    #[error(transparent)]
    StrumParse(#[from] strum::ParseError),
}

macro_rules! lock_db {
    ($storage:expr) => {
        $storage
            .conn
            .lock()
            .map_err(|e| StorageError::Internal(e.to_string()))?
    };
}

use lock_db;

pub struct Storage {
    conn: Mutex<Connection>,
}

impl Storage {
    fn init() -> Result<Self, StorageError> {
        let db_path = env::db_path();
        let conn = Connection::open(db_path)?;
        let storage: Storage = Storage {
            conn: Mutex::new(conn),
        };
        storage.create_tables()?;
        Ok(storage)
    }
}

/// 获取全局数据库句柄
pub fn global() -> &'static Storage {
    GLOBAL_DATABASE.get_or_init(|| Storage::init().unwrap())
}
