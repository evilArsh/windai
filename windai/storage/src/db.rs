use crate::error::Result;
use crate::schema;
use std::path::Path;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::time::Duration;
pub type DbPool = sqlx::SqlitePool;
pub type DbDriver = sqlx::Sqlite;
pub type DbTransaction = sqlx::Transaction<'static, sqlx::Sqlite>;
pub type DbRow = sqlx::sqlite::SqliteRow;
pub type DbQueryResult = <DbDriver as sqlx::Database>::QueryResult;
pub(crate) const BEGIN_WRITE: &str = "BEGIN IMMEDIATE";

pub async fn open_pool(path: &Path) -> Result<DbPool> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(8))
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    Ok(pool)
}

#[derive(Clone, Debug)]
pub struct Db {
    pool: DbPool,
}

impl Db {
    pub async fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            pool: open_pool(path).await?,
        })
    }

    pub async fn init_schema(&self) -> Result<()> {
        schema::create_if_missing(&self.pool).await
    }

    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
}
