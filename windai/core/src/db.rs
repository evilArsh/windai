#[cfg(feature = "postgres")]
use super::error::CoreError;
use super::error::Result;
use super::schema;
use std::path::Path;

#[cfg(all(feature = "sqlite", feature = "postgres"))]
compile_error!(
    "Multiple database drivers detected! You can only enable ONE driver at a time.\n\
    Valid options: --features \"sqlite\", or --features \"postgres\"\n\
    Example: cargo build --no-default-features --features \"postgres\""
);

#[cfg(feature = "sqlite")]
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
#[cfg(feature = "sqlite")]
use std::str::FromStr;
#[cfg(feature = "sqlite")]
use std::time::Duration;

#[cfg(feature = "sqlite")]
pub type DbPool = sqlx::SqlitePool;
#[cfg(feature = "postgres")]
pub type DbPool = sqlx::PgPool;

#[cfg(feature = "sqlite")]
pub type DbDriver = sqlx::Sqlite;
#[cfg(feature = "postgres")]
pub type DbDriver = sqlx::Postgres;

#[cfg(feature = "sqlite")]
pub type DbTransaction = sqlx::Transaction<'static, sqlx::Sqlite>;
#[cfg(feature = "postgres")]
pub type DbTransaction = sqlx::Transaction<'static, sqlx::Postgres>;

#[cfg(feature = "sqlite")]
pub type DbRow = sqlx::sqlite::SqliteRow;
#[cfg(feature = "postgres")]
pub type DbRow = sqlx::postgres::PgRow;

pub type DbQueryResult = <DbDriver as sqlx::Database>::QueryResult;

/// 写事务的 `BEGIN` 语句
///
/// SQLite 的裸 `BEGIN` 是 deferred 的：先读后写的事务在 WAL 下会以
/// `SQLITE_BUSY_SNAPSHOT` 立刻失败，`busy_timeout` 对这种锁升级不生效。
/// `BEGIN IMMEDIATE` 在事务开头就取写锁，并发写者按 `busy_timeout` 排队。
#[cfg(feature = "sqlite")]
pub(crate) const BEGIN_WRITE: &str = "BEGIN IMMEDIATE";
#[cfg(feature = "postgres")]
pub(crate) const BEGIN_WRITE: &str = "BEGIN";

#[cfg(feature = "sqlite")]
pub async fn open_pool(path: &Path) -> Result<DbPool> {
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    Ok(pool)
}

#[cfg(feature = "postgres")]
pub async fn open_pool(_path: &Path) -> Result<DbPool> {
    Err(CoreError::Database(
        "postgres backend is not implemented yet".into(),
    ))
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
