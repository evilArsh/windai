#[cfg(all(feature = "sqlite", feature = "postgres"))]
compile_error!(
    "Multiple database drivers detected! You can only enable ONE driver at a time.\n\
    Valid options: --features \"sqlite\", or --features \"postgres\"\n\
    Example: cargo build --no-default-features --features \"postgres\""
);
#[cfg(feature = "sqlite")]
mod driver_impl {
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use std::str::FromStr;
    use std::time::Duration;

    pub type DbPool = sqlx::SqlitePool;
    pub type DbDriver = sqlx::Sqlite;
    pub type DbTransaction = sqlx::Transaction<'static, sqlx::Sqlite>;
    pub type DbRow = sqlx::sqlite::SqliteRow;
    pub async fn create_pool(db_url: &str) -> Result<DbPool, sqlx::Error> {
        let options = SqliteConnectOptions::from_str(db_url)?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
    }
}

// TODO: postgres 驱动尚未验证，当前仅在 sqlite 下测试
#[cfg(feature = "postgres")]
mod driver_impl {
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::str::FromStr;

    pub type DbPool = sqlx::PgPool;
    pub type DbRow = sqlx::postgres::PgRow;
    pub type DbTransaction = sqlx::Transaction<'static, sqlx::Postgres>;
    pub type DbDriver = sqlx::Postgres;
    pub async fn create_pool(db_url: &str) -> Result<DbPool, sqlx::Error> {
        let options = PgConnectOptions::from_str(db_url)?;
        PgPoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
    }
}

pub use driver_impl::*;
pub type DbQueryResult = <DbDriver as sqlx::Database>::QueryResult;
/// 初始化 SQL 连接池
pub async fn init_db(db_url: &str) -> Result<DbPool, sqlx::Error> {
    create_pool(db_url).await
}
