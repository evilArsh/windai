use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Sqlite, Transaction};
use std::str::FromStr;

/// 初始化SQLite连接池
pub async fn connect(db_url: &str) -> Result<sqlx::SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::from_str(db_url)?
        .create_if_missing(true)
        .foreign_keys(true);

    SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
}

/// Begin a new transaction.
pub async fn begin_tx(
    pool: &sqlx::SqlitePool,
) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
    pool.begin().await
}
