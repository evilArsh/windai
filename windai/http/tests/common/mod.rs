//! 共享测试辅助：单连接内存 SQLite 池 + Arc<WindCore>。
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::str::FromStr;
use std::sync::Arc;
use wind_core::WindCore;

async fn test_pool() -> sqlx::SqlitePool {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")
        .unwrap()
        .shared_cache(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal);
    SqlitePoolOptions::new()
        // `sqlite::memory:` 按物理连接隔离，必须单连接，保证建表与后续查询命中同一内存库。
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap()
}

pub async fn test_core() -> Arc<WindCore> {
    test_core_with_pool().await.0
}

/// 返回 core 及其底层单连接池，供需要直接 SQL 断言（如「未插入孤儿行」）的测试使用。
pub async fn test_core_with_pool() -> (Arc<WindCore>, sqlx::SqlitePool) {
    let pool = test_pool().await;
    let core = Arc::new(WindCore::init_with_pool(pool.clone()).await.unwrap());
    (core, pool)
}
