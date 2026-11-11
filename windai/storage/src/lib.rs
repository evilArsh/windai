use self::db::{Db, DbPool};
use self::error::Result;
use self::executor::{StorageExecutor, TxMode};

use std::future::Future;
use std::path::Path;

pub(crate) mod db;
pub mod error;
pub(crate) mod executor;
pub mod models;
pub(crate) mod schema;
pub(crate) mod utils;
pub mod vendors;

pub use models::*;

#[derive(Clone)]
pub struct Storage {
    executor: StorageExecutor,
}

impl Storage {
    /// 打开数据库并确保 schema 就绪
    pub async fn open(path: &Path) -> Result<Self> {
        let db = Db::open(path).await?;
        db.init_schema().await?;
        Ok(Self::new(db))
    }

    /// 用数据库句柄建立存储
    pub(crate) fn new(db: Db) -> Self {
        Self::from_executor(StorageExecutor::new(db.pool().clone()))
    }

    fn from_executor(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    /// 写事务，BEGIN IMMEDIATE
    pub async fn with_tx<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Storage) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        self.executor
            .with_tx(TxMode::Write, |executor| f(Self::from_executor(executor)))
            .await
    }

    /// 只读事务，BEGIN
    pub async fn with_read_tx<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Storage) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        self.executor
            .with_tx(TxMode::Read, |executor| f(Self::from_executor(executor)))
            .await
    }

    /// 开启写事务并返回事务句柄
    pub async fn begin(&self) -> Result<StorageTx> {
        Ok(StorageTx {
            storage: Self::from_executor(
                StorageExecutor::new_transaction(self.executor.pool().clone(), TxMode::Write)
                    .await?,
            ),
        })
    }

    /// 取出底层连接池
    pub(crate) fn pool(&self) -> &DbPool {
        self.executor.pool()
    }
}

pub struct StorageTx {
    storage: Storage,
}

impl StorageTx {
    pub fn storage(&self) -> Storage {
        self.storage.clone()
    }

    pub async fn commit(self) -> Result<()> {
        self.storage.executor.commit().await
    }

    pub async fn rollback(self) -> Result<()> {
        self.storage.executor.rollback().await
    }
}

#[test]
fn storage_handle_stays_pointer_sized() {
    assert_eq!(std::mem::size_of::<Storage>(), 16);
}
