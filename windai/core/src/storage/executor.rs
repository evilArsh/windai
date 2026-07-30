use std::{future::Future, sync::Arc};

use sqlx::{
    Database, FromRow, IntoArguments,
    query::{Query, QueryAs},
};
use tokio::sync::Mutex;

use crate::{
    db::{DbDriver, DbPool, DbRow, DbTransaction},
    error::{CoreError, Result},
};

#[derive(Clone)]
pub(crate) struct StorageExecutor {
    pool: DbPool,
    tx: Option<Arc<Mutex<Option<DbTransaction>>>>,
}

impl StorageExecutor {
    pub(crate) fn pool(pool: DbPool) -> Self {
        Self { pool, tx: None }
    }

    /// 创建一个新的事务执行器
    pub(crate) async fn transaction(pool: DbPool) -> Result<Self> {
        Ok(Self {
            tx: Some(Arc::new(Mutex::new(Some(pool.begin().await?)))),
            pool,
        })
    }

    pub(crate) fn pool_ref(&self) -> &DbPool {
        &self.pool
    }

    pub(crate) fn pool_clone(&self) -> DbPool {
        self.pool.clone()
    }

    pub(crate) fn is_transaction(&self) -> bool {
        self.tx.is_some()
    }

    pub(crate) async fn transaction_required<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        if self.is_transaction() {
            return f(self.clone()).await;
        }

        let tx_executor = Self::transaction(self.pool_clone()).await?;
        let result = f(tx_executor.clone()).await;
        match result {
            Ok(value) => {
                tx_executor.commit().await?;
                Ok(value)
            }
            Err(err) => {
                let _ = tx_executor.rollback().await;
                Err(err)
            }
        }
    }
    async fn get_tx(&self) -> Result<Option<DbTransaction>> {
        match &self.tx {
            Some(tx) => {
                let mut guard = tx.lock().await;
                let tx = guard
                    .take()
                    .ok_or_else(|| CoreError::Internal("transaction already closed".into()))?;
                Ok(Some(tx))
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn commit(&self) -> Result<()> {
        match self.get_tx().await? {
            Some(tx) => Ok(tx.commit().await?),
            None => Ok(()),
        }
    }

    pub(crate) async fn rollback(&self) -> Result<()> {
        match self.get_tx().await? {
            Some(tx) => Ok(tx.rollback().await?),
            None => Ok(()),
        }
    }

    pub(crate) async fn execute<'q>(
        &self,
        query: Query<'q, DbDriver, <DbDriver as Database>::Arguments<'q>>,
    ) -> Result<<DbDriver as Database>::QueryResult>
    where
        <DbDriver as Database>::Arguments<'q>: IntoArguments<'q, DbDriver> + Send,
    {
        match self.get_tx().await? {
            Some(mut tx) => Ok(query.execute(&mut *tx).await?),
            None => Ok(query.execute(&self.pool).await?),
        }
    }

    pub(crate) async fn fetch_optional<'q, O>(
        &self,
        query: QueryAs<'q, DbDriver, O, <DbDriver as Database>::Arguments<'q>>,
    ) -> Result<Option<O>>
    where
        O: for<'r> FromRow<'r, DbRow> + Send + Unpin,
        <DbDriver as Database>::Arguments<'q>: IntoArguments<'q, DbDriver> + Send,
    {
        match self.get_tx().await? {
            Some(mut tx) => Ok(query.fetch_optional(&mut *tx).await?),
            None => Ok(query.fetch_optional(&self.pool).await?),
        }
    }

    pub(crate) async fn fetch_all<'q, O>(
        &self,
        query: QueryAs<'q, DbDriver, O, <DbDriver as Database>::Arguments<'q>>,
    ) -> Result<Vec<O>>
    where
        O: for<'r> FromRow<'r, DbRow> + Send + Unpin,
        <DbDriver as Database>::Arguments<'q>: IntoArguments<'q, DbDriver> + Send,
    {
        match self.get_tx().await? {
            Some(mut tx) => Ok(query.fetch_all(&mut *tx).await?),
            None => Ok(query.fetch_all(&self.pool).await?),
        }
    }
}
