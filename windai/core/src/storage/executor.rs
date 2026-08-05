use std::{future::Future, sync::Arc};

use sqlx::{
    Database, FromRow, IntoArguments,
    query::{Query, QueryAs},
};
use tokio::sync::Mutex;

use crate::{
    db::{DbDriver, DbPool, DbRow, DbTransaction},
    error::Result,
};

macro_rules! with_transaction {
    ($self:expr, $tx:ident, $body:expr) => {
        match &$self.tx {
            Some(tx) => {
                let mut guard = tx.lock().await;
                match guard.take() {
                    Some($tx) => $body,
                    None => Err($crate::error::CoreError::Internal(
                        "transaction already closed".into(),
                    )),
                }
            }
            None => Ok(()),
        }
    };
}

macro_rules! with_connection {
    ($self:expr, $conn:ident, $body:expr) => {
        match &$self.tx {
            Some(tx) => {
                let mut guard = tx.lock().await;
                match &mut *guard {
                    Some(t) => {
                        let $conn = &mut **t;
                        $body
                    }
                    None => Err($crate::error::CoreError::Internal(
                        "transaction already closed".into(),
                    )),
                }
            }
            None => {
                let mut _conn = $self.pool.acquire().await?;
                let $conn = &mut *_conn;
                $body
            }
        }
    };
}

#[derive(Clone)]
pub(crate) struct StorageExecutor {
    pool: DbPool,
    tx: Option<Arc<Mutex<Option<DbTransaction>>>>,
}

impl StorageExecutor {
    /// 创建一个普通执行器
    pub(crate) fn new(pool: DbPool) -> Self {
        Self { pool, tx: None }
    }

    /// 创建一个新的事务执行器
    pub(crate) async fn new_transaction(pool: DbPool) -> Result<Self> {
        Ok(Self {
            tx: Some(Arc::new(Mutex::new(Some(pool.begin().await?)))),
            pool,
        })
    }

    pub(crate) fn pool(&self) -> &DbPool {
        &self.pool
    }

    pub(crate) fn is_transaction(&self) -> bool {
        self.tx.is_some()
    }

    pub(crate) async fn with_tx<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        if self.is_transaction() {
            return f(self.clone()).await;
        }

        let tx_executor = Self::new_transaction(self.pool().clone()).await?;
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

    pub(crate) async fn commit(&self) -> Result<()> {
        with_transaction!(self, tx, { Ok(tx.commit().await?) })
    }

    pub(crate) async fn rollback(&self) -> Result<()> {
        with_transaction!(self, tx, { Ok(tx.rollback().await?) })
    }

    pub(crate) async fn execute<'q>(
        &self,
        query: Query<'q, DbDriver, <DbDriver as Database>::Arguments<'q>>,
    ) -> Result<<DbDriver as Database>::QueryResult>
    where
        <DbDriver as Database>::Arguments<'q>: IntoArguments<'q, DbDriver> + Send,
    {
        with_connection!(self, executor, Ok(query.execute(executor).await?))
    }

    pub(crate) async fn fetch_optional<'q, O>(
        &self,
        query: QueryAs<'q, DbDriver, O, <DbDriver as Database>::Arguments<'q>>,
    ) -> Result<Option<O>>
    where
        O: for<'r> FromRow<'r, DbRow> + Send + Unpin,
        <DbDriver as Database>::Arguments<'q>: IntoArguments<'q, DbDriver> + Send,
    {
        with_connection!(self, executor, {
            Ok(query.fetch_optional(executor).await?)
        })
    }

    pub(crate) async fn fetch_all<'q, O>(
        &self,
        query: QueryAs<'q, DbDriver, O, <DbDriver as Database>::Arguments<'q>>,
    ) -> Result<Vec<O>>
    where
        O: for<'r> FromRow<'r, DbRow> + Send + Unpin,
        <DbDriver as Database>::Arguments<'q>: IntoArguments<'q, DbDriver> + Send,
    {
        with_connection!(self, executor, Ok(query.fetch_all(executor).await?))
    }
}
