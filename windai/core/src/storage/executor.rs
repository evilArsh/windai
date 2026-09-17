use crate::{
    db::{DbDriver, DbPool, DbRow, DbTransaction},
    error::Result,
};
use sqlx::{
    Database, Decode, FromRow, IntoArguments, Type,
    query::{Query, QueryAs, QueryScalar},
};
use std::{future::Future, sync::Arc};
use tokio::sync::Mutex;

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

    /// 查询多行原始结果。
    /// 用于需要按列名取值、而不是映射到某个 `FromRow` 类型的场景
    /// （例如 `INSERT ... RETURNING id, tool_call_id` 后按自然键组装）
    pub(crate) async fn fetch_all_rows<'q>(
        &self,
        query: Query<'q, DbDriver, <DbDriver as Database>::Arguments<'q>>,
    ) -> Result<Vec<DbRow>>
    where
        <DbDriver as Database>::Arguments<'q>: IntoArguments<'q, DbDriver> + Send,
    {
        with_connection!(self, executor, Ok(query.fetch_all(executor).await?))
    }

    /// 查询单个标量值，无结果或结果多于一行时报错。
    /// 用于 `INSERT ... RETURNING id` 这类必然返回一行的场景
    pub(crate) async fn fetch_one_scalar<'q, O>(
        &self,
        query: QueryScalar<'q, DbDriver, O, <DbDriver as Database>::Arguments<'q>>,
    ) -> Result<O>
    where
        O: for<'r> Decode<'r, DbDriver> + Type<DbDriver> + Send + Unpin,
        <DbDriver as Database>::Arguments<'q>: IntoArguments<'q, DbDriver> + Send,
    {
        with_connection!(self, executor, Ok(query.fetch_one(executor).await?))
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
