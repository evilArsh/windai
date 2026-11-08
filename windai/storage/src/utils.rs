use super::executor::StorageExecutor;
use crate::db::{DbDriver, DbQueryResult};
use crate::error::{Result, StorageError};
use chrono::Utc;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::QueryBuilder;
use std::str::FromStr;
use wind_wire::AdapterType;

/// 当前时刻的秒级时间戳
pub(crate) fn now_ts() -> i64 {
    Utc::now().timestamp()
}

/// 当前时刻的毫秒时间戳
pub(crate) fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

pub(crate) fn vec_to_str_default<T>(vec: Option<&[T]>) -> Result<String>
where
    T: Serialize,
{
    match vec {
        Some(items) => serde_json::to_string(items).map_err(Into::into),
        None => Ok("[]".to_string()),
    }
}

pub(crate) fn vec_to_str_optional<T>(vec: Option<&[T]>) -> Result<Option<String>>
where
    T: Serialize,
{
    vec.map(|items| serde_json::to_string(items).map_err(Into::into))
        .transpose()
}

pub(crate) fn map_to_str_default<T>(map: Option<&T>) -> Result<String>
where
    T: Serialize,
{
    match map {
        Some(value) => serde_json::to_string(value).map_err(Into::into),
        None => Ok("{}".to_string()),
    }
}

pub(crate) fn map_to_str_optional<T>(map: Option<&T>) -> Result<Option<String>>
where
    T: Serialize,
{
    map.map(|value| serde_json::to_string(value).map_err(Into::into))
        .transpose()
}

pub(crate) fn de_str_to<T>(text: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_str(text).map_err(Into::into)
}

pub(crate) fn parse_str_to<T>(text: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: Into<StorageError>,
{
    text.parse::<T>().map_err(Into::into)
}

pub(crate) fn ensure_affected(result: DbQueryResult) -> Result<()> {
    if result.rows_affected() == 0 {
        return Err(StorageError::NoRowsAffected);
    }
    Ok(())
}

pub(crate) fn not_found(entity: &str, key: i64) -> StorageError {
    StorageError::NotFound {
        entity: entity.into(),
        key: key.to_string(),
    }
}

pub(crate) fn conflict(
    entity: &str,
    key: impl ToString,
    detail: impl Into<String>,
) -> StorageError {
    StorageError::Conflict {
        entity: entity.into(),
        key: key.to_string(),
        detail: detail.into(),
    }
}

pub(crate) fn is_unique_violation(error: &StorageError) -> bool {
    matches!(
        error,
        StorageError::Database(sqlx::Error::Database(source)) if source.is_unique_violation()
    )
}

pub(crate) fn ensure_lte_one<T>(rows: Vec<T>) -> Result<Option<T>> {
    let found = rows.len();
    match found {
        0 | 1 => Ok(rows.into_iter().next()),
        found => Err(StorageError::TooManyRows { found }),
    }
}

pub(crate) async fn execute_expecting_rows<'q>(
    executor: &StorageExecutor,
    mut query: QueryBuilder<'q, DbDriver>,
) -> Result<()> {
    let result = executor.execute(query.build()).await?;
    ensure_affected(result)
}

pub(crate) async fn execute_allowing_zero_rows<'q>(
    executor: &StorageExecutor,
    mut query: QueryBuilder<'q, DbDriver>,
) -> Result<()> {
    executor.execute(query.build()).await?;
    Ok(())
}

pub(crate) fn parse_enum<T>(column: &str, text: &str) -> sqlx::Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    text.parse().map_err(|error| column_error(column, error))
}

pub(crate) fn parse_json<T>(column: &str, text: &str) -> sqlx::Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_str(text).map_err(|error| column_error(column, error))
}

pub(crate) fn parse_optional_json<T>(column: &str, text: Option<String>) -> sqlx::Result<Option<T>>
where
    T: DeserializeOwned,
{
    text.map(|text| parse_json(column, &text)).transpose()
}

pub(crate) fn decode_adapter(column: &str, text: &str) -> sqlx::Result<AdapterType> {
    AdapterType::parse(text).map_err(|error| column_error(column, error))
}

pub(crate) fn column_error(
    column: &str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> sqlx::Error {
    sqlx::Error::ColumnDecode {
        index: column.to_string(),
        source: Box::new(error),
    }
}
