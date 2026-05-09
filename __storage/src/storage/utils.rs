use crate::storage::StorageError;

/// 如果查询成功但是查询结果为空，则返回 None 但不报错
pub fn value_or_none<T>(result: Result<T, rusqlite::Error>) -> Result<Option<T>, StorageError> {
    match result {
        Ok(model) => Ok(Some(model)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(err.into()),
    }
}
