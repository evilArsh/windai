use super::executor::StorageExecutor;
use crate::db::{DbDriver, DbQueryResult};
use crate::error::{Result, StorageError};
// use crate::models::AdapterType;
use chrono::Utc;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::QueryBuilder;
use wind_wire::AdapterType;

/// 当前时刻的秒级时间戳
pub fn now_ts() -> i64 {
    Utc::now().timestamp()
}

pub fn vec_to_str_default<T>(vec: Option<&[T]>) -> Result<String>
where
    T: Serialize,
{
    match vec {
        Some(items) => serde_json::to_string(items).map_err(Into::into),
        None => Ok("[]".to_string()),
    }
}

pub fn vec_to_str_optional<T>(vec: Option<&[T]>) -> Result<Option<String>>
where
    T: Serialize,
{
    vec.map(|items| serde_json::to_string(items).map_err(Into::into))
        .transpose()
}

pub fn map_to_str_default<T>(map: Option<&T>) -> Result<String>
where
    T: Serialize,
{
    match map {
        Some(value) => serde_json::to_string(value).map_err(Into::into),
        None => Ok("{}".to_string()),
    }
}

pub fn map_to_str_optional<T>(map: Option<&T>) -> Result<Option<String>>
where
    T: Serialize,
{
    map.map(|value| serde_json::to_string(value).map_err(Into::into))
        .transpose()
}

pub fn de_str_to<T>(text: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_str(text).map_err(Into::into)
}

pub fn parse_str_to<T>(text: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: Into<StorageError>,
{
    text.parse::<T>().map_err(Into::into)
}

pub fn decode_adapter(column: &str, text: &str) -> Result<AdapterType> {
    AdapterType::parse(text).map_err(|error| StorageError::MalformedColumn {
        column: column.into(),
        detail: error.to_string(),
    })
}

pub fn ensure_affected(result: DbQueryResult) -> Result<()> {
    if result.rows_affected() == 0 {
        return Err(StorageError::NoRowsAffected);
    }
    Ok(())
}

pub fn ensure_lte_one<T>(rows: Vec<T>) -> Result<Option<T>> {
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

// /// 拼接单行 UPDATE，并自动追加 `updated_at`
// ///
// /// 每个值都用 `Option` 包装，为 `None` 的字段跳过。全部字段都是 `None` 时
// /// 不生成任何 SQL
// #[macro_export]
// macro_rules! update {
//     ($table:expr, $id:expr, $(($col:literal, $val:expr)),+ $(,)?) => {{
//         let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
//         let mut __count = 0usize;
//         let mut __need_prefix = true;
//         $(
//             if let Some(__v) = $val {
//                 if __need_prefix {
//                     __qb.push("UPDATE ").push($table).push(" SET ");
//                     __need_prefix = false;
//                 } else {
//                     __qb.push(", ");
//                 }
//                 __qb.push($col).push(" = ").push_bind(__v);
//                 __count += 1;
//             }
//         )+
//         if __count > 0 {
//             __qb.push(", updated_at = ");
//             __qb.push_bind($crate::utils::now_ts());
//             __qb.push(" WHERE id = ").push_bind($id);
//         }
//         __qb
//     }};
// }

// /// 拼接单行 UPDATE，并自动追加 `updated_at`
// ///
// /// 传入的值不做 `Option` 解包，`None` 会写入 NULL
// #[macro_export]
// macro_rules! update_strict {
//     ($table:expr, $id:expr, $(($col:literal, $val:expr)),+ $(,)?) => {{
//         let mut __qb: sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
//         let mut __first = true;
//         $(
//             if __first {
//                 __qb.push("UPDATE ").push($table).push(" SET ");
//                 __first = false;
//             } else {
//                 __qb.push(", ");
//             }
//             __qb.push($col).push(" = ").push_bind($val);
//         )+
//         __qb.push(", updated_at = ");
//         __qb.push_bind($crate::utils::now_ts());
//         __qb.push(" WHERE id = ").push_bind($id);
//         __qb
//     }};
// }

// /// UPDATE 语句拼接，只拼 SET，不拼 WHERE
// #[macro_export]
// macro_rules! update_fields {
//     ($table:expr, $(($col:literal, $val:expr)),+ $(,)?) => {{
//         let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
//         let mut __need_prefix = true;
//         $(
//             if let Some(__v) = $val {
//                 if __need_prefix {
//                     __qb.push("UPDATE ").push($table).push(" SET ");
//                     __need_prefix = false;
//                 } else {
//                     __qb.push(", ");
//                 }
//                 __qb.push($col).push(" = ").push_bind(__v);
//             }
//         )+
//         __qb
//     }};
// }

// /// INSERT 语句的列前缀拼接
// #[macro_export]
// macro_rules! insert_fields {
//     ($table:expr, ($($field:literal),+ $(,)?)) => {{
//         let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
//         __qb.push("INSERT INTO ").push($table).push(" (");
//         {
//             let mut __sep = __qb.separated(", ");
//             $(
//                 __sep.push($field);
//             )+
//         }
//         __qb.push(") ");
//         __qb
//     }};
// }

// /// 单行 INSERT
// #[macro_export]
// macro_rules! insert {
//     ($table:expr, $(($col:literal, $val:expr)),+ $(,)?) => {{
//         let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
//         __qb.push("INSERT INTO ").push($table).push(" (");
//         {
//             let mut __sep = __qb.separated(", ");
//             $(
//                 __sep.push($col);
//             )+
//         }
//         __qb.push(") VALUES (");
//         {
//             let mut __sep = __qb.separated(", ");
//             $(
//                 __sep.push_bind($val);
//             )+
//         }
//         __qb.push(")");
//         __qb
//     }};
// }

// /// 单行主键 id 删除
// #[macro_export]
// macro_rules! delete_by_id {
//     ($table:expr, $id:expr) => {{
//         let mut __qb = $crate::delete_from!($table);
//         __qb.push(" WHERE id = ");
//         __qb.push_bind($id);
//         __qb
//     }};
// }

// /// DELETE 语句前缀拼接，不带条件
// #[macro_export]
// macro_rules! delete_from {
//     ($table:expr) => {{
//         let mut __qb: sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
//         __qb.push("DELETE FROM ");
//         __qb.push($table);
//         __qb
//     }};
// }

// /// SELECT 查询字段
// #[macro_export]
// macro_rules! select_fields {
//     ($table:expr, ($($field:literal),+ $(,)?)) => {{
//         let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
//         __qb.push("SELECT ");
//         {
//             let mut __first = true;
//             $(
//                 if !__first { __qb.push(", "); }
//                 __first = false;
//                 __qb.push($field);
//             )+
//         }
//         __qb.push(" FROM ");
//         __qb.push($table);
//         __qb.push(" ");
//         __qb
//     }};
// }

// /// 单行主键 id 查询
// #[macro_export]
// macro_rules! get_by_id {
//     // 不指定字段时，查询所有字段
//     ($table:expr, $id:expr) => {{
//         let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
//         __qb.push("SELECT * FROM ");
//         __qb.push($table);
//         __qb.push(" WHERE id = ");
//         __qb.push_bind($id);
//         __qb
//     }};

//     // 指定查询字段
//     ($table:expr, $id:expr, ($($field:literal),+ $(,)?)) => {{
//         let mut __qb = $crate::select_fields!($table, ($($field),+));
//         __qb.push("WHERE id = ");
//         __qb.push_bind($id);
//         __qb
//     }};
// }
