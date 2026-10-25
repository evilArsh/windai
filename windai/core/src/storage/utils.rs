use super::executor::StorageExecutor;
use crate::db::{DbDriver, DbQueryResult};
use crate::error::{CoreError, Result};
use crate::models::AdapterType;
use chrono::Utc;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::QueryBuilder;

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
    T::Err: Into<CoreError>,
{
    text.parse::<T>().map_err(Into::into)
}

pub fn decode_adapter(column: &str, text: &str) -> Result<AdapterType> {
    AdapterType::parse(text).map_err(|error| CoreError::MalformedColumn {
        column: column.into(),
        detail: error.to_string(),
    })
}

pub fn decode_enum<T>(column: &str, text: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    text.parse::<T>()
        .map_err(|error| CoreError::MalformedColumn {
            column: column.into(),
            detail: error.to_string(),
        })
}

pub fn ensure_affected(result: DbQueryResult) -> Result<()> {
    if result.rows_affected() == 0 {
        return Err(CoreError::NoRowsAffected);
    }
    Ok(())
}

pub fn ensure_lte_one<T>(rows: Vec<T>) -> Result<Option<T>> {
    let found = rows.len();
    match found {
        0 | 1 => Ok(rows.into_iter().next()),
        found => Err(CoreError::TooManyRows { found }),
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

/// 拼接单行 UPDATE，并自动追加 `updated_at`
///
/// 每个值都用 `Option` 包装，为 `None` 的字段跳过。全部字段都是 `None` 时
/// 不生成任何 SQL
#[macro_export]
macro_rules! update {
    ($table:expr, $id:expr, $(($col:literal, $val:expr)),+ $(,)?) => {{
        let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        let mut __count = 0usize;
        let mut __need_prefix = true;
        $(
            if let Some(__v) = $val {
                if __need_prefix {
                    __qb.push("UPDATE ").push($table).push(" SET ");
                    __need_prefix = false;
                } else {
                    __qb.push(", ");
                }
                __qb.push($col).push(" = ").push_bind(__v);
                __count += 1;
            }
        )+
        if __count > 0 {
            __qb.push(", updated_at = ");
            __qb.push_bind($crate::storage::utils::now_ts());
            __qb.push(" WHERE id = ").push_bind($id);
        }
        __qb
    }};
}

/// 拼接单行 UPDATE，并自动追加 `updated_at`
///
/// 传入的值不做 `Option` 解包，`None` 会写入 NULL
#[macro_export]
macro_rules! update_strict {
    ($table:expr, $id:expr, $(($col:literal, $val:expr)),+ $(,)?) => {{
        let mut __qb: sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        let mut __first = true;
        $(
            if __first {
                __qb.push("UPDATE ").push($table).push(" SET ");
                __first = false;
            } else {
                __qb.push(", ");
            }
            __qb.push($col).push(" = ").push_bind($val);
        )+
        __qb.push(", updated_at = ");
        __qb.push_bind($crate::storage::utils::now_ts());
        __qb.push(" WHERE id = ").push_bind($id);
        __qb
    }};
}

/// UPDATE 语句拼接，只拼 SET，不拼 WHERE
#[macro_export]
macro_rules! update_fields {
    ($table:expr, $(($col:literal, $val:expr)),+ $(,)?) => {{
        let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        let mut __need_prefix = true;
        $(
            if let Some(__v) = $val {
                if __need_prefix {
                    __qb.push("UPDATE ").push($table).push(" SET ");
                    __need_prefix = false;
                } else {
                    __qb.push(", ");
                }
                __qb.push($col).push(" = ").push_bind(__v);
            }
        )+
        __qb
    }};
}

/// INSERT 语句的列前缀拼接
#[macro_export]
macro_rules! insert_fields {
    ($table:expr, ($($field:literal),+ $(,)?)) => {{
        let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        __qb.push("INSERT INTO ").push($table).push(" (");
        {
            let mut __sep = __qb.separated(", ");
            $(
                __sep.push($field);
            )+
        }
        __qb.push(") ");
        __qb
    }};
}

/// 单行 INSERT
#[macro_export]
macro_rules! insert {
    ($table:expr, $(($col:literal, $val:expr)),+ $(,)?) => {{
        let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        __qb.push("INSERT INTO ").push($table).push(" (");
        {
            let mut __sep = __qb.separated(", ");
            $(
                __sep.push($col);
            )+
        }
        __qb.push(") VALUES (");
        {
            let mut __sep = __qb.separated(", ");
            $(
                __sep.push_bind($val);
            )+
        }
        __qb.push(")");
        __qb
    }};
}

/// 单行主键 id 删除
#[macro_export]
macro_rules! delete_by_id {
    ($table:expr, $id:expr) => {{
        let mut __qb = $crate::delete_from!($table);
        __qb.push(" WHERE id = ");
        __qb.push_bind($id);
        __qb
    }};
}

/// DELETE 语句前缀拼接，不带条件
#[macro_export]
macro_rules! delete_from {
    ($table:expr) => {{
        let mut __qb: sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        __qb.push("DELETE FROM ");
        __qb.push($table);
        __qb
    }};
}

/// SELECT 查询字段
#[macro_export]
macro_rules! select_fields {
    ($table:expr, ($($field:literal),+ $(,)?)) => {{
        let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        __qb.push("SELECT ");
        {
            let mut __first = true;
            $(
                if !__first { __qb.push(", "); }
                __first = false;
                __qb.push($field);
            )+
        }
        __qb.push(" FROM ");
        __qb.push($table);
        __qb.push(" ");
        __qb
    }};
}

/// 单行主键 id 查询
#[macro_export]
macro_rules! get_by_id {
    // 不指定字段时，查询所有字段
    ($table:expr, $id:expr) => {{
        let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        __qb.push("SELECT * FROM ");
        __qb.push($table);
        __qb.push(" WHERE id = ");
        __qb.push_bind($id);
        __qb
    }};

    // 指定查询字段
    ($table:expr, $id:expr, ($($field:literal),+ $(,)?)) => {{
        let mut __qb = $crate::select_fields!($table, ($($field),+));
        __qb.push("WHERE id = ");
        __qb.push_bind($id);
        __qb
    }};
}

/// 断言 `QueryBuilder::sql()` 的产物，占位符按驱动变化
/// SQLite 是 `?`，PostgreSQL 是 `$1, $2…`
#[cfg(all(test, feature = "sqlite"))]
mod tests {
    #[test]
    fn insert_lists_columns_and_placeholders() {
        let qb = insert!("users", ("name", "alice"), ("age", 30));
        assert_eq!(qb.sql(), "INSERT INTO users (name, age) VALUES (?, ?)");
    }

    #[test]
    fn update_skips_none_fields_and_appends_updated_at() {
        let qb = update!(
            "users",
            42,
            ("name", Some("bob")),
            ("email", Option::<&str>::None),
        );
        assert_eq!(
            qb.sql(),
            "UPDATE users SET name = ?, updated_at = ? WHERE id = ?"
        );
    }

    #[test]
    fn update_with_every_field_none_produces_no_statement() {
        let qb = update!("users", 1, ("name", Option::<&str>::None));
        assert_eq!(qb.sql(), "");
    }

    #[test]
    fn update_strict_binds_none_as_null() {
        let qb = update_strict!(
            "agent_instances",
            7,
            ("phase", "running"),
            ("phase_payload", Option::<&str>::None)
        );
        assert_eq!(
            qb.sql(),
            "UPDATE agent_instances SET phase = ?, phase_payload = ?, updated_at = ? WHERE id = ?"
        );
    }

    #[test]
    fn update_fields_emits_no_where_clause() {
        let qb = update_fields!(
            "external_waits",
            ("answered_at", Some(5)),
            ("decision_json", Some("{}"))
        );
        assert_eq!(
            qb.sql(),
            "UPDATE external_waits SET answered_at = ?, decision_json = ?"
        );
    }

    #[test]
    fn insert_fields_emits_an_open_prefix() {
        let qb = insert_fields!("users", ("id", "name"));
        assert_eq!(qb.sql(), "INSERT INTO users (id, name) ");
    }

    #[test]
    fn delete_by_id_targets_the_primary_key() {
        let qb = delete_by_id!("topics", 3);
        assert_eq!(qb.sql(), "DELETE FROM topics WHERE id = ?");
    }

    #[test]
    fn delete_from_emits_no_condition() {
        let qb = delete_from!("mod_state");
        assert_eq!(qb.sql(), "DELETE FROM mod_state");
    }

    #[test]
    fn select_fields_lists_the_columns() {
        let qb = select_fields!("block", ("id", "kind"));
        assert_eq!(qb.sql(), "SELECT id, kind FROM block ");
    }

    #[test]
    fn get_by_id_selects_every_column() {
        let qb = get_by_id!("topics", 1);
        assert_eq!(qb.sql(), "SELECT * FROM topics WHERE id = ?");
    }
}

/// 列值变换的测试
#[cfg(test)]
mod transforms {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn an_absent_vector_lands_as_an_empty_array() {
        assert_eq!(vec_to_str_default::<String>(None).expect("encode"), "[]");
    }

    #[test]
    fn a_present_vector_lands_as_its_items() {
        let items = vec!["a".to_string(), "b".to_string()];
        assert_eq!(
            vec_to_str_default(Some(&items)).expect("encode"),
            "[\"a\",\"b\"]"
        );
    }

    #[test]
    fn an_absent_optional_vector_lands_as_null() {
        assert_eq!(vec_to_str_optional::<String>(None).expect("encode"), None);
    }

    #[test]
    fn an_absent_map_lands_as_an_empty_object() {
        assert_eq!(
            map_to_str_default::<BTreeMap<String, i64>>(None).expect("encode"),
            "{}"
        );
    }

    #[test]
    fn a_present_map_lands_as_its_entries() {
        let mut map = BTreeMap::new();
        map.insert("a".to_string(), 1_i64);
        assert_eq!(map_to_str_default(Some(&map)).expect("encode"), "{\"a\":1}");
    }

    #[test]
    fn an_absent_optional_map_lands_as_null() {
        assert_eq!(
            map_to_str_optional::<BTreeMap<String, i64>>(None).expect("encode"),
            None
        );
    }

    #[test]
    fn json_text_decodes_back_into_a_value() {
        let decoded: serde_json::Value = de_str_to(r#"{"temp":22}"#).expect("decode");
        assert_eq!(decoded, serde_json::json!({ "temp": 22 }));
    }

    #[test]
    fn text_that_is_not_json_reports() {
        assert!(de_str_to::<serde_json::Value>("{oops").is_err());
    }

    /// 文本列里存的是枚举名，读回来靠 `FromStr`
    #[derive(Debug, PartialEq, strum::EnumString)]
    #[strum(serialize_all = "snake_case")]
    enum Phase {
        Running,
        Suspended,
    }

    #[test]
    fn text_decodes_into_a_from_str_type() {
        assert_eq!(
            parse_str_to::<Phase>("running").expect("parse"),
            Phase::Running
        );
        assert_eq!(
            parse_str_to::<Phase>("suspended").expect("parse"),
            Phase::Suspended
        );
    }

    #[test]
    fn text_that_is_not_a_from_str_value_reports() {
        assert!(parse_str_to::<Phase>("sideways").is_err());
    }
}
