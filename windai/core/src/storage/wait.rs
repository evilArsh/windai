use super::Storage;
use super::TableName;
use super::utils::{de_str_to, ensure_lte_one, execute_expecting_rows, now_ts};
use crate::db::{DbDriver, DbRow};
use crate::error::Result;
use crate::insert;
use crate::models::{ExternalWait, WaitOpen};
use crate::select_fields;
use crate::update_fields;
use sqlx::{FromRow, QueryBuilder, Row};

fn select_waits<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::EXTERNAL_WAITS,
        (
            "handle",
            "instance_id",
            "kind",
            "message_id",
            "payload_json",
            "created_at",
            "answered_at",
            "decision_json"
        )
    )
}

impl<'s> FromRow<'s, DbRow> for ExternalWait {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        let raw: Option<String> = row.try_get("decision_json")?;
        let decision: Option<serde_json::Value> = match raw {
            Some(text) => {
                Some(de_str_to(&text).map_err(|error| sqlx::Error::Decode(Box::new(error)))?)
            }
            None => None,
        };
        let raw: String = row.try_get("payload_json")?;
        let payload = de_str_to(&raw).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        Ok(Self {
            handle: row.try_get("handle")?,
            instance_id: row.try_get("instance_id")?,
            kind: row.try_get("kind")?,
            message_id: row.try_get("message_id")?,
            payload,
            created_at: row.try_get("created_at")?,
            answered_at: row.try_get("answered_at")?,
            decision,
        })
    }
}

impl Storage {
    /// 记录一次外部等待，handle 重复时报错
    pub async fn wait_open(&self, input: WaitOpen) -> Result<()> {
        let encoded = serde_json::to_string(&input.payload)?;
        execute_expecting_rows(
            &self.executor,
            insert!(
                TableName::EXTERNAL_WAITS,
                ("handle", input.handle),
                ("instance_id", input.instance_id),
                ("kind", input.kind),
                ("message_id", input.message_id),
                ("payload_json", encoded),
                ("created_at", now_ts()),
            ),
        )
        .await
    }

    /// 写入应答，已应答或不存在时报错
    pub async fn wait_answer(&self, handle: &str, decision: serde_json::Value) -> Result<()> {
        let encoded = serde_json::to_string(&decision)?;
        let mut qb = update_fields!(
            TableName::EXTERNAL_WAITS,
            ("answered_at", Some(now_ts())),
            ("decision_json", Some(encoded)),
        );
        qb.push(" WHERE handle = ")
            .push_bind(handle)
            .push(" AND answered_at IS NULL");
        execute_expecting_rows(&self.executor, qb).await
    }

    /// 按 handle 取出等待，不存在时为 None
    pub async fn wait_get(&self, handle: &str) -> Result<Option<ExternalWait>> {
        let rows = self
            .executor
            .fetch_all(
                select_waits()
                    .push(" WHERE handle = ")
                    .push_bind(handle)
                    .build_query_as::<ExternalWait>(),
            )
            .await?;
        ensure_lte_one(rows)
    }

    /// 按 handle 批量取出，空列表直接返回空
    pub async fn wait_list_by_handles(&self, handles: &[String]) -> Result<Vec<ExternalWait>> {
        if handles.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = select_waits();
        qb.push(" WHERE handle IN (");
        {
            let mut separated = qb.separated(", ");
            for handle in handles {
                separated.push_bind(handle.as_str());
            }
        }
        qb.push(")");
        self.executor
            .fetch_all(qb.build_query_as::<ExternalWait>())
            .await
    }

    /// 列出实例尚未应答的等待，按创建时间升序
    pub async fn wait_list_open_by_instance(&self, instance_id: i64) -> Result<Vec<ExternalWait>> {
        self.executor
            .fetch_all(
                select_waits()
                    .push(" WHERE instance_id = ")
                    .push_bind(instance_id)
                    .push(" AND answered_at IS NULL ORDER BY created_at ASC, handle ASC")
                    .build_query_as::<ExternalWait>(),
            )
            .await
    }
}
