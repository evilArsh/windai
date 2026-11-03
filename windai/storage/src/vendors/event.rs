use serde_json::Value;
use sqlx::QueryBuilder;
use wind_event::{Envelope, Scope, Shape, Source};

use crate::db::DbDriver;
use crate::error::Result;
use crate::{LogRecord, Storage};

impl Storage {
    /// 创建事件日志
    ///
    /// 须在调用方事务内调用，与产生该事件的域写入同提交
    pub async fn create_event_log(&self, envelope: &Envelope<Value>) -> Result<i64> {
        let (topic_id, instance_id) = match &envelope.scope {
            Scope::Global => (None, None),
            Scope::Topic {
                topic_id,
                instance_id,
            } => (Some(*topic_id), Some(*instance_id)),
        };
        let (source_name, source_version) = match &envelope.source {
            Source::Core { subsystem } => (subsystem.as_str(), 0_i64),
            Source::Mod { name, version } => (name.as_str(), i64::from(*version)),
        };
        let payload = serde_json::to_string(&envelope.payload)?;

        let id = self
            .executor
            .fetch_one_scalar(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO event_log
                        (create_time, name, shape, topic_id, instance_id, origin,
                         source_kind, source_name, source_version, payload)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING id",
                )
                .bind(envelope.time)
                .bind(envelope.name.as_str())
                .bind(envelope.shape.as_ref())
                .bind(topic_id)
                .bind(instance_id)
                .bind(envelope.origin.as_ref())
                .bind(envelope.source.as_ref())
                .bind(source_name)
                .bind(source_version)
                .bind(payload),
            )
            .await?;
        Ok(id)
    }

    /// 作用域内已落库日志的最大 id
    pub async fn event_log_boundary(&self, topic_id: Option<i64>) -> Result<Option<i64>> {
        let boundary = match topic_id {
            Some(topic_id) => {
                self.executor
                    .fetch_one_scalar(
                        sqlx::query_scalar::<_, Option<i64>>(
                            "SELECT MAX(id) FROM event_log WHERE topic_id = ?",
                        )
                        .bind(topic_id),
                    )
                    .await?
            }
            None => {
                self.executor
                    .fetch_one_scalar(sqlx::query_scalar::<_, Option<i64>>(
                        "SELECT MAX(id) FROM event_log WHERE topic_id IS NULL",
                    ))
                    .await?
            }
        };
        Ok(boundary)
    }

    /// 按 id 升序读取一段日志
    pub async fn event_log_range(
        &self,
        topic_id: Option<i64>,
        since: Option<i64>,
        until: Option<i64>,
        shape: Option<Shape>,
        limit: i64,
    ) -> Result<Vec<LogRecord>> {
        let mut query = QueryBuilder::<DbDriver>::new(
            "SELECT id, create_time, name, shape, topic_id, instance_id, origin,
                    source_kind, source_name, source_version, payload
            FROM event_log
            WHERE 1 = 1",
        );
        match topic_id {
            Some(topic_id) => {
                query.push(" AND topic_id = ").push_bind(topic_id);
            }
            None => {
                query.push(" AND topic_id IS NULL");
            }
        }
        if let Some(since) = since {
            query.push(" AND id > ").push_bind(since);
        }
        if let Some(until) = until {
            query.push(" AND id <= ").push_bind(until);
        }
        if let Some(shape) = shape {
            query
                .push(" AND shape = ")
                .push_bind(shape.as_ref().to_string());
        }
        query.push(" ORDER BY id ASC LIMIT ").push_bind(limit);

        let rows: Vec<LogRecord> = self.executor.fetch_all(query.build_query_as()).await?;
        Ok(rows)
    }
}
