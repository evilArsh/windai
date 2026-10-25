use super::Storage;
use super::TableName;
use super::utils::{de_str_to, execute_allowing_zero_rows, execute_expecting_rows};
use crate::db::{DbDriver, DbRow};
use crate::delete_from;
use crate::error::{CoreError, Result};
use crate::feed::log::LogRecord;
use crate::select_fields;
use sqlx::{FromRow, QueryBuilder, Row};
use wind_event::def::Shape;
use wind_event::envelope::{EventSeq, Origin, Scope};

const SCOPE_GLOBAL: &str = "global";
const SCOPE_TOPIC: &str = "topic";
const SHAPE_INTERCEPT: &str = "intercept";
const SHAPE_OBSERVE: &str = "observe";

fn select_log<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::EVENT_LOG,
        (
            "seq",
            "time",
            "name",
            "shape",
            "scope_kind",
            "scope_id",
            "origin",
            "source",
            "cause",
            "payload"
        )
    )
}

impl<'s> FromRow<'s, DbRow> for LogRecord {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        let decode = |error: CoreError| sqlx::Error::Decode(Box::new(error));
        let seq: i64 = row.try_get("seq")?;
        let shape: String = row.try_get("shape")?;
        let scope_kind: String = row.try_get("scope_kind")?;
        let scope_id: i64 = row.try_get("scope_id")?;
        let origin: String = row.try_get("origin")?;
        let source: String = row.try_get("source")?;
        let cause: Option<i64> = row.try_get("cause")?;
        let payload: String = row.try_get("payload")?;
        Ok(Self {
            seq: seq_from_i64(seq).map_err(decode)?,
            time: row.try_get("time")?,
            name: row.try_get("name")?,
            shape: shape_from(&shape).map_err(decode)?,
            scope: scope_from(&scope_kind, scope_id).map_err(decode)?,
            origin: origin_from(&origin).map_err(decode)?,
            source: de_str_to(&source).map_err(decode)?,
            cause: cause.map(seq_from_i64).transpose().map_err(decode)?,
            payload: de_str_to(&payload).map_err(decode)?,
        })
    }
}

fn seq_to_i64(seq: EventSeq) -> Result<i64> {
    i64::try_from(seq.0).map_err(|_| CoreError::MalformedColumn {
        column: "event_log.seq".into(),
        detail: "event sequence exceeds the signed 64-bit range".into(),
    })
}

fn seq_from_i64(seq: i64) -> Result<EventSeq> {
    u64::try_from(seq)
        .map(EventSeq)
        .map_err(|_| CoreError::MalformedColumn {
            column: "event_log.seq".into(),
            detail: "event sequence is negative".into(),
        })
}

fn shape_name(shape: Shape) -> &'static str {
    match shape {
        Shape::Intercept => SHAPE_INTERCEPT,
        Shape::Observe => SHAPE_OBSERVE,
    }
}

fn shape_from(name: &str) -> Result<Shape> {
    match name {
        SHAPE_INTERCEPT => Ok(Shape::Intercept),
        SHAPE_OBSERVE => Ok(Shape::Observe),
        other => Err(CoreError::MalformedColumn {
            column: "event_log.shape".into(),
            detail: format!("unknown shape {other}"),
        }),
    }
}

fn scope_key(scope: &Scope) -> (&'static str, i64) {
    match scope {
        Scope::Global => (SCOPE_GLOBAL, 0),
        Scope::Topic { topic_id } => (SCOPE_TOPIC, *topic_id),
    }
}

fn scope_from(kind: &str, id: i64) -> Result<Scope> {
    match kind {
        SCOPE_GLOBAL => Ok(Scope::Global),
        SCOPE_TOPIC => Ok(Scope::Topic { topic_id: id }),
        other => Err(CoreError::MalformedColumn {
            column: "event_log.scope_kind".into(),
            detail: format!("unknown scope {other}"),
        }),
    }
}

fn origin_name(origin: &Origin) -> Result<String> {
    match serde_json::to_value(origin)? {
        serde_json::Value::String(name) => Ok(name),
        other => Err(CoreError::MalformedColumn {
            column: "event_log.origin".into(),
            detail: format!("origin path is not text {other}"),
        }),
    }
}

fn origin_from(name: &str) -> Result<Origin> {
    serde_json::from_value(serde_json::Value::String(name.to_owned())).map_err(Into::into)
}

struct LogRow {
    seq: i64,
    time: i64,
    name: String,
    shape: &'static str,
    scope_kind: &'static str,
    scope_id: i64,
    origin: String,
    source: String,
    cause: Option<i64>,
    payload: String,
}

fn encode_row(record: &LogRecord) -> Result<LogRow> {
    let (scope_kind, scope_id) = scope_key(&record.scope);
    Ok(LogRow {
        seq: seq_to_i64(record.seq)?,
        time: record.time,
        name: record.name.clone(),
        shape: shape_name(record.shape),
        scope_kind,
        scope_id,
        origin: origin_name(&record.origin)?,
        source: serde_json::to_string(&record.source)?,
        cause: record.cause.map(seq_to_i64).transpose()?,
        payload: serde_json::to_string(&record.payload)?,
    })
}

impl Storage {
    /// 批量追加事件，空切片直接返回
    pub async fn event_log_append(&self, records: &[LogRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let rows: Vec<LogRow> = records.iter().map(encode_row).collect::<Result<_>>()?;
        let mut qb = QueryBuilder::<DbDriver>::new("");
        qb.push("INSERT INTO ").push(TableName::EVENT_LOG).push(
            " (seq, time, name, shape, scope_kind, scope_id, origin, source, cause, payload) ",
        );
        qb.push_values(rows, |mut row, entry| {
            let LogRow {
                seq,
                time,
                name,
                shape,
                scope_kind,
                scope_id,
                origin,
                source,
                cause,
                payload,
            } = entry;
            row.push_bind(seq)
                .push_bind(time)
                .push_bind(name)
                .push_bind(shape)
                .push_bind(scope_kind)
                .push_bind(scope_id)
                .push_bind(origin)
                .push_bind(source)
                .push_bind(cause)
                .push_bind(payload);
        });
        execute_expecting_rows(&self.executor, qb).await
    }

    /// 当前最大序号，空表为 0
    pub async fn event_log_max_seq(&self) -> Result<u64> {
        let max: i64 = self
            .executor
            .fetch_one_scalar(
                QueryBuilder::<DbDriver>::new("SELECT COALESCE(MAX(seq), 0) AS seq FROM ")
                    .push(TableName::EVENT_LOG)
                    .build_query_scalar::<i64>(),
            )
            .await?;
        Ok(seq_from_i64(max)?.0)
    }

    /// 取出某个作用域与序号区间的事件，按序号升序并限制条数
    pub async fn event_log_range(
        &self,
        topic_id: Option<i64>,
        since: u64,
        limit: i64,
        until: u64,
    ) -> Result<Vec<LogRecord>> {
        let (scope_kind, scope_id) = match topic_id {
            Some(id) => (SCOPE_TOPIC, id),
            None => (SCOPE_GLOBAL, 0),
        };
        let since = seq_to_i64(EventSeq(since))?;
        let until = seq_to_i64(EventSeq(until))?;
        self.executor
            .fetch_all(
                select_log()
                    .push("WHERE scope_kind = ")
                    .push_bind(scope_kind)
                    .push(" AND scope_id = ")
                    .push_bind(scope_id)
                    .push(" AND seq > ")
                    .push_bind(since)
                    .push(" AND seq <= ")
                    .push_bind(until)
                    .push(" ORDER BY seq ASC LIMIT ")
                    .push_bind(limit)
                    .build_query_as::<LogRecord>(),
            )
            .await
    }

    /// 删除 topic 的全部事件，没有记录不报错
    pub async fn event_log_delete_topic(&self, topic_id: i64) -> Result<()> {
        let mut qb = delete_from!(TableName::EVENT_LOG);
        qb.push(" WHERE scope_kind = ")
            .push_bind(SCOPE_TOPIC)
            .push(" AND scope_id = ")
            .push_bind(topic_id);
        execute_allowing_zero_rows(&self.executor, qb).await
    }
}
