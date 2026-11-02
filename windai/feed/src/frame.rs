use serde::{Deserialize, Serialize};
use serde_json::Value;
use wind_event::Envelope;
use wind_storage::LogRecord;

/// 订阅者收到的帧
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    /// 一条事件；未落库的实时增量没有 log_id
    Event {
        log_id: Option<i64>,
        envelope: Envelope<Value>,
    },
    /// 订阅建立时的状态快照
    Snapshot { data: Value },
}

impl Frame {
    pub(crate) fn from_record(record: LogRecord, payload: Option<Value>) -> Self {
        Frame::Event {
            log_id: Some(record.id),
            envelope: Envelope {
                time: record.create_time,
                name: record.name,
                scope: record.scope,
                origin: record.origin,
                source: record.source,
                payload: payload.unwrap_or(record.payload),
                shape: record.shape,
            },
        }
    }
}
