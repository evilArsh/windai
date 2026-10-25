//! 事件的持久记录

use wind_event::def::{Shape, visible_to_frontend};
use wind_event::envelope::{EventEnvelope, EventSeq, Origin, Scope, Source};

/// 单次回补的条数上限
pub const BACKFILL_LIMIT: i64 = 1000;

#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    pub seq: EventSeq,
    /// 产生时刻，epoch 毫秒
    pub time: i64,
    pub name: String,
    pub shape: Shape,
    pub scope: Scope,
    pub origin: Origin,
    pub source: Source,
    /// 顶层操作为 None
    pub cause: Option<EventSeq>,
    pub payload: serde_json::Value,
}

impl LogRecord {
    pub fn from_envelope(name: &str, shape: Shape, envelope: &serde_json::Value) -> Option<Self> {
        match serde_json::from_value::<EventEnvelope<serde_json::Value>>(envelope.clone()) {
            Ok(envelope) => Some(Self {
                seq: envelope.seq,
                time: envelope.time,
                name: envelope.name,
                shape,
                scope: envelope.scope,
                origin: envelope.origin,
                source: envelope.source,
                cause: envelope.cause,
                payload: envelope.payload,
            }),
            Err(error) => {
                log::warn!("[feed] event {name} carries no usable envelope: {error}");
                None
            }
        }
    }

    pub fn visible_to_frontend(&self) -> bool {
        visible_to_frontend(self.shape)
    }

    pub fn envelope(&self) -> serde_json::Value {
        let mut envelope = serde_json::json!({
            "seq": self.seq.0,
            "time": self.time,
            "name": self.name,
            "scope": self.scope,
            "origin": self.origin,
            "source": self.source,
            "payload": self.payload,
        });
        if let Some(cause) = self.cause {
            envelope["cause"] = serde_json::json!(cause.0);
        }
        envelope
    }
}
