use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::context::DispatchCx;
use crate::def::EventDef;

/// 触发操作的作用域
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Scope {
    Global,
    Topic { topic_id: i64 },
}

impl Scope {
    pub fn topic_id(&self) -> Option<i64> {
        match self {
            Scope::Global => None,
            Scope::Topic { topic_id } => Some(*topic_id),
        }
    }
}

/// 操作从哪条路径来
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Origin {
    Agent,
    Ui,
    Internal,
}

/// 事件由谁发出
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Source {
    Core { subsystem: String },
    Mod { name: String, version: u32 },
}

impl Source {
    pub fn core(subsystem: impl Into<String>) -> Self {
        Source::Core {
            subsystem: subsystem.into(),
        }
    }

    pub fn mod_(name: impl Into<String>, version: u32) -> Self {
        Source::Mod {
            name: name.into(),
            version,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<P> {
    /// 产生时刻，epoch 毫秒
    pub time: i64,
    pub name: String,
    pub scope: Scope,
    pub origin: Origin,
    pub source: Source,
    pub payload: P,
}

impl<P> Envelope<P> {
    pub fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    pub fn of(def: &EventDef<P>, cx: &DispatchCx, value: P) -> Self {
        Self {
            time: Self::now_ms(),
            name: def.name.to_string(),
            scope: cx.scope.clone(),
            origin: cx.origin,
            source: cx.source.clone(),
            payload: value,
        }
    }
}
