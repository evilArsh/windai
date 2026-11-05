use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// 一次 feed 运行内的事件身份与全序
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventKey {
    #[serde(with = "decimal")]
    pub epoch: i64,
    #[serde(with = "decimal")]
    pub seq: i64,
}

impl EventKey {
    pub const ZERO: Self = Self { epoch: 0, seq: 0 };

    /// 两个分量都必须非负
    pub fn new(epoch: i64, seq: i64) -> Option<Self> {
        (epoch >= 0 && seq >= 0).then_some(Self { epoch, seq })
    }

    pub fn event_id(&self) -> String {
        format!("{}:{}", self.epoch, self.seq)
    }
}

impl fmt::Display for EventKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.epoch, self.seq)
    }
}

mod decimal {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
        if *value < 0 {
            return Err(serde::ser::Error::custom(
                "event key component must not be negative",
            ));
        }
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
        let text = String::deserialize(deserializer)?;
        parse(&text).map_err(D::Error::custom)
    }

    fn parse(text: &str) -> Result<i64, String> {
        if text.is_empty() {
            return Err("event key component is empty".into());
        }
        if !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(format!(
                "event key component is not a decimal integer: {text}"
            ));
        }
        if text.len() > 1 && text.starts_with('0') {
            return Err(format!("event key component has leading zeros: {text}"));
        }
        text.parse::<i64>()
            .map_err(|_| format!("event key component is out of range: {text}"))
    }
}

/// 触发操作的作用域
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Scope {
    Global,
    Topic { topic_id: i64, instance_id: i64 },
}

impl Scope {
    pub fn topic_id(&self) -> Option<i64> {
        match self {
            Scope::Global => None,
            Scope::Topic { topic_id, .. } => Some(*topic_id),
        }
    }

    pub fn instance_id(&self) -> Option<i64> {
        match self {
            Scope::Global => None,
            Scope::Topic { instance_id, .. } => Some(*instance_id),
        }
    }
}

/// 操作从哪条路径来
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr, strum::EnumString,
)]
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

    pub fn module(name: impl Into<String>, version: u32) -> Self {
        Source::Mod {
            name: name.into(),
            version,
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    strum::AsRefStr,
    strum::EnumString,
    Serialize,
    Deserialize,
)]
#[strum(serialize_all = "snake_case")]
pub enum Shape {
    Intercept,
    Observe,
}

impl Shape {
    pub const fn visible_to_frontend(self) -> bool {
        matches!(self, Self::Observe)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope<P> {
    /// 产生时刻，epoch 毫秒
    pub time: i64,
    pub name: String,
    pub scope: Scope,
    pub origin: Origin,
    pub source: Source,
    pub payload: Option<P>,
    pub shape: Shape,
}

impl Envelope<Value> {
    pub fn new(
        name: impl Into<String>,
        scope: Scope,
        origin: Origin,
        source: Source,
        payload: Option<Value>,
        shape: Shape,
    ) -> Self {
        Self {
            time: Self::now_ms(),
            name: name.into(),
            scope,
            origin,
            source,
            payload,
            shape,
        }
    }
}

impl<P> Envelope<P> {
    pub fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

/// 事件的展示去向
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Target {
    Core,
    Mod { name: String, channel: String },
}

impl Target {
    pub fn module(name: impl Into<String>, channel: impl Into<String>) -> Self {
        Target::Mod {
            name: name.into(),
            channel: channel.into(),
        }
    }
}

/// 实时流与历史共用的唯一事件结构
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeedEvent {
    pub key: EventKey,
    pub envelope: Envelope<Value>,
    pub target: Target,
}
