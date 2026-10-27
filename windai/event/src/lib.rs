use self::def::{Directory, EventDef, Shape};
use self::tracking::DeliveryTarget;
use serde::{Deserialize, Serialize};

pub mod context;
pub mod def;
pub mod dispatch;
pub mod envelope;
pub mod guard;
pub mod mod_call;
pub mod outlet;
pub mod subscription;
pub mod tracking;

pub fn schema<T: schemars::JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("the generated schema is valid JSON")
}

pub(crate) fn to_json<P: Serialize>(
    event: &'static str,
    payload: &P,
) -> Result<serde_json::Value, DispatchError> {
    serde_json::to_value(payload).map_err(|e| DispatchError::BadPayload {
        event,
        detail: e.to_string(),
    })
}

/// 派发失败，被拒绝不在此列
#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("event {name} has the wrong shape for this call")]
    WrongShape { name: &'static str },
    #[error("mod {mod_name} rewrote {event} into a value the declared schema rejects")]
    BadRewrite {
        mod_name: String,
        event: &'static str,
    },
    #[error("payload for {event} could not be serialized: {detail}")]
    BadPayload { event: &'static str, detail: String },
    #[error("event chain exceeded max depth {max} at {name}")]
    TooDeep { max: u32, name: &'static str },
    #[error("single trigger exceeded {max} events")]
    TooManyEvents { max: u32 },
    #[error("event {name} is not in the directory")]
    NotInDirectory { name: &'static str },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DispatchTimeout {
    /// 被超时的订阅事件名
    pub event: String,
    /// 被超时事件的序号
    pub event_seq: u64,
    /// 超时的订阅方
    pub target: DeliveryTarget,
    /// 订阅方耗掉的时间
    pub duration_ms: u64,
}

/// 派发失败的类别
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum DispatchFailure {
    /// mod 的改写无法解析为该事件的载荷
    BadRewrite,
    /// 处理函数抛错
    HandlerError,
    /// 宿主内部错误
    HostError,
    /// 嵌套派发超过深度上限
    TooDeep,
    /// 单次触发的事件预算耗尽
    TooManyEvents,
    /// 载荷无法序列化
    BadPayload,
    /// 调用方用错了形态
    WrongShape,
    /// 事件不在目录里
    NotInDirectory,
}

/// 一次派发失败
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DispatchFailed {
    /// 失败的事件名
    pub event: String,
    /// 失败类别
    pub failure: DispatchFailure,
    /// 出错的订阅方；与具体 mod 无关的失败缺席
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<DeliveryTarget>,
    /// 失败事件的序号；序号尚未分配时缺席
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_seq: Option<u64>,
    /// 面向排查的说明
    pub reason: String,
}

pub const CORE_DISPATCH_TIMEOUT: EventDef<DispatchTimeout> = EventDef::new(
    "core/dispatch.timeout",
    Shape::Observe,
    true,
    schema::<DispatchTimeout>,
);

pub const CORE_DISPATCH_FAILED: EventDef<DispatchFailed> = EventDef::new(
    "core/dispatch.failed",
    Shape::Observe,
    true,
    schema::<DispatchFailed>,
);

/// 派发层自身产生的事件
pub fn directory() -> Directory {
    Directory::from_metas([CORE_DISPATCH_TIMEOUT.meta(), CORE_DISPATCH_FAILED.meta()])
}
