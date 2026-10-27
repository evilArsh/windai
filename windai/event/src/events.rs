use crate::def::{Directory, EventDef, Shape, schema};
use serde::{Deserialize, Serialize};

/// 一次投递的接收方
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr, schemars::JsonSchema,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum DeliveryTarget {
    Mod { name: String },
    Observer { name: String },
}

impl DeliveryTarget {
    pub fn mod_(name: impl Into<String>) -> Self {
        DeliveryTarget::Mod { name: name.into() }
    }

    pub fn observer(name: impl Into<String>) -> Self {
        DeliveryTarget::Observer { name: name.into() }
    }

    pub fn name(&self) -> &str {
        match self {
            DeliveryTarget::Mod { name } | DeliveryTarget::Observer { name } => name,
        }
    }
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
    /// mod 运行时自身出错
    CallError,
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

/// 一次投递超时
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DispatchTimeout {
    /// 被超时的订阅事件名
    pub event: String,
    /// 超时的订阅方
    pub target: DeliveryTarget,
    /// 订阅方耗掉的时间
    pub duration_ms: u64,
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
