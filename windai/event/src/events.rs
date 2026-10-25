use super::def::{Directory, EventDef, Mount, Shape};
use super::tracking::DeliveryTarget;
use serde::{Deserialize, Serialize};

/// 一次投递超时
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    Mount::Subsystem,
    timeout_schema,
);

pub const CORE_DISPATCH_FAILED: EventDef<DispatchFailed> = EventDef::new(
    "core/dispatch.failed",
    Shape::Observe,
    true,
    Mount::Subsystem,
    failed_schema,
);

fn timeout_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["event", "event_seq", "target", "duration_ms"],
        "properties": {
            "event": { "type": "string" },
            "event_seq": { "type": "integer" },
            "target": { "type": "object" },
            "duration_ms": { "type": "integer" }
        }
    })
}

fn failed_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["event", "failure", "reason"],
        "properties": {
            "event": { "type": "string" },
            "failure": {
                "enum": [
                    "bad_rewrite",
                    "handler_error",
                    "host_error",
                    "too_deep",
                    "too_many_events",
                    "bad_payload",
                    "wrong_shape",
                    "not_in_directory"
                ]
            },
            "target": { "type": ["object", "null"] },
            "event_seq": { "type": ["integer", "null"] },
            "reason": { "type": "string" }
        }
    })
}

/// 派发层自身产生的事件
pub fn directory() -> Directory {
    Directory::from_metas([CORE_DISPATCH_TIMEOUT.meta(), CORE_DISPATCH_FAILED.meta()])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::def::visible_to_frontend;

    #[test]
    fn the_failure_event_reaches_the_frontend_and_the_log() {
        assert_eq!(CORE_DISPATCH_FAILED.shape, Shape::Observe);
        assert!(visible_to_frontend(CORE_DISPATCH_FAILED.shape));
        assert!(CORE_DISPATCH_FAILED.log);
    }
}
