use crate::envelope::Envelope;
use serde_json::Value;
use wind_wire::event::Target;

/// 事件在 feed 中的保留方式
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Retention {
    /// 保留到日志提交确认
    Journal,
    /// 保留到指定组关闭
    Group(GroupKey),
    /// 向当前连接排入后关闭指定组
    Close(GroupKey),
    /// 只向当前连接推送
    Live,
}

/// 保留组标识
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GroupKey {
    Message(i64),
}

/// 一条已经定稿、等待落库与广播的事件
pub struct Pending {
    pub envelope: Envelope<Value>,
    pub retention: Retention,
    pub target: Target,
}

#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    #[error("the feed outlet is closed")]
    Closed,
}

/// 事件离开总线的唯一出口，实现方负责落库与广播
pub trait EventOutlet: Send + Sync {
    fn submit(&self, pending: Pending) -> Result<(), SubmitError>;
}
