use crate::def::Shape;
use crate::envelope::Envelope;
use serde_json::Value;

/// 一条已经定稿、等待落库与广播的事件
pub struct Pending {
    pub log: bool,
    pub envelope: Envelope<Value>,
}

/// 一条已提交的事件，带上了日志id
pub struct Committed {
    /// 日志 id
    pub log_id: i64,
    pub envelope: Envelope<Value>,
}

/// 事件离开总线的唯一出口，实现方负责落库与广播
pub trait EventOutlet: Send + Sync {
    fn submit(&self, pending: Pending);
    fn submit_committed(&self, committed: Committed);
}
