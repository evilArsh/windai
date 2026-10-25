use std::time::Duration;

#[derive(Default, Debug, Eq, PartialEq, Clone)]
pub struct Event {
    /// 事件名，空字符串表示未给出
    pub event: String,
    pub data: String,
    /// 事件 id，空字符串表示未给出
    pub id: String,
    pub retry: Option<Duration>,
}
