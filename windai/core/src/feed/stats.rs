/// writer 当前所处的阶段
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum WriterState {
    Idle,
    Writing,
    Faulted,
    Stopped,
}

#[derive(Debug, Clone)]
pub struct FeedStats {
    pub buffer_events: usize,
    /// 序列化载荷的近似字节数
    pub buffer_bytes: usize,
    pub groups: usize,
    pub pending_write: usize,
    pub oldest_pending_ms: u64,
    pub connections: usize,
    pub max_connection_backlog: usize,
    pub writer: WriterState,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownReport {
    pub journal_remaining: usize,
    pub buffer_remaining: usize,
    pub timed_out: bool,
}
