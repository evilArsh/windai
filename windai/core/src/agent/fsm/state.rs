/// Topic 状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicState {
    /// 空闲
    Idle,
    /// 主任务运行中
    Running,
    /// 已终止
    Stopped,
}
