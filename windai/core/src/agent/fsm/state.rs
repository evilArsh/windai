use crate::models::AgentStatus;

/// 任务级状态状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum TaskState {
    /// 空闲
    Idle,
    /// Agent 循环运行中
    Running,
    /// 等待工具审批（暂停，可恢复）
    WaitingApproval,
    /// 等待子 Agent 完成（暂停，可恢复）
    WaitingChild,
    /// 终态，已完成
    Finished,
    /// 终态，已失败
    Failed,
    /// 终态，已取消
    Cancelled,
}

impl From<TaskState> for AgentStatus {
    fn from(s: TaskState) -> Self {
        match s {
            TaskState::Idle => AgentStatus::Created,
            TaskState::Running => AgentStatus::Running,
            TaskState::WaitingApproval => AgentStatus::WaitingApproval,
            TaskState::WaitingChild => AgentStatus::WaitingChild,
            TaskState::Finished => AgentStatus::Finished,
            TaskState::Failed => AgentStatus::Failed,
            TaskState::Cancelled => AgentStatus::Cancelled,
        }
    }
}

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
