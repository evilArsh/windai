use super::domain::Domain;
use serde::{Deserialize, Serialize};

/// 取消的来源
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum CancelOrigin {
    /// 外部发起
    External,
    /// 由祖先节点级联而来
    Ancestor(i64),
}

/// 节点终态
#[derive(Debug, Clone, PartialEq)]
pub enum Terminal<D: Domain> {
    Done(D::Output),
    Failed(D::Error),
    Cancelled(CancelOrigin),
    /// worker 在运行过程中被终止：panic，或恢复时那个 worker 早已不存在
    Aborted,
}

/// 节点生命周期状态
#[derive(Debug, Clone, PartialEq)]
pub enum Phase<D: Domain> {
    /// worker 在跑
    Running,
    /// 等外部输入，worker 已退出
    Suspended(D::Suspend),
    /// 等子任务，worker 已退出
    WaitingChildren,
    /// 已请求停止，等确认
    Cancelling(CancelOrigin),
    /// 终态
    Done(D::Output),
    /// 终态
    Failed(D::Error),
    /// 终态
    Cancelled(CancelOrigin),
    /// 终态：worker 在运行过程中被终止
    Aborted,
}

impl<D: Domain> Phase<D> {
    /// 终态是吸收态
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Phase::Done(_) | Phase::Failed(_) | Phase::Cancelled(_) | Phase::Aborted
        )
    }

    /// 静息相：不依赖任何未来事件就能保持不动
    pub fn is_resting(&self) -> bool {
        self.is_terminal() || matches!(self, Phase::Suspended(_) | Phase::WaitingChildren)
    }

    /// 终态对应的结果视图
    pub fn terminal(&self) -> Option<Terminal<D>> {
        match self {
            Phase::Done(output) => Some(Terminal::Done(output.clone())),
            Phase::Failed(error) => Some(Terminal::Failed(error.clone())),
            Phase::Cancelled(origin) => Some(Terminal::Cancelled(*origin)),
            Phase::Aborted => Some(Terminal::Aborted),
            _ => None,
        }
    }

    /// 在途相且预期有在跑的 worker
    pub fn has_live_job(&self) -> bool {
        matches!(self, Phase::Running | Phase::Cancelling(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tree::domain::test_support::{
        TestDomain, TestError, TestOutput, TestSuspend,
    };

    #[test]
    fn resting_and_terminal_are_distinct() {
        let suspended: Phase<TestDomain> = Phase::Suspended(TestSuspend::Ask);
        assert!(!suspended.is_terminal(), "挂起不是终态");
        assert!(suspended.is_resting(), "挂起是静息相");

        let cancelling: Phase<TestDomain> = Phase::Cancelling(CancelOrigin::External);
        assert!(!cancelling.is_terminal(), "Cancelling 是中间态");
        assert!(!cancelling.is_resting(), "Cancelling 是在途相");

        let done: Phase<TestDomain> = Phase::Done(TestOutput::Ok);
        assert!(done.is_terminal());
        assert!(done.is_resting());

        let aborted: Phase<TestDomain> = Phase::Aborted;
        assert!(aborted.is_terminal(), "Aborted 是终态");
        assert!(aborted.is_resting(), "Aborted 是静息相");
        assert!(!aborted.has_live_job(), "Aborted 没有在跑的 worker");

        assert!(Phase::<TestDomain>::WaitingChildren.is_resting());
        assert!(!Phase::<TestDomain>::Running.is_resting());
    }

    #[test]
    fn terminal_view_carries_the_payload() {
        let done: Phase<TestDomain> = Phase::Done(TestOutput::Ok);
        assert!(matches!(
            done.terminal(),
            Some(Terminal::Done(TestOutput::Ok))
        ));

        let cancelled: Phase<TestDomain> = Phase::Cancelled(CancelOrigin::Ancestor(7));
        assert!(matches!(
            cancelled.terminal(),
            Some(Terminal::Cancelled(CancelOrigin::Ancestor(7)))
        ));

        let failed: Phase<TestDomain> = Phase::Failed(TestError::Boom);
        assert!(matches!(
            failed.terminal(),
            Some(Terminal::Failed(TestError::Boom))
        ));

        let aborted: Phase<TestDomain> = Phase::Aborted;
        assert!(
            matches!(aborted.terminal(), Some(Terminal::Aborted)),
            "Aborted 不带载荷"
        );
        assert!(Phase::<TestDomain>::Running.terminal().is_none());
    }

    #[test]
    fn only_running_and_cancelling_carry_a_live_job() {
        assert!(Phase::<TestDomain>::Running.has_live_job());
        assert!(Phase::<TestDomain>::Cancelling(CancelOrigin::External).has_live_job());
        assert!(!Phase::<TestDomain>::WaitingChildren.has_live_job());
        assert!(!Phase::<TestDomain>::Done(TestOutput::Ok).has_live_job());
    }
}
