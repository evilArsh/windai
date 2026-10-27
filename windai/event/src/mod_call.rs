use super::envelope::Scope;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// 一次调用的模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum CallMode {
    /// 拦截模式, 返回修改后的值
    Intercept,
    /// 只用于观测
    Observe,
    /// 调用方在等返回值
    Request,
}

impl CallMode {
    /// 该模式下是否注入 I/O 能力
    pub fn allows_io(&self) -> bool {
        !matches!(self, Self::Intercept)
    }
}

/// 一次调用的约束
#[derive(Debug, Clone)]
pub struct CallCx {
    /// 本次调用的截止时间
    pub deadline: Instant,
    pub cancel: CancellationToken,
    pub mode: CallMode,
    pub scope: Scope,
}

impl CallCx {
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CallError {
    #[error("mod handler failed: {0}")]
    Handler(String),
    #[error("mod failure: {0}")]
    Internal(String),
}
