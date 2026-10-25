use super::envelope::Scope;
use super::subscription::{EnabledSet, Subscription};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// 一次调用的模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum CallMode {
    /// 拦截模式：操作继续之前必须给出决定，决定会影响别的操作
    Intercept,
    /// 只用于观测
    Observe,
    /// 调用方在等返回值
    Request,
}

impl CallMode {
    /// 该模式下是否注入 I/O 能力
    pub fn allows_io(self) -> bool {
        !matches!(self, CallMode::Intercept)
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
pub enum HostError {
    #[error("mod handler failed: {0}")]
    Handler(String),
    #[error("host failure: {0}")]
    Internal(String),
}

/// 调用一个 mod 的运行时接口
#[async_trait::async_trait]
pub trait ModHost: Send + Sync {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError>;
}

/// 某个作用域下启用的 mod 集
pub trait ModEnablement: Send + Sync {
    fn enabled(&self, scope: &Scope) -> EnabledSet;
}

impl<T: ModEnablement + ?Sized> ModEnablement for Arc<T> {
    fn enabled(&self, scope: &Scope) -> EnabledSet {
        (**self).enabled(scope)
    }
}
