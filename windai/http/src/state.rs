use axum::extract::FromRef;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use wind_core::WindCore;

use crate::config::AppConfig;

/// 应用级共享状态。只放跨请求共享对象，不放请求级临时数据。
#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub core: Arc<WindCore>,
    pub started_at: i64,
    /// 进程停机信号：SSE 流 select 该 token，取消即终止，避免无限流阻塞优雅停机（见 bug.md）
    pub cancel: CancellationToken,
}

impl AppState {
    pub fn new(config: AppConfig, core: Arc<WindCore>, started_at: i64) -> Self {
        Self::with_cancel(config, core, started_at, CancellationToken::new())
    }

    pub fn with_cancel(
        config: AppConfig,
        core: Arc<WindCore>,
        started_at: i64,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            config,
            core,
            started_at,
            cancel,
        }
    }
}

impl FromRef<AppState> for AppConfig {
    fn from_ref(input: &AppState) -> Self {
        input.config.clone()
    }
}

impl FromRef<AppState> for Arc<WindCore> {
    fn from_ref(input: &AppState) -> Self {
        input.core.clone()
    }
}
