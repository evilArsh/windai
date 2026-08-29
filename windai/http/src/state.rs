use axum::extract::FromRef;
use std::sync::Arc;
use wind_core::WindCore;

use crate::config::AppConfig;

/// 应用级共享状态。只放跨请求共享对象，不放请求级临时数据。
#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub core: Arc<WindCore>,
    pub started_at: i64,
}

impl AppState {
    pub fn new(config: AppConfig, core: Arc<WindCore>, started_at: i64) -> Self {
        Self {
            config,
            core,
            started_at,
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
