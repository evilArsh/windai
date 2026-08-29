use chrono::Utc;
use serde_json::{Value, json};
use std::sync::Arc;
use wind_core::WindCore;

use crate::dto::envelope::ApiResponse;

pub struct SystemFacade {
    _core: Arc<WindCore>,
    started_at: i64,
}

impl SystemFacade {
    pub fn new(core: Arc<WindCore>, started_at: i64) -> Self {
        Self {
            _core: core,
            started_at,
        }
    }

    /// 健康检查：只读启动信息与构建信息
    pub fn health(&self) -> ApiResponse<Value> {
        let uptime = Utc::now().timestamp().saturating_sub(self.started_at);
        ApiResponse::ok(json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION"),
            "started_at": self.started_at,
            "uptime_seconds": uptime,
        }))
    }
}
