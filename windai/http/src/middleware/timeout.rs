use std::time::Duration;

/// CRUD / 短请求的默认超时；SSE 路由不套此层。
pub const CRUD_TIMEOUT: Duration = Duration::from_secs(30);
