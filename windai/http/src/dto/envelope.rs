use serde::Serialize;
use utoipa::ToSchema;
use wind_core::error::CoreError;

/// 统一响应对象。
#[derive(Debug, Serialize, Clone, ToSchema)]
pub struct ApiResponse<T> {
    /// 业务码：200 成功，404 资源不存在，500 失败
    pub code: u16,
    /// 业务数据（列表/对象/空值）
    pub data: Option<T>,
    /// 人类可读的提示信息
    pub msg: String,
}

impl<T> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            code: 200,
            data: Some(data),
            msg: "ok".into(),
        }
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            code: 404,
            data: None,
            msg: msg.into(),
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            code: 500,
            data: None,
            msg: msg.into(),
        }
    }
}

pub fn map_core_error<T>(e: CoreError) -> ApiResponse<T> {
    match e {
        CoreError::RowNotFound(_) => ApiResponse::not_found("not found"),
        other => {
            log::error!("core error: {other:?}");
            ApiResponse::internal("internal error")
        }
    }
}
