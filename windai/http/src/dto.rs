use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use wind_ai::message::Content;
use wind_core::error::CoreError;

/// 统一响应对象
///
/// 业务成功/失败一律以 HTTP 200 承载，`code` 表达业务结果；
/// HTTP 状态码只留给协议层错误（extractor 拒绝、中间件、404 兜底）
#[derive(Debug, Serialize, Clone, ToSchema)]
pub struct ApiResponse<T> {
    /// 业务码：200 成功，400 参数错误，404 资源不存在，500 失败
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

    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            code: 400,
            data: None,
            msg: msg.into(),
        }
    }

    /// 丢弃 data，把响应转成另一种 data 类型，供内部 helper 透传错误
    pub fn without_data<U>(self) -> ApiResponse<U> {
        ApiResponse {
            code: self.code,
            data: None,
            msg: self.msg,
        }
    }
}

pub fn map_core_error<T>(e: CoreError) -> ApiResponse<T> {
    match e {
        CoreError::RowNotFound(_) => ApiResponse::not_found("not found"),
        CoreError::Validation(msg) => ApiResponse::bad_request(msg),
        other => {
            // log::error!("core error: {other:?}");
            ApiResponse::internal(other)
        }
    }
}

/// 提交对话输入：向 TopicRuntime 提交用户消息，不创建 Message 记录
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct CreateChatRequest {
    /// 对话内容列表，支持文本/图片/文件/音频/函数调用结果
    pub content: Vec<Content>,
}

/// 审批工具调用请求：允许或拒绝挂起的 tool call
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct ApproveToolCallsRequest {
    /// 批准的工具调用 id 列表
    pub allow_ids: Vec<i64>,
    /// 拒绝的工具调用 id 列表
    pub deny_ids: Vec<i64>,
}
