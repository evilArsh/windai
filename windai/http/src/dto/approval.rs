use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// 审批工具调用请求：允许或拒绝挂起的 tool call
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct ApproveToolCallsRequest {
    /// 批准的工具调用 id 列表
    pub allow_ids: Vec<i64>,
    /// 拒绝的工具调用 id 列表
    pub deny_ids: Vec<i64>,
}
