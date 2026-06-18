use std::future::Future;
use std::pin::Pin;

use crate::error::Result;
use crate::models::agent::{CreateToolApprovalRequests, ToolApprovalDecision, ToolApprovalRequest};

/// 普通 Chat 和 Agent 共用的审批服务。
pub trait ToolApprovalService: Send + Sync {
    fn create_requests<'a>(
        &'a self,
        input: CreateToolApprovalRequests,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ToolApprovalRequest>>> + Send + 'a>>;

    fn approve<'a>(
        &'a self,
        id: i64,
        decision: ToolApprovalDecision,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    fn deny<'a>(
        &'a self,
        id: i64,
        decision: ToolApprovalDecision,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    fn list_pending_by_message<'a>(
        &'a self,
        message_id: i64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ToolApprovalRequest>>> + Send + 'a>>;
}
