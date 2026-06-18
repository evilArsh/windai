use std::future::Future;
use std::pin::Pin;

use crate::error::Result;
use crate::models::agent::{PolicyContext, PolicyDecision, SpawnAgentRequest};

/// Agent 调度与工具调用的策略校验器。
pub trait PolicyEngine: Send + Sync {
    fn check_spawn_agent<'a>(
        &'a self,
        ctx: &'a PolicyContext,
        request: &'a SpawnAgentRequest,
    ) -> Pin<Box<dyn Future<Output = Result<PolicyDecision>> + Send + 'a>>;

    fn check_tool_call<'a>(
        &'a self,
        ctx: &'a PolicyContext,
        tool_name: &'a str,
        arguments: &'a serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<PolicyDecision>> + Send + 'a>>;
}
