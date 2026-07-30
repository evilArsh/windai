use super::task::AgentOutput;
use super::tool::ListAgentsResponse;
use super::tool::{SpawnAgentRequest, SpawnAgentResponse};
use crate::error::Result;
use crate::models::ToolApprovalRequest;
use async_trait::async_trait;
use wind_ai::message::Message;
use wind_ai::tool::FunctionCall;

#[async_trait]
pub trait AgentHost: Send + Sync {
    async fn emit(&self, output: AgentOutput);

    async fn list_agents(&self) -> Result<ListAgentsResponse>;

    /// 获取所有工具审批请求记录
    async fn list_approvals(&self, message_id: i64) -> Result<Vec<ToolApprovalRequest>>;

    async fn spawn_agent(
        &self,
        call_id: String,
        request: SpawnAgentRequest,
    ) -> Result<SpawnAgentResponse>;

    /// 执行MCP工具调用
    async fn execute_tool_calls(&self, calls: &[FunctionCall]) -> Result<Message>;
}
