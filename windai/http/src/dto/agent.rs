use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// 克隆 AgentDefinition 请求：将全局 Agent 复制为 Topic 专属
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct CloneAgentDefinitionRequest {
    /// 要复制的源 AgentDefinition id
    pub agent_id: i64,
}
