use serde::Serialize;
use utoipa::ToSchema;
use wind_mcp::client::ClientStatus;

/// start 端点响应：连接在后台进行，立即返回受理结果。
#[derive(Debug, Serialize, Clone, ToSchema)]
pub struct StartMcpServerResult {
    /// 是否已受理（后台开始连接）
    pub accepted: bool,
    /// MCP 服务名
    pub name: String,
}

/// MCP 服务运行期状态。`running=false` 表示该服务当前未在 registry 中运行。
#[derive(Debug, Serialize, Clone, ToSchema)]
pub struct McpServerStatusDto {
    /// 是否在运行
    pub running: bool,
    /// MCP 服务名
    pub name: String,
    /// 运行状态（未运行时为 None）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ClientStatus>,
    /// 持有该服务的 session 引用（未运行时为 None）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_sessions: Option<Vec<String>>,
}
