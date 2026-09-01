use std::sync::Arc;

use wind_core::WindCore;
use wind_core::models::{McpServerParam, Topic};
use wind_mcp::client::{ClientSnapshot, McpError, ServerParams};

use crate::dto::envelope::{ApiResponse, map_core_error};
use crate::dto::mcp::{McpServerStatusDto, StartMcpServerResult};

/// MCP 服务运行时 facade：启动 / 停止 / 查询运行期状态。
///
/// 与 `McpStorageFacade`（配置 CRUD）分离：本层只操作 `wind-mcp` 的 `RegistryHandle`，
/// 配置先落库（CRUD），运行期操作在此发起。
///
/// **session 语义**：`acquire`/`release` 的 `session_id` 即调用方的 `topic_id`。
/// 同一 mcp server 可被多个 topic 共享（各持一个引用），最后一个 topic 释放时服务停止。
pub struct McpRuntimeFacade {
    core: Arc<WindCore>,
}

impl McpRuntimeFacade {
    pub fn new(core: Arc<WindCore>) -> Self {
        Self { core }
    }

    /// 加载 topic，不存在时返回 ApiResponse 错误（调用方直接 return）。
    async fn load_topic(&self, topic_id: i64) -> Result<Topic, ApiResponse<()>> {
        match self.core.storage().topic().get_topic(topic_id).await {
            Ok(Some(t)) => Ok(t),
            Ok(None) => Err(ApiResponse::not_found("topic not found")),
            Err(e) => Err(map_core_error(e)),
        }
    }

    /// 加载 DB 配置；不存在时返回 ApiResponse 错误（调用方直接 return）。
    async fn load_param(&self, id: i64) -> Result<McpServerParam, ApiResponse<()>> {
        match self.core.storage().mcp().get(id).await {
            Ok(Some(m)) => Ok(m),
            Ok(None) => Err(ApiResponse::not_found("mcp server not found")),
            Err(e) => Err(map_core_error(e)),
        }
    }

    /// 启动 MCP 服务（供 topic 使用）：立即返回 accepted，连接在后台任务中进行。
    ///
    /// `session_id = topic_id`；`Connecting → Connected | Error` 事件由 registry 广播（见 `GET /events`）。
    /// `acquire` 按名幂等：同一 topic 重复 start 只增加引用或等待既有连接，不会重复拉起进程。
    pub async fn start_server(&self, topic_id: i64, id: i64) -> ApiResponse<StartMcpServerResult> {
        let _topic = match self.load_topic(topic_id).await {
            Ok(t) => t,
            Err(e) => return erase(e),
        };
        let param = match self.load_param(id).await {
            Ok(p) => p,
            Err(e) => return erase(e),
        };
        let params = match ServerParams::try_from(param.clone()) {
            Ok(p) => p,
            Err(e) => return map_core_error(e),
        };
        let name = param.name.clone();
        let task_name = name.clone();
        let session = topic_id.to_string();
        let registry = self.core.registry().clone();
        tokio::spawn(async move {
            match registry.acquire(&session, params).await {
                Ok(snapshot) => log::info!("mcp server '{task_name}' connected: {snapshot:?}"),
                Err(e) => log::error!("mcp server '{task_name}' failed to connect: {e}"),
            }
        });
        ApiResponse::ok(StartMcpServerResult {
            accepted: true,
            name,
        })
    }

    /// 停止 MCP 服务：移除该 topic 的引用，最后一个 topic 释放时服务停止。
    /// 服务未被该 topic 引用（未启动 / 已停止）时返回 404。
    pub async fn stop_server(&self, topic_id: i64, id: i64) -> ApiResponse<McpServerStatusDto> {
        let _topic = match self.load_topic(topic_id).await {
            Ok(t) => t,
            Err(e) => return erase(e),
        };
        let param = match self.load_param(id).await {
            Ok(p) => p,
            Err(e) => return erase(e),
        };
        let name = param.name;
        let session = topic_id.to_string();
        match self.core.registry().release(&session, &name).await {
            Ok(snapshot) => ApiResponse::ok(dto_from_snapshot(snapshot)),
            Err(e) => map_mcp_error(e),
        }
    }

    /// 查询运行期状态：服务未运行（registry 无此名）时返回 `running: false`，HTTP 仍 200。
    pub async fn server_status(&self, id: i64) -> ApiResponse<McpServerStatusDto> {
        let param = match self.load_param(id).await {
            Ok(p) => p,
            Err(e) => return erase(e),
        };
        let name = param.name;
        match self.core.registry().get_client(&name).await {
            Some(snapshot) => ApiResponse::ok(dto_from_snapshot(snapshot)),
            None => ApiResponse::ok(McpServerStatusDto {
                running: false,
                name,
                status: None,
                ref_sessions: None,
            }),
        }
    }
}

/// `ApiResponse<()>` → `ApiResponse<T>`：错误响应的 `data` 恒为 `None`，可安全类型擦除。
fn erase<T>(e: ApiResponse<()>) -> ApiResponse<T> {
    ApiResponse {
        code: e.code,
        data: None,
        msg: e.msg,
    }
}

fn dto_from_snapshot(snapshot: ClientSnapshot) -> McpServerStatusDto {
    McpServerStatusDto {
        running: !snapshot.ref_sessions.is_empty(),
        name: snapshot.name,
        status: Some(snapshot.status),
        ref_sessions: Some(snapshot.ref_sessions.into_iter().collect()),
    }
}

fn map_mcp_error<T>(e: McpError) -> ApiResponse<T> {
    match e {
        McpError::ServerNotFound(_) => ApiResponse::not_found("mcp server not running"),
        other => {
            log::error!("mcp error: {other:?}");
            ApiResponse::internal("mcp error")
        }
    }
}
