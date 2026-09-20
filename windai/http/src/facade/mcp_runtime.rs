use crate::dto::{ApiResponse, map_core_error};
use std::borrow::Cow;
use std::sync::Arc;
use wind_core::WindCore;
use wind_core::models::{McpServerParam, Topic};
use wind_mcp::client::{
    BUILTIN_SESSION, ClientSnapshot, McpError, Prompt, Resource, ServerParams, Tool,
};

/// MCP 服务运行时 facade：启动 / 停止 / 查询运行期状态
///
/// 与 `McpStorageFacade`（配置 CRUD）分离：本层只操作 `wind-mcp` 的 `RegistryHandle`，
/// 配置先落库（CRUD），运行期操作在此发起
///
/// **session 语义**：`acquire`/`release` 的 `session_id` 即调用方的 `topic_id`
/// 同一 mcp server 可被多个 topic 共享（各持一个引用），最后一个 topic 释放时服务停止
pub struct McpRuntimeFacade {
    core: Arc<WindCore>,
}

impl McpRuntimeFacade {
    pub fn new(core: Arc<WindCore>) -> Self {
        Self { core }
    }

    async fn resolve_session_id(
        &self,
        topic_id: i64,
    ) -> Result<Cow<'static, str>, ApiResponse<()>> {
        match topic_id {
            0 => Ok(Cow::Borrowed(BUILTIN_SESSION)),
            id => match self.load_topic(id).await {
                Ok(_) => Ok(Cow::Owned(topic_id.to_string())),
                Err(e) => Err(e),
            },
        }
    }

    /// 加载 topic，不存在时返回 ApiResponse 错误（调用方直接 return）
    async fn load_topic(&self, topic_id: i64) -> Result<Topic, ApiResponse<()>> {
        match self.core.storage().topic().get_topic(topic_id).await {
            Ok(Some(t)) => Ok(t),
            Ok(None) => Err(ApiResponse::not_found("topic not found")),
            Err(e) => Err(map_core_error(e)),
        }
    }

    /// 加载 DB 配置；不存在时返回 ApiResponse 错误（调用方直接 return）
    async fn load_param(&self, id: i64) -> Result<McpServerParam, ApiResponse<()>> {
        match self.core.storage().mcp().get(id).await {
            Ok(Some(m)) => Ok(m),
            Ok(None) => Err(ApiResponse::not_found("mcp server not found")),
            Err(e) => Err(map_core_error(e)),
        }
    }

    /// 启动 MCP 服务（供 topic 使用）：立即返回 accepted，连接在后台任务中进行
    pub async fn start_server(&self, topic_id: i64, id: i64) -> ApiResponse<()> {
        let session_id = match self.resolve_session_id(topic_id).await {
            Ok(s) => s,
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
        let registry = self.core.registry().clone();
        tokio::spawn(async move {
            match registry.acquire(&session_id, params).await {
                Ok(snapshot) => log::info!("mcp server '{task_name}' connected: {snapshot:?}"),
                Err(e) => log::error!("mcp server '{task_name}' failed to connect: {e}"),
            }
        });
        ApiResponse::ok(())
    }

    /// 停止 MCP 服务：移除该 topic 的引用，最后一个 topic 释放时服务停止
    /// 服务未被该 topic 引用（未启动 / 已停止）时返回 404
    pub async fn stop_server(&self, topic_id: i64, id: i64) -> ApiResponse<ClientSnapshot> {
        let session_id = match self.resolve_session_id(topic_id).await {
            Ok(s) => s,
            Err(e) => return erase(e),
        };
        let param = match self.load_param(id).await {
            Ok(p) => p,
            Err(e) => return erase(e),
        };
        let name = param.name;
        match self.core.registry().release(&session_id, &name).await {
            Ok(snapshot) => ApiResponse::ok(snapshot),
            Err(e) => map_mcp_error(e),
        }
    }

    /// 彻底终止mcp服务，删除所有引用
    pub(crate) async fn terminate_server(&self, name: &str) -> ApiResponse<ClientSnapshot> {
        match self.core.registry().terminate(name).await {
            Ok(snapshot) => ApiResponse::ok(snapshot),
            Err(e) => map_mcp_error(e),
        }
    }

    /// 按 server name 让 topic 引用一个已运行的 client（内建即此场景）
    /// 服务已启动则只加引用计数（幂等），不发起连接；服务未运行返回 404
    pub async fn attach_server(&self, topic_id: i64, name: &str) -> ApiResponse<ClientSnapshot> {
        let session_id = match self.resolve_session_id(topic_id).await {
            Ok(s) => s,
            Err(e) => return erase(e),
        };
        match self.core.registry().attach_session(&session_id, name).await {
            Ok(snapshot) => ApiResponse::ok(snapshot),
            Err(e) => map_mcp_error(e),
        }
    }

    /// 列出所有运行中的 MCP 客户端
    pub async fn list_clients(&self) -> ApiResponse<Vec<ClientSnapshot>> {
        ApiResponse::ok(self.core.registry().list_clients().await)
    }

    /// 按 server name 查询运行中的 MCP 客户端；未运行返回 404
    pub async fn get_client(&self, name: &str) -> ApiResponse<ClientSnapshot> {
        match self.core.registry().get_client(name).await {
            Some(snapshot) => ApiResponse::ok(snapshot),
            None => ApiResponse::not_found("mcp server not running"),
        }
    }

    /// 列出指定 client 的工具
    pub async fn list_tools(&self, name: &str) -> ApiResponse<Vec<Tool>> {
        match self.core.registry().list_tools(name).await {
            Ok(tools) => ApiResponse::ok(tools),
            Err(e) => map_mcp_error(e),
        }
    }

    /// 按 server names 批量列出工具
    pub async fn list_tools_by_names(&self, names: Vec<String>) -> ApiResponse<Vec<Tool>> {
        match self.core.registry().list_tools_by_names(&names).await {
            Ok(tools) => ApiResponse::ok(tools),
            Err(e) => map_mcp_error(e),
        }
    }

    /// 列出所有运行中 client 的工具
    pub async fn list_all_tools(&self) -> ApiResponse<Vec<Tool>> {
        match self.core.registry().list_all_tools().await {
            Ok(tools) => ApiResponse::ok(tools),
            Err(e) => map_mcp_error(e),
        }
    }

    /// 列出指定 client 的提示词
    pub async fn list_prompts(&self, name: &str) -> ApiResponse<Vec<Prompt>> {
        match self.core.registry().list_prompts(name).await {
            Ok(prompts) => ApiResponse::ok(prompts),
            Err(e) => map_mcp_error(e),
        }
    }

    /// 列出指定 client 的资源
    pub async fn list_resources(&self, name: &str) -> ApiResponse<Vec<Resource>> {
        match self.core.registry().list_resources(name).await {
            Ok(resources) => ApiResponse::ok(resources),
            Err(e) => map_mcp_error(e),
        }
    }
}

/// `ApiResponse<()>` → `ApiResponse<T>`：错误响应的 `data` 恒为 `None`，可安全类型擦除
fn erase<T>(e: ApiResponse<()>) -> ApiResponse<T> {
    e.without_data()
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
