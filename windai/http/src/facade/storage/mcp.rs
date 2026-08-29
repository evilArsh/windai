use std::sync::Arc;
use wind_core::WindCore;
use wind_core::models::{CreateMcpServer, McpServerParam, UpdateMcpServer};

use crate::dto::envelope::{ApiResponse, map_core_error};

pub struct McpStorageFacade {
    core: Arc<WindCore>,
}

impl McpStorageFacade {
    pub fn new(core: Arc<WindCore>) -> Self {
        Self { core }
    }

    pub async fn list_mcp_servers(&self) -> ApiResponse<Vec<McpServerParam>> {
        match self.core.storage().mcp().list().await {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn create_mcp_server(&self, input: CreateMcpServer) -> ApiResponse<McpServerParam> {
        match self.core.storage().mcp().create(input).await {
            Ok(m) => ApiResponse::ok(m),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_mcp_server(&self, id: i64) -> ApiResponse<McpServerParam> {
        match self.core.storage().mcp().get(id).await {
            Ok(Some(m)) => ApiResponse::ok(m),
            Ok(None) => ApiResponse::not_found("mcp server not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn update_mcp_server(
        &self,
        id: i64,
        input: UpdateMcpServer,
    ) -> ApiResponse<McpServerParam> {
        if let Err(e) = self.core.storage().mcp().update(id, input).await {
            return map_core_error(e);
        }
        self.get_mcp_server(id).await
    }

    pub async fn delete_mcp_server(&self, id: i64) -> ApiResponse<()> {
        match self.core.storage().mcp().delete(id).await {
            Ok(()) => ApiResponse::ok(()),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_mcp_server_by_name(&self, name: String) -> ApiResponse<McpServerParam> {
        match self.core.storage().mcp().get_by_name(&name).await {
            Ok(Some(m)) => ApiResponse::ok(m),
            Ok(None) => ApiResponse::not_found("mcp server not found"),
            Err(e) => map_core_error(e),
        }
    }
}
