use crate::dto::{ApiResponse, map_core_error};
use crate::facade::mcp_runtime::McpRuntimeFacade;
use std::collections::HashSet;
use std::sync::Arc;
use wind_core::WindCore;
use wind_core::models::{CreateMcpServer, McpServerParam, UpdateMcpServer};
use wind_mcp::builtin::{BUILTIN_SERVERS, BuiltinSpec};
use wind_mcp::client::{ClientSnapshot, ClientStatus};

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

    pub fn list_mcp_servers_builtin(&self) -> ApiResponse<Vec<BuiltinSpec>> {
        ApiResponse::ok(BUILTIN_SERVERS.to_vec())
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

    pub async fn delete_mcp_server(&self, id: i64) -> ApiResponse<ClientSnapshot> {
        let res = self.get_mcp_server(id).await;
        let server = match res.data {
            Some(server) => server,
            None => return res.without_data(),
        };

        if let Err(e) = self.core.storage().mcp().delete(server.id).await {
            return map_core_error(e);
        }

        match McpRuntimeFacade::new(self.core.clone())
            .terminate_server(&server.name)
            .await
        {
            ApiResponse { code: 404, .. } => ApiResponse::ok(ClientSnapshot {
                name: server.name,
                transport: server.r#type,
                status: ClientStatus::Disconnected,
                ref_sessions: HashSet::new(),
            }),
            resp => resp,
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
