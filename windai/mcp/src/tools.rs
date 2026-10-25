use super::events::McpToolInfo;
use super::registry::{McpError, Registry};
use std::sync::Arc;
use wind_core::error::CoreError;
use wind_core::llm::ports::{DeclaredTool, FunctionTool, McpTools, ToolOrigin, Tools};
use wind_event::context::DispatchCx;

pub struct McpToolSource {
    registry: Arc<Registry>,
}

impl McpToolSource {
    pub fn new(registry: Arc<Registry>) -> Self {
        Self { registry }
    }
}

fn declared(info: &McpToolInfo) -> DeclaredTool {
    let origin = ToolOrigin::Mcp {
        server: info.server_name.clone(),
        tool: info.name.clone(),
    };
    let name = origin.exposed();
    DeclaredTool {
        declaration: Tools::Function(FunctionTool {
            name,
            description: info.description.clone(),
            parameters: Some(info.input_schema.clone()),
            strict: None,
        }),
        origin,
    }
}

fn internal(error: McpError) -> CoreError {
    CoreError::Internal(error.to_string())
}

#[async_trait::async_trait]
impl McpTools for McpToolSource {
    async fn declarations(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
    ) -> Result<Vec<DeclaredTool>, CoreError> {
        let tools = self
            .registry
            .list_tools(cx, topic_id)
            .await
            .map_err(internal)?;
        Ok(tools.iter().map(declared).collect())
    }

    async fn call(
        &self,
        cx: &DispatchCx,
        call_id: &str,
        server: &str,
        tool: &str,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let outcome = self
            .registry
            .call_tool(cx, call_id, server, tool, arguments)
            .await
            .map_err(internal)?;
        Ok(outcome.result)
    }
}
