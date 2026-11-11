use crate::{FunctionCallOutput, Tools};
use serde_json::Value;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[async_trait::async_trait]
pub trait ModService: Send + Sync {
    async fn prompt_fragments(&self, topic_id: i64) -> Result<Vec<String>, String>;
    async fn version_of(&self, mod_name: &str) -> Result<i64, String>;
    // TODO: skills相关?
    // TODO: mcp资源相关?
}

pub struct ToolCallParam {
    pub call_id: String,
    pub server: String,
    pub name: String,
    pub arguments: Value,
}

#[async_trait::async_trait]
pub trait McpService: Send + Sync {
    async fn list_tools(&self, topic_id: i64) -> Result<Vec<Tools>, String>;

    async fn tool_call(
        &self,
        deadline: Instant,
        token: CancellationToken,
        param: ToolCallParam,
    ) -> Result<Value, String>;

    async fn tool_calls(
        &self,
        deadline: Instant,
        token: CancellationToken,
        params: Vec<ToolCallParam>,
    ) -> Result<Vec<FunctionCallOutput>, String>;
}
