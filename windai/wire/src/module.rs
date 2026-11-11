use crate::{FunctionCallOutput, Tools};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledScripts {
    pub modules: HashMap<String, Vec<u8>>,
    pub fingerprint: String,
}

#[async_trait::async_trait]
pub trait ScriptService: Send + Sync {
    fn compile(
        &self,
        root: &Path,
        handlers: &HashSet<String>,
    ) -> Result<Option<CompiledScripts>, String>;

    /// TODO: 参数简化?
    async fn call(
        &self,
        mod_name: &str,
        version: i64,
        request: &str,
        input: Value,
    ) -> Result<Value, String>;
}

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
