use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// 一个 MCP 服务的连接参数
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpConfig {
    /// 子进程通过标准输入输出通信
    Stdio {
        /// 可执行程序，以 `./` 开头时相对版本目录解析
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
        /// 子进程的工作目录，必须为绝对路径
        cwd: PathBuf,
    },
    /// 通过 Streamable HTTP 连接远端服务
    StreamableHttp { url: String },
}

/// 一个具名的 MCP 服务声明
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct McpDefinition {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub config: McpConfig,
}
