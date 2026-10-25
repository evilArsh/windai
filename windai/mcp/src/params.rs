use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ServerParams {
    Stdio(StdioParams),
    Streamable(StreamableParams),
}

impl ServerParams {
    /// 名字只是引用键，不参与身份判定
    pub fn name(&self) -> &str {
        match self {
            ServerParams::Stdio(params) => &params.name,
            ServerParams::Streamable(params) => &params.name,
        }
    }

    pub fn description(&self) -> Option<&str> {
        match self {
            ServerParams::Stdio(params) => params.description.as_deref(),
            ServerParams::Streamable(params) => Some(params.description.as_str()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StdioParams {
    pub name: String,
    pub description: Option<String>,
    /// 以 `./` 开头时按资产根目录解析
    pub command: String,
    pub args: Vec<String>,
    pub env: Option<HashMap<String, String>>,
    pub cwd: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamableParams {
    pub name: String,
    pub description: String,
    pub url: String,
}
