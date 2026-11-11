use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::error::ScriptsError;

/// 脚本引擎的配置
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// 解释器线程数
    pub threads: usize,
    /// 单个运行时的内存上限，字节
    pub memory_limit: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            threads: 4,
            memory_limit: 64 * 1024 * 1024,
        }
    }
}

/// 一次调用的作用域
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptScope {
    /// 不绑定话题
    Global,
    /// 绑定话题实例
    Topic { topic_id: i64, instance_id: i64 },
}

/// 一次 handler 调用的输入
pub struct ScriptCall {
    /// mod 名
    pub mod_name: String,
    /// mod 版本
    pub mod_version: u32,
    /// 调用作用域
    pub scope: ScriptScope,
    /// mod 包根目录
    pub root: PathBuf,
    /// 要调用的导出函数名
    pub handler: String,
    /// 调用截止时间
    pub deadline: std::time::Instant,
    /// 取消信号
    pub cancel: CancellationToken,
    /// 传给 handler 的输入
    pub input: Value,
    /// 脚本访问宿主能力的入口
    pub host: Arc<dyn ScriptHost>,
}

/// 脚本访问宿主能力的入口
pub trait ScriptHost: Send + Sync {
    fn emit(&self, channel: &str, payload: Value) -> Result<(), ScriptsError>;

    fn notify(&self, level: &str, text: &str) -> Result<(), ScriptsError>;
}

/// 脚本 mod 的编译与调用入口
#[async_trait]
pub trait ScriptService: Send + Sync {
    /// 只校验入口语法，不解析 import
    fn compile(&self, root: &Path, handlers: &HashSet<String>) -> Result<(), ScriptsError>;

    /// 调用一个导出函数，`undefined` 与 `null` 都返回 `None`
    async fn call(&self, call: ScriptCall) -> Result<Option<Value>, ScriptsError>;
}
