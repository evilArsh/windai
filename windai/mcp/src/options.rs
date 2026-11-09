use std::sync::Arc;
use std::time::Duration;

/// 子进程 stderr 的逐行回调
pub type LogSink = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct McpOptions {
    /// 建立连接并完成握手的上限
    pub connect_timeout: Duration,
    /// 读取工具清单的上限
    pub list_timeout: Duration,
    /// 单次工具调用的上限
    pub call_timeout: Duration,
    /// 关闭连接时等待在飞调用与清理的上限
    pub shutdown_grace: Duration,
    /// stderr 每行的接收者，None 时仍然排空
    pub log: Option<LogSink>,
}

impl Default for McpOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            list_timeout: Duration::from_secs(10),
            call_timeout: Duration::from_secs(60),
            shutdown_grace: Duration::from_secs(3),
            log: None,
        }
    }
}
