//! 内建 MCP 服务

pub mod fs;
pub mod skills;

use super::connector::Connection;
use super::error::Result;
use super::options::McpOptions;
use rmcp::{ServerHandler, ServiceExt};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// 进程内通信缓冲区字节数
const DUPLEX_BYTES: usize = 64 * 1024;

/// 在进程内启动服务端并完成客户端握手
pub(crate) async fn connect<S>(
    server: S,
    options: &McpOptions,
) -> Result<(Arc<Connection>, JoinHandle<()>)>
where
    S: ServerHandler,
{
    let (client, server_io) = tokio::io::duplex(DUPLEX_BYTES);
    let (reader, writer) = tokio::io::split(client);
    let task = tokio::spawn(async move {
        match server.serve(server_io).await {
            Ok(service) => {
                let _ = service.waiting().await;
            }
            Err(error) => log::warn!("the builtin mcp server failed: {error}"),
        }
    });
    match Connection::attach(reader, writer, options).await {
        Ok(connection) => Ok((connection, task)),
        Err(error) => {
            task.abort();
            Err(error)
        }
    }
}
