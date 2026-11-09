use std::sync::Arc;

use rmcp::{ServerHandler, ServiceExt};
use tokio::task::JoinHandle;

use crate::connector::Connection;
use crate::error::Result;
use crate::options::McpOptions;
use crate::registry::BuiltinKind;
use crate::roots::{Roots, ScopeId};

pub mod fs;
mod fs_ops;
mod sandbox;
pub mod skills;
mod skills_scan;

/// duplex 通道的缓冲字节数
const DUPLEX_BYTES: usize = 64 * 1024;

pub async fn connect_builtin(
    kind: BuiltinKind,
    scope: ScopeId,
    roots: Roots,
    options: &McpOptions,
) -> Result<(Arc<Connection>, JoinHandle<()>)> {
    match kind {
        BuiltinKind::Fs => serve_builtin(fs::FsServer::new(roots, scope), options).await,
        BuiltinKind::Skills => {
            serve_builtin(skills::SkillsServer::new(roots, scope), options).await
        }
    }
}

async fn serve_builtin<S>(
    server: S,
    options: &McpOptions,
) -> Result<(Arc<Connection>, JoinHandle<()>)>
where
    S: ServerHandler,
{
    let (client, server_io) = tokio::io::duplex(DUPLEX_BYTES);
    let (client_read, client_write) = tokio::io::split(client);
    let task = tokio::spawn(async move {
        match server.serve(server_io).await {
            Ok(service) => {
                let _ = service.waiting().await;
            }
            Err(error) => log::warn!("[mcp] the builtin server failed: {error}"),
        }
    });
    match Connection::attach(client_read, client_write, options).await {
        Ok(connection) => Ok((connection, task)),
        Err(error) => {
            task.abort();
            Err(error)
        }
    }
}
