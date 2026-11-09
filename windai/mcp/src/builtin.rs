use std::sync::Arc;

use rmcp::ServiceExt;
use tokio::task::JoinHandle;

use crate::connector::Connection;
use crate::error::{McpError, Result};
use crate::options::McpOptions;
use crate::registry::BuiltinKind;
use crate::roots::{Roots, ScopeId};

pub mod fs;
mod fs_ops;
mod sandbox;

/// duplex 通道的缓冲字节数
const DUPLEX_BYTES: usize = 64 * 1024;

pub async fn connect_builtin(
    kind: BuiltinKind,
    scope: ScopeId,
    roots: Roots,
    options: &McpOptions,
) -> Result<(Arc<Connection>, JoinHandle<()>)> {
    let server = match kind {
        BuiltinKind::Fs => fs::FsServer::new(roots, scope),
        BuiltinKind::Skills => {
            return Err(McpError::InvalidSpec(
                "skills builtin is not connected yet".to_owned(),
            ));
        }
    };
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
