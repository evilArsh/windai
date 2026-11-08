//! 内建 MCP 服务

use rmcp::ServerHandler;
use rmcp::ServiceExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinHandle;

use crate::connector::{Connection, Connector};
use crate::error::{McpError, Result};
use crate::registry::{BuiltinKind, BuiltinRoots};

mod fs;
mod paths;
mod skills;

pub use fs::{
    DirEntry, EntryKind, ExecRequest, ExecResult, FsServer, ListDirRequest, ListDirResult,
    ReadFileRequest, ReadFileResult, WriteFileRequest, WriteFileResult,
};
pub use skills::{SkillError, SkillsListRequest, SkillsListResult, SkillsMeta, SkillsServer};

const DUPLEX_BYTES: usize = 128 * 1024;

/// 建立一个内建服务的连接
pub(crate) async fn connect(
    kind: BuiltinKind,
    roots: &BuiltinRoots,
    connector: &Connector,
) -> Result<(Connection, Option<JoinHandle<()>>)> {
    let (client, server) = tokio::io::duplex(DUPLEX_BYTES);
    let (client_read, client_write) = tokio::io::split(client);
    let (server_read, server_write) = tokio::io::split(server);
    let task = match kind {
        BuiltinKind::Fs => {
            let workspace = roots
                .workspace
                .clone()
                .ok_or_else(|| McpError::Builtin("the workspace root is missing".to_string()))?;
            spawn_server(FsServer::new(vec![workspace]), server_read, server_write)
        }
        BuiltinKind::Skills => spawn_server(
            SkillsServer::new(roots.skills.clone()),
            server_read,
            server_write,
        ),
    };
    match connector.connect_duplex(client_read, client_write).await {
        Ok(connection) => Ok((connection, Some(task))),
        Err(error) => {
            task.abort();
            Err(error)
        }
    }
}

fn spawn_server<S, R, W>(handler: S, reader: R, writer: W) -> JoinHandle<()>
where
    S: ServerHandler,
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        match handler.serve((reader, writer)).await {
            Ok(service) => {
                let _ = service.waiting().await;
            }
            Err(error) => log::warn!("[mcp] builtin server failed: {error}"),
        }
    })
}
