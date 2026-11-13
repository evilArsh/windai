//! 内建 MCP 服务

pub mod fs;
pub mod skills;
pub mod workspace;

use self::workspace::WorkspaceInfo;
use super::connector::Connection;
use super::error::Result;
use crate::McpOptions;
use crate::RegistryHandle;
use rmcp::ServerHandler;
use rmcp::ServiceExt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;

/// 进程内通信缓冲区字节数
const DUPLEX_BYTES: usize = 64 * 1024;

/// 内建服务的挂载范围
#[derive(Clone, Copy)]
pub enum BuiltinScope {
    /// 全部话题共用一份
    Global,
    /// 每个话题一份
    Topic(i64),
}

impl BuiltinScope {
    fn service_name(self, slot: &str) -> String {
        match self {
            BuiltinScope::Global => format!("wd_{slot}"),
            BuiltinScope::Topic(topic_id) => format!("wd_{slot}_{topic_id}"),
        }
    }
}

const SLOTS: [&str; 3] = [fs::SLOT, skills::SLOT, workspace::SLOT];

pub async fn init(
    registry: &RegistryHandle,
    scope: BuiltinScope,
    info: Arc<RwLock<WorkspaceInfo>>,
) {
    let name = scope.service_name(fs::SLOT);
    start(
        registry,
        name.clone(),
        fs::FsServer::new(name, info.clone()),
    )
    .await;
    let name = scope.service_name(skills::SLOT);
    start(
        registry,
        name.clone(),
        skills::SkillsServer::new(name, info.clone()),
    )
    .await;
    let name = scope.service_name(workspace::SLOT);
    start(
        registry,
        name.clone(),
        workspace::WorkspaceServer::new(name, info),
    )
    .await;
}

pub(crate) fn canonical_path(path: PathBuf) -> PathBuf {
    std::fs::canonicalize(&path).unwrap_or(path)
}

/// 该范围内的全部内建 ref 名
pub fn refs(scope: BuiltinScope) -> Vec<String> {
    SLOTS.iter().map(|slot| scope.service_name(slot)).collect()
}

pub fn release(registry: &RegistryHandle, scope: BuiltinScope) {
    for slot in SLOTS {
        registry.release(scope.service_name(slot));
    }
}

async fn start<S: ServerHandler>(registry: &RegistryHandle, name: String, server: S) {
    if let Err(error) = registry.acquire_builtin(name.clone(), server).await {
        log::warn!("the builtin mcp service {name} failed to start: {error}");
    }
}

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
