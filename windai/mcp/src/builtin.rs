//! 内建 MCP 服务

pub mod fs;
pub mod skills;

use self::skills::SkillRoot;
use super::connector::Connection;
use super::error::Result;
use crate::McpOptions;
use crate::RegistryHandle;
use rmcp::ServerHandler;
use rmcp::ServiceExt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::JoinHandle;

/// 进程内通信缓冲区字节数
const DUPLEX_BYTES: usize = 64 * 1024;

/// 内建服务用到的公共资源
#[derive(Clone)]
pub struct BuiltinResources {
    pub workspace: PathBuf,
    pub skill_roots: Vec<SkillRoot>,
}

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

/// 各项资源相对上次同步是否变化
#[derive(Default)]
struct Changed {
    workspace: bool,
    skill_roots: bool,
}

impl BuiltinResources {
    fn changed_from(&self, previous: Option<&BuiltinResources>) -> Changed {
        Changed {
            workspace: previous.is_none_or(|previous| previous.workspace != self.workspace),
            skill_roots: previous.is_none_or(|previous| previous.skill_roots != self.skill_roots),
        }
    }
}

const SLOTS: [&str; 2] = [fs::SLOT, skills::SLOT];

/// 只重建依赖资源发生变化的内建服务
pub async fn sync(
    registry: &RegistryHandle,
    scope: BuiltinScope,
    previous: Option<&BuiltinResources>,
    resources: &BuiltinResources,
) {
    let changed = resources.changed_from(previous);
    if changed.workspace {
        fs::sync(registry, scope, resources).await;
    }
    if changed.skill_roots {
        skills::sync(registry, scope, resources).await;
    }
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

/// 停掉同名实例后按需启动新实例
async fn replace<S: ServerHandler>(registry: &RegistryHandle, name: String, server: Option<S>) {
    registry.release(name.clone());
    let Some(server) = server else {
        return;
    };
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

#[cfg(test)]
mod tests {
    use super::{BuiltinResources, BuiltinScope, refs};
    use crate::builtin::skills::SkillRoot;
    use std::path::PathBuf;

    fn resources(workspace: &str, roots: &[&str]) -> BuiltinResources {
        BuiltinResources {
            workspace: PathBuf::from(workspace),
            skill_roots: roots
                .iter()
                .map(|root| SkillRoot {
                    source: (*root).to_owned(),
                    path: PathBuf::from(root),
                })
                .collect(),
        }
    }

    #[test]
    fn a_scope_prefixes_the_service_name_with_its_topic() {
        assert_eq!(refs(BuiltinScope::Global), ["wd_fs", "wd_skills"]);
        assert_eq!(refs(BuiltinScope::Topic(7)), ["wd_fs_7", "wd_skills_7"]);
    }

    #[test]
    fn a_changed_resource_is_flagged_alone() {
        let previous = resources("a", &["s"]);
        let changed = resources("b", &["s"]).changed_from(Some(&previous));
        assert!(changed.workspace);
        assert!(!changed.skill_roots);

        let changed = resources("a", &["t"]).changed_from(Some(&previous));
        assert!(!changed.workspace);
        assert!(changed.skill_roots);
    }

    #[test]
    fn the_first_sync_flags_every_resource() {
        let changed = resources("a", &["s"]).changed_from(None);
        assert!(changed.workspace);
        assert!(changed.skill_roots);
    }
}
