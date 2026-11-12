//! 内建 MCP 服务

pub mod fs;
pub mod skills;
pub mod workspace;

use self::skills::SkillRoots;
use self::workspace::{ModDir, WorkspaceInfo};
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
    /// 话题工作区
    pub workspace: PathBuf,
    pub public_skills: PathBuf,
    /// 仅含本话题启用的 mod
    pub mods: Vec<ModDir>,
    pub skill_roots: SkillRoots,
    pub workspace_info: WorkspaceInfo,
}

impl BuiltinResources {
    pub fn roots(&self) -> Vec<PathBuf> {
        let mut roots = Vec::with_capacity(2 + self.mods.len());
        roots.push(self.workspace.clone());
        roots.push(self.public_skills.clone());
        roots.extend(self.mods.iter().map(|module| module.root.clone()));
        roots
    }
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

/// 各项资源相对上次同步的变化
#[derive(Default)]
struct Changed {
    fs: bool,
    /// 技能根原地替换，只有首次同步需要建服务
    skills: bool,
    workspace: bool,
}

impl BuiltinResources {
    fn changed_from(&self, previous: Option<&BuiltinResources>) -> Changed {
        Changed {
            fs: previous.is_none_or(|previous| previous.roots() != self.roots()),
            skills: previous.is_none(),
            workspace: previous.is_none(),
        }
    }
}

const SLOTS: [&str; 3] = [fs::SLOT, skills::SLOT, workspace::SLOT];

/// 只重建依赖资源发生变化的内建服务
pub async fn sync(
    registry: &RegistryHandle,
    scope: BuiltinScope,
    previous: Option<&BuiltinResources>,
    resources: &BuiltinResources,
) {
    let changed = resources.changed_from(previous);
    if changed.fs {
        fs::sync(registry, scope, resources).await;
    }
    if changed.skills {
        skills::sync(registry, scope, resources.skill_roots.clone()).await;
    }
    if changed.workspace {
        workspace::sync(registry, scope, resources.workspace_info.clone()).await;
    }
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
    use crate::builtin::skills::SkillRoots;
    use crate::builtin::workspace::{ModDir, WorkspaceInfo, WorkspaceSnapshot};
    use std::path::PathBuf;

    fn resources(workspace: &str, mods: &[&str]) -> BuiltinResources {
        let workspace = PathBuf::from(workspace);
        let public_skills = PathBuf::from("public");
        let mods: Vec<ModDir> = mods
            .iter()
            .map(|name| ModDir {
                name: (*name).to_owned(),
                version: 1,
                root: PathBuf::from(name),
                skills: None,
                ui: None,
            })
            .collect();
        let snapshot = WorkspaceSnapshot::new(
            workspace.clone(),
            public_skills.clone(),
            Vec::new(),
            mods.clone(),
        );
        BuiltinResources {
            workspace,
            public_skills,
            mods,
            skill_roots: SkillRoots::new(Vec::new()),
            workspace_info: WorkspaceInfo::new(snapshot),
        }
    }

    #[test]
    fn a_scope_prefixes_the_service_name_with_its_topic() {
        assert_eq!(
            refs(BuiltinScope::Global),
            ["wd_fs", "wd_skills", "wd_workspace"]
        );
        assert_eq!(
            refs(BuiltinScope::Topic(7)),
            ["wd_fs_7", "wd_skills_7", "wd_workspace_7"]
        );
    }

    #[test]
    fn a_moved_workspace_rebuilds_the_file_service() {
        let previous = resources("a", &["m"]);
        let changed = resources("b", &["m"]).changed_from(Some(&previous));
        assert!(changed.fs);
        assert!(!changed.skills);
        assert!(!changed.workspace);
    }

    #[test]
    fn an_enabled_mod_rebuilds_the_file_service() {
        let previous = resources("a", &["m"]);
        let changed = resources("a", &["m", "n"]).changed_from(Some(&previous));
        assert!(changed.fs);
    }

    #[test]
    fn a_repeated_sync_keeps_the_running_services() {
        let previous = resources("a", &["m"]);
        let changed = resources("a", &["m"]).changed_from(Some(&previous));
        assert!(!changed.fs);
        assert!(!changed.skills);
        assert!(!changed.workspace);
    }

    #[test]
    fn the_first_sync_starts_every_service() {
        let changed = resources("a", &["m"]).changed_from(None);
        assert!(changed.fs);
        assert!(changed.skills);
        assert!(changed.workspace);
    }
}
