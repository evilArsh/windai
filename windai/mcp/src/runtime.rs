use super::builtin::{NAMES, register_builtin, roots_for};
use super::identity::AssetRoot;
use super::params::{ServerParams, StdioParams, StreamableParams};
use super::registry::{McpError, Registry};
use std::collections::BTreeMap;
use std::fs as std_fs;
use std::path::PathBuf;
use std::sync::Arc;
use wind_core::storage::Storage;
use wind_event::context::DispatchCx;
use wind_event::dispatch::Dispatcher;
use wind_mod::dirs::AppDirs;
use wind_mod::installer::TopicServers;
use wind_mod::manifest::{Manifest, McpServer, parse};

/// mod 中 mcpServers 部分
pub struct ModServers {
    registry: Arc<Registry>,
    storage: Storage,
    dirs: AppDirs,
    bus: Arc<Dispatcher>,
}

impl ModServers {
    pub fn new(
        registry: Arc<Registry>,
        storage: Storage,
        dirs: AppDirs,
        bus: Arc<Dispatcher>,
    ) -> Self {
        Self {
            registry,
            storage,
            dirs,
            bus,
        }
    }

    pub async fn apply(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
        manifests: &[Manifest],
    ) -> Result<(), McpError> {
        let wanted = wanted_of(manifests, &self.dirs)?;

        // 失效的服务
        let gone: Vec<String> = self
            .registry
            .server_names(topic_id)
            .into_iter()
            .filter(|name| !NAMES.contains(&name.as_str()) && !wanted.contains_key(name))
            .collect();
        for name in gone {
            self.registry.detach(cx, topic_id, &name).await;
        }
        // FIXME: 串行执行
        for (name, (params, root)) in &wanted {
            self.registry
                .attach(cx, topic_id, name, params, root)
                .await?;
        }

        let mod_roots: Vec<PathBuf> = manifests
            .iter()
            .map(|manifest| self.dirs.mod_version(&manifest.name, manifest.version))
            .collect();
        let roots = roots_for(&self.dirs, topic_id, &mod_roots);
        register_builtin(&self.registry, cx, topic_id, &roots).await
    }

    async fn recompute(&self, cx: &DispatchCx, topic_id: i64) -> Result<(), String> {
        let outcome = async {
            let manifests = self.manifests_of(topic_id).await?;
            self.apply(cx, topic_id, &manifests).await
        }
        .await;
        outcome.map_err(|error| {
            log::error!("[mcp] cannot derive the servers of topic {topic_id}: {error}");
            error.to_string()
        })
    }

    /// 该话题启用的 mod 清单
    async fn manifests_of(&self, topic_id: i64) -> Result<Vec<Manifest>, McpError> {
        let names = self
            .storage
            .mod_enabled_in(topic_id)
            .await
            .map_err(|error| unreadable(&format!("topic {topic_id}"), error))?;
        let mut manifests = Vec::new();
        for name in names {
            manifests.push(self.manifest_of(&name).await?);
        }
        Ok(manifests)
    }

    async fn manifest_of(&self, name: &str) -> Result<Manifest, McpError> {
        let row = self
            .storage
            .mod_get_by_name(name)
            .await
            .map_err(|error| unreadable(name, error))?
            .ok_or_else(|| McpError::Servers {
                context: name.to_string(),
                reason: "it is enabled but has no row".to_string(),
            })?;
        let current = self
            .storage
            .mod_list_versions(row.id)
            .await
            .map_err(|error| unreadable(name, error))?
            .into_iter()
            .find(|version| version.version == row.current_version)
            .ok_or_else(|| McpError::Servers {
                context: name.to_string(),
                reason: format!("its current version {} is not stored", row.current_version),
            })?;
        parse(&current.manifest, self.bus.directory()).map_err(|error| McpError::Servers {
            context: name.to_string(),
            reason: format!("its manifest is invalid: {error}"),
        })
    }
}

#[async_trait::async_trait]
impl TopicServers for ModServers {
    async fn attach(&self, cx: &DispatchCx, topic_id: i64, _mod_name: &str) -> Result<(), String> {
        self.recompute(cx, topic_id).await
    }

    async fn detach(&self, cx: &DispatchCx, topic_id: i64, _mod_name: &str) -> Result<(), String> {
        self.recompute(cx, topic_id).await
    }

    async fn refresh(&self, cx: &DispatchCx, topic_id: i64) -> Result<(), String> {
        self.recompute(cx, topic_id).await
    }
}

fn unreadable(context: &str, error: impl std::fmt::Display) -> McpError {
    McpError::Servers {
        context: context.to_string(),
        reason: error.to_string(),
    }
}

/// 提取出有效的 mcp servers
fn wanted_of(
    manifests: &[Manifest],
    dirs: &AppDirs,
) -> Result<BTreeMap<String, (ServerParams, AssetRoot)>, McpError> {
    let mut wanted = BTreeMap::new();
    for manifest in manifests {
        /*
         * ~/.windai/
         *  ├── windai.db
         *  ├── skills/
         *  ├── topics/<topic_id>/
         *  └── mods/<name>/
         *      ├── data/
         *      └── <version>/               mod.json、scripts/、ui/、skills/、mcp/
         */
        let root = AssetRoot::new(dirs.mod_version(&manifest.name, manifest.version));
        for (name, decl) in &manifest.mcp_servers {
            let params = params_of(manifest, name, decl, dirs)?;
            wanted.insert(name.clone(), (params, root.clone()));
        }
    }
    Ok(wanted)
}

/// 提取出 mcp server 运行参数
fn params_of(
    manifest: &Manifest,
    name: &str,
    decl: &McpServer,
    dirs: &AppDirs,
) -> Result<ServerParams, McpError> {
    match (&decl.command, &decl.url) {
        (Some(command), None) => {
            let root = AssetRoot::new(dirs.mod_version(&manifest.name, manifest.version));
            // TODO: 本地MCP只能是可执行文件；目标是`command: "./index.js"` 执行
            if command.starts_with("./") && root.resolve(command).is_none() {
                return Err(McpError::Servers {
                    context: name.to_string(),
                    reason: format!("the command {command} does not resolve inside the asset root"),
                });
            }
            Ok(ServerParams::Stdio(StdioParams {
                name: name.to_string(),
                description: decl.description.clone(),
                command: command.clone(),
                args: decl.args.clone(),
                env: Some(decl.env.clone().into_iter().collect()),
                cwd: Some(workdir_of(dirs, &manifest.name)?),
            }))
        }
        (None, Some(url)) => Ok(ServerParams::Streamable(StreamableParams {
            name: name.to_string(),
            description: decl.description.clone().unwrap_or_default(),
            url: url.clone(),
        })),
        _ => Ok(ServerParams::Streamable(StreamableParams {
            name: name.to_string(),
            description: decl.description.clone().unwrap_or_default(),
            url: String::new(),
        })),
    }
}

/// mod 包根路径
fn workdir_of(dirs: &AppDirs, name: &str) -> Result<PathBuf, McpError> {
    let path = dirs.mod_data(name);
    std_fs::create_dir_all(&path).map_err(|error| McpError::Directory {
        path: path.display().to_string(),
        reason: error.to_string(),
    })?;
    Ok(path)
}
