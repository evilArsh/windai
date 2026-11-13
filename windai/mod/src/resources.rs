use crate::error::{ModError, Result};
use crate::manifest::Manifest;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use wind_storage::Storage;
use wind_wire::{
    AppDirs, MOD_PROMPT_EXTS, MOD_PROMPTS_DIR, MOD_SKILLS_DIR, MOD_UI_ENTRY, McpConfig,
    McpDefinition,
};

/// 一个已发布版本对外提供的资源
#[derive(Debug, Clone)]
pub struct ModResources {
    pub name: String,
    pub version: u32,
    pub root: PathBuf,
    pub manifest: Manifest,
    pub prompts: Vec<String>,
    /// skills 根路径
    pub skills: Option<PathBuf>,
    pub ui: Option<PathBuf>,
    pub mcp: BTreeMap<String, McpDefinition>,
    pub requires_scripts: bool,
}

/// 按话题读取已发布版本的资源
#[derive(Clone)]
pub struct Resources {
    storage: Storage,
    dirs: AppDirs,
}

impl Resources {
    pub fn new(storage: Storage, dirs: AppDirs) -> Self {
        Self { storage, dirs }
    }

    pub fn public_skills(&self) -> PathBuf {
        self.dirs.public_skills()
    }

    pub fn mods_root(&self) -> PathBuf {
        self.dirs.mods_root()
    }

    /// 话题启用的 mod 资源，按 mod 名排序
    pub async fn topic(&self, topic_id: i64) -> Result<Vec<ModResources>> {
        let rows = self
            .storage
            .with_read_tx(async move |tx| tx.enabled_mods(topic_id).await)
            .await?;
        let mut resources = Vec::with_capacity(rows.len());
        for row in rows {
            let loaded = self.load(row.name, row.version, row.manifest)?;
            resources.push(loaded);
        }
        resources.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(resources)
    }

    /// 读取一个已发布的版本
    pub async fn version(&self, name: &str, version: u32) -> Result<ModResources> {
        let module =
            self.storage
                .mod_by_name(name)
                .await?
                .ok_or_else(|| ModError::NotInstalled {
                    name: name.to_string(),
                })?;
        if module.version != version {
            return Err(ModError::VersionNotPublished {
                name: name.to_string(),
                version,
            });
        }
        self.load(module.name, module.version, module.manifest)
    }

    fn load(&self, name: String, version: u32, manifest_raw: String) -> Result<ModResources> {
        let root = self.dirs.mod_version(&name, version);
        let canonical_root = fs::canonicalize(&root)
            .map_err(|error| resource_error(&name, version, &root, error))?;
        let manifest = Manifest::parse(&manifest_raw)
            .map_err(|error| resource_error(&name, version, &canonical_root, error))?;
        let prompts = read_prompts(&canonical_root, &name, version)?;
        let skills = optional_dir(&canonical_root, MOD_SKILLS_DIR, &name, version)?;
        let ui = optional_file(&canonical_root, MOD_UI_ENTRY, &name, version)?;
        let mcp = mcp_definitions(&manifest, &canonical_root, &name, version)?;
        let requires_scripts = manifest.requires_scripts();
        Ok(ModResources {
            name,
            version,
            root: canonical_root,
            manifest,
            prompts,
            skills,
            ui,
            mcp,
            requires_scripts,
        })
    }
}

fn read_prompts(root: &Path, name: &str, version: u32) -> Result<Vec<String>> {
    let dir = root.join(MOD_PROMPTS_DIR);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(resource_error(name, version, &dir, error)),
    };
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| resource_error(name, version, &dir, error))?;
        let path = entry.path();
        let metadata =
            fs::metadata(&path).map_err(|error| resource_error(name, version, &path, error))?;
        if !metadata.is_file() {
            continue;
        }
        let extension = path.extension().and_then(OsStr::to_str);
        let is_prompt = extension.is_some_and(|extension| {
            MOD_PROMPT_EXTS
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(extension))
        });
        if !is_prompt {
            continue;
        }
        let resolved = ensure_within(root, &path, name, version)?;
        files.push((entry.file_name(), resolved));
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
        .into_iter()
        .map(|(_, path)| {
            fs::read_to_string(&path).map_err(|error| resource_error(name, version, &path, error))
        })
        .collect()
}

fn optional_dir(root: &Path, relative: &str, name: &str, version: u32) -> Result<Option<PathBuf>> {
    let path = root.join(relative);
    match fs::metadata(&path) {
        Ok(metadata) if metadata.is_dir() => Ok(Some(ensure_within(root, &path, name, version)?)),
        Ok(_) => Err(resource_error(
            name,
            version,
            &path,
            "the path must be a directory",
        )),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(resource_error(name, version, &path, error)),
    }
}

fn optional_file(root: &Path, relative: &str, name: &str, version: u32) -> Result<Option<PathBuf>> {
    let path = root.join(relative);
    match fs::metadata(&path) {
        Ok(metadata) if metadata.is_file() => Ok(Some(ensure_within(root, &path, name, version)?)),
        Ok(_) => Err(resource_error(
            name,
            version,
            &path,
            "the path must be a file",
        )),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(resource_error(name, version, &path, error)),
    }
}

fn ensure_within(root: &Path, path: &Path, name: &str, version: u32) -> Result<PathBuf> {
    let resolved =
        fs::canonicalize(path).map_err(|error| resource_error(name, version, path, error))?;
    if !resolved.starts_with(root) {
        return Err(resource_error(
            name,
            version,
            path,
            format!(
                "the resolved path {} leaves the version root",
                resolved.display()
            ),
        ));
    }
    Ok(resolved)
}

fn mcp_definitions(
    manifest: &Manifest,
    root: &Path,
    name: &str,
    version: u32,
) -> Result<BTreeMap<String, McpDefinition>> {
    let mut definitions = BTreeMap::new();
    for (local, server) in &manifest.mcp_servers {
        let config = match (&server.command, &server.url) {
            (Some(command), None) => McpConfig::Stdio {
                command: resolve_command(command, root, name, version)?,
                args: server.args.clone(),
                env: server.env.clone(),
                cwd: root.to_path_buf(),
            },
            (None, Some(url)) => McpConfig::StreamableHttp { url: url.clone() },
            _ => {
                return Err(resource_error(
                    name,
                    version,
                    root,
                    format!("mcp server {local} must declare exactly one of command or url"),
                ));
            }
        };
        definitions.insert(
            local.clone(),
            McpDefinition {
                name: local.clone(),
                description: server.description.clone(),
                config,
            },
        );
    }
    Ok(definitions)
}

fn resolve_command(command: &str, root: &Path, name: &str, version: u32) -> Result<String> {
    let Some(relative) = command.strip_prefix("./") else {
        return Ok(command.to_string());
    };
    let path = root.join(relative);
    let resolved = ensure_within(root, &path, name, version)?;
    if !resolved.is_file() {
        return Err(resource_error(
            name,
            version,
            &path,
            "the command must be a file",
        ));
    }
    resolved
        .to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| resource_error(name, version, &path, "the command path is not valid UTF-8"))
}

fn resource_error(
    name: &str,
    version: u32,
    path: &Path,
    reason: impl std::fmt::Display,
) -> ModError {
    ModError::Resource {
        name: name.to_string(),
        version,
        path: path.to_path_buf(),
        reason: reason.to_string(),
    }
}
