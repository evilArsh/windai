use serde::Deserialize;
use std::path::{Component, Path, PathBuf};

pub const MCP_DIR: &str = "mcp";
pub const SKILLS_DIR: &str = "skills";
pub const MANIFEST_FILE: &str = "mod.json";
pub const SCRIPT_DIR: &str = "scripts";
pub const SCRIPT_ENTRY: &str = "scripts/index.js";
pub const SCRIPT_EXT: &str = "js";
pub const UI_ENTRY: &str = "ui/index.js";

#[derive(Debug, thiserror::Error)]
pub enum PackageError {
    #[error("cannot read the manifest at {path}: {source}")]
    ManifestIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("the manifest at {path} does not declare a usable identity: {reason}")]
    Identity { path: PathBuf, reason: String },
    #[error("asset {name} is not present under {root}")]
    MissingAsset { name: String, root: PathBuf },
    #[error("cannot walk {path}: {source}")]
    AssetIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Deserialize)]
struct Identity {
    name: String,
    version: i64,
}

/// mod 包元数据
#[derive(Debug, Clone)]
pub struct Package {
    pub name: String,
    pub version: i64,
    pub root: PathBuf,
    pub manifest_raw: String,
}

impl Package {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, PackageError> {
        let root = root.into();
        let path = root.join(MANIFEST_FILE);
        let manifest_raw =
            std::fs::read_to_string(&path).map_err(|source| PackageError::ManifestIo {
                path: path.clone(),
                source,
            })?;
        let identity: Identity =
            serde_json::from_str(&manifest_raw).map_err(|error| PackageError::Identity {
                path,
                reason: error.to_string(),
            })?;
        Ok(Self {
            name: identity.name,
            version: identity.version,
            root,
            manifest_raw,
        })
    }

    pub fn manifest_raw(&self) -> &str {
        &self.manifest_raw
    }

    pub fn script_entry(&self) -> PathBuf {
        self.root.join(SCRIPT_ENTRY)
    }

    pub fn ui_entry(&self) -> PathBuf {
        self.root.join(UI_ENTRY)
    }

    pub fn skills_dir(&self) -> PathBuf {
        self.root.join(SKILLS_DIR)
    }

    pub fn mcp_dir(&self) -> PathBuf {
        self.root.join(MCP_DIR)
    }

    pub fn resolve_asset(&self, command: &str) -> Option<PathBuf> {
        resolve_under(&self.root, command.strip_prefix("./")?)
    }

    pub fn asset_fingerprint(&self, name: &str) -> Result<String, PackageError> {
        asset_fingerprint(&self.root, name)
    }
}

pub fn asset_fingerprint(root: &Path, name: &str) -> Result<String, PackageError> {
    let base = root.join(MCP_DIR);
    let missing = || PackageError::MissingAsset {
        name: name.to_string(),
        root: root.to_path_buf(),
    };
    let asset = resolve_under(&base, name)
        .filter(|path| *path != base)
        .ok_or_else(missing)?;
    if !is_real_dir(&asset) {
        return Err(missing());
    }
    let mut entries = Vec::new();
    collect(&asset, &asset, &mut entries)?;
    entries.sort();
    let mut hash = Fnv::new();
    for (relative, bytes) in entries {
        hash.write(relative.as_bytes());
        hash.write(&[0]);
        hash.write(&bytes);
        hash.write(&[0xff]);
    }
    Ok(format!("{:016x}", hash.finish()))
}

fn resolve_under(base: &Path, relative: &str) -> Option<PathBuf> {
    let mut resolved = base.to_path_buf();
    let mut depth = 0usize;
    for component in Path::new(relative).components() {
        match component {
            Component::Normal(part) => {
                resolved.push(part);
                depth += 1;
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if depth == 0 {
                    return None;
                }
                resolved.pop();
                depth -= 1;
            }
            Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(resolved)
}

fn is_real_dir(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .map(|meta| meta.is_dir())
        .unwrap_or(false)
}

fn collect(base: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) -> Result<(), PackageError> {
    let read = std::fs::read_dir(dir).map_err(|source| PackageError::AssetIo {
        path: dir.to_path_buf(),
        source,
    })?;
    for entry in read {
        let entry = entry.map_err(|source| PackageError::AssetIo {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|source| PackageError::AssetIo {
            path: path.clone(),
            source,
        })?;
        if file_type.is_dir() {
            collect(base, &path, out)?;
        } else if file_type.is_file() {
            let bytes = std::fs::read(&path).map_err(|source| PackageError::AssetIo {
                path: path.clone(),
                source,
            })?;
            let relative = path
                .strip_prefix(base)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            out.push((relative, bytes));
        }
    }
    Ok(())
}

/// 64 位 FNV-1a 哈希器
struct Fnv {
    state: u64,
}

impl Fnv {
    fn new() -> Self {
        Self {
            state: 0xcbf2_9ce4_8422_2325,
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finish(&self) -> u64 {
        self.state
    }
}
