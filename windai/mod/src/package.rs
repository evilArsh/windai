use crate::error::{ModError, Result};
use serde::Deserialize;
use std::path::PathBuf;

pub const MCP_DIR: &str = "mcp";
pub const SKILLS_DIR: &str = "skills";
pub const MANIFEST_FILE: &str = "mod.json";

pub const SCRIPT_DIR: &str = "scripts";
pub const SCRIPT_ENTRY: &str = "scripts/index.js";
pub const SCRIPT_EXT: &str = "js";

pub const PROMPTS_DIR: &str = "prompts";

pub const UI_ENTRY: &str = "ui/index.js";

#[derive(Deserialize)]
struct Identity {
    name: String,
    description: String,
    version: i64,
}

/// mod 包元数据
#[derive(Debug, Clone)]
pub struct Package {
    pub name: String,
    pub description: String,
    pub version: i64,
    pub root: PathBuf,
    pub manifest_raw: String,
}
impl Package {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        let manifest_path = root.join(MANIFEST_FILE);
        let manifest_raw =
            std::fs::read_to_string(&manifest_path).map_err(|source| ModError::ManifestIo {
                path: manifest_path.clone(),
                source,
            })?;
        let identity: Identity =
            serde_json::from_str(&manifest_raw).map_err(|error| ModError::Identity {
                path: manifest_path,
                reason: error.to_string(),
            })?;
        Ok(Self {
            name: identity.name,
            description: identity.description,
            version: identity.version,
            root,
            manifest_raw,
        })
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
}
