use crate::error::{ModError, Result};
use crate::manifest::Manifest;
use std::path::PathBuf;
use wind_wire::{MOD_MANIFEST, MOD_MCP_DIR, MOD_PROMPTS_DIR, MOD_SKILLS_DIR, MOD_UI_ENTRY};

/// 一个位于版本目录中的 mod 包
#[derive(Debug, Clone)]
pub struct Package {
    /// 版本目录
    pub root: PathBuf,
    /// 清单原文
    pub manifest_raw: String,
    /// 解析后的清单
    pub manifest: Manifest,
}

impl Package {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        let manifest_path = root.join(MOD_MANIFEST);
        let manifest_raw =
            std::fs::read_to_string(&manifest_path).map_err(|source| ModError::ManifestIo {
                path: manifest_path.clone(),
                source,
            })?;
        let manifest = Manifest::parse(&manifest_raw)?;
        let expected = format!("{}/{}", manifest.name, manifest.version);
        let actual = root
            .parent()
            .and_then(|parent| parent.file_name())
            .zip(root.file_name())
            .and_then(|(name, version)| Some(format!("{}/{}", name.to_str()?, version.to_str()?)))
            .unwrap_or_default();
        if actual != expected {
            return Err(ModError::Mismatch {
                path: root.display().to_string(),
                reason: format!("it belongs at {expected}"),
            });
        }
        Ok(Self {
            root,
            manifest_raw,
            manifest,
        })
    }

    pub fn name(&self) -> &str {
        &self.manifest.name
    }

    pub fn version(&self) -> u32 {
        self.manifest.version
    }

    pub fn ui_entry(&self) -> PathBuf {
        self.root.join(MOD_UI_ENTRY)
    }

    pub fn skills_dir(&self) -> PathBuf {
        self.root.join(MOD_SKILLS_DIR)
    }

    pub fn mcp_dir(&self) -> PathBuf {
        self.root.join(MOD_MCP_DIR)
    }

    pub fn prompts_dir(&self) -> PathBuf {
        self.root.join(MOD_PROMPTS_DIR)
    }
}
