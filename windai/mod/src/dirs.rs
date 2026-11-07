//! TODO: 考虑将这个文件放到合适的位置

use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct AppDirs {
    root: PathBuf,
}

impl AppDirs {
    pub fn under(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn resolve() -> Self {
        let root = match std::env::var("WIND_ROOT_DIR") {
            Ok(value) => PathBuf::from(value),
            Err(_) => dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".windai"),
        };
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn db_file(&self) -> PathBuf {
        self.root.join("windai.db")
    }

    pub fn public_skills(&self) -> PathBuf {
        self.root.join("skills")
    }

    pub fn mod_root(&self, name: &str) -> PathBuf {
        self.root.join("mods").join(name)
    }

    pub fn mod_version(&self, name: &str, version: u32) -> PathBuf {
        self.mod_root(name).join(version.to_string())
    }

    pub fn mod_data(&self, name: &str) -> PathBuf {
        self.mod_root(name).join("data")
    }
}
