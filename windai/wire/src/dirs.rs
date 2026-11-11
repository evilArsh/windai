use std::env::home_dir;
use std::path::{Path, PathBuf};

pub const MOD_MCP_DIR: &str = "mcp";
pub const MOD_SKILLS_DIR: &str = "skills";
pub const MOD_MANIFEST: &str = "mod.json";

pub const MOD_SCRIPT_DIR: &str = "scripts";
pub const MOD_SCRIPT_ENTRY: &str = "scripts/index.js";
pub const MOD_SCRIPT_EXT: &str = "js";

pub const MOD_PROMPTS_DIR: &str = "prompts";
pub const MOD_PROMPT_EXTS: [&str; 3] = ["md", "txt", "html"];

pub const MOD_UI_ENTRY: &str = "ui/index.js";

pub const PUB_DIR: &str = ".windai";
pub const PUB_DB_PATH: &str = "windai.db";
pub const PUB_SKILLS_DIR: &str = "skills";
pub const PUB_TOPICS_DIR: &str = "topics";
pub const PUB_MODS_DIR: &str = "mods";

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
            Err(_) => home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(PUB_DIR),
        };
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn db_file(&self) -> PathBuf {
        self.root.join(PUB_DB_PATH)
    }

    pub fn public_skills(&self) -> PathBuf {
        self.root.join(PUB_SKILLS_DIR)
    }

    pub fn topics_root(&self) -> PathBuf {
        self.root.join(PUB_TOPICS_DIR)
    }

    pub fn mods_root(&self) -> PathBuf {
        self.root.join(PUB_MODS_DIR)
    }

    pub fn mod_root(&self, name: &str) -> PathBuf {
        self.mods_root().join(name)
    }

    pub fn mod_version(&self, name: &str, version: u32) -> PathBuf {
        self.mod_root(name).join(version.to_string())
    }

    pub fn mod_data(&self, name: &str) -> PathBuf {
        self.mod_root(name).join("data")
    }
}
