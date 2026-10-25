use std::path::{Path, PathBuf};

const APP_DIR: &str = ".windai";
const ROOT_ENV: &str = "WIND_ROOT_DIR";
const DB_FILE: &str = "windai.db";

#[derive(Debug, Clone)]
pub struct AppDirs {
    root: PathBuf,
}

impl AppDirs {
    pub fn under(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn resolve() -> Self {
        let root = match std::env::var(ROOT_ENV) {
            Ok(value) => PathBuf::from(value),
            Err(_) => dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(APP_DIR),
        };
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn db_file(&self) -> PathBuf {
        self.root.join(DB_FILE)
    }

    pub fn public_skills(&self) -> PathBuf {
        self.root.join("skills")
    }

    pub fn topic_dir(&self, topic_id: i64) -> PathBuf {
        self.root.join("topics").join(topic_id.to_string())
    }

    pub fn mod_root(&self, name: &str) -> PathBuf {
        self.root.join("mods").join(name)
    }

    pub fn mod_version(&self, name: &str, version: i64) -> PathBuf {
        self.mod_root(name).join(version.to_string())
    }

    pub fn mod_data(&self, name: &str) -> PathBuf {
        self.mod_root(name).join("data")
    }
}
