//! windai 应用目录管理器
//!
//! 目录结构:
//! ```text
//! ~/.windai/      (或 $WIND_ROOT_DIR)
//!   windai.db     SQLite 数据库
//! ```
//!
//! 跨平台路径由 `dirs` crate 处理:
//! - Linux:   `~/.windai/`
//! - macOS:   `~/.windai/`
//! - Windows: `%HOMEPATH%\.windai\`
//!
//! 可通过环境变量 `WIND_ROOT_DIR` 覆盖根目录
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub const APP_NAME: &str = ".windai";
pub const DB_FILENAME: &str = "windai.db";
pub const TOPIC_DIR_NAME: &str = "topics";
pub const SKILLS_DIR_NAME: &str = "skills";

static DIRS: OnceLock<AppDirs> = OnceLock::new();

/// 应用数据目录
pub struct AppDirs {
    root: PathBuf,
    db: PathBuf,
    topic: PathBuf,
    skills: PathBuf,
}

impl AppDirs {
    fn new() -> Self {
        let root = Self::resolve_root();
        let db = root.join(DB_FILENAME);
        let topic = root.join(TOPIC_DIR_NAME);
        let skills = root.join(SKILLS_DIR_NAME);
        Self::ensure_dir(&root);
        Self::ensure_dir(&topic);
        Self::ensure_dir(&skills);
        Self {
            root,
            db,
            topic,
            skills,
        }
    }

    fn ensure_dir(path: &Path) {
        if let Err(e) = std::fs::create_dir_all(path) {
            panic!("failed to create directory '{}': {e}", path.display());
        }
    }

    /// 解析根目录: 优先 WIND_ROOT_DIR
    fn resolve_root() -> PathBuf {
        if let Ok(env_root) = std::env::var("WIND_ROOT_DIR") {
            return PathBuf::from(env_root);
        }
        // 回退到平台标准数据目录
        let base = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        base.join(APP_NAME)
    }

    /// 根目录
    pub fn root_dir(&self) -> &Path {
        &self.root
    }

    /// topic目录
    pub fn topic_dir(&self) -> &Path {
        &self.topic
    }

    /// 技能目录
    pub fn skills_dir(&self) -> &Path {
        &self.skills
    }

    /// SQLite 数据库文件路径
    pub fn db_path(&self) -> &Path {
        &self.db
    }

}

/// 获取应用目录句柄
pub fn app_dirs() -> &'static AppDirs {
    DIRS.get_or_init(AppDirs::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dirs_initialization() {
        let d = app_dirs();
        assert!(d.root.is_dir());
        assert!(d.db.parent().unwrap().is_dir());
    }

    #[test]
    fn test_db_path_is_not_directory() {
        let d = app_dirs();
        assert!(
            !d.db.is_dir(),
            "db path must not be a pre-created directory: {}",
            d.db.display()
        );
    }

    #[test]
    fn test_db_path() {
        let p = app_dirs().db_path();
        assert!(p.ends_with(DB_FILENAME));
    }

    #[test]
    fn test_env_override() {
        let d = app_dirs();
        if std::env::var("WIND_ROOT_DIR").is_ok() {
            let env_root = std::env::var("WIND_ROOT_DIR").unwrap();
            assert_eq!(d.root.to_str().unwrap(), env_root);
        }
    }
}
