//! windai 应用目录管理器
//!
//! 目录结构:
//! ```text
//! ~/.windai/      (或 $WINDAI_ROOT_DIR)
//!   windai.db     SQLite 数据库
//! ```
//!
//! 跨平台路径由 `dirs` crate 处理:
//! - Linux:   `~/.windai/`
//! - macOS:   `~/.windai/`
//! - Windows: `%HOMEPATH%\.windai\`
//!
//! 可通过环境变量 `WINDAI_ROOT_DIR` 覆盖根目录
use std::path::PathBuf;
use std::sync::OnceLock;

pub const APP_NAME: &str = ".windai";
pub const DB_FILENAME: &str = "windai.db";

static DIRS: OnceLock<AppDirs> = OnceLock::new();

/// 应用数据目录
pub struct AppDirs {
    #[allow(dead_code)]
    pub root: PathBuf,
    pub db: PathBuf,
}

impl AppDirs {
    fn new() -> Self {
        let root = resolve_root();
        let db = root.join(DB_FILENAME);
        std::fs::create_dir_all(&root)
            .unwrap_or_else(|e| panic!("failed to create directory '{}': {e}", root.display()));
        std::fs::create_dir_all(&db)
            .unwrap_or_else(|e| panic!("failed to create directory '{}': {e}", db.display()));
        Self { root, db }
    }
}
/// 解析根目录: 优先 WINDAI_ROOT_DIR，否则用 dirs::data_local_dir
fn resolve_root() -> PathBuf {
    if let Ok(env_root) = std::env::var("WINDAI_ROOT_DIR") {
        return PathBuf::from(env_root);
    }
    // 回退到平台标准数据目录
    let base = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    base.join(APP_NAME)
}

/// 获取应用目录句柄
pub fn dirs() -> &'static AppDirs {
    DIRS.get_or_init(AppDirs::new)
}

/// 获取根目录
#[allow(dead_code)]
pub fn root_dir() -> PathBuf {
    dirs().root.clone()
}

/// SQLite 数据库文件路径
pub fn db_path() -> PathBuf {
    dirs().db.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dirs_initialization() {
        let d = dirs();
        assert!(d.root.exists());
        assert!(d.db.exists());
    }

    #[test]
    fn test_db_path() {
        let p = db_path();
        assert!(p.ends_with(DB_FILENAME));
    }

    #[test]
    fn test_env_override() {
        let d = dirs();
        if std::env::var("WINDAI_ROOT_DIR").is_ok() {
            let env_root = std::env::var("WINDAI_ROOT_DIR").unwrap();
            assert_eq!(d.root.to_str().unwrap(), env_root);
        }
    }
}
