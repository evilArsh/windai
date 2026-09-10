use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// 测试用临时目录，`Drop` 时自动清理。
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new(label: &str) -> Self {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path =
            std::env::temp_dir().join(format!("wind-skills-{label}-{}-{id}", std::process::id()));
        fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    /// 写入文件（自动创建父目录）。
    pub fn write(&self, rel: &str, content: &str) {
        let path = self.path.join(rel_path(rel));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent dir");
        }
        fs::write(path, content).expect("write file");
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// 把 `/` 分隔的相对路径按平台分隔符安全拼接，避免 Windows 上出现 `\` 与 `/` 混用。
pub fn rel_path(rel: &str) -> PathBuf {
    rel.split('/').collect()
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
