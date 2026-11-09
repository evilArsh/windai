use std::ffi::OsString;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::result::Result as StdResult;

/// 能力层错误码
#[derive(thiserror::Error, Debug, strum::AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum FsError {
    #[error("path not found: {0}")]
    NotFound(String),

    #[error("operation not allowed: {0}")]
    NotAllowed(String),

    #[error("is a directory: {0}")]
    IsDir(String),

    #[error("is a file: {0}")]
    IsFile(String),

    #[error("binary file: {0}")]
    Binary(String),

    #[error("content too large: {0}")]
    TooLarge(String),

    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("command timed out")]
    Timeout,

    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub type Result<T> = StdResult<T, FsError>;

/// 符号链接解析深度上限
const MAX_SYMLINK_DEPTH: u16 = 40;

#[derive(Clone)]
pub struct Sandbox {
    snapshot: Snapshot,
}

impl Sandbox {
    pub fn new(roots: Vec<PathBuf>) -> Self {
        Self {
            snapshot: Snapshot::new(roots),
        }
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        if self.snapshot.roots.is_empty() {
            return Err(not_configured());
        }
        Ok(self.snapshot.clone())
    }
}

fn not_configured() -> FsError {
    FsError::NotAllowed("no workspace roots are configured".to_owned())
}

/// 一次工具调用使用的根集合，构造时已 canonicalize
#[derive(Debug, Clone)]
pub struct Snapshot {
    roots: Vec<PathBuf>,
}

impl Snapshot {
    pub fn new(roots: Vec<PathBuf>) -> Self {
        let roots = roots
            .into_iter()
            .filter(|root| !root.as_os_str().is_empty())
            .map(|root| fs::canonicalize(&root).unwrap_or(root))
            .collect();
        Self { roots }
    }

    /// 解析路径为规范化绝对路径，并校验落在某个允许根内
    ///
    /// 已存在则 canonicalize（解 symlink）后复查；不存在则向上找到最近已存在祖先，
    /// 校验其在允许根内后，逐段重放缺失段——软链接（含悬空）解析并复查，普通段直接拼回
    pub fn resolve(&self, path: &Path) -> Result<PathBuf> {
        self.resolve_inner(path, 0)
    }

    /// 递归实现：`depth` 用于限制符号链接解析层数，防止符号链接环导致死循环
    fn resolve_inner(&self, path: &Path, depth: u16) -> Result<PathBuf> {
        if depth > MAX_SYMLINK_DEPTH {
            return Err(FsError::InvalidPath(format!(
                "too many symbolic links: {}",
                path.to_string_lossy().into_owned()
            )));
        }

        let abs = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| FsError::Io(e.to_string()))?
                .join(path)
        };

        if let Ok(canon) = fs::canonicalize(&abs) {
            return self.check_within(canon, &abs);
        }

        let mut missing: Vec<OsString> = Vec::new();
        let mut current = abs.clone();
        let canon_base = loop {
            match fs::canonicalize(&current) {
                Ok(canon) => break canon,
                Err(_) => {
                    if let Some(name) = current.file_name() {
                        missing.push(name.to_os_string());
                    }
                    match current.parent() {
                        Some(parent) => current = parent.to_path_buf(),
                        None => return Err(FsError::NotFound(path.to_string_lossy().into_owned())),
                    }
                }
            }
        };

        let mut resolved = self.check_within(canon_base, &abs)?;
        for seg in missing.iter().rev() {
            let cand = resolved.join(seg);
            match fs::symlink_metadata(&cand) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    let target = fs::read_link(&cand).map_err(|e| FsError::Io(e.to_string()))?;
                    let target_abs = if target.is_absolute() {
                        target
                    } else {
                        resolved.join(target)
                    };
                    resolved = self.resolve_inner(&target_abs, depth + 1)?;
                }
                Ok(_) => {
                    resolved = cand;
                }
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    resolved = cand;
                }
                Err(e) => return Err(FsError::Io(e.to_string())),
            }
        }

        Ok(resolved)
    }

    fn check_within(&self, canon: PathBuf, path: &Path) -> Result<PathBuf> {
        if self.within(&canon) {
            Ok(canon)
        } else {
            Err(FsError::NotAllowed(path.to_string_lossy().into_owned()))
        }
    }

    fn within(&self, canon: &Path) -> bool {
        self.roots.iter().any(|root| canon.starts_with(root))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::{FsError, Sandbox};

    #[test]
    fn empty_workspace_roots_are_rejected() {
        let error = Sandbox::new(vec![PathBuf::new()])
            .snapshot()
            .expect_err("an empty workspace root must be rejected");
        assert!(
            matches!(&error, FsError::NotAllowed(_)),
            "the rejection must be NotAllowed: {error:?}"
        );
        assert!(
            error.to_string().contains("no workspace roots"),
            "the rejection must name the missing roots: {error}"
        );
    }

    #[test]
    fn empty_workspace_roots_are_filtered_out() {
        let tmp = TempDir::new().expect("create a temp dir");
        let workspace = tmp.path().join("ws");
        std::fs::create_dir_all(&workspace).expect("create the workspace");

        let snapshot = Sandbox::new(vec![PathBuf::new(), workspace.clone()])
            .snapshot()
            .expect("the valid root must survive filtering");
        let expected = std::fs::canonicalize(&workspace).expect("canonicalize the workspace");
        assert_eq!(snapshot.roots, vec![expected]);
    }
}
