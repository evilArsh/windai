use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

/// 符号链接解析深度上限
const MAX_SYMLINK_DEPTH: u16 = 40;

/// 路径解析错误
#[derive(Debug, thiserror::Error)]
pub enum PathError {
    /// 路径不在允许的根目录内
    #[error("path not allowed: {0}")]
    NotAllowed(String),
    /// 路径形态无效
    #[error("invalid path: {0}")]
    InvalidPath(String),
    /// 最近存在的父目录也不存在
    #[error("path not found: {0}")]
    NotFound(String),
    /// 文件系统错误
    #[error("io error: {0}")]
    Io(String),
}

/// 允许访问的根目录集合
#[derive(Debug, Clone)]
pub struct Paths {
    roots: Vec<PathBuf>,
}

impl Paths {
    pub fn new(roots: Vec<PathBuf>) -> Self {
        let roots = roots
            .into_iter()
            .map(|root| fs::canonicalize(&root).unwrap_or(root))
            .collect();
        Self { roots }
    }

    /// 解析实际路径并确保它在某个根目录内
    pub fn resolve(&self, path: &Path) -> Result<PathBuf, PathError> {
        self.resolve_inner(path, 0)
    }

    fn resolve_inner(&self, path: &Path, depth: u16) -> Result<PathBuf, PathError> {
        if depth > MAX_SYMLINK_DEPTH {
            return Err(PathError::InvalidPath(format!(
                "too many symbolic links: {}",
                path.to_string_lossy()
            )));
        }

        let abs = if path.is_absolute() {
            path.to_path_buf()
        } else {
            env::current_dir()
                .map_err(|error| PathError::Io(error.to_string()))?
                .join(path)
        };

        if let Ok(canonical) = fs::canonicalize(&abs) {
            return self.check_within(canonical, &abs);
        }

        let mut missing: Vec<OsString> = Vec::new();
        let mut current = abs.clone();
        let canon_base = loop {
            match fs::canonicalize(&current) {
                Ok(canonical) => break canonical,
                Err(_) => {
                    if let Some(name) = current.file_name() {
                        missing.push(name.to_os_string());
                    }
                    match current.parent() {
                        Some(parent) => current = parent.to_path_buf(),
                        None => {
                            return Err(PathError::NotFound(path.to_string_lossy().into_owned()));
                        }
                    }
                }
            }
        };

        let mut resolved = self.check_within(canon_base, &abs)?;
        for segment in missing.iter().rev() {
            let candidate = resolved.join(segment);
            match fs::symlink_metadata(&candidate) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    let target = fs::read_link(&candidate)
                        .map_err(|error| PathError::Io(error.to_string()))?;
                    let target_abs = if target.is_absolute() {
                        target
                    } else {
                        resolved.join(target)
                    };
                    resolved = self.resolve_inner(&target_abs, depth + 1)?;
                }
                Ok(_) => resolved = candidate,
                Err(error) if error.kind() == ErrorKind::NotFound => resolved = candidate,
                Err(error) => return Err(PathError::Io(error.to_string())),
            }
        }

        Ok(resolved)
    }

    fn check_within(&self, canonical: PathBuf, path: &Path) -> Result<PathBuf, PathError> {
        if self.within(&canonical) {
            Ok(canonical)
        } else {
            Err(PathError::NotAllowed(path.to_string_lossy().into_owned()))
        }
    }

    fn within(&self, canonical: &Path) -> bool {
        self.roots.iter().any(|root| canonical.starts_with(root))
    }
}
