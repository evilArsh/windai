use std::{
    ffi::OsString,
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use super::error::{FsError, Result};

/// 符号链接解析深度上限
const MAX_SYMLINK_DEPTH: u16 = 40;

/// 文件操作沙箱：允许根集合 + 越界检查。
#[derive(Debug, Clone)]
pub struct Sandbox {
    roots: Vec<PathBuf>,
}

impl Sandbox {
    pub fn new(roots: Vec<PathBuf>) -> Self {
        let roots = roots
            .into_iter()
            .map(|r| fs::canonicalize(&r).unwrap_or(r))
            .collect();
        Self { roots }
    }

    /// 解析路径为规范化绝对路径，并校验落在某个允许根内。
    ///
    /// 已存在则 canonicalize（解 symlink）后复查；不存在则向上找到最近已存在祖先，
    /// 校验其在允许根内后，逐段重放缺失段——软链接（含悬空）解析并复查，普通段直接拼回。
    pub fn resolve(&self, path: &Path) -> Result<PathBuf> {
        self.resolve_inner(path, 0)
    }

    /// 递归实现：`depth` 用于限制符号链接解析层数，防止符号链接环导致死循环。
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

    /// 检查路径是否在指定的安全了路径内
    ///
    /// !!! 如果不设置安全路径则放行所有
    fn within(&self, canon: &Path) -> bool {
        if self.roots.is_empty() {
            log::warn!(
                "[Sandbox] danger! root path not specified, path: {}",
                canon.display()
            );
            return true;
        }
        self.roots.iter().any(|root| canon.starts_with(root))
    }
}
