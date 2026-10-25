use super::normalizer::normalize;
use super::params::ServerParams;
use std::path::{Component, Path, PathBuf};
use wind_mod::package::asset_fingerprint;

/// 由连接方式决定，与名字无关
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Identity {
    /// 由包管理器启动，键是包名
    Package { package: String },
    /// 由本包内的相对路径启动，键是资产内容指纹
    Asset { asset: String, fingerprint: String },
    /// 由系统 PATH 查找或绝对路径启动，键是完整的命令行
    Command { program: String, args: Vec<String> },
    /// 远端，键是归一化后的地址
    Remote { url: String },
    /// 宿主自带的进程内服务，键是名字与作用域
    Builtin { name: String, scope: String },
}

#[derive(Debug, Clone)]
pub struct AssetRoot {
    root: PathBuf,
}

impl AssetRoot {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// 只在词法层面解析，结果位于资产根目录之内
    pub fn resolve(&self, command: &str) -> Option<PathBuf> {
        let relative = command.strip_prefix("./")?;
        let mut resolved = self.root.clone();
        let mut depth = 0usize;
        for component in Path::new(relative).components() {
            match component {
                Component::Normal(part) => {
                    resolved.push(part);
                    depth += 1;
                }
                Component::CurDir => {}
                Component::ParentDir => {
                    if depth == 0 {
                        return None;
                    }
                    resolved.pop();
                    depth -= 1;
                }
                Component::RootDir | Component::Prefix(_) => return None,
            }
        }
        Some(resolved)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    #[error("asset {asset} is not present under {root}")]
    MissingAsset { asset: String, root: String },
    #[error("relative command {command} does not name an asset")]
    UnnamedAsset { command: String },
}

pub fn identity_of(params: &ServerParams, root: &AssetRoot) -> Result<Identity, IdentityError> {
    match params {
        ServerParams::Streamable(remote) => Ok(Identity::Remote {
            url: normalize_url(&remote.url),
        }),
        ServerParams::Stdio(stdio) => {
            if let Some(normalized) = normalize(stdio) {
                return Ok(Identity::Package {
                    package: normalized.dedup_key,
                });
            }
            if !stdio.command.starts_with("./") {
                return Ok(Identity::Command {
                    program: stdio.command.clone(),
                    args: stdio.args.clone(),
                });
            }
            let asset = asset_name(&stdio.command)?;
            let missing = || IdentityError::MissingAsset {
                asset: asset.clone(),
                root: root.root().display().to_string(),
            };
            let fingerprint = asset_fingerprint(root.root(), &asset).map_err(|_| missing())?;
            Ok(Identity::Asset { asset, fingerprint })
        }
    }
}

fn asset_name(command: &str) -> Result<String, IdentityError> {
    let relative = command.strip_prefix("./").unwrap_or(command);
    let mut parts = relative.split('/').filter(|part| !part.is_empty());
    let first = parts.next().ok_or_else(|| IdentityError::UnnamedAsset {
        command: command.to_string(),
    })?;
    let name = if first == "mcp" {
        parts.next().unwrap_or(first)
    } else {
        first
    };
    Ok(name.to_string())
}

pub fn normalize_url(url: &str) -> String {
    let trimmed = url.trim();
    let (scheme, rest) = match trimmed.split_once("://") {
        Some((scheme, rest)) => (scheme.to_ascii_lowercase(), rest),
        None => (String::new(), trimmed),
    };
    let rest = rest.split(['?', '#']).next().unwrap_or("").to_string();
    let (authority, path) = match rest.find('/') {
        Some(index) => (rest[..index].to_string(), rest[index..].to_string()),
        None => (rest.clone(), String::new()),
    };
    let authority = authority.to_ascii_lowercase();
    let authority = match (scheme.as_str(), authority.split_once(':')) {
        ("https", Some((host, "443"))) => host.to_string(),
        ("http", Some((host, "80"))) => host.to_string(),
        _ => authority,
    };
    let path = path.trim_end_matches('/').to_string();
    format!("{scheme}://{authority}{path}")
}
