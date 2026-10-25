use std::collections::BTreeMap;
use std::path::PathBuf;
use wind_mod::manifest::Manifest;

#[derive(Debug, thiserror::Error)]
#[error("{name} v{version} is not deployed")]
pub struct NotDeployed {
    pub name: String,
    pub version: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum DeployError {
    #[error("cannot open the deployment of {name} v{version}: {reason}")]
    Read {
        name: String,
        version: i64,
        reason: String,
    },
    #[error(transparent)]
    NotDeployed(#[from] NotDeployed),
}

/// 一次调用要用的已安装版本：安装目录、清单和字节码。
#[derive(Debug, Clone)]
pub struct Deployment {
    pub name: String,
    pub version: i64,
    pub root: PathBuf,
    pub manifest: Manifest,
    pub compiled: BTreeMap<String, Vec<u8>>,
}
