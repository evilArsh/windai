pub mod fs;
pub mod skills;

use super::registry::{McpError, Registry};
use std::fs as std_fs;
use std::path::{Path, PathBuf};
use wind_event::context::DispatchCx;
use wind_mod::dirs::AppDirs;

/// 稳定的公开标识
pub const NAMES: &[&str] = &["fs", "skills"];

pub const FS: &str = "fs";
pub const SKILLS: &str = "skills";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltinRoots {
    /// 按顺序排列
    pub skills: Vec<PathBuf>,
    pub workspace: PathBuf,
}

pub fn roots_for(dirs: &AppDirs, topic_id: i64, mod_roots: &[PathBuf]) -> BuiltinRoots {
    let mut skills: Vec<PathBuf> = mod_roots.iter().map(|root| root.join("skills")).collect();
    skills.push(dirs.public_skills());
    BuiltinRoots {
        skills,
        workspace: dirs.topic_dir(topic_id),
    }
}

pub async fn register_builtin(
    registry: &Registry,
    cx: &DispatchCx,
    topic_id: i64,
    roots: &BuiltinRoots,
) -> Result<(), McpError> {
    std_fs_create(&roots.workspace)?;
    for root in &roots.skills {
        std_fs_create(root)?;
    }
    let scope = format!("topic:{topic_id}");
    registry
        .connect_builtin(
            cx,
            topic_id,
            FS,
            &scope,
            fs::FsServer::new(vec![roots.workspace.clone()]),
        )
        .await?;
    registry
        .connect_builtin(
            cx,
            topic_id,
            SKILLS,
            &scope,
            skills::SkillsServer::new(roots.skills.clone()),
        )
        .await?;
    Ok(())
}

fn std_fs_create(path: &Path) -> Result<(), McpError> {
    std_fs::create_dir_all(path).map_err(|error| McpError::Directory {
        path: path.display().to_string(),
        reason: error.to_string(),
    })
}
