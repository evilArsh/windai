use crate::builtin::canonical_path;
use crate::builtin::skills::SkillRoot;
use schemars::JsonSchema;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
pub struct SkillDir {
    /// 公共 skills 或 mod 名
    pub source: String,
    pub path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
pub struct ModDir {
    pub name: String,
    pub version: u32,
    pub root: PathBuf,
    pub skills: Option<PathBuf>,
    pub ui: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
pub struct WorkspaceSnapshot {
    pub summary: String,
    pub workspace: PathBuf,
    /// 文件服务接受的目录根
    pub roots: Vec<PathBuf>,
    pub skills: Vec<SkillDir>,
    pub mods: Vec<ModDir>,
}

impl WorkspaceSnapshot {
    pub fn new(
        workspace: PathBuf,
        public_skills: PathBuf,
        skills: Vec<SkillRoot>,
        mods: Vec<ModDir>,
    ) -> Self {
        let workspace = canonical_path(workspace);
        let public_skills = canonical_path(public_skills);
        let skills = skills
            .into_iter()
            .map(|root| SkillDir {
                source: root.source,
                path: canonical_path(root.path),
            })
            .collect();
        let mods: Vec<ModDir> = mods
            .into_iter()
            .map(|module| ModDir {
                name: module.name,
                version: module.version,
                root: canonical_path(module.root),
                skills: module.skills.map(canonical_path),
                ui: module.ui.map(canonical_path),
            })
            .collect();
        let mut roots = Vec::with_capacity(2 + mods.len());
        roots.push(workspace.clone());
        roots.push(public_skills.clone());
        roots.extend(mods.iter().map(|module| module.root.clone()));
        let summary = summary(&workspace, &public_skills, &mods);
        Self {
            summary,
            workspace,
            roots,
            skills,
            mods,
        }
    }
}

fn summary(workspace: &Path, public_skills: &Path, mods: &[ModDir]) -> String {
    let mods = if mods.is_empty() {
        "none".to_owned()
    } else {
        mods.iter()
            .map(|module| format!("{}@{}", module.name, module.version))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "Workspace for this topic: {}. Public skills: {}. Enabled mods: {mods}.",
        workspace.display(),
        public_skills.display()
    )
}

#[derive(Clone)]
pub struct WorkspaceInfo(Arc<RwLock<WorkspaceSnapshot>>);

impl WorkspaceInfo {
    pub fn new(snapshot: WorkspaceSnapshot) -> Self {
        Self(Arc::new(RwLock::new(snapshot)))
    }

    pub fn replace(&self, snapshot: WorkspaceSnapshot) {
        *self
            .0
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = snapshot;
    }

    pub fn snapshot(&self) -> WorkspaceSnapshot {
        self.0
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}
