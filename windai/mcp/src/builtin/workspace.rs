use super::canonical_path;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Json;
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
pub struct SkillRoot {
    /// skill 来源的 mod 名, 公共 skill 为 None
    pub source: Option<String>,
    /// skill 根
    pub root: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
pub struct ModDir {
    /// mod 名
    pub name: String,
    /// mod 版本
    pub version: u32,
    /// mod 根目录
    pub root: PathBuf,
}

/// 话题工作区元数据
#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
pub struct WorkspaceInfo {
    /// 唯一话题 id
    pub topic_id: i64,
    /// 话题工作区
    pub workspace: PathBuf,
    /// 公共 skills 和 mod skills 目录
    pub skills: Vec<SkillRoot>,
    /// 仅含本话题启用的 mod
    pub mods: Vec<ModDir>,
}

impl WorkspaceInfo {
    pub fn new(
        topic_id: i64,
        workspace: PathBuf,
        skills: Vec<SkillRoot>,
        mods: Vec<ModDir>,
    ) -> Self {
        Self {
            topic_id,
            workspace: canonical_path(workspace),
            skills: skills
                .into_iter()
                .map(|mut skill| {
                    skill.root = canonical_path(skill.root);
                    skill
                })
                .collect(),
            mods: mods
                .into_iter()
                .map(|mut module| {
                    module.root = canonical_path(module.root);
                    module
                })
                .collect(),
        }
    }

    pub fn roots(&self) -> Vec<PathBuf> {
        let mut roots = Vec::with_capacity(1 + self.skills.len() + self.mods.len());
        roots.push(self.workspace.clone());
        roots.extend(self.skills.iter().map(|skill| skill.root.clone()));
        roots.extend(self.mods.iter().map(|module| module.root.clone()));
        roots
    }
}

pub struct WorkspaceServer {
    name: String,
    info: Arc<RwLock<WorkspaceInfo>>,
    tool_router: ToolRouter<Self>,
}

impl WorkspaceServer {
    pub fn new(name: impl Into<String>, info: Arc<RwLock<WorkspaceInfo>>) -> Self {
        Self {
            name: name.into(),
            info,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router(router = tool_router)]
impl WorkspaceServer {
    #[tool(
        name = "workspace_info",
        description = "Return this topic's runtime paths.
        Call it before working with files or processes instead of guessing paths:
        it reports the topic workspace, the skills root,
        and the directories of the mods enabled for this topic."
    )]
    async fn workspace_info(&self) -> Json<WorkspaceInfo> {
        let info = self
            .info
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        Json(info)
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for WorkspaceServer {
    fn get_info(&self) -> ServerConfig {
        let mut server_info = Implementation::new(self.name.clone(), env!("CARGO_PKG_VERSION"));
        server_info.description = Some("runtime path inventory for this topic".to_owned());
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(server_info)
    }
}

pub(super) const SLOT: &str = "workspace";
