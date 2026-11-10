use super::{BuiltinResources, BuiltinScope, replace};
use crate::RegistryHandle;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::{ErrorData, ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

mod scan;

pub use scan::{SkillError, SkillsListResult, SkillsMeta};

/// 一个技能根及其来源名
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillRoot {
    pub source: String,
    pub path: PathBuf,
}

/// 列举技能的请求
#[derive(Deserialize, JsonSchema)]
pub struct SkillsListRequest {
    /// 要扫描的目录绝对路径，必须在某个技能根之内
    pub dir: String,
    /// 是否递归子目录，默认为 true
    pub recursive: Option<bool>,
    /// 允许递归的子目录层数，默认不限
    pub max_depth: Option<u16>,
}

/// 技能目录工具
pub struct SkillsServer {
    name: String,
    roots: Vec<SkillRoot>,
    tool_router: ToolRouter<Self>,
}

impl SkillsServer {
    /// 空路径的技能根会被忽略
    pub fn new(name: impl Into<String>, roots: Vec<SkillRoot>) -> Self {
        Self {
            name: name.into(),
            roots,
            tool_router: Self::tool_router(),
        }
    }

    fn skill_roots(&self) -> Result<Vec<SkillRoot>, ErrorData> {
        let roots: Vec<SkillRoot> = self
            .roots
            .iter()
            .filter(|root| !root.path.as_os_str().is_empty())
            .map(|root| SkillRoot {
                source: root.source.clone(),
                path: fs::canonicalize(&root.path).unwrap_or_else(|_| root.path.clone()),
            })
            .collect();
        if roots.is_empty() {
            return Err(ErrorData::internal_error(
                "no skill roots are configured".to_owned(),
                None,
            ));
        }
        Ok(roots)
    }
}

fn resolve_dir(roots: &[SkillRoot], dir: &str) -> Result<PathBuf, ErrorData> {
    let path = fs::canonicalize(dir).map_err(|error| {
        ErrorData::internal_error(format!("cannot resolve the directory {dir}: {error}"), None)
    })?;
    if roots.iter().any(|root| path.starts_with(&root.path)) {
        Ok(path)
    } else {
        Err(ErrorData::internal_error(
            format!("the directory {dir} is not inside any skill root"),
            None,
        ))
    }
}

#[tool_router(router = tool_router)]
impl SkillsServer {
    #[tool(
        name = "skills_list",
        description = "Scan a directory and collect metadata for every SKILL.md skill, keeping the source of each skill."
    )]
    async fn skills_list(
        &self,
        Parameters(request): Parameters<SkillsListRequest>,
    ) -> Result<Json<SkillsListResult>, ErrorData> {
        let roots = self.skill_roots()?;
        let dir = resolve_dir(&roots, &request.dir)?;
        let result = tokio::task::spawn_blocking(move || {
            scan::scan(&roots, &dir, request.recursive, request.max_depth)
        })
        .await
        .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
        Ok(Json(result))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for SkillsServer {
    fn get_info(&self) -> ServerConfig {
        let mut server_info = Implementation::new(self.name.clone(), env!("CARGO_PKG_VERSION"));
        server_info.description = Some("skill discovery capability".to_owned());
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(server_info)
    }
}

/// 服务名里的固定段
pub(super) const SLOT: &str = "skills";

pub(super) async fn sync(
    registry: &RegistryHandle,
    scope: BuiltinScope,
    resources: &BuiltinResources,
) {
    let name = scope.service_name(SLOT);
    let server = if resources.skill_roots.is_empty() {
        None
    } else {
        Some(SkillsServer::new(
            name.clone(),
            resources.skill_roots.clone(),
        ))
    };
    replace(registry, name, server).await;
}
