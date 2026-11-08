mod scan;

pub use scan::{SkillError, SkillsListResult, SkillsMeta};

use std::fs;
use std::path::Path;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::ErrorData;
use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;

use super::paths::Paths;
use crate::registry::SkillRoot;

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
    roots: Vec<SkillRoot>,
    paths: Paths,
    tool_router: ToolRouter<Self>,
}


/// TODO: mod系统会动态载入和删除skills目录,待重构
impl SkillsServer {
    pub fn new(roots: Vec<SkillRoot>) -> Self {
        let roots: Vec<SkillRoot> = roots
            .into_iter()
            .map(|root| SkillRoot {
                source: root.source,
                path: fs::canonicalize(&root.path).unwrap_or(root.path),
            })
            .collect();
        let paths = Paths::new(roots.iter().map(|root| root.path.clone()).collect());
        Self {
            roots,
            paths,
            tool_router: Self::tool_router(),
        }
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
        let dir = self.paths.resolve(Path::new(&request.dir))?;
        let roots = self.roots.clone();
        tokio::task::spawn_blocking(move || {
            scan::scan(&roots, &dir, request.recursive, request.max_depth)
        })
        .await
        .map(Json)
        .map_err(|error| ErrorData::internal_error(error.to_string(), None))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for SkillsServer {}
