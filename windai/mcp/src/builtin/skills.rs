use std::fs;
use std::path::PathBuf;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::{ErrorData, ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;

use super::skills_scan;
use crate::roots::{Roots, ScopeId, SkillRoot};

pub use super::skills_scan::{SkillError, SkillsListResult, SkillsMeta};

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
    roots: Roots,
    scope: ScopeId,
    tool_router: ToolRouter<Self>,
}

impl SkillsServer {
    pub fn new(roots: Roots, scope: ScopeId) -> Self {
        Self {
            roots,
            scope,
            tool_router: Self::tool_router(),
        }
    }

    fn skill_roots(&self) -> Result<Vec<SkillRoot>, ErrorData> {
        let Some(scope) = self.roots.scope(&self.scope) else {
            return Err(self.not_configured());
        };
        let roots: Vec<SkillRoot> = scope
            .skills
            .into_iter()
            .filter(|root| !root.path.as_os_str().is_empty())
            .map(|root| SkillRoot {
                source: root.source,
                path: fs::canonicalize(&root.path).unwrap_or(root.path),
            })
            .collect();
        if roots.is_empty() {
            return Err(self.not_configured());
        }
        Ok(roots)
    }

    fn not_configured(&self) -> ErrorData {
        ErrorData::internal_error(
            format!("no skill roots are configured for the scope {}", self.scope),
            None,
        )
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
            skills_scan::scan(&roots, &dir, request.recursive, request.max_depth)
        })
        .await
        .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
        Ok(Json(result))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for SkillsServer {}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::SkillsServer;
    use crate::roots::{Roots, ScopeId, ScopeRoots, SkillRoot};

    #[test]
    fn empty_skill_roots_are_filtered_from_the_snapshot() {
        let tmp = TempDir::new().expect("create a temp dir");
        let roots = Roots::new(tmp.path().join("mods"));
        let scope = ScopeId::new("topic:1");
        roots.set_scope(
            &scope,
            ScopeRoots {
                workspace: Vec::new(),
                skills: vec![
                    SkillRoot {
                        source: "empty".to_owned(),
                        path: PathBuf::new(),
                    },
                    SkillRoot {
                        source: "valid".to_owned(),
                        path: tmp.path().to_path_buf(),
                    },
                ],
            },
        );

        let snapshot = SkillsServer::new(roots, scope)
            .skill_roots()
            .expect("the valid skill root must survive");
        assert_eq!(
            snapshot.len(),
            1,
            "only the valid root may remain: {snapshot:?}"
        );
        assert_eq!(snapshot[0].source, "valid");
    }

    #[test]
    fn an_all_empty_skill_scope_fails_the_snapshot() {
        let tmp = TempDir::new().expect("create a temp dir");
        let roots = Roots::new(tmp.path().join("mods"));
        let scope = ScopeId::new("topic:1");
        roots.set_scope(
            &scope,
            ScopeRoots {
                workspace: Vec::new(),
                skills: vec![SkillRoot {
                    source: "empty".to_owned(),
                    path: PathBuf::new(),
                }],
            },
        );

        let error = SkillsServer::new(roots, scope)
            .skill_roots()
            .expect_err("an empty skill root must be rejected");
        assert!(
            error.to_string().contains("no skill roots"),
            "the rejection must name the missing roots: {error:?}"
        );
    }
}
