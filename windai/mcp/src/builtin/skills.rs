mod ops;

use std::path::{Path, PathBuf};

use rmcp::{
    ErrorData, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Json, wrapper::Parameters},
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;
use wind_skills::SkillsMeta;

use crate::BuiltinMcp;

use super::fs::Sandbox;

#[derive(Deserialize, JsonSchema)]
pub struct SkillsListRequest {
    /// 要扫描的目录绝对路径。
    pub dir: String,
    /// 是否递归子目录，默认 true。
    pub recursive: Option<bool>,
    /// 允许递归的子目录层数，默认不限。
    pub max_depth: Option<u16>,
}

/// wind-skills MCP 服务：技能披露。
pub struct SkillsServer {
    sandbox: Sandbox,
    tool_router: ToolRouter<Self>,
}

impl SkillsServer {
    pub fn new(roots: Vec<PathBuf>) -> Self {
        Self {
            sandbox: Sandbox::new(roots),
            tool_router: Self::tool_router(),
        }
    }
}

impl BuiltinMcp for SkillsServer {
    fn get_name(&self) -> &'static str {
        super::BUILTIN_SKILLS.name
    }

    fn get_description(&self) -> &'static str {
        super::BUILTIN_SKILLS.description
    }
}

#[tool_router(router = tool_router)]
impl SkillsServer {
    /// 扫描目录，收集所有 skill（SKILL.md）的元数据。
    #[tool(
        name = "skills_list",
        description = "Scan a directory and collect metadata (name/description/skill_dir) for every SKILL.md skill."
    )]
    async fn skills_list(
        &self,
        Parameters(req): Parameters<SkillsListRequest>,
    ) -> Result<Json<Vec<SkillsMeta>>, ErrorData> {
        let dir = self
            .sandbox
            .resolve(Path::new(&req.dir))
            .map_err(ErrorData::from)?;
        Ok(Json(ops::skills_list(dir, req.recursive, req.max_depth)))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for SkillsServer {}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::{ServiceExt, model::CallToolRequestParams, model::CallToolResponse};
    use serde_json::json;
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!(
                "wind-skills-handler-{label}-{}-{id}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn write(&self, rel: &str, content: &str) {
            let path = self.path.join(rel);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create parent dir");
            }
            fs::write(path, content).expect("write file");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn args(map: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        map.as_object().expect("object").clone()
    }

    #[tokio::test]
    async fn skills_list_via_mcp() {
        let dir = TempDir::new("skills");
        dir.write("SKILL.md", "---\nname: demo\ndescription: a demo\n---\n");

        let (server_tx, client_rx) = tokio::io::duplex(4096);
        let roots = vec![dir.path.to_path_buf()];
        tokio::spawn(async move {
            let server = SkillsServer::new(roots)
                .serve(server_tx)
                .await
                .expect("serve");
            let _ = server.waiting().await;
        });

        let client = ().serve(client_rx).await.expect("client serve");
        let result = client
            .call_tool_once(
                CallToolRequestParams::new("skills_list".to_string())
                    .with_arguments(args(json!({ "dir": dir.path.to_string_lossy() }))),
            )
            .await
            .expect("call skills_list");

        match result {
            CallToolResponse::Complete(r) => {
                assert!(r.is_error.is_none() || r.is_error == Some(false));
                let structured = r.structured_content.expect("structured content");
                assert_eq!(structured[0]["name"], json!("demo"));
                assert_eq!(structured[0]["description"], json!("a demo"));
            }
            other => panic!("expected Complete, got: {other:?}"),
        }
    }
}
