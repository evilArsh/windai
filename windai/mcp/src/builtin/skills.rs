mod scan;

use super::fs::FsError;
use rmcp::ErrorData;
use rmcp::ServerHandler;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::tool;
use rmcp::tool_handler;
use rmcp::tool_router;
use scan::SkillsMeta;
use schemars::JsonSchema;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use wind_core::sandbox::Sandbox;

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

#[tool_router(router = tool_router)]
impl SkillsServer {
    #[tool(
        name = "skills_list",
        description = "Scan a directory and collect metadata (name/description/skill_dir) for every SKILL.md skill."
    )]
    async fn skills_list(
        &self,
        Parameters(request): Parameters<SkillsListRequest>,
    ) -> Result<Json<Vec<SkillsMeta>>, ErrorData> {
        let dir = self
            .sandbox
            .resolve(Path::new(&request.dir))
            .map_err(FsError::from)?;
        Ok(Json(scan::scan(dir, request.recursive, request.max_depth)))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for SkillsServer {}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::ServiceExt;
    use rmcp::model::{CallToolRequestParams, CallToolResponse};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// 目录名计数器，保证并发运行的测试各取一个目录
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// 用完即删的临时目录
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        /// 按标签创建临时目录
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!(
                "wind-skills-handler-{label}-{}-{id}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        /// 写一个文件，父目录一并创建
        fn write(&self, relative: &str, content: &str) {
            let path = self.path.join(relative);
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

    /// 把 JSON 对象展开成调用参数
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
                CallToolRequestParams::new("skills_list".to_string()).with_arguments(args(
                    serde_json::json!({ "dir": dir.path.to_string_lossy() }),
                )),
            )
            .await
            .expect("call skills_list");

        match result {
            CallToolResponse::Complete(result) => {
                assert!(result.is_error.is_none() || result.is_error == Some(false));
                let structured = result.structured_content.expect("structured content");
                assert_eq!(structured[0]["name"], serde_json::json!("demo"));
                assert_eq!(structured[0]["description"], serde_json::json!("a demo"));
            }
            other => panic!("expected Complete, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_directory_outside_the_roots_is_refused() {
        let inside = TempDir::new("in");
        let outside = TempDir::new("out");
        outside.write(
            "SKILL.md",
            "---\nname: secret\ndescription: not inside this topic's root\n---\n",
        );

        let (server_tx, client_rx) = tokio::io::duplex(4096);
        let roots = vec![inside.path.to_path_buf()];
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
                CallToolRequestParams::new("skills_list".to_string()).with_arguments(args(
                    serde_json::json!({ "dir": outside.path.to_string_lossy() }),
                )),
            )
            .await;

        match result {
            // 处理函数交回的错误按协议错误返回，两种情况都算拒绝
            Err(_) => {}
            Ok(CallToolResponse::Complete(result)) => {
                assert_eq!(
                    result.is_error,
                    Some(true),
                    "a directory outside the roots must be refused, got {result:?}"
                );
            }
            Ok(other) => panic!("expected a refusal, got: {other:?}"),
        }
    }
}
