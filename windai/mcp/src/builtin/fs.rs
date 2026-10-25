mod error;
mod ops;

pub use error::FsError;
pub use ops::{
    DirEntry, EntryKind, ExecResult, ListDirResult, ReadFileResult, WriteFileResult, exec,
    list_dir, read_file, write_file,
};
use rmcp::ErrorData;
use rmcp::ServerHandler;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::tool;
use rmcp::tool_handler;
use rmcp::tool_router;
use schemars::JsonSchema;
use serde::Deserialize;
use std::path::PathBuf;
use wind_core::sandbox::Sandbox;

impl From<FsError> for ErrorData {
    fn from(error: FsError) -> Self {
        let code = error.as_ref().to_string();
        ErrorData::internal_error(error.to_string(), Some(serde_json::json!({ "code": code })))
    }
}

/// 扫描目录的请求
#[derive(Deserialize, JsonSchema)]
pub struct ListDirRequest {
    /// 要扫描的目录绝对路径
    pub path: String,
    /// 是否递归子目录，默认为 true
    pub recursive: Option<bool>,
    /// 允许递归的子目录层数，默认不限
    pub max_depth: Option<u16>,
}

/// 读文件的请求
#[derive(Deserialize, JsonSchema)]
pub struct ReadFileRequest {
    /// 文件绝对路径
    pub path: String,
    /// 起始字节偏移，默认 0
    pub offset: Option<u64>,
    /// 读取字节上限，默认 1MB
    pub limit: Option<u64>,
}

/// 写文件的请求
#[derive(Deserialize, JsonSchema)]
pub struct WriteFileRequest {
    /// 文件绝对路径
    pub path: String,
    /// 要写入的文本
    pub data: String,
}

/// 执行子进程的请求
#[derive(Deserialize, JsonSchema)]
pub struct ExecRequest {
    /// 可执行程序名
    pub command: String,
    /// 工作目录绝对路径，必须在沙箱内
    pub cwd: String,
    /// 命令行参数。直接传参数而不拼接 shell 字符串，可以避免注入
    pub args: Option<Vec<String>>,
    /// 环境变量，形如 KEY=value
    pub env: Option<Vec<String>>,
    /// 超时时间，单位为毫秒
    pub timeout: u64,
}

pub struct FsServer {
    sandbox: Sandbox,
    tool_router: ToolRouter<Self>,
}

impl FsServer {
    pub fn new(roots: Vec<PathBuf>) -> Self {
        Self {
            sandbox: Sandbox::new(roots),
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router(router = tool_router)]
impl FsServer {
    #[tool(
        name = "list_dir",
        description = "Scan a directory, returning the root path and a relative-path listing of files/dirs. Hidden dirs (except whitelisted) and large dependency dirs are skipped."
    )]
    async fn list_dir(
        &self,
        Parameters(request): Parameters<ListDirRequest>,
    ) -> Json<ListDirResult> {
        Json(list_dir(
            &self.sandbox,
            PathBuf::from(request.path),
            request.recursive,
            request.max_depth,
        ))
    }

    #[tool(
        name = "read_file",
        description = "Read a text file (UTF-8). Binary files return only metadata."
    )]
    async fn read_file(
        &self,
        Parameters(request): Parameters<ReadFileRequest>,
    ) -> Json<ReadFileResult> {
        let sandbox = self.sandbox.clone();
        let path = PathBuf::from(request.path);
        let failed = path.clone();
        let read = tokio::task::spawn_blocking(move || {
            read_file(&sandbox, path, request.offset, request.limit)
        })
        .await
        .unwrap_or_else(|error| ReadFileResult::failure(&failed, error.to_string()));
        Json(read)
    }

    #[tool(
        name = "write_file",
        description = "Write a text file, creating parent directories as needed."
    )]
    async fn write_file(
        &self,
        Parameters(request): Parameters<WriteFileRequest>,
    ) -> Json<WriteFileResult> {
        Json(write_file(
            &self.sandbox,
            PathBuf::from(request.path),
            request.data,
        ))
    }

    #[tool(
        name = "exec",
        description = "Execute a subprocess. cwd must be inside the sandbox; timeout is in milliseconds."
    )]
    async fn exec(&self, Parameters(request): Parameters<ExecRequest>) -> Json<ExecResult> {
        Json(
            exec(
                &self.sandbox,
                request.command,
                request.cwd,
                request.args,
                request.env,
                request.timeout,
            )
            .await,
        )
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for FsServer {}

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
                "wind-fs-handler-{label}-{}-{id}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        /// 写一个文件，父目录一并创建
        fn write(&self, relative: &str, content: &str) {
            let path = self.path.join(relative_path(relative));
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create parent dir");
            }
            fs::write(path, content).expect("write file");
        }
    }

    /// 把以 `/` 分隔的相对路径按平台分隔符拼接，避免两种分隔符混用
    fn relative_path(relative: &str) -> PathBuf {
        relative.split('/').collect()
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
    async fn list_dir_via_mcp() {
        let dir = TempDir::new("list");
        dir.write("a.txt", "aaa");
        dir.write("node_modules/x.js", "x");

        let (server_tx, client_rx) = tokio::io::duplex(4096);
        let roots = vec![dir.path.to_path_buf()];
        tokio::spawn(async move {
            let server = FsServer::new(roots).serve(server_tx).await.expect("serve");
            let _ = server.waiting().await;
        });

        let client = ().serve(client_rx).await.expect("client serve");
        let result = client
            .call_tool_once(
                CallToolRequestParams::new("list_dir".to_string()).with_arguments(args(
                    serde_json::json!({ "path": dir.path.to_string_lossy() }),
                )),
            )
            .await
            .expect("call list_dir");

        match result {
            CallToolResponse::Complete(result) => {
                assert!(result.is_error.is_none() || result.is_error == Some(false));
                let structured = result.structured_content.expect("structured content");
                let names: Vec<&str> = structured["entries"]
                    .as_array()
                    .expect("entries array")
                    .iter()
                    .filter_map(|entry| entry["name"].as_str())
                    .collect();
                assert!(names.contains(&"a.txt"));
                assert!(!names.contains(&"node_modules"));
            }
            other => panic!("expected Complete, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_outside_sandbox_denied() {
        let inside = TempDir::new("in");
        let outside = TempDir::new("out");
        outside.write("secret.txt", "top secret");

        let (server_tx, client_rx) = tokio::io::duplex(4096);
        let roots = vec![inside.path.to_path_buf()];
        tokio::spawn(async move {
            let server = FsServer::new(roots).serve(server_tx).await.expect("serve");
            let _ = server.waiting().await;
        });

        let client = ().serve(client_rx).await.expect("client serve");
        let result = client
            .call_tool_once(
                CallToolRequestParams::new("read_file".to_string()).with_arguments(args(
                    serde_json::json!({ "path": outside.path.join("secret.txt").to_string_lossy() }),
                )),
            )
            .await;

        match result {
            Ok(CallToolResponse::Complete(result)) => {
                assert!(result.is_error.is_none() || result.is_error == Some(false));
                let structured = result.structured_content.expect("structured content");
                assert_eq!(structured["bytes"], serde_json::json!(0));
            }
            other => panic!("expected Complete, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_service_without_roots_refuses_every_path() {
        let outside = TempDir::new("no-root-out");
        outside.write("secret.txt", "top secret");

        let (server_tx, client_rx) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let server = FsServer::new(Vec::new())
                .serve(server_tx)
                .await
                .expect("serve");
            let _ = server.waiting().await;
        });

        let client = ().serve(client_rx).await.expect("client serve");
        let result = client
            .call_tool_once(
                CallToolRequestParams::new("read_file".to_string()).with_arguments(args(
                    serde_json::json!({ "path": outside.path.join("secret.txt").to_string_lossy() }),
                )),
            )
            .await;

        match result {
            Ok(CallToolResponse::Complete(result)) => {
                let structured = result.structured_content.expect("structured content");
                assert_eq!(
                    structured["bytes"],
                    serde_json::json!(0),
                    "a sandbox without roots must refuse everything, got {structured}"
                );
            }
            other => panic!("expected Complete, got: {other:?}"),
        }
    }
}
