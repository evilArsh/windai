mod error;
mod ops;
mod sandbox;

pub use error::FsError;
pub use ops::{
    ExecResult, ListDirResult, ReadFileResult, WriteFileResult, exec, list_dir, read_file,
    write_file,
};
pub use sandbox::Sandbox;

use std::path::PathBuf;

use rmcp::{
    ErrorData, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Json, wrapper::Parameters},
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::BuiltinMcp;

impl From<FsError> for ErrorData {
    fn from(e: FsError) -> Self {
        let code = e.as_ref().to_string();
        ErrorData::internal_error(e.to_string(), Some(serde_json::json!({ "code": code })))
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct ListDirRequest {
    /// 要扫描的目录绝对路径。
    pub path: String,
    /// 是否递归子目录，默认 true。
    pub recursive: Option<bool>,
    /// 允许递归的子目录层数，默认不限。
    pub max_depth: Option<u16>,
}

#[derive(Deserialize, JsonSchema)]
pub struct ReadFileRequest {
    /// 文件绝对路径。
    pub path: String,
    /// 字节偏移，默认 0。
    pub offset: Option<u64>,
    /// 读取字节上限，默认 64KB。
    pub limit: Option<u64>,
}

#[derive(Deserialize, JsonSchema)]
pub struct WriteFileRequest {
    /// 文件绝对路径。
    pub path: String,
    /// 要写入的文本内容。
    pub data: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct ExecRequest {
    /// 可执行程序名（argv[0]）。
    pub command: String,
    /// 工作目录绝对路径，必须在沙箱内。
    pub cwd: String,
    /// 命令行参数（argv[1..]），优先于 shell 字符串以防注入。
    pub args: Option<Vec<String>>,
    /// 环境变量，形如 KEY=value。
    pub env: Option<Vec<String>>,
    /// 超时时间（毫秒）。
    pub timeout: u64,
}

/// wind-fs MCP 服务：文件/进程能力。
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

impl BuiltinMcp for FsServer {
    fn get_name(&self) -> &'static str {
        super::BUILTIN_FS.name
    }

    fn get_description(&self) -> &'static str {
        super::BUILTIN_FS.description
    }
}

#[tool_router(router = tool_router)]
impl FsServer {
    /// 扫描目录，返回根目录路径与文件/目录相对路径清单。忽略隐藏目录（白名单除外）与 node_modules 等大目录。
    #[tool(
        name = "list_dir",
        description = "Scan a directory, returning the root path and a relative-path listing of files/dirs. Hidden dirs (except whitelisted) and large dependency dirs are skipped."
    )]
    async fn list_dir(
        &self,
        Parameters(req): Parameters<ListDirRequest>,
    ) -> Result<Json<ListDirResult>, ErrorData> {
        list_dir(
            &self.sandbox,
            PathBuf::from(req.path),
            req.recursive,
            req.max_depth,
        )
        .map(Json)
        .map_err(ErrorData::from)
    }

    /// 读文本文件；二进制文件仅返回元信息。
    #[tool(
        name = "read_file",
        description = "Read a text file (UTF-8). Binary files return only metadata (mime/size)."
    )]
    async fn read_file(
        &self,
        Parameters(req): Parameters<ReadFileRequest>,
    ) -> Result<Json<ReadFileResult>, ErrorData> {
        read_file(
            &self.sandbox,
            PathBuf::from(req.path),
            req.offset,
            req.limit,
        )
        .map(Json)
        .map_err(ErrorData::from)
    }

    /// 写文本文件，自动创建父目录。
    #[tool(
        name = "write_file",
        description = "Write a text file, creating parent directories as needed."
    )]
    async fn write_file(
        &self,
        Parameters(req): Parameters<WriteFileRequest>,
    ) -> Result<Json<WriteFileResult>, ErrorData> {
        write_file(&self.sandbox, PathBuf::from(req.path), req.data)
            .map(Json)
            .map_err(ErrorData::from)
    }

    /// 执行子进程；cwd 必须在沙箱内，timeout 单位毫秒。
    #[tool(
        name = "exec",
        description = "Execute a subprocess. cwd must be inside the sandbox; timeout is in milliseconds."
    )]
    async fn exec(
        &self,
        Parameters(req): Parameters<ExecRequest>,
    ) -> Result<Json<ExecResult>, ErrorData> {
        exec(
            &self.sandbox,
            req.command,
            req.cwd,
            req.args,
            req.env,
            req.timeout,
        )
        .await
        .map(Json)
        .map_err(ErrorData::from)
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for FsServer {}

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
                "wind-fs-handler-{label}-{}-{id}",
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
                CallToolRequestParams::new("list_dir".to_string())
                    .with_arguments(args(json!({ "path": dir.path.to_string_lossy() }))),
            )
            .await
            .expect("call list_dir");

        match result {
            CallToolResponse::Complete(r) => {
                assert!(r.is_error.is_none() || r.is_error == Some(false));
                let structured = r.structured_content.expect("structured content");
                let names: Vec<&str> = structured["entries"]
                    .as_array()
                    .expect("entries array")
                    .iter()
                    .filter_map(|e| e["name"].as_str())
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
                    json!({ "path": outside.path.join("secret.txt").to_string_lossy() }),
                )),
            )
            .await;

        match result {
            Err(e) => {
                let msg = format!("{e:?}");
                assert!(msg.contains("NOT_ALLOWED"), "unexpected error: {msg}");
            }
            Ok(_) => panic!("expected error for out-of-sandbox read"),
        }
    }
}
