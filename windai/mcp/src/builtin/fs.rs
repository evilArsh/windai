mod error;
mod ops;

pub use ops::{DirEntry, EntryKind, ExecResult, ListDirResult, ReadFileResult, WriteFileResult};

use std::path::PathBuf;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;

use super::paths::Paths;

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
    /// 可执行程序
    pub command: String,
    /// 工作目录绝对路径，必须在允许的根目录内
    pub cwd: String,
    /// 命令行参数，直接传递而不拼接 shell 字符串
    pub args: Option<Vec<String>>,
    /// 环境变量，形如 KEY=value
    pub env: Option<Vec<String>>,
    /// 超时时间，单位为毫秒
    pub timeout: u64,
}

/// 文件操作工具
pub struct FsServer {
    paths: Paths,
    tool_router: ToolRouter<Self>,
}

impl FsServer {
    pub fn new(roots: Vec<PathBuf>) -> Self {
        Self {
            paths: Paths::new(roots),
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router(router = tool_router)]
impl FsServer {
    #[tool(
        name = "list_dir",
        description = "Scan a directory and return its entries relative to the root. Hidden and dependency directories are skipped."
    )]
    async fn list_dir(
        &self,
        Parameters(request): Parameters<ListDirRequest>,
    ) -> Json<ListDirResult> {
        let paths = self.paths.clone();
        let ListDirRequest {
            path,
            recursive,
            max_depth,
        } = request;
        let path = PathBuf::from(path);
        let failed = path.clone();
        let result =
            tokio::task::spawn_blocking(move || ops::list_dir(&paths, path, recursive, max_depth))
                .await
                .unwrap_or_else(|error| ListDirResult::failure(&failed, error.to_string()));
        Json(result)
    }

    #[tool(
        name = "read_file",
        description = "Read a UTF-8 text file with a byte offset and a byte limit."
    )]
    async fn read_file(
        &self,
        Parameters(request): Parameters<ReadFileRequest>,
    ) -> Json<ReadFileResult> {
        let paths = self.paths.clone();
        let ReadFileRequest {
            path,
            offset,
            limit,
        } = request;
        let path = PathBuf::from(path);
        let failed = path.clone();
        let result =
            tokio::task::spawn_blocking(move || ops::read_file(&paths, path, offset, limit))
                .await
                .unwrap_or_else(|error| ReadFileResult::failure(&failed, error.to_string()));
        Json(result)
    }

    #[tool(
        name = "write_file",
        description = "Write a text file, creating missing parent directories."
    )]
    async fn write_file(
        &self,
        Parameters(request): Parameters<WriteFileRequest>,
    ) -> Json<WriteFileResult> {
        let paths = self.paths.clone();
        let WriteFileRequest { path, data } = request;
        let path = PathBuf::from(path);
        let failed = path.clone();
        let result = tokio::task::spawn_blocking(move || ops::write_file(&paths, path, data))
            .await
            .unwrap_or_else(|error| WriteFileResult::failure(&failed, error.to_string()));
        Json(result)
    }

    #[tool(
        name = "exec",
        description = "Run a subprocess with explicit arguments. The cwd must be inside the allowed roots; timeout is in milliseconds."
    )]
    async fn exec(&self, Parameters(request): Parameters<ExecRequest>) -> Json<ExecResult> {
        let ExecRequest {
            command,
            cwd,
            args,
            env,
            timeout,
        } = request;
        Json(ops::exec(&self.paths, command, cwd, args, env, timeout).await)
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for FsServer {}
