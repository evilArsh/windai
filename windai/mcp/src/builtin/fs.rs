use super::workspace::WorkspaceInfo;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::{ErrorData, ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

mod ops;
mod sandbox;

use ops::{
    ExecResult, ListDirResult, ReadFileResult, WriteFileResult, exec, list_dir, read_file,
    write_file,
};
pub use sandbox::FsError;
use sandbox::Snapshot;

impl From<FsError> for ErrorData {
    fn from(e: FsError) -> Self {
        let code = e.as_ref().to_string();
        ErrorData::internal_error(e.to_string(), Some(serde_json::json!({ "code": code })))
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct ListDirRequest {
    /// 要扫描的目录绝对路径
    pub path: String,
    /// 是否递归子目录，默认 true
    pub recursive: Option<bool>,
    /// 允许递归的子目录层数，默认不限
    pub max_depth: Option<u16>,
}

#[derive(Deserialize, JsonSchema)]
pub struct ReadFileRequest {
    /// 文件绝对路径
    pub path: String,
    /// 字节偏移,默认 0.
    pub offset: Option<u64>,
    /// 读取字节上限,默认 1MB.
    pub limit: Option<u64>,
}

#[derive(Deserialize, JsonSchema)]
pub struct WriteFileRequest {
    /// 文件绝对路径
    pub path: String,
    /// 要写入的文本内容
    pub data: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct ExecRequest {
    /// 可执行程序名（argv[0]）
    pub command: String,
    /// 工作目录绝对路径，必须在沙箱内
    pub cwd: String,
    /// 命令行参数（argv[1..]），优先于 shell 字符串以防注入
    pub args: Option<Vec<String>>,
    /// 环境变量，形如 KEY=value
    pub env: Option<Vec<String>>,
    /// 超时时间（毫秒）
    pub timeout: u64,
}

/// 文件与进程能力服务
pub struct FsServer {
    name: String,
    info: Arc<RwLock<WorkspaceInfo>>,
    tool_router: ToolRouter<Self>,
}

impl FsServer {
    pub fn new(name: impl Into<String>, info: Arc<RwLock<WorkspaceInfo>>) -> Self {
        Self {
            name: name.into(),
            info,
            tool_router: Self::tool_router(),
        }
    }

    fn snapshot(&self) -> sandbox::Result<Snapshot> {
        let roots = self
            .info
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .roots();
        Snapshot::new(roots)
    }
}

#[tool_router(router = tool_router)]
impl FsServer {
    /// 扫描目录，返回根目录路径与文件/目录相对路径清单。忽略隐藏目录（白名单除外）与 node_modules 等大目录
    #[tool(
        name = "list_dir",
        description = "Scan a directory, returning the root path and a relative-path listing of files/dirs. Hidden dirs (except whitelisted) and large dependency dirs are skipped."
    )]
    async fn list_dir(&self, Parameters(req): Parameters<ListDirRequest>) -> Json<ListDirResult> {
        let path = PathBuf::from(req.path);
        let snapshot = match self.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => return Json(ListDirResult::failure(&path, error.to_string())),
        };
        let task_path = path.clone();
        let recursive = req.recursive;
        let max_depth = req.max_depth;
        let result = tokio::task::spawn_blocking(move || {
            list_dir(&snapshot, task_path, recursive, max_depth)
        })
        .await;
        match result {
            Ok(result) => Json(result),
            Err(error) => Json(ListDirResult::failure(
                &path,
                format!("the blocking task failed: {error}"),
            )),
        }
    }

    /// 读文本文件；二进制文件仅返回元信息
    #[tool(
        name = "read_file",
        description = "Read a text file (UTF-8). Binary files return only metadata."
    )]
    async fn read_file(
        &self,
        Parameters(req): Parameters<ReadFileRequest>,
    ) -> Json<ReadFileResult> {
        let path = PathBuf::from(req.path);
        let snapshot = match self.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => return Json(ReadFileResult::failure(&path, error.to_string())),
        };
        let task_path = path.clone();
        let offset = req.offset;
        let limit = req.limit;
        let result =
            tokio::task::spawn_blocking(move || read_file(&snapshot, task_path, offset, limit))
                .await;
        match result {
            Ok(result) => Json(result),
            Err(error) => Json(ReadFileResult::failure(
                &path,
                format!("the blocking task failed: {error}"),
            )),
        }
    }

    /// 写文本文件，自动创建父目录
    #[tool(
        name = "write_file",
        description = "Write a text file, creating parent directories as needed."
    )]
    async fn write_file(
        &self,
        Parameters(req): Parameters<WriteFileRequest>,
    ) -> Json<WriteFileResult> {
        let path = PathBuf::from(req.path);
        let snapshot = match self.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => return Json(WriteFileResult::failure(&path, error.to_string())),
        };
        let task_path = path.clone();
        let data = req.data;
        let result =
            tokio::task::spawn_blocking(move || write_file(&snapshot, task_path, data)).await;
        match result {
            Ok(result) => Json(result),
            Err(error) => Json(WriteFileResult::failure(
                &path,
                format!("the blocking task failed: {error}"),
            )),
        }
    }

    /// 执行子进程；cwd 必须在沙箱内，timeout 单位毫秒
    #[tool(
        name = "exec",
        description = "Execute a subprocess. cwd must be inside the sandbox; timeout is in milliseconds."
    )]
    async fn exec(&self, Parameters(req): Parameters<ExecRequest>) -> Json<ExecResult> {
        let ExecRequest {
            command,
            cwd,
            args,
            env,
            timeout,
        } = req;
        match self.snapshot() {
            Ok(snapshot) => Json(exec(&snapshot, command, cwd, args, env, timeout).await),
            Err(error) => {
                let mut result = ExecResult::empty(command, cwd);
                result.error = Some(error.to_string());
                Json(result)
            }
        }
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for FsServer {
    fn get_info(&self) -> ServerConfig {
        let mut server_info = Implementation::new(self.name.clone(), env!("CARGO_PKG_VERSION"));
        server_info.description = Some("file and process capability".to_owned());
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(server_info)
    }
}

/// 服务名里的固定段
pub(super) const SLOT: &str = "fs";

#[cfg(test)]
mod tests {
    use super::{FsServer, WorkspaceInfo};
    use std::fs;
    use std::sync::{Arc, RwLock};
    use tempfile::TempDir;

    #[test]
    fn acquired_snapshot_survives_workspace_update() {
        let old = TempDir::new().expect("create the old workspace");
        let new = TempDir::new().expect("create the new workspace");
        let info = Arc::new(RwLock::new(WorkspaceInfo::new(
            1,
            old.path().to_path_buf(),
            Vec::new(),
            Vec::new(),
        )));
        let server = FsServer::new("fs", info.clone());
        let acquired = server.snapshot().expect("acquire the initial snapshot");
        *info
            .try_write()
            .expect("update without waiting for the call") =
            WorkspaceInfo::new(1, new.path().to_path_buf(), Vec::new(), Vec::new());
        let next = server.snapshot().expect("acquire the updated snapshot");

        assert!(acquired.resolve(old.path()).is_ok());
        assert!(acquired.resolve(new.path()).is_err());
        assert!(next.resolve(old.path()).is_err());
        assert!(next.resolve(new.path()).is_ok());
    }

    #[test]
    fn replacing_published_root_with_symlink_does_not_expand_access() {
        let tmp = TempDir::new().expect("create a temp dir");
        let root = tmp.path().join("workspace");
        let outside = tmp.path().join("outside");
        fs::create_dir_all(&root).expect("create the workspace");
        fs::create_dir_all(&outside).expect("create the outside directory");
        let info = Arc::new(RwLock::new(WorkspaceInfo::new(
            1,
            root.clone(),
            Vec::new(),
            Vec::new(),
        )));
        let server = FsServer::new("fs", info);
        assert!(
            server
                .snapshot()
                .expect("read the published roots")
                .resolve(&root)
                .is_ok()
        );

        fs::remove_dir(&root).expect("remove the empty workspace");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, &root).expect("link the outside directory");
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(&outside, &root).expect("link the outside directory");

        let next = server.snapshot().expect("read the published roots again");
        assert!(
            next.resolve(&outside).is_err(),
            "the outside directory must stay denied"
        );
        assert!(
            next.resolve(&root).is_err(),
            "the replaced root must stay denied"
        );
    }
}
