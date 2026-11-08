use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::str;
use std::time::{Duration, UNIX_EPOCH};

use schemars::JsonSchema;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::task::JoinHandle;

use crate::builtin::paths::Paths;

/// 单次读文件的默认上限，单位为字节
pub const MAX_READ_BYTES: usize = 1024 * 1024;
/// 子进程单条输出流的上限，单位为字节
pub const MAX_OUTPUT_BYTES: usize = 32 * 1024;
/// 输出流收集在进程退出后的等待上限
const CAPTURE_GRACE: Duration = Duration::from_secs(2);

const IGNORED_DIRS: &[&str] = &[
    "node_modules",
    "target",
    "dist",
    "build",
    "__pycache__",
    "vendor",
    "coverage",
    "out",
    "venv",
];

const ALLOWED_HIDDEN_DIRS: &[&str] = &[".skills", ".agents"];

fn is_ignored_dir(name: &str) -> bool {
    if name.starts_with('.') {
        return !ALLOWED_HIDDEN_DIRS.contains(&name);
    }
    IGNORED_DIRS.contains(&name)
}

/// 目录扫描的结果
#[derive(Debug, Serialize, JsonSchema)]
pub struct ListDirResult {
    /// 扫描根的绝对路径
    pub path: String,
    /// 相对根目录的条目
    pub entries: Vec<DirEntry>,
    /// 是否递归子目录
    pub recursive: bool,
    /// 条目是否被截断
    pub truncated: bool,
    /// 条目总数
    pub total: usize,
    /// 扫描失败时的原因
    pub error: Option<String>,
}

impl ListDirResult {
    pub fn failure(path: &Path, error: String) -> Self {
        Self {
            path: path.to_string_lossy().into_owned(),
            entries: Vec::new(),
            recursive: false,
            truncated: false,
            total: 0,
            error: Some(error),
        }
    }

    fn collect(
        root: &Path,
        dir: &Path,
        recursive: bool,
        max_depth: u16,
        depth: u16,
        entries: &mut Vec<DirEntry>,
        total: &mut usize,
    ) {
        let mut items: Vec<(PathBuf, fs::FileType)> = match fs::read_dir(dir) {
            Ok(items) => items
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| entry.file_type().ok().map(|kind| (entry.path(), kind)))
                .collect(),
            Err(_) => return,
        };
        items.sort_by(|left, right| left.0.cmp(&right.0));

        for (path, kind) in items {
            let is_symlink = kind.is_symlink();
            let is_dir = kind.is_dir();

            if is_dir
                && path
                    .file_name()
                    .is_some_and(|name| is_ignored_dir(&name.to_string_lossy()))
            {
                continue;
            }

            let name = match path.strip_prefix(root) {
                Ok(relative) => relative.to_string_lossy().into_owned(),
                Err(_) => path.to_string_lossy().into_owned(),
            };
            let entry_kind = if is_symlink {
                EntryKind::Symlink
            } else if is_dir {
                EntryKind::Dir
            } else {
                EntryKind::File
            };
            let (size, mtime) = if is_symlink {
                (None, None)
            } else {
                let meta = fs::metadata(&path).ok();
                (
                    meta.as_ref()
                        .and_then(|meta| meta.is_file().then_some(meta.len())),
                    meta.as_ref()
                        .and_then(|meta| meta.modified().ok())
                        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                        .map(|since| since.as_secs()),
                )
            };

            entries.push(DirEntry {
                name,
                kind: entry_kind,
                size,
                mtime,
            });
            *total += 1;

            if is_dir && recursive && depth < max_depth {
                Self::collect(root, &path, recursive, max_depth, depth + 1, entries, total);
            }
        }
    }
}

/// 目录条目
#[derive(Debug, Serialize, JsonSchema)]
pub struct DirEntry {
    /// 相对扫描根目录的路径
    pub name: String,
    /// 条目类型
    pub kind: EntryKind,
    /// 文件字节数，目录与软链接没有
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// 最后修改时刻，epoch 秒
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mtime: Option<u64>,
}

/// 目录条目的类型
#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    /// 普通文件
    File,
    /// 目录
    Dir,
    /// 软链接
    Symlink,
}

/// 读文件的结果
#[derive(Debug, Serialize, JsonSchema)]
pub struct ReadFileResult {
    /// 读取到的文件绝对路径
    pub path: String,
    /// 读到的文本，失败时是失败原因
    pub content: String,
    /// 起点在文件中的字节偏移
    pub offset: u64,
    /// 实际读取的字节数
    pub limit: u64,
    /// 文件是否还有未读取的部分
    pub truncated: bool,
    /// 文件总字节数
    pub bytes: u64,
}

impl ReadFileResult {
    pub fn failure(path: &Path, error: String) -> Self {
        Self {
            path: path.to_string_lossy().into_owned(),
            content: error,
            offset: 0,
            limit: 0,
            truncated: false,
            bytes: 0,
        }
    }
}

/// 写文件的结果
#[derive(Debug, Serialize, JsonSchema)]
pub struct WriteFileResult {
    /// 写入的文件绝对路径
    pub path: String,
    /// 写入的字节数
    pub bytes: u64,
    /// 写入失败时的原因
    pub error: Option<String>,
}

impl WriteFileResult {
    pub fn failure(path: &Path, error: String) -> Self {
        Self {
            path: path.to_string_lossy().into_owned(),
            bytes: 0,
            error: Some(error),
        }
    }
}

/// 子进程执行的结果
#[derive(Debug, Serialize, JsonSchema)]
pub struct ExecResult {
    /// 标准输出
    pub stdout: String,
    /// 标准错误
    pub stderr: String,
    /// 退出码，被信号终止或未能启动时为空
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// 是否因为超时被放弃
    pub timed_out: bool,
    /// 执行的程序名
    pub cmd: String,
    /// 执行时的工作目录
    pub cwd: String,
    /// 标准输出是否被截断
    pub truncated_out: bool,
    /// 标准错误是否被截断
    pub truncated_err: bool,
    /// 未能启动时的原因
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ExecResult {
    fn empty(cmd: String, cwd: String) -> Self {
        Self {
            stdout: String::new(),
            stderr: String::new(),
            exit_code: None,
            timed_out: false,
            cmd,
            cwd,
            truncated_out: false,
            truncated_err: false,
            error: None,
        }
    }
}

pub fn list_dir(
    paths: &Paths,
    path: PathBuf,
    recursive: Option<bool>,
    max_depth: Option<u16>,
) -> ListDirResult {
    let root = match paths.resolve(&path) {
        Ok(resolved) => resolved,
        Err(error) => return ListDirResult::failure(&path, error.to_string()),
    };
    if !root.is_dir() {
        return ListDirResult::failure(&root, "path is a file".to_string());
    }
    let recursive = recursive.unwrap_or(true);
    let max_depth = max_depth.unwrap_or(u16::MAX);

    let mut entries = Vec::new();
    let mut total = 0usize;
    ListDirResult::collect(
        &root,
        &root,
        recursive,
        max_depth,
        0,
        &mut entries,
        &mut total,
    );

    ListDirResult {
        path: root.to_string_lossy().into_owned(),
        entries,
        recursive,
        truncated: false,
        total,
        error: None,
    }
}

/// `offset` 与 `limit` 均为字节
pub fn read_file(
    paths: &Paths,
    path: PathBuf,
    offset: Option<u64>,
    limit: Option<u64>,
) -> ReadFileResult {
    let resolved = match paths.resolve(&path) {
        Ok(resolved) => resolved,
        Err(error) => return ReadFileResult::failure(&path, error.to_string()),
    };
    if resolved.is_dir() {
        return ReadFileResult::failure(&resolved, "path is a directory".to_string());
    }
    let total = match fs::metadata(&resolved) {
        Ok(meta) => meta.len(),
        Err(error) => return ReadFileResult::failure(&resolved, error.to_string()),
    };
    let path_str = resolved.to_string_lossy().into_owned();

    let offset = offset.unwrap_or(0).min(total);
    let limit = limit.unwrap_or(MAX_READ_BYTES as u64);
    if offset >= total || limit == 0 {
        return ReadFileResult {
            path: path_str,
            content: String::new(),
            offset,
            limit: 0,
            truncated: false,
            bytes: total,
        };
    }

    let read_start = offset.saturating_sub(3);
    let read_end = offset.saturating_add(limit).min(total);

    let mut file = match fs::File::open(&resolved) {
        Ok(file) => file,
        Err(error) => return ReadFileResult::failure(&resolved, error.to_string()),
    };
    if let Err(error) = file.seek(SeekFrom::Start(read_start)) {
        return ReadFileResult::failure(&resolved, error.to_string());
    }
    let mut buf = vec![0u8; (read_end - read_start) as usize];
    let read = match file.read(&mut buf) {
        Ok(read) => read,
        Err(error) => return ReadFileResult::failure(&resolved, error.to_string()),
    };
    buf.truncate(read);
    if read == 0 {
        return ReadFileResult {
            path: path_str,
            content: String::new(),
            offset,
            limit: 0,
            truncated: false,
            bytes: total,
        };
    }

    let relative_offset = (offset - read_start) as usize;
    let start = char_boundary_down(&buf, relative_offset);
    let end = start + utf8_valid_prefix(&buf[start..]);

    let content = String::from_utf8_lossy(&buf[start..end]).into_owned();
    let actual_offset = read_start + start as u64;
    let actual_limit = (end - start) as u64;
    let truncated = actual_offset.saturating_add(actual_limit) < total;

    ReadFileResult {
        path: path_str,
        content,
        offset: actual_offset,
        limit: actual_limit,
        truncated,
        bytes: total,
    }
}

pub fn write_file(paths: &Paths, path: PathBuf, data: String) -> WriteFileResult {
    let resolved = match paths.resolve(&path) {
        Ok(resolved) => resolved,
        Err(error) => return WriteFileResult::failure(&path, error.to_string()),
    };
    if resolved.is_dir() {
        return WriteFileResult::failure(&resolved, "path is a directory".to_string());
    }
    if let Some(parent) = resolved.parent()
        && let Err(error) = fs::create_dir_all(parent)
    {
        return WriteFileResult::failure(&path, error.to_string());
    }

    let bytes = data.len() as u64;
    match fs::write(&resolved, data.as_bytes()) {
        Ok(_) => WriteFileResult {
            path: resolved.to_string_lossy().into_owned(),
            bytes,
            error: None,
        },
        Err(error) => WriteFileResult::failure(&path, error.to_string()),
    }
}

/// `timeout` 以毫秒计
pub async fn exec(
    paths: &Paths,
    command: String,
    cwd: String,
    args: Option<Vec<String>>,
    env: Option<Vec<String>>,
    timeout: u64,
) -> ExecResult {
    let cwd_path = match paths.resolve(Path::new(&cwd)) {
        Ok(resolved) => resolved,
        Err(error) => {
            let mut result = ExecResult::empty(command, cwd);
            result.error = Some(error.to_string());
            return result;
        }
    };
    let cwd_str = cwd_path.to_string_lossy().into_owned();
    if !cwd_path.is_dir() {
        let mut result = ExecResult::empty(command, cwd_str.clone());
        result.error = Some(format!("cwd is not a directory: {cwd_str}"));
        return result;
    }

    let mut cmd = Command::new(&command);
    cmd.current_dir(&cwd_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(args) = args {
        cmd.args(args);
    }
    if let Some(envs) = env {
        for pair in envs {
            if let Some((key, value)) = pair.split_once('=') {
                cmd.env(key, value);
            }
        }
    }

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => {
            let mut result = ExecResult::empty(command, cwd_str);
            result.error = Some(error.to_string());
            return result;
        }
    };
    let out_task = tokio::spawn(capture(child.stdout.take(), MAX_OUTPUT_BYTES));
    let err_task = tokio::spawn(capture(child.stderr.take(), MAX_OUTPUT_BYTES));

    match tokio::time::timeout(Duration::from_millis(timeout), child.wait()).await {
        Ok(Ok(status)) => {
            let (stdout, truncated_out) = finish(out_task).await;
            let (stderr, truncated_err) = finish(err_task).await;
            ExecResult {
                stdout: String::from_utf8_lossy(&stdout).into_owned(),
                stderr: String::from_utf8_lossy(&stderr).into_owned(),
                exit_code: status.code(),
                timed_out: false,
                cmd: command,
                cwd: cwd_str,
                truncated_out,
                truncated_err,
                error: None,
            }
        }
        Ok(Err(error)) => {
            out_task.abort();
            err_task.abort();
            let mut result = ExecResult::empty(command, cwd_str);
            result.error = Some(error.to_string());
            result
        }
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = finish(out_task).await;
            let _ = finish(err_task).await;
            let mut result = ExecResult::empty(command, cwd_str);
            result.timed_out = true;
            result
        }
    }
}

async fn capture<R>(reader: Option<R>, limit: usize) -> (Vec<u8>, bool)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let Some(mut reader) = reader else {
        return (Vec::new(), false);
    };
    let mut output = Vec::new();
    let mut truncated = false;
    let mut buffer = [0u8; 8192];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(read) => {
                if output.len() < limit {
                    let take = read.min(limit - output.len());
                    output.extend_from_slice(&buffer[..take]);
                    truncated |= take < read;
                } else {
                    truncated = true;
                }
            }
            Err(_) => break,
        }
    }
    (output, truncated)
}

async fn finish(task: JoinHandle<(Vec<u8>, bool)>) -> (Vec<u8>, bool) {
    match tokio::time::timeout(CAPTURE_GRACE, task).await {
        Ok(Ok(value)) => value,
        _ => (Vec::new(), false),
    }
}

fn char_boundary_down(buf: &[u8], index: usize) -> usize {
    let mut index = index.min(buf.len());
    while index > 0 {
        if index == buf.len() || (buf[index] & 0xC0) != 0x80 {
            break;
        }
        index -= 1;
    }
    index
}

fn utf8_valid_prefix(buf: &[u8]) -> usize {
    match str::from_utf8(buf) {
        Ok(_) => buf.len(),
        Err(error) => error.valid_up_to(),
    }
}
