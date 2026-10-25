use crate::process::detach;
use schemars::JsonSchema;
use serde::Serialize;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process;
use std::process::Stdio;
use std::str;
use std::time::{Duration, UNIX_EPOCH};
use tokio::process::Command;
use wind_core::sandbox::Sandbox;

/// 单次读文件的默认上限，单位为字节
const MAX_READ_BYTES: usize = 1024 * 1024;
/// 子进程单条输出流的上限，单位为字节
const MAX_OUTPUT_BYTES: usize = 32 * 1024;

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
    /// 扫描本身失败时的原因，正常时为 `None`
    pub error: Option<String>,
}

impl ListDirResult {
    pub fn failure(path: &Path, desc: String) -> Self {
        ListDirResult {
            path: path.to_string_lossy().into_owned(),
            entries: vec![],
            recursive: false,
            truncated: false,
            total: 0,
            error: Some(desc),
        }
    }

    pub fn collect(
        root: &Path,
        dir: &Path,
        recursive: bool,
        max_depth: u16,
        depth: u16,
        entries: &mut Vec<DirEntry>,
        total: &mut usize,
    ) {
        let mut items: Vec<(PathBuf, fs::FileType)> = match fs::read_dir(dir) {
            Ok(entries) => entries
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
#[derive(Debug, Serialize, JsonSchema, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
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
    /// 文件的类型描述
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<String>,
}

impl ReadFileResult {
    pub fn failure(path: &Path, desc: String) -> Self {
        ReadFileResult {
            path: path.to_string_lossy().into_owned(),
            content: desc,
            offset: 0,
            limit: 0,
            truncated: false,
            bytes: 0,
            meta: None,
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
    /// 写入失败时的原因，正常时为 `None`
    pub error: Option<String>,
}

impl WriteFileResult {
    pub fn failure(path: &Path, desc: String) -> Self {
        WriteFileResult {
            path: path.to_string_lossy().into_owned(),
            bytes: 0,
            error: Some(desc),
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
    /// 退出码，被信号终止或未能启动时为 `None`
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
    /// 未能启动时的原因，正常时为 `None`
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
    sandbox: &Sandbox,
    path: PathBuf,
    recursive: Option<bool>,
    max_depth: Option<u16>,
) -> ListDirResult {
    let root = match sandbox.resolve(&path) {
        Ok(resolved) => resolved,
        Err(error) => {
            return ListDirResult::failure(&path, error.to_string());
        }
    };
    if !root.is_dir() {
        return ListDirResult::failure(&root, String::from("path is a file"));
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
    sandbox: &Sandbox,
    path: PathBuf,
    offset: Option<u64>,
    limit: Option<u64>,
) -> ReadFileResult {
    let resolved = match sandbox.resolve(&path) {
        Ok(resolved) => resolved,
        Err(error) => {
            return ReadFileResult::failure(&path, error.to_string());
        }
    };
    if resolved.is_dir() {
        return ReadFileResult::failure(&resolved, String::from("path is a directory"));
    }
    let total = match fs::metadata(&resolved) {
        Ok(meta) => meta.len(),
        Err(error) => return ReadFileResult::failure(&resolved, error.to_string()),
    };
    let path_str = resolved.to_string_lossy().into_owned();
    if total == 0 {
        return ReadFileResult::failure(&resolved, String::from("empty content"));
    }

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
            meta: None,
        };
    }

    let file_meta = probe_file(&resolved);
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
            meta: None,
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
        meta: Some(file_meta),
    }
}

pub fn write_file(sandbox: &Sandbox, path: PathBuf, data: String) -> WriteFileResult {
    let resolved = match sandbox.resolve(&path) {
        Ok(resolved) => resolved,
        Err(error) => {
            return WriteFileResult::failure(&path, error.to_string());
        }
    };
    if resolved.is_dir() {
        return WriteFileResult::failure(&resolved, String::from("path is a directory"));
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
    sandbox: &Sandbox,
    command: String,
    cwd: String,
    args: Option<Vec<String>>,
    env: Option<Vec<String>>,
    timeout: u64,
) -> ExecResult {
    let cwd_path = match sandbox.resolve(Path::new(&cwd)) {
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
    detach(cmd.as_std_mut());

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

    let child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => {
            let mut result = ExecResult::empty(command, cwd_str);
            result.error = Some(error.to_string());
            return result;
        }
    };

    let output =
        tokio::time::timeout(Duration::from_millis(timeout), child.wait_with_output()).await;

    match output {
        Ok(Ok(output)) => {
            let (stdout, truncated_out) = truncate_output(&output.stdout);
            let (stderr, truncated_err) = truncate_output(&output.stderr);
            ExecResult {
                stdout,
                stderr,
                exit_code: output.status.code(),
                timed_out: false,
                cmd: command,
                cwd: cwd_str,
                truncated_out,
                truncated_err,
                error: None,
            }
        }
        Ok(Err(error)) => {
            let mut result = ExecResult::empty(command, cwd_str);
            result.error = Some(error.to_string());
            result
        }
        Err(_) => {
            let mut result = ExecResult::empty(command, cwd_str);
            result.timed_out = true;
            result
        }
    }
}

#[cfg(unix)]
fn probe_file(path: &Path) -> String {
    file_description(path)
}

#[cfg(windows)]
fn probe_file(path: &Path) -> String {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some(extension) => format!(".{extension}"),
        None => String::from("binary file"),
    }
}

#[cfg(unix)]
fn file_output(path: &Path, extra_args: &[&str]) -> String {
    let mut cmd = process::Command::new("file");
    cmd.arg("--brief");
    cmd.args(extra_args);
    cmd.arg(path);
    detach(&mut cmd);
    let output = match cmd.output() {
        Ok(output) => output,
        Err(error) => return error.to_string(),
    };
    if !output.status.success() {
        return String::new();
    }
    String::from_utf8(output.stdout)
        .map(|description| description.trim().to_string())
        .unwrap_or_default()
}

#[cfg(unix)]
fn file_description(path: &Path) -> String {
    file_output(path, &[])
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

fn truncate_output(bytes: &[u8]) -> (String, bool) {
    let truncated = bytes.len() > MAX_OUTPUT_BYTES;
    let slice = &bytes[..bytes.len().min(MAX_OUTPUT_BYTES)];
    (String::from_utf8_lossy(slice).into_owned(), truncated)
}

#[cfg(test)]
mod tests {
    use super::*;
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
            let path =
                std::env::temp_dir().join(format!("wind-fs-{label}-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        /// 写一个文件，父目录一并创建
        fn write(&self, relative: &str, content: &[u8]) {
            let path = self.path.join(relative_path(relative));
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create parent dir");
            }
            fs::write(path, content).expect("write file");
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    /// 把 `/` 分隔的相对路径按平台分隔符拼起来
    fn relative_path(relative: &str) -> PathBuf {
        relative.split('/').collect()
    }

    /// 取得 `relative_path` 的字符串形式，用于与扫描返回的相对路径比对
    ///
    /// 扫描返回的路径去掉前缀，使用平台原生分隔符
    fn relative_name(relative: &str) -> String {
        relative_path(relative).to_string_lossy().into_owned()
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    /// 以给定临时目录为唯一根的沙箱
    fn sandbox(dir: &TempDir) -> Sandbox {
        Sandbox::new(vec![dir.path().to_path_buf()])
    }

    #[test]
    fn list_dir_collects_entries() {
        let dir = TempDir::new("list");
        dir.write("a.txt", b"aaa");
        dir.write("sub/b.txt", b"bbb");
        let nested = relative_name("sub/b.txt");

        let sb = sandbox(&dir);
        let result = list_dir(&sb, dir.path().to_path_buf(), None, None);

        assert_eq!(result.error, None);
        assert_eq!(result.total, 3);
        let names: Vec<&str> = result
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        assert!(names.contains(&"a.txt"));
        assert!(names.contains(&"sub"));
        assert!(names.contains(&nested.as_str()));
    }

    #[test]
    fn list_dir_non_recursive() {
        let dir = TempDir::new("list-nr");
        dir.write("a.txt", b"aaa");
        dir.write("sub/b.txt", b"bbb");
        let nested = relative_name("sub/b.txt");

        let sb = sandbox(&dir);
        let result = list_dir(&sb, dir.path().to_path_buf(), Some(false), None);

        assert_eq!(result.error, None);
        assert_eq!(result.total, 2);
        let names: Vec<&str> = result
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        assert!(names.contains(&"a.txt"));
        assert!(names.contains(&"sub"));
        assert!(!names.contains(&nested.as_str()));
    }

    #[test]
    fn list_dir_ignores_hidden_and_large_dirs() {
        let dir = TempDir::new("list-ignore");
        dir.write("a.txt", b"aaa");
        dir.write(".git/config", b"cfg");
        dir.write("node_modules/x.js", b"x");
        dir.write(".skills/SKILL.md", b"skill");

        let sb = sandbox(&dir);
        let result = list_dir(&sb, dir.path().to_path_buf(), None, None);

        assert_eq!(result.error, None);
        let names: Vec<&str> = result
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        assert!(names.contains(&"a.txt"));
        assert!(names.contains(&".skills"));
        assert!(!names.contains(&".git"));
        assert!(!names.contains(&"node_modules"));
    }

    #[test]
    fn read_text_file() {
        let dir = TempDir::new("read-text");
        dir.write("hello.txt", "hello, world".as_bytes());

        let sb = sandbox(&dir);
        let result = read_file(&sb, dir.path().join("hello.txt"), None, None);

        assert_eq!(result.content, "hello, world");
        assert_eq!(result.offset, 0);
        assert_eq!(result.limit, 12);
        assert_eq!(result.bytes, 12);
        assert!(!result.truncated);
    }

    #[test]
    fn read_file_respects_offset() {
        let dir = TempDir::new("read-offset");
        dir.write("hello.txt", b"hello world");

        let sb = sandbox(&dir);
        let result = read_file(&sb, dir.path().join("hello.txt"), Some(6), None);

        assert_eq!(result.content, "world");
        assert_eq!(result.offset, 6);
        assert_eq!(result.limit, 5);
        assert!(!result.truncated);
    }

    #[test]
    fn read_file_respects_limit() {
        let dir = TempDir::new("read-limit");
        dir.write("hello.txt", b"hello world");

        let sb = sandbox(&dir);
        let result = read_file(&sb, dir.path().join("hello.txt"), None, Some(5));

        assert_eq!(result.content, "hello");
        assert_eq!(result.offset, 0);
        assert_eq!(result.limit, 5);
        assert!(result.truncated);
    }

    #[test]
    fn read_file_does_not_split_utf8_char() {
        let dir = TempDir::new("read-utf8-limit");
        dir.write("hello.txt", "你好世界".as_bytes());

        let sb = sandbox(&dir);
        // 上限 4 切在「好」的中间，结束位置应裁剪到字符边界
        let result = read_file(&sb, dir.path().join("hello.txt"), None, Some(4));

        assert_eq!(result.content, "你");
        assert_eq!(result.offset, 0);
        assert_eq!(result.limit, 3);
        assert!(result.truncated);
    }

    #[test]
    fn read_file_aligns_offset_to_char_boundary() {
        let dir = TempDir::new("read-utf8-offset");
        dir.write("hello.txt", "你好世界".as_bytes());

        let sb = sandbox(&dir);
        // 起始偏移 4 切在「好」的中间，应向前对齐到字符边界（字节 3）
        let result = read_file(&sb, dir.path().join("hello.txt"), Some(4), None);

        assert_eq!(result.content, "好世界");
        assert_eq!(result.offset, 3);
        assert_eq!(result.limit, 9);
        assert!(!result.truncated);
    }

    #[test]
    fn read_file_offset_beyond_eof_returns_empty() {
        let dir = TempDir::new("read-eof");
        dir.write("hello.txt", b"hello");

        let sb = sandbox(&dir);
        let result = read_file(&sb, dir.path().join("hello.txt"), Some(100), None);

        assert_eq!(result.content, "");
        assert_eq!(result.offset, 5);
        assert_eq!(result.limit, 0);
        assert!(!result.truncated);
    }

    #[test]
    fn read_empty_file_fails() {
        let dir = TempDir::new("read-empty");
        dir.write("empty.txt", b"");

        let sb = sandbox(&dir);
        let result = read_file(&sb, dir.path().join("empty.txt"), None, None);

        assert_eq!(result.bytes, 0);
        assert_eq!(result.content, "empty content");
    }

    #[test]
    fn read_dir_fails() {
        let dir = TempDir::new("read-dir");
        dir.write("a.txt", b"a");

        let sb = sandbox(&dir);
        let result = read_file(&sb, dir.path().to_path_buf(), None, None);

        assert_eq!(result.bytes, 0);
        assert_eq!(result.content, "path is a directory");
    }

    #[test]
    fn write_creates_parents_and_reads_back() {
        let dir = TempDir::new("write");
        let sb = sandbox(&dir);

        let written = write_file(
            &sb,
            dir.path().join(relative_path("x/y.txt")),
            "hello".into(),
        );
        assert_eq!(written.error, None);
        assert_eq!(written.bytes, 5);

        let read = read_file(&sb, dir.path().join(relative_path("x/y.txt")), None, None);
        assert_eq!(read.content, "hello");
    }

    #[test]
    fn read_outside_sandbox_denied() {
        let inside = TempDir::new("inside");
        let outside = TempDir::new("outside");
        outside.write("secret.txt", b"top secret");

        let sb = sandbox(&inside);
        let result = read_file(&sb, outside.path().join("secret.txt"), None, None);

        assert_eq!(result.bytes, 0);
        assert!(!result.content.is_empty());
    }

    #[tokio::test]
    async fn exec_runs_command() {
        let dir = TempDir::new("exec");
        let sb = sandbox(&dir);

        #[cfg(windows)]
        let (command, args) = ("powershell.exe", vec!["-Command", "echo", "hello"]);
        #[cfg(unix)]
        let (command, args) = ("echo", vec!["hello"]);

        let result = exec(
            &sb,
            command.to_string(),
            dir.path().to_string_lossy().into_owned(),
            Some(args.into_iter().map(String::from).collect()),
            None,
            5000,
        )
        .await;

        assert_eq!(result.stdout.trim(), "hello");
        assert_eq!(result.exit_code, Some(0));
        assert!(!result.timed_out);
        assert!(result.error.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_runs_in_its_own_process_group() {
        let dir = TempDir::new("exec-group");
        let sb = sandbox(&dir);

        let result = exec(
            &sb,
            "sh".to_string(),
            dir.path().to_string_lossy().into_owned(),
            // 打印子进程自己的进程号与进程组号，两者在组长上相等
            Some(vec![
                "-c".to_string(),
                "echo $$; ps -o pgid= -p $$".to_string(),
            ]),
            None,
            5000,
        )
        .await;

        assert!(result.error.is_none(), "got {result:?}");
        let mut lines = result
            .stdout
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty());
        let pid = lines.next().expect("read the child pid");
        let pgid = lines.next().expect("read the child pgid");
        assert_eq!(
            pgid, pid,
            "the child forms its own group and is the leader of that group"
        );
    }

    #[tokio::test]
    async fn exec_outside_cwd_denied() {
        let inside = TempDir::new("exec-in");
        let outside = TempDir::new("exec-out");

        let sb = sandbox(&inside);
        let result = exec(
            &sb,
            "echo".to_string(),
            outside.path().to_string_lossy().into_owned(),
            None,
            None,
            1000,
        )
        .await;

        let error = result.error.expect("reject cwd outside the sandbox");
        assert!(error.contains("not allowed"), "got: {error}");
    }

    #[tokio::test]
    async fn exec_command_not_found_folds_error() {
        let dir = TempDir::new("exec-missing");
        let sb = sandbox(&dir);

        let result = exec(
            &sb,
            "definitely-not-a-real-cmd".to_string(),
            dir.path().to_string_lossy().into_owned(),
            None,
            None,
            5000,
        )
        .await;

        assert_eq!(result.exit_code, None);
        assert!(result.error.is_some());
        assert!(!result.timed_out);
    }

    #[cfg(unix)]
    #[test]
    fn write_through_dangling_symlink_denied() {
        let inside = TempDir::new("dangling-in");
        let outside = TempDir::new("dangling-out");
        let sb = sandbox(&inside);

        // 悬空软链接。inside/evil 指向 outside/newfile，目标不存在
        std::os::unix::fs::symlink(outside.path().join("newfile"), inside.path().join("evil"))
            .expect("create dangling symlink");

        let written = write_file(&sb, inside.path().join("evil"), "owned".to_string());
        assert!(written.error.is_some(), "got: {written:?}");
        assert!(!outside.path().join("newfile").exists());
    }

    #[cfg(unix)]
    #[test]
    fn write_through_dangling_symlink_dir_denied() {
        let inside = TempDir::new("dangling-dir-in");
        let outside = TempDir::new("dangling-dir-out");
        let sb = sandbox(&inside);

        // 悬空软链接目录。inside/evil 指向 outside/nonexistent，目录不存在
        std::os::unix::fs::symlink(
            outside.path().join("nonexistent"),
            inside.path().join("evil"),
        )
        .expect("create dangling symlink");

        let written = write_file(
            &sb,
            inside.path().join(relative_path("evil/sub/f.txt")),
            "owned".to_string(),
        );
        assert!(written.error.is_some(), "got: {written:?}");
        assert!(!outside.path().join("nonexistent").exists());
    }

    #[cfg(unix)]
    #[test]
    fn list_dir_does_not_follow_symlink_outside() {
        let inside = TempDir::new("list-leak-in");
        let outside = TempDir::new("list-leak-out");
        outside.write("secret.txt", b"top secret");
        let sb = sandbox(&inside);

        // inside/leak 指向沙箱之外的目录
        std::os::unix::fs::symlink(outside.path(), inside.path().join("leak"))
            .expect("create symlink");

        let result = list_dir(&sb, inside.path().to_path_buf(), None, None);
        let names: Vec<&str> = result
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        assert!(names.contains(&"leak"), "leak entry should exist");
        assert!(
            !names.contains(&relative_name("leak/secret.txt").as_str()),
            "must not recurse into symlink: {names:?}"
        );
        let leak = result
            .entries
            .iter()
            .find(|entry| entry.name == "leak")
            .expect("find the leak entry");
        assert!(matches!(leak.kind, EntryKind::Symlink));
    }

    #[cfg(unix)]
    #[test]
    fn write_through_internal_symlink_ok() {
        let inside = TempDir::new("internal-link");
        let sb = sandbox(&inside);

        // 沙箱内的软链接。inside/link 指向已存在的 inside/real
        fs::create_dir_all(inside.path().join("real")).expect("create real dir");
        std::os::unix::fs::symlink("real", inside.path().join("link")).expect("create symlink");

        let written = write_file(
            &sb,
            inside.path().join(relative_path("link/file.txt")),
            "hello".to_string(),
        );
        assert_eq!(written.error, None);
        assert_eq!(written.bytes, 5);
        assert!(inside.path().join(relative_path("real/file.txt")).exists());
    }

    #[cfg(unix)]
    #[test]
    fn resolve_symlink_loop_errors() {
        let inside = TempDir::new("loop");
        let sb = sandbox(&inside);

        // a 指向 b，b 指向 a
        std::os::unix::fs::symlink("b", inside.path().join("a")).expect("symlink a");
        std::os::unix::fs::symlink("a", inside.path().join("b")).expect("symlink b");

        let written = write_file(&sb, inside.path().join("a"), "x".to_string());
        assert!(
            written.error.is_some(),
            "symlink loop must error: {written:?}"
        );
    }
}
