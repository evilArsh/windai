use super::sandbox::Sandbox;
use schemars::JsonSchema;
use serde::Serialize;
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, UNIX_EPOCH},
};
use tokio::process::Command;

/// 读文件单次默认上限（字节）。
const MAX_READ_BYTES: usize = 1024 * 1024;
/// 命令输出单流上限（字节）。
const MAX_OUTPUT_BYTES: usize = 32 * 1024;

/// 扫描时忽略的目录名（语言特定依赖/构建产物，体积大）。
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

/// 允许扫描的隐藏目录（以 `.` 开头）。
const ALLOWED_HIDDEN_DIRS: &[&str] = &[".skills", ".agents"];

/// 判断目录是否应被忽略：隐藏目录（白名单除外）与语言特定大目录。
fn is_ignored_dir(name: &str) -> bool {
    if name.starts_with('.') {
        return !ALLOWED_HIDDEN_DIRS.contains(&name);
    }
    IGNORED_DIRS.contains(&name)
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ListDirResult {
    pub path: String,
    pub entries: Vec<DirEntry>,
    pub recursive: bool,
    pub truncated: bool,
    pub total: usize,
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
        truncated: &mut bool,
    ) {
        let mut items: Vec<(PathBuf, fs::FileType)> = match fs::read_dir(dir) {
            Ok(rd) => rd
                .filter_map(|e| e.ok())
                .filter_map(|e| e.file_type().ok().map(|ft| (e.path(), ft)))
                .collect(),
            Err(_) => return,
        };
        items.sort_by(|a, b| a.0.cmp(&b.0));

        for (path, ft) in items {
            let is_symlink = ft.is_symlink();
            let is_dir = ft.is_dir();

            if is_dir
                && path
                    .file_name()
                    .is_some_and(|n| is_ignored_dir(&n.to_string_lossy()))
            {
                continue;
            }

            let name = match path.strip_prefix(root) {
                Ok(rel) => rel.to_string_lossy().into_owned(),
                Err(_) => path.to_string_lossy().into_owned(),
            };

            let kind = if is_symlink {
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
                    meta.as_ref().and_then(|m| m.is_file().then_some(m.len())),
                    meta.as_ref()
                        .and_then(|m| m.modified().ok())
                        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                        .map(|d| d.as_secs()),
                )
            };

            entries.push(DirEntry {
                name,
                kind,
                size,
                mtime,
            });
            *total += 1;

            if is_dir && recursive && depth < max_depth {
                Self::collect(
                    root,
                    &path,
                    recursive,
                    max_depth,
                    depth + 1,
                    entries,
                    total,
                    truncated,
                );
            }
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DirEntry {
    pub name: String,
    pub kind: EntryKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mtime: Option<u64>,
}

#[derive(Debug, Serialize, JsonSchema, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum EntryKind {
    File,
    Dir,
    Symlink,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ReadFileResult {
    pub path: String,
    pub content: String,
    /// 实际读取的起始字节偏移。
    pub offset: u64,
    /// 实际读取的字节数。
    pub limit: u64,
    pub truncated: bool,
    pub bytes: u64,
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
#[derive(Debug, Serialize, JsonSchema)]
pub struct WriteFileResult {
    pub path: String,
    pub bytes: u64,
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

#[derive(Debug, Serialize, JsonSchema)]
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub cmd: String,
    pub cwd: String,
    pub truncated_out: bool,
    pub truncated_err: bool,
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

/// 扫描目录，返回根目录路径与文件/目录相对路径清单。
pub fn list_dir(
    sandbox: &Sandbox,
    path: PathBuf,
    recursive: Option<bool>,
    max_depth: Option<u16>,
) -> ListDirResult {
    let root = match sandbox.resolve(&path) {
        Ok(p) => p,
        Err(e) => {
            return ListDirResult::failure(&path, e.to_string());
        }
    };
    if !root.is_dir() {
        return ListDirResult::failure(&root, String::from("path is a file"));
    }
    let recursive = recursive.unwrap_or(true);
    let max_depth = max_depth.unwrap_or(u16::MAX);

    let mut entries = Vec::new();
    let mut total = 0usize;
    let mut truncated = false;
    ListDirResult::collect(
        &root,
        &root,
        recursive,
        max_depth,
        0,
        &mut entries,
        &mut total,
        &mut truncated,
    );

    ListDirResult {
        path: root.to_string_lossy().into_owned(),
        entries,
        recursive,
        truncated,
        total,
        error: None,
    }
}

/// 读文本文件；按 `offset`/`limit` 分块读取，返回实际读取的偏移与字节数。
///
/// `offset`/`limit` 均为字节。为避免 `limit` 在多字节 UTF-8 字符中间截断，
/// 结束位置向下对齐到字符边界；`offset` 落在字符中间时同样向前对齐到字符边界。
pub fn read_file(
    sandbox: &Sandbox,
    path: PathBuf,
    offset: Option<u64>,
    limit: Option<u64>,
) -> ReadFileResult {
    let resolved = match sandbox.resolve(&path) {
        Ok(p) => p,
        Err(e) => {
            return ReadFileResult::failure(&path, e.to_string());
        }
    };
    if resolved.is_dir() {
        return ReadFileResult::failure(&resolved, String::from("path is a directory"));
    }
    let total = match fs::metadata(&resolved) {
        Ok(m) => m.len(),
        Err(e) => return ReadFileResult::failure(&resolved, e.to_string()),
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
    // 向前多读 3 字节，以便 `offset` 落在多字节字符中间时能对齐到字符边界。
    let read_start = offset.saturating_sub(3);
    let read_end = offset.saturating_add(limit).min(total);

    let mut file = match fs::File::open(&resolved) {
        Ok(f) => f,
        Err(e) => return ReadFileResult::failure(&resolved, e.to_string()),
    };
    if let Err(e) = file.seek(SeekFrom::Start(read_start)) {
        return ReadFileResult::failure(&resolved, e.to_string());
    }
    let mut buf = vec![0u8; (read_end - read_start) as usize];
    let n = match file.read(&mut buf) {
        Ok(n) => n,
        Err(e) => return ReadFileResult::failure(&resolved, e.to_string()),
    };
    buf.truncate(n);
    if n == 0 {
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

    let rel_offset = (offset - read_start) as usize;
    let start = char_boundary_down(&buf, rel_offset);
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

/// 写文本文件
pub fn write_file(sandbox: &Sandbox, path: PathBuf, data: String) -> WriteFileResult {
    let resolved = match sandbox.resolve(&path) {
        Ok(p) => p,
        Err(err) => {
            return WriteFileResult::failure(&path, err.to_string());
        }
    };
    if resolved.is_dir() {
        return WriteFileResult::failure(&resolved, String::from("path is a directory"));
    }

    if let Some(parent) = resolved.parent()
        && let Err(err) = fs::create_dir_all(parent)
    {
        return WriteFileResult::failure(&path, err.to_string());
    }

    let bytes = data.len() as u64;
    match fs::write(&resolved, data.as_bytes()) {
        Ok(_) => WriteFileResult {
            path: resolved.to_string_lossy().into_owned(),
            bytes,
            error: None,
        },
        Err(err) => WriteFileResult::failure(&path, err.to_string()),
    }
}

/// 执行子进程；`cwd` 必须在沙箱内，`timeout` 单位为毫秒
pub async fn exec(
    sandbox: &Sandbox,
    command: String,
    cwd: String,
    args: Option<Vec<String>>,
    env: Option<Vec<String>>,
    timeout: u64,
) -> ExecResult {
    let cwd_path = match sandbox.resolve(Path::new(&cwd)) {
        Ok(p) => p,
        Err(e) => {
            let mut r = ExecResult::empty(command, cwd);
            r.error = Some(e.to_string());
            return r;
        }
    };
    let cwd_str = cwd_path.to_string_lossy().into_owned();

    if !cwd_path.is_dir() {
        let mut r = ExecResult::empty(command, cwd_str.clone());
        r.error = Some(format!("cwd is not a directory: {cwd_str}"));
        return r;
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

    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            let mut r = ExecResult::empty(command, cwd_str);
            r.error = Some(e.to_string());
            return r;
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
        Ok(Err(e)) => {
            let mut r = ExecResult::empty(command, cwd_str);
            r.error = Some(e.to_string());
            r
        }
        Err(_) => {
            let mut r = ExecResult::empty(command, cwd_str);
            r.timed_out = true;
            r
        }
    }
}

#[cfg(unix)]
fn probe_file(path: &Path) -> String {
    file_description(path)
}

#[cfg(windows)]
fn probe_file(path: &Path) -> String {
    match path.extension().and_then(|e| e.to_str()) {
        Some(ext) => format!(".{ext}"),
        None => String::from("binary file"),
    }
}

#[cfg(unix)]
fn file_output(path: &Path, extra_args: &[&str]) -> String {
    let mut cmd = std::process::Command::new("file");
    cmd.arg("--brief");
    cmd.args(extra_args);
    cmd.arg(path);
    let out = match cmd.output() {
        Ok(res) => res,
        Err(e) => return e.to_string(),
    };
    if !out.status.success() {
        return String::new();
    }
    String::from_utf8(out.stdout)
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

// #[cfg(unix)]
// fn file_mime(path: &Path) -> String {
//     file_output(path, &["--mime-type"])
// }

#[cfg(unix)]
fn file_description(path: &Path) -> String {
    file_output(path, &[])
}

/// 返回不超过 `i` 的 UTF-8 字符边界（向下对齐）。
fn char_boundary_down(buf: &[u8], i: usize) -> usize {
    let mut i = i.min(buf.len());
    while i > 0 {
        if i == buf.len() || (buf[i] & 0xC0) != 0x80 {
            break;
        }
        i -= 1;
    }
    i
}

/// 返回 `buf` 中最长的合法 UTF-8 前缀长度（去除末尾不完整的多字节字符）。
fn utf8_valid_prefix(buf: &[u8]) -> usize {
    match std::str::from_utf8(buf) {
        Ok(_) => buf.len(),
        Err(e) => e.valid_up_to(),
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
            let path =
                std::env::temp_dir().join(format!("wind-fs-{label}-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn write(&self, rel: &str, content: &[u8]) {
            let path = self.path.join(rel);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create parent dir");
            }
            fs::write(path, content).expect("write file");
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn sandbox(dir: &TempDir) -> Sandbox {
        Sandbox::new(vec![dir.path().to_path_buf()])
    }

    #[test]
    fn list_dir_collects_entries() {
        let dir = TempDir::new("list");
        dir.write("a.txt", b"aaa");
        dir.write("sub/b.txt", b"bbb");

        let sb = sandbox(&dir);
        let result = list_dir(&sb, dir.path().to_path_buf(), None, None);

        assert_eq!(result.error, None);
        assert_eq!(result.total, 3);
        let names: Vec<&str> = result.entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"a.txt"));
        assert!(names.contains(&"sub"));
        assert!(names.contains(&"sub/b.txt"));
    }

    #[test]
    fn list_dir_non_recursive() {
        let dir = TempDir::new("list-nr");
        dir.write("a.txt", b"aaa");
        dir.write("sub/b.txt", b"bbb");

        let sb = sandbox(&dir);
        let result = list_dir(&sb, dir.path().to_path_buf(), Some(false), None);

        assert_eq!(result.error, None);
        assert_eq!(result.total, 2);
        let names: Vec<&str> = result.entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"a.txt"));
        assert!(names.contains(&"sub"));
        assert!(!names.contains(&"sub/b.txt"));
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
        let names: Vec<&str> = result.entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"a.txt"));
        assert!(names.contains(&".skills"));
        assert!(!names.contains(&".git"));
        assert!(!names.contains(&"node_modules"));
    }

    #[test]
    fn read_text_file() {
        let dir = TempDir::new("read-text");
        dir.write("hello.txt", "你好世界".as_bytes());

        let sb = sandbox(&dir);
        let result = read_file(&sb, dir.path().join("hello.txt"), None, None);

        assert_eq!(result.content, "你好世界");
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
        // limit=4 落在「好」（字节 3..6）中间，结束位置应裁剪到字符边界。
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
        // offset=4 落在「好」中间，起始位置应向前对齐到字符边界（字节 3）。
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

        let w = write_file(&sb, dir.path().join("x/y.txt"), "hello".to_string());
        assert_eq!(w.error, None);
        assert_eq!(w.bytes, 5);

        let r = read_file(&sb, dir.path().join("x/y.txt"), None, None);
        assert_eq!(r.content, "hello");
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

        let result = exec(
            &sb,
            "echo".to_string(),
            dir.path().to_string_lossy().into_owned(),
            Some(vec!["hello".to_string()]),
            None,
            5000,
        )
        .await;

        assert_eq!(result.stdout.trim(), "hello");
        assert_eq!(result.exit_code, Some(0));
        assert!(!result.timed_out);
        assert!(result.error.is_none());
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

        let err = result.error.expect("outside cwd must fold into error");
        assert!(err.contains("not allowed"), "got: {err}");
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

        // 悬空软链接：inside/evil -> outside/newfile（目标不存在）
        std::os::unix::fs::symlink(outside.path().join("newfile"), inside.path().join("evil"))
            .expect("create dangling symlink");

        let w = write_file(&sb, inside.path().join("evil"), "owned".to_string());
        assert!(w.error.is_some(), "got: {w:?}");
        assert!(!outside.path().join("newfile").exists());
    }

    #[cfg(unix)]
    #[test]
    fn write_through_dangling_symlink_dir_denied() {
        let inside = TempDir::new("dangling-dir-in");
        let outside = TempDir::new("dangling-dir-out");
        let sb = sandbox(&inside);

        // 悬空软链接目录：inside/evil -> outside/nonexistent（目录不存在）
        std::os::unix::fs::symlink(
            outside.path().join("nonexistent"),
            inside.path().join("evil"),
        )
        .expect("create dangling symlink");

        let w = write_file(
            &sb,
            inside.path().join("evil/sub/f.txt"),
            "owned".to_string(),
        );
        assert!(w.error.is_some(), "got: {w:?}");
        assert!(!outside.path().join("nonexistent").exists());
    }

    #[cfg(unix)]
    #[test]
    fn list_dir_does_not_follow_symlink_outside() {
        let inside = TempDir::new("list-leak-in");
        let outside = TempDir::new("list-leak-out");
        outside.write("secret.txt", b"top secret");
        let sb = sandbox(&inside);

        // inside/leak -> outside（指向沙箱外目录）
        std::os::unix::fs::symlink(outside.path(), inside.path().join("leak"))
            .expect("create symlink");

        let result = list_dir(&sb, inside.path().to_path_buf(), None, None);
        let names: Vec<&str> = result.entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"leak"), "leak entry should exist");
        assert!(
            !names.contains(&"leak/secret.txt"),
            "must not recurse into symlink: {names:?}"
        );
        let leak = result.entries.iter().find(|e| e.name == "leak").unwrap();
        assert!(matches!(leak.kind, EntryKind::Symlink));
    }

    #[cfg(unix)]
    #[test]
    fn write_through_internal_symlink_ok() {
        let inside = TempDir::new("internal-link");
        let sb = sandbox(&inside);

        // 沙箱内软链接：inside/link -> inside/real（real 存在）
        std::fs::create_dir_all(inside.path().join("real")).expect("create real dir");
        std::os::unix::fs::symlink("real", inside.path().join("link")).expect("create symlink");

        let w = write_file(
            &sb,
            inside.path().join("link/file.txt"),
            "hello".to_string(),
        );
        assert_eq!(w.error, None);
        assert_eq!(w.bytes, 5);
        assert!(inside.path().join("real/file.txt").exists());
    }

    #[cfg(unix)]
    #[test]
    fn resolve_symlink_loop_errors() {
        let inside = TempDir::new("loop");
        let sb = sandbox(&inside);

        // a -> b, b -> a
        std::os::unix::fs::symlink("b", inside.path().join("a")).expect("symlink a");
        std::os::unix::fs::symlink("a", inside.path().join("b")).expect("symlink b");

        let w = write_file(&sb, inside.path().join("a"), "x".to_string());
        assert!(w.error.is_some(), "symlink loop must error: {w:?}");
    }
}
