use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, UNIX_EPOCH},
};

use schemars::JsonSchema;
use serde::Serialize;
use tokio::process::Command;

use super::error::{FsError, Result};
use super::sandbox::Sandbox;

/// 读文件单次默认上限（字节）。
const MAX_READ_BYTES: usize = 64 * 1024;
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
    pub encoding: String,
    pub truncated: bool,
    pub bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WriteFileResult {
    pub path: String,
    pub bytes: u64,
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
}

/// 扫描目录，返回根目录路径与文件/目录相对路径清单。
pub fn list_dir(
    sandbox: &Sandbox,
    path: PathBuf,
    recursive: Option<bool>,
    max_depth: Option<u16>,
) -> Result<ListDirResult> {
    let root = sandbox.resolve(&path)?;
    if !root.is_dir() {
        return Err(FsError::IsFile(root.to_string_lossy().into_owned()));
    }

    let recursive = recursive.unwrap_or(true);
    let max_depth = max_depth.unwrap_or(u16::MAX);

    let mut entries = Vec::new();
    let mut total = 0usize;
    let mut truncated = false;
    collect(
        &root,
        &root,
        recursive,
        max_depth,
        0,
        &mut entries,
        &mut total,
        &mut truncated,
    );

    Ok(ListDirResult {
        path: root.to_string_lossy().into_owned(),
        entries,
        recursive,
        truncated,
        total,
    })
}

fn collect(
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

        // 符号链接不取目标元数据（metadata 会跟随，泄露沙箱外信息）
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
            collect(
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

/// 读文本文件；二进制文件仅返回元信息（mime/尺寸）。
pub fn read_file(
    sandbox: &Sandbox,
    path: PathBuf,
    offset: Option<u64>,
    limit: Option<u64>,
) -> Result<ReadFileResult> {
    let resolved = sandbox.resolve(&path)?;
    if resolved.is_dir() {
        return Err(FsError::IsDir(resolved.to_string_lossy().into_owned()));
    }

    let bytes = fs::read(&resolved).map_err(|e| FsError::Io(e.to_string()))?;
    let total = bytes.len() as u64;
    let path_str = resolved.to_string_lossy().into_owned();

    if is_binary(&bytes) {
        return Ok(ReadFileResult {
            path: path_str,
            content: String::new(),
            encoding: "binary".to_string(),
            truncated: false,
            bytes: total,
            mime: mime_of(&resolved),
        });
    }

    let text = String::from_utf8_lossy(&bytes).into_owned();
    let offset = offset.unwrap_or(0) as usize;
    let limit = limit.unwrap_or(MAX_READ_BYTES as u64) as usize;

    let start = safe_byte_index(&text, offset);
    let end = safe_byte_index(&text, offset.saturating_add(limit));
    let truncated = end < text.len();

    Ok(ReadFileResult {
        path: path_str,
        content: text[start..end].to_string(),
        encoding: "utf-8".to_string(),
        truncated,
        bytes: total,
        mime: None,
    })
}

/// 写文本文件（自动建父目录）。
pub fn write_file(sandbox: &Sandbox, path: PathBuf, data: String) -> Result<WriteFileResult> {
    let resolved = sandbox.resolve(&path)?;
    if resolved.is_dir() {
        return Err(FsError::IsDir(resolved.to_string_lossy().into_owned()));
    }

    if let Some(parent) = resolved.parent() {
        fs::create_dir_all(parent).map_err(|e| FsError::Io(e.to_string()))?;
    }

    let bytes = data.len() as u64;
    fs::write(&resolved, data.as_bytes()).map_err(|e| FsError::Io(e.to_string()))?;
    Ok(WriteFileResult {
        path: resolved.to_string_lossy().into_owned(),
        bytes,
    })
}

/// 执行子进程；`cwd` 必须在沙箱内，`timeout` 单位为毫秒。
pub async fn exec(
    sandbox: &Sandbox,
    command: String,
    cwd: String,
    args: Option<Vec<String>>,
    env: Option<Vec<String>>,
    timeout: u64,
) -> Result<ExecResult> {
    let cwd_path = sandbox.resolve(Path::new(&cwd))?;
    if !cwd_path.is_dir() {
        return Err(FsError::NotAllowed(format!(
            "cwd is not a directory: {}",
            cwd_path.to_string_lossy().into_owned()
        )));
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

    let child = cmd.spawn().map_err(|e| FsError::Io(e.to_string()))?;
    let output =
        tokio::time::timeout(Duration::from_millis(timeout), child.wait_with_output()).await;

    let cwd_str = cwd_path.to_string_lossy().into_owned();

    match output {
        Ok(Ok(output)) => {
            let (stdout, truncated_out) = truncate_output(&output.stdout);
            let (stderr, truncated_err) = truncate_output(&output.stderr);
            Ok(ExecResult {
                stdout,
                stderr,
                exit_code: output.status.code(),
                timed_out: false,
                cmd: command,
                cwd: cwd_str,
                truncated_out,
                truncated_err,
            })
        }
        Ok(Err(e)) => Err(FsError::Io(e.to_string())),
        Err(_) => Ok(ExecResult {
            stdout: String::new(),
            stderr: String::new(),
            exit_code: None,
            timed_out: true,
            cmd: command,
            cwd: cwd_str,
            truncated_out: false,
            truncated_err: false,
        }),
    }
}

/// 探测二进制：前 8KB 出现 NUL 字节即视为二进制。
fn is_binary(bytes: &[u8]) -> bool {
    bytes.iter().take(8192).any(|&b| b == 0)
}

/// 按扩展名猜测 MIME 类型。
fn mime_of(path: &Path) -> Option<String> {
    let ext = path.extension()?.to_str()?.to_ascii_lowercase();
    let mime = match ext.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "json" => "application/json",
        "html" | "htm" => "text/html",
        _ => return None,
    };
    Some(mime.to_string())
}

/// 返回不超过 `byte_idx` 的 UTF-8 字符边界。
fn safe_byte_index(s: &str, byte_idx: usize) -> usize {
    if byte_idx >= s.len() {
        return s.len();
    }
    let mut i = byte_idx;
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
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
        let result = list_dir(&sb, dir.path().to_path_buf(), None, None).expect("list dir");

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
        let result = list_dir(&sb, dir.path().to_path_buf(), Some(false), None).expect("list");

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
        let result = list_dir(&sb, dir.path().to_path_buf(), None, None).expect("list");

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
        let result = read_file(&sb, dir.path().join("hello.txt"), None, None).expect("read");

        assert_eq!(result.encoding, "utf-8");
        assert_eq!(result.content, "你好世界");
        assert_eq!(result.bytes, 12);
        assert!(!result.truncated);
    }

    #[test]
    fn read_binary_returns_meta() {
        let dir = TempDir::new("read-bin");
        dir.write("img.png", &[0x89, 0x50, 0x4e, 0x47, 0x00, 0x0d]);

        let sb = sandbox(&dir);
        let result = read_file(&sb, dir.path().join("img.png"), None, None).expect("read");

        assert_eq!(result.encoding, "binary");
        assert!(result.content.is_empty());
        assert_eq!(result.mime.as_deref(), Some("image/png"));
    }

    #[test]
    fn read_dir_is_error() {
        let dir = TempDir::new("read-dir");
        dir.write("a.txt", b"a");

        let sb = sandbox(&dir);
        let err = read_file(&sb, dir.path().to_path_buf(), None, None).expect_err("read dir");
        assert!(matches!(err, FsError::IsDir(_)));
    }

    #[test]
    fn write_creates_parents_and_reads_back() {
        let dir = TempDir::new("write");
        let sb = sandbox(&dir);

        let w = write_file(&sb, dir.path().join("x/y.txt"), "hello".to_string()).expect("write");
        assert_eq!(w.bytes, 5);

        let r = read_file(&sb, dir.path().join("x/y.txt"), None, None).expect("read");
        assert_eq!(r.content, "hello");
    }

    #[test]
    fn read_outside_sandbox_denied() {
        let inside = TempDir::new("inside");
        let outside = TempDir::new("outside");
        outside.write("secret.txt", b"top secret");

        let sb = sandbox(&inside);
        let err = read_file(&sb, outside.path().join("secret.txt"), None, None)
            .expect_err("outside read");
        assert!(matches!(err, FsError::NotAllowed(_)));
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
        .await
        .expect("exec");

        assert_eq!(result.stdout.trim(), "hello");
        assert_eq!(result.exit_code, Some(0));
        assert!(!result.timed_out);
    }

    #[tokio::test]
    async fn exec_outside_cwd_denied() {
        let inside = TempDir::new("exec-in");
        let outside = TempDir::new("exec-out");

        let sb = sandbox(&inside);
        let err = exec(
            &sb,
            "echo".to_string(),
            outside.path().to_string_lossy().into_owned(),
            None,
            None,
            1000,
        )
        .await
        .expect_err("outside cwd");
        assert!(matches!(err, FsError::NotAllowed(_)));
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

        let err = write_file(&sb, inside.path().join("evil"), "owned".to_string())
            .expect_err("write through dangling symlink must be denied");
        assert!(matches!(err, FsError::NotAllowed(_)), "got: {err:?}");
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

        let err = write_file(
            &sb,
            inside.path().join("evil/sub/f.txt"),
            "owned".to_string(),
        )
        .expect_err("write through dangling symlink dir must be denied");
        assert!(matches!(err, FsError::NotAllowed(_)), "got: {err:?}");
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

        let result = list_dir(&sb, inside.path().to_path_buf(), None, None).expect("list");
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
        )
        .expect("write through internal symlink");
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

        let err = write_file(&sb, inside.path().join("a"), "x".to_string())
            .expect_err("symlink loop must error");
        assert!(
            matches!(err, FsError::InvalidPath(_)),
            "expected InvalidPath, got: {err:?}"
        );
    }
}
