#[path = "common/lib.rs"]
mod common;
#[path = "common/registry_support.rs"]
mod support;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use wind_mcp::registry::{BindingState, BoundTool, BuiltinRoots, Registry, SkillRoot};
use wind_mcp::{Connector, McpError, McpOptions};

struct Harness {
    dir: TempDir,
    registry: Arc<Registry>,
    workspace: PathBuf,
}

fn harness() -> Harness {
    let dir = TempDir::new().expect("a temporary directory is required");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace must be created");
    let outlet = Arc::new(support::RecordingOutlet::default());
    let registry = Arc::new(Registry::new(
        Connector::from_host(McpOptions::default()),
        support::dispatcher(outlet),
    ));
    Harness {
        dir,
        registry,
        workspace,
    }
}

async fn sync_builtin(registry: &Registry, topic_id: i64, roots: BuiltinRoots) {
    let report = registry
        .sync_builtin(topic_id, roots)
        .await
        .expect("the builtin roots must sync");
    for binding in &report.bindings {
        assert_eq!(
            binding.state,
            BindingState::Ready,
            "the builtin server must connect: {report:?}"
        );
    }
}

async fn tools(registry: &Registry, topic_id: i64) -> Vec<BoundTool> {
    registry
        .tools(topic_id, CancellationToken::new())
        .await
        .expect("the builtin tools must list")
}

fn tool<'a>(tools: &'a [BoundTool], name: &str) -> &'a BoundTool {
    tools
        .iter()
        .find(|tool| tool.target.tool == name)
        .unwrap_or_else(|| panic!("the {name} tool must be listed"))
}

async fn call(registry: &Registry, tool: &BoundTool, arguments: Value) -> Value {
    registry
        .admit(&tool.target)
        .expect("the tool must be admitted")
        .execute(
            arguments,
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .expect("the tool must answer")
}

fn structured(value: &Value) -> &Value {
    &value["structuredContent"]
}

fn exec_helper() -> String {
    env!("CARGO_BIN_EXE_mcp_exec_helper").to_string()
}

fn write_skill(root: &Path, directory: &str, source: &str) -> PathBuf {
    let dir = root.join(directory);
    std::fs::create_dir_all(&dir).expect("the skill directory must be created");
    std::fs::write(
        dir.join("SKILL.md"),
        format!("---\nname: shared\ndescription: {source}\n---\nbody\n"),
    )
    .expect("the skill file must be written");
    dir
}

fn workspace_roots(workspace: &Path) -> BuiltinRoots {
    BuiltinRoots {
        workspace: Some(workspace.to_path_buf()),
        skills: Vec::new(),
    }
}

#[tokio::test]
async fn directory_listing_is_recursive() {
    let harness = harness();
    std::fs::create_dir_all(harness.workspace.join("docs/nested"))
        .expect("the nested directories must be created");
    std::fs::write(harness.workspace.join("root.txt"), "root").expect("the file must be written");
    std::fs::write(harness.workspace.join("docs/readme.md"), "# readme")
        .expect("the file must be written");
    std::fs::write(harness.workspace.join("docs/nested/deep.txt"), "deep")
        .expect("the file must be written");
    sync_builtin(&harness.registry, 1, workspace_roots(&harness.workspace)).await;
    let listed = tools(&harness.registry, 1).await;
    let value = call(
        &harness.registry,
        tool(&listed, "list_dir"),
        json!({"path": harness.workspace.to_string_lossy(), "recursive": true}),
    )
    .await;
    let content = structured(&value);
    assert!(
        content["error"].is_null(),
        "the listing must succeed: {content}"
    );
    let names: Vec<&str> = content["entries"]
        .as_array()
        .expect("the entries must be an array")
        .iter()
        .map(|entry| entry["name"].as_str().expect("the name must be a string"))
        .collect();
    assert!(names.contains(&"root.txt"));
    assert!(names.contains(&"docs"));
    assert!(names.contains(&"docs/readme.md"));
    assert!(names.contains(&"docs/nested/deep.txt"));
    harness.registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn read_file_respects_offset_and_limit() {
    let harness = harness();
    let path = harness.workspace.join("sample.txt");
    std::fs::write(&path, "hello world").expect("the file must be written");
    sync_builtin(&harness.registry, 1, workspace_roots(&harness.workspace)).await;
    let listed = tools(&harness.registry, 1).await;
    let value = call(
        &harness.registry,
        tool(&listed, "read_file"),
        json!({"path": path.to_string_lossy(), "offset": 6, "limit": 5}),
    )
    .await;
    let content = structured(&value);
    assert_eq!(content["content"], json!("world"));
    assert_eq!(content["offset"], json!(6));
    assert_eq!(content["limit"], json!(5));
    assert_eq!(content["truncated"], json!(false));
    assert_eq!(content["bytes"], json!(11));
    harness.registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn read_file_accepts_an_empty_file() {
    let harness = harness();
    let path = harness.workspace.join("empty.txt");
    std::fs::write(&path, "").expect("the file must be written");
    sync_builtin(&harness.registry, 1, workspace_roots(&harness.workspace)).await;
    let listed = tools(&harness.registry, 1).await;
    let value = call(
        &harness.registry,
        tool(&listed, "read_file"),
        json!({"path": path.to_string_lossy()}),
    )
    .await;
    let content = structured(&value);
    assert_eq!(content["content"], json!(""));
    assert_eq!(content["offset"], json!(0));
    assert_eq!(content["limit"], json!(0));
    assert_eq!(content["truncated"], json!(false));
    assert_eq!(content["bytes"], json!(0));
    harness.registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn write_file_creates_new_file() {
    let harness = harness();
    let path = harness.workspace.join("new/deep/file.txt");
    sync_builtin(&harness.registry, 1, workspace_roots(&harness.workspace)).await;
    let listed = tools(&harness.registry, 1).await;
    let value = call(
        &harness.registry,
        tool(&listed, "write_file"),
        json!({"path": path.to_string_lossy(), "data": "data"}),
    )
    .await;
    let content = structured(&value);
    assert!(
        content["error"].is_null(),
        "the write must succeed: {content}"
    );
    assert_eq!(content["bytes"], json!(4));
    assert_eq!(
        std::fs::read_to_string(&path).expect("the file must exist"),
        "data"
    );
    harness.registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn exec_runs_successfully() {
    let harness = harness();
    sync_builtin(&harness.registry, 1, workspace_roots(&harness.workspace)).await;
    let listed = tools(&harness.registry, 1).await;
    let value = call(
        &harness.registry,
        tool(&listed, "exec"),
        json!({
            "command": exec_helper(),
            "cwd": harness.workspace.to_string_lossy(),
            "args": ["echo", "hello"],
            "timeout": 5000,
        }),
    )
    .await;
    let content = structured(&value);
    assert!(
        content["error"].is_null(),
        "the command must start: {content}"
    );
    assert_eq!(content["stdout"], json!("hello\n"));
    assert_eq!(content["exit_code"], json!(0));
    assert_eq!(content["timed_out"], json!(false));
    harness.registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn exec_times_out() {
    let harness = harness();
    sync_builtin(&harness.registry, 1, workspace_roots(&harness.workspace)).await;
    let listed = tools(&harness.registry, 1).await;
    let value = call(
        &harness.registry,
        tool(&listed, "exec"),
        json!({
            "command": exec_helper(),
            "cwd": harness.workspace.to_string_lossy(),
            "args": ["sleep", "5000"],
            "timeout": 200,
        }),
    )
    .await;
    let content = structured(&value);
    assert_eq!(content["timed_out"], json!(true));
    harness.registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn exec_truncates_output() {
    let harness = harness();
    sync_builtin(&harness.registry, 1, workspace_roots(&harness.workspace)).await;
    let listed = tools(&harness.registry, 1).await;
    let value = call(
        &harness.registry,
        tool(&listed, "exec"),
        json!({
            "command": exec_helper(),
            "cwd": harness.workspace.to_string_lossy(),
            "args": ["large", "65536"],
            "timeout": 5000,
        }),
    )
    .await;
    let content = structured(&value);
    assert_eq!(content["truncated_out"], json!(true));
    assert_eq!(
        content["stdout"]
            .as_str()
            .expect("stdout must be a string")
            .len(),
        32 * 1024
    );
    assert_eq!(content["exit_code"], json!(0));
    harness.registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn outside_paths_are_rejected() {
    let harness = harness();
    let outside = harness.dir.path().join("outside");
    std::fs::create_dir_all(&outside).expect("the outside directory must be created");
    std::fs::write(outside.join("secret.txt"), "secret").expect("the file must be written");
    sync_builtin(&harness.registry, 1, workspace_roots(&harness.workspace)).await;
    let listed = tools(&harness.registry, 1).await;
    let read = call(
        &harness.registry,
        tool(&listed, "read_file"),
        json!({"path": outside.join("secret.txt").to_string_lossy()}),
    )
    .await;
    assert!(
        structured(&read)["content"]
            .as_str()
            .is_some_and(|content| content.contains("not allowed")),
        "reading outside the workspace must fail: {read}"
    );

    let missing = outside.join("missing/file.txt");
    let write = call(
        &harness.registry,
        tool(&listed, "write_file"),
        json!({"path": missing.to_string_lossy(), "data": "data"}),
    )
    .await;
    assert!(
        structured(&write)["error"].is_string(),
        "writing outside the workspace must fail: {write}"
    );
    assert!(
        !missing.exists(),
        "a rejected write must not create the file"
    );

    let list = call(
        &harness.registry,
        tool(&listed, "list_dir"),
        json!({"path": outside.to_string_lossy()}),
    )
    .await;
    assert!(
        structured(&list)["error"].is_string(),
        "listing outside the workspace must fail: {list}"
    );
    harness.registry.shutdown().await.expect("shutdown");
}

#[cfg(unix)]
fn make_symlink(target: &Path, link: &Path) {
    std::os::unix::fs::symlink(target, link).expect("the symlink must be created");
}

#[cfg(windows)]
fn make_symlink(target: &Path, link: &Path) {
    std::os::windows::fs::symlink_file(target, link).expect("the symlink must be created");
}

#[tokio::test]
async fn symlink_escape_is_rejected() {
    let harness = harness();
    let outside = harness.dir.path().join("outside");
    std::fs::create_dir_all(&outside).expect("the outside directory must be created");
    std::fs::write(outside.join("secret.txt"), "secret").expect("the file must be written");
    make_symlink(
        &outside.join("secret.txt"),
        &harness.workspace.join("link.txt"),
    );
    sync_builtin(&harness.registry, 1, workspace_roots(&harness.workspace)).await;
    let listed = tools(&harness.registry, 1).await;
    let value = call(
        &harness.registry,
        tool(&listed, "read_file"),
        json!({"path": harness.workspace.join("link.txt").to_string_lossy()}),
    )
    .await;
    assert!(
        structured(&value)["content"]
            .as_str()
            .is_some_and(|content| content.contains("not allowed")),
        "a symlink must not escape the workspace: {value}"
    );
    harness.registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn same_name_skills_keep_their_source() {
    let harness = harness();
    let first = harness.dir.path().join("skills-one");
    let second = harness.dir.path().join("skills-two");
    write_skill(&first, "shared", "one");
    write_skill(&second, "shared", "two");
    sync_builtin(
        &harness.registry,
        1,
        BuiltinRoots {
            workspace: None,
            skills: vec![
                SkillRoot {
                    source: "one".to_string(),
                    path: first.clone(),
                },
                SkillRoot {
                    source: "two".to_string(),
                    path: second.clone(),
                },
            ],
        },
    )
    .await;
    let listed = tools(&harness.registry, 1).await;
    let skills = tool(&listed, "skills_list");
    for (source, root) in [("one", &first), ("two", &second)] {
        let value = call(
            &harness.registry,
            skills,
            json!({"dir": root.to_string_lossy()}),
        )
        .await;
        let content = structured(&value);
        let entries = content["skills"]
            .as_array()
            .expect("the skills must be an array");
        assert_eq!(entries.len(), 1, "each root must list its skill: {content}");
        assert_eq!(entries[0]["name"], json!("shared"));
        assert_eq!(entries[0]["source"], json!(source));
        assert_eq!(entries[0]["description"], json!(source));
        assert!(
            content["errors"]
                .as_array()
                .expect("the errors must be an array")
                .is_empty()
        );
    }
    harness.registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn broken_front_matter_is_reported() {
    let harness = harness();
    let root = harness.dir.path().join("skills");
    write_skill(&root, "good", "good");
    let broken = root.join("broken");
    std::fs::create_dir_all(&broken).expect("the broken directory must be created");
    std::fs::write(
        broken.join("SKILL.md"),
        "---\nname: [unclosed\ndescription: bad\n---\n",
    )
    .expect("the broken skill must be written");
    sync_builtin(
        &harness.registry,
        1,
        BuiltinRoots {
            workspace: None,
            skills: vec![SkillRoot {
                source: "local".to_string(),
                path: root.clone(),
            }],
        },
    )
    .await;
    let listed = tools(&harness.registry, 1).await;
    let value = call(
        &harness.registry,
        tool(&listed, "skills_list"),
        json!({"dir": root.to_string_lossy()}),
    )
    .await;
    let content = structured(&value);
    let entries = content["skills"]
        .as_array()
        .expect("the skills must be an array");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["name"], json!("shared"));
    let errors = content["errors"]
        .as_array()
        .expect("the errors must be an array");
    assert_eq!(
        errors.len(),
        1,
        "the broken skill must be reported: {content}"
    );
    assert!(
        errors[0]["path"]
            .as_str()
            .is_some_and(|path| path.contains("broken")),
        "the error must name the skill directory: {content}"
    );
    assert!(
        errors[0]["error"]
            .as_str()
            .is_some_and(|error| !error.is_empty()),
        "the error must carry a reason: {content}"
    );
    harness.registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn missing_skill_root_is_not_created() {
    let harness = harness();
    let root = harness.dir.path().join("missing-skills");
    sync_builtin(
        &harness.registry,
        1,
        BuiltinRoots {
            workspace: None,
            skills: vec![SkillRoot {
                source: "local".to_string(),
                path: root.clone(),
            }],
        },
    )
    .await;
    let listed = tools(&harness.registry, 1).await;
    let result = harness
        .registry
        .admit(&tool(&listed, "skills_list").target)
        .expect("the tool must be admitted")
        .execute(
            json!({"dir": root.to_string_lossy()}),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    let error = result.expect_err("a missing root must be reported");
    assert!(
        error.to_string().contains("not allowed"),
        "the error must explain the path: {error}"
    );
    assert!(!root.exists(), "the tool must not create the root");
    harness.registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn root_update_invalidates_old_target() {
    let harness = harness();
    let first = harness.dir.path().join("skills-one");
    let second = harness.dir.path().join("skills-two");
    write_skill(&first, "shared", "one");
    write_skill(&second, "shared", "two");
    sync_builtin(
        &harness.registry,
        1,
        BuiltinRoots {
            workspace: None,
            skills: vec![SkillRoot {
                source: "one".to_string(),
                path: first.clone(),
            }],
        },
    )
    .await;
    let listed = tools(&harness.registry, 1).await;
    let old = tool(&listed, "skills_list").target.clone();

    sync_builtin(
        &harness.registry,
        1,
        BuiltinRoots {
            workspace: None,
            skills: vec![
                SkillRoot {
                    source: "one".to_string(),
                    path: first,
                },
                SkillRoot {
                    source: "two".to_string(),
                    path: second.clone(),
                },
            ],
        },
    )
    .await;
    assert!(
        matches!(harness.registry.admit(&old), Err(McpError::ToolUnavailable)),
        "a changed root list must invalidate the old generation"
    );
    let listed = tools(&harness.registry, 1).await;
    let current = tool(&listed, "skills_list");
    assert!(current.target.generation > old.generation);
    let value = call(
        &harness.registry,
        current,
        json!({"dir": second.to_string_lossy()}),
    )
    .await;
    let entries = structured(&value)["skills"]
        .as_array()
        .expect("the skills must be an array")
        .len();
    assert_eq!(entries, 1, "the new roots must be served: {value}");
    harness.registry.shutdown().await.expect("shutdown");
}
