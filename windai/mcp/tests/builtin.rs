use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wind_mcp::builtin::connect_builtin;
use wind_mcp::connector::{CallOutcome, Connection};
use wind_mcp::options::McpOptions;
use wind_mcp::registry::BuiltinKind;
use wind_mcp::roots::{Roots, ScopeId, ScopeRoots, SkillRoot};

const SCOPE: &str = "topic:1";

async fn builtin_fs(roots: &Roots, scope: &ScopeId) -> (Arc<Connection>, JoinHandle<()>) {
    connect_builtin(
        BuiltinKind::Fs,
        scope.clone(),
        roots.clone(),
        &McpOptions::default(),
    )
    .await
    .expect("the builtin fs server must connect")
}

async fn builtin_skills(roots: &Roots, scope: &ScopeId) -> (Arc<Connection>, JoinHandle<()>) {
    connect_builtin(
        BuiltinKind::Skills,
        scope.clone(),
        roots.clone(),
        &McpOptions::default(),
    )
    .await
    .expect("the builtin skills server must connect")
}

fn skill_scope(source: &str, path: &Path) -> ScopeRoots {
    ScopeRoots {
        workspace: Vec::new(),
        skills: vec![SkillRoot {
            source: source.to_owned(),
            path: path.to_path_buf(),
        }],
    }
}

fn write_skill(dir: &Path, name: &str, description: &str) {
    std::fs::create_dir_all(dir).expect("the skill directory must be created");
    let content = format!("---\nname: {name}\ndescription: {description}\n---\n");
    std::fs::write(dir.join("SKILL.md"), content).expect("SKILL.md must be written");
}

async fn call(conn: &Connection, tool: &str, args: Value) -> CallOutcome {
    conn.call(
        tool,
        args,
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(5),
    )
    .await
}

fn structured(outcome: &CallOutcome) -> &Value {
    match outcome {
        CallOutcome::Ok(value) => &value["structuredContent"],
        CallOutcome::Failed(error) => panic!("the tool call must succeed: {error}"),
    }
}

fn scope_workspace(roots: &Roots, scope: &ScopeId, workspace: &Path) {
    roots.set_scope(
        scope,
        ScopeRoots {
            workspace: vec![workspace.to_path_buf()],
            skills: Vec::new(),
        },
    );
}

fn outside_path(tmp: &Path) -> PathBuf {
    #[cfg(unix)]
    {
        let _ = tmp;
        PathBuf::from("/etc/passwd")
    }
    #[cfg(windows)]
    {
        let path = tmp.join("outside.txt");
        std::fs::write(&path, "outside").expect("the outside file must be written");
        path
    }
}

#[tokio::test]
async fn builtin_fs_rejects_paths_outside_the_scope_workspace() {
    let tmp = TempDir::new().expect("a temporary directory is required");
    let ws_a = tmp.path().join("ws-a");
    let ws_b = tmp.path().join("ws-b");
    std::fs::create_dir_all(&ws_a).expect("workspace a must be created");
    std::fs::create_dir_all(&ws_b).expect("workspace b must be created");
    let notes = ws_a.join("notes.md");
    std::fs::write(&notes, "notes content").expect("the notes file must be written");

    let roots = Roots::new(tmp.path().join("mods"));
    let scope = ScopeId::new(SCOPE);
    scope_workspace(&roots, &scope, &ws_a);
    let (conn, _task) = builtin_fs(&roots, &scope).await;

    let read = call(&conn, "read_file", json!({"path": notes.to_string_lossy()})).await;
    let read = structured(&read);
    assert!(
        read["bytes"].as_u64().is_some_and(|bytes| bytes > 0),
        "a file inside the scope workspace must be readable: {read}"
    );
    assert!(
        read["content"]
            .as_str()
            .is_some_and(|content| content.contains("notes content")),
        "the file content must be returned: {read}"
    );

    let outside = outside_path(tmp.path());
    let denied = call(
        &conn,
        "read_file",
        json!({"path": outside.to_string_lossy()}),
    )
    .await;
    let denied = structured(&denied);
    assert_eq!(
        denied["bytes"],
        json!(0),
        "a path outside the scope workspace must return no bytes: {denied}"
    );
    assert!(
        denied["content"]
            .as_str()
            .is_some_and(|content| content.contains("not allowed")),
        "the rejection must carry a reason: {denied}"
    );

    scope_workspace(&roots, &scope, &ws_b);
    let switched = call(&conn, "read_file", json!({"path": notes.to_string_lossy()})).await;
    let switched = structured(&switched);
    assert_eq!(
        switched["bytes"],
        json!(0),
        "the replaced workspace must be out of scope: {switched}"
    );
    assert!(
        switched["content"]
            .as_str()
            .is_some_and(|content| content.contains("not allowed")),
        "the replaced workspace must be rejected: {switched}"
    );
}

#[tokio::test]
async fn builtin_fs_lists_and_round_trips_files() {
    let tmp = TempDir::new().expect("a temporary directory is required");
    let ws_a = tmp.path().join("ws-a");
    std::fs::create_dir_all(&ws_a).expect("workspace a must be created");
    let roots = Roots::new(tmp.path().join("mods"));
    let scope = ScopeId::new(SCOPE);
    scope_workspace(&roots, &scope, &ws_a);
    let (conn, _task) = builtin_fs(&roots, &scope).await;

    let tools = conn.list_tools().await.expect("the tools must be listed");
    let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_str()).collect();
    for expected in ["list_dir", "read_file", "write_file", "exec"] {
        assert!(
            names.contains(&expected),
            "the {expected} tool must be listed: {names:?}"
        );
    }

    let path = ws_a.join("sub").join("a.txt");
    let written = call(
        &conn,
        "write_file",
        json!({"path": path.to_string_lossy(), "data": "round trip"}),
    )
    .await;
    let written = structured(&written);
    assert!(
        written["error"].is_null(),
        "the write must succeed: {written}"
    );
    assert_eq!(written["bytes"], json!(10));

    let read = call(&conn, "read_file", json!({"path": path.to_string_lossy()})).await;
    let read = structured(&read);
    assert_eq!(read["content"], json!("round trip"));
}

#[tokio::test]
async fn builtin_fs_fails_without_scope_roots() {
    let tmp = TempDir::new().expect("a temporary directory is required");
    let roots = Roots::new(tmp.path().join("mods"));
    let scope = ScopeId::new(SCOPE);
    let (conn, _task) = builtin_fs(&roots, &scope).await;

    let read = call(
        &conn,
        "read_file",
        json!({"path": tmp.path().join("notes.md").to_string_lossy()}),
    )
    .await;
    let read = structured(&read);
    assert_eq!(
        read["bytes"],
        json!(0),
        "a call without scope roots must return no bytes: {read}"
    );
    assert!(
        read["content"]
            .as_str()
            .is_some_and(|content| content.contains("no workspace roots")),
        "the missing roots must be reported: {read}"
    );
}

#[tokio::test]
async fn builtin_exec_runs_argv_inside_the_scope_and_rejects_outside_cwd() {
    let tmp = TempDir::new().expect("a temporary directory is required");
    let ws_a = tmp.path().join("ws-a");
    let outside = tmp.path().join("outside");
    std::fs::create_dir_all(&ws_a).expect("workspace a must be created");
    std::fs::create_dir_all(&outside).expect("the outside directory must be created");
    let roots = Roots::new(tmp.path().join("mods"));
    let scope = ScopeId::new(SCOPE);
    scope_workspace(&roots, &scope, &ws_a);
    let (conn, _task) = builtin_fs(&roots, &scope).await;

    #[cfg(unix)]
    let (command, args) = ("echo", vec!["a", "b"]);
    #[cfg(windows)]
    let (command, args) = ("cmd", vec!["/C", "echo", "a", "b"]);

    let ran = call(
        &conn,
        "exec",
        json!({
            "command": command,
            "cwd": ws_a.to_string_lossy(),
            "args": args,
            "timeout": 5000,
        }),
    )
    .await;
    let ran = structured(&ran);
    assert!(
        ran["error"].is_null(),
        "the command must start inside the workspace: {ran}"
    );
    assert!(
        ran["stdout"]
            .as_str()
            .is_some_and(|stdout| stdout.contains("a b")),
        "the argv must reach the command: {ran}"
    );
    assert_eq!(ran["timed_out"], json!(false));

    let denied = call(
        &conn,
        "exec",
        json!({
            "command": command,
            "cwd": outside.to_string_lossy(),
            "args": args,
            "timeout": 5000,
        }),
    )
    .await;
    let denied = structured(&denied);
    assert!(
        denied["error"].is_string(),
        "a cwd outside the workspace must be rejected: {denied}"
    );
    assert_eq!(denied["timed_out"], json!(false));
}

#[tokio::test]
async fn builtin_skills_lists_skills_within_the_scope() {
    let tmp = TempDir::new().expect("a temporary directory is required");
    let skills_a = tmp.path().join("skills-a");
    let skills_b = tmp.path().join("skills-b");
    write_skill(&skills_a.join("a"), "demo", "demo skill");
    write_skill(&skills_b.join("b"), "other", "other skill");

    let roots = Roots::new(tmp.path().join("mods"));
    let scope = ScopeId::new(SCOPE);
    roots.set_scope(&scope, skill_scope("s1", &skills_a));
    let (conn, task) = builtin_skills(&roots, &scope).await;

    let listed = call(
        &conn,
        "skills_list",
        json!({"dir": skills_a.to_string_lossy()}),
    )
    .await;
    let listed = structured(&listed);
    assert_eq!(
        listed["skills"].as_array().map(Vec::len),
        Some(1),
        "only the skill under skills-a must be listed: {listed}"
    );
    assert_eq!(listed["skills"][0]["name"], json!("demo"));
    assert_eq!(listed["skills"][0]["source"], json!("s1"));

    roots.set_scope(&scope, skill_scope("s2", &skills_b));
    let listed = call(
        &conn,
        "skills_list",
        json!({"dir": skills_b.to_string_lossy()}),
    )
    .await;
    let listed = structured(&listed);
    assert_eq!(
        listed["skills"].as_array().map(Vec::len),
        Some(1),
        "the replaced scope must expose only skills-b: {listed}"
    );
    assert_eq!(listed["skills"][0]["name"], json!("other"));
    assert_eq!(listed["skills"][0]["source"], json!("s2"));

    let outside = tmp.path().join("outside");
    std::fs::create_dir_all(&outside).expect("the outside directory must be created");
    let denied = call(
        &conn,
        "skills_list",
        json!({"dir": outside.to_string_lossy()}),
    )
    .await;
    assert!(
        matches!(denied, CallOutcome::Failed(_)),
        "a directory outside the skill roots must be rejected: {denied:?}"
    );

    task.abort();
}
