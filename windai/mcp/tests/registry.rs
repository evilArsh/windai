use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use wind_mcp::builtin::fs::FsServer;
use wind_mcp::builtin::skills::SkillsServer;
use wind_mcp::builtin::workspace::{ModDir, SkillRoot, WorkspaceInfo, WorkspaceServer};
use wind_mcp::builtin::{self, BuiltinScope};
use wind_mcp::{BoundTool, CallOutcome, RegistryHandle, init};

#[path = "support/mod.rs"]
mod support;

use support::{
    RecordingBus, bus, call_tool, definition as server_definition, find_tool, options,
    stderr_definition, temporary_directory, tool_names, wait_gone, wait_started, with_env,
    with_envs,
};

#[tokio::test]
async fn acquire_returns_before_the_service_is_ready() {
    let tmp = TempDir::new().expect("create a temp dir");
    let gate = tmp.path().join("handshake.gate");
    let definition = with_env(
        server_definition("demo"),
        "WIND_TEST_HANDSHAKE_GATE",
        gate.to_str().expect("utf-8 path"),
    );
    let handle = init(options(), bus());
    handle.acquire("r1".to_owned(), definition);
    assert!(
        handle.list_tools().await.is_empty(),
        "no catalog is published before the handshake completes"
    );
    fs::write(&gate, "go").expect("open the handshake gate");
    wait_started(&handle, "demo").await;
    assert_eq!(
        tool_names(&handle).await,
        vec!["echo", "context", "wait", "unsupported", "failing"]
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn list_tools_by_ref_filters_by_reference() {
    let handle = init(options(), bus());
    handle.acquire("r1".to_owned(), server_definition("alpha"));
    handle.acquire("r2".to_owned(), server_definition("beta"));
    wait_started(&handle, "alpha").await;
    wait_started(&handle, "beta").await;

    let first = handle.list_tools_by_ref("r1").await;
    assert!(
        !first.is_empty(),
        "the first reference must expose its tools"
    );
    assert!(
        first.iter().all(|bound| bound.server == "alpha"),
        "only the first reference's tools may be listed"
    );
    assert!(handle.list_tools_by_ref("missing").await.is_empty());
    handle.shutdown().await;
}

#[tokio::test]
async fn call_by_name_executes_a_running_tool() {
    let handle = init(options(), bus());
    handle.acquire("r1".to_owned(), server_definition("demo"));
    wait_started(&handle, "demo").await;
    let outcome = handle
        .call(
            "demo",
            "echo",
            json!({ "value": 7 }),
            CancellationToken::new(),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await;
    assert!(matches!(outcome, CallOutcome::Ok(_)));
    handle.shutdown().await;
}

#[tokio::test]
async fn call_to_an_unknown_server_returns_failure() {
    let handle = init(options(), bus());
    let outcome = handle
        .call(
            "missing",
            "echo",
            json!({}),
            CancellationToken::new(),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await;
    assert!(matches!(outcome, CallOutcome::Failed(_)));
    handle.shutdown().await;
}

#[tokio::test]
async fn duplicate_acquires_share_one_start() {
    let tmp = TempDir::new().expect("create a temp dir");
    let log = tmp.path().join("start.log");
    let definition = with_env(
        server_definition("demo"),
        "WIND_TEST_START_LOG",
        log.to_str().expect("utf-8 path"),
    );
    let handle = init(options(), bus());
    handle.acquire("r1".to_owned(), definition.clone());
    handle.acquire("r2".to_owned(), definition);
    wait_started(&handle, "demo").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        start_log_lines(log.to_str().expect("utf-8 path")),
        1,
        "a shared definition starts once"
    );

    handle.release("r1".to_owned());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        tool_names(&handle).await.contains(&"echo".to_owned()),
        "the remaining reference must keep the service alive"
    );
    handle.release("r2".to_owned());
    wait_gone(&handle, "demo").await;
    handle.shutdown().await;
}

#[tokio::test]
async fn same_name_with_a_new_definition_restarts() {
    let bus = RecordingBus::new();
    let handle = init(options(), bus.dispatcher());
    handle.acquire(
        "r1".to_owned(),
        with_env(server_definition("demo"), "WIND_TEST_MARK", "1"),
    );
    wait_mark(&handle, "demo", "1").await;
    handle.acquire(
        "r2".to_owned(),
        with_env(server_definition("demo"), "WIND_TEST_MARK", "2"),
    );
    bus.poll_until_seen("core/mcp.stopped", |payload| {
        payload["reason"] == json!("replaced")
    })
    .await;
    wait_mark(&handle, "demo", "2").await;

    let replaced = bus.payloads("core/mcp.replaced");
    let replaced = replaced
        .first()
        .expect("a replaced event must be published");
    assert_eq!(
        replaced["old"]["config"]["env"]["WIND_TEST_MARK"],
        json!("1")
    );
    assert_eq!(
        replaced["new"]["config"]["env"]["WIND_TEST_MARK"],
        json!("2")
    );
    let stopped = bus.payloads("core/mcp.stopped");
    assert_eq!(stopped[0]["refs"], json!(["r1", "r2"]));
    handle.shutdown().await;
}

#[tokio::test]
async fn release_stops_a_running_service() {
    let tmp = TempDir::new().expect("create a temp dir");
    let exit = tmp.path().join("exit");
    let definition = with_env(
        server_definition("demo"),
        "WIND_TEST_EXIT_FILE",
        exit.to_str().expect("utf-8 path"),
    );
    let handle = init(options(), bus());
    handle.acquire("r1".to_owned(), definition);
    wait_started(&handle, "demo").await;
    handle.release("r1".to_owned());
    wait_gone(&handle, "demo").await;
    wait_for_path(&exit).await;
    handle.shutdown().await;
}

#[tokio::test]
async fn acquire_after_release_restarts_the_service() {
    let handle = init(options(), bus());
    handle.acquire("r1".to_owned(), server_definition("demo"));
    wait_started(&handle, "demo").await;
    handle.release("r1".to_owned());
    handle.acquire("r2".to_owned(), server_definition("demo"));

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if !handle.list_tools_by_ref("r2").await.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "a re-acquire during the stop must restart the service"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.shutdown().await;
}

#[tokio::test]
async fn launch_can_be_rewritten_before_connecting() {
    let bus = RecordingBus::new();
    bus.rewrite("core/mcp.launch", |mut payload| {
        payload["definition"]["config"]["env"]["WIND_TEST_MARK"] = json!("injected");
        payload
    });
    let handle = init(options(), bus.dispatcher());
    handle.acquire("r1".to_owned(), server_definition("demo"));
    wait_mark(&handle, "demo", "injected").await;
    let launches = bus.payloads("core/mcp.launch");
    assert_eq!(
        launches[0]["definition"]["config"]["env"]["WIND_TEST_MARK"],
        json!("injected"),
        "the recorded launch must carry the rewritten definition"
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn stderr_lines_are_published() {
    let bus = RecordingBus::new();
    let handle = init(options(), bus.dispatcher());
    handle.acquire("r1".to_owned(), stderr_definition("demo", "the log line"));
    bus.poll_seen("core/mcp.log").await;
    let logs = bus.payloads("core/mcp.log");
    assert_eq!(logs[0]["name"], json!("demo"));
    assert_eq!(logs[0]["stream"], json!("stderr"));
    assert_eq!(logs[0]["line"], json!("the log line"));
    handle.shutdown().await;
}

#[tokio::test]
async fn lifecycle_events_follow_acquire_start_release() {
    let bus = RecordingBus::new();
    let handle = init(options(), bus.dispatcher());
    handle.acquire("r1".to_owned(), server_definition("demo"));
    bus.poll_seen("core/mcp.starting").await;
    assert_eq!(bus.payloads("core/mcp.starting")[0]["refs"], json!(["r1"]));

    wait_started(&handle, "demo").await;
    bus.poll_seen("core/mcp.started").await;
    let started = bus.payloads("core/mcp.started");
    assert_eq!(
        started[0]["tools"],
        json!(["echo", "context", "wait", "unsupported", "failing"])
    );

    handle.release("r1".to_owned());
    wait_gone(&handle, "demo").await;
    bus.poll_until_seen("core/mcp.stopped", |payload| {
        payload["reason"] == json!("released")
    })
    .await;
    let stopped = bus.payloads("core/mcp.stopped");
    assert_eq!(stopped[0]["refs"], json!(["r1"]));
    handle.shutdown().await;
}

#[tokio::test]
async fn same_startup_key_serializes_external_launches() {
    let tmp = TempDir::new().expect("create a temp dir");
    let gate = tmp.path().join("handshake.gate");
    let log = tmp.path().join("start.log");
    let gate = gate.to_string_lossy().into_owned();
    let log = log.to_string_lossy().into_owned();
    let envs = [
        ("WIND_TEST_HANDSHAKE_GATE", gate.as_str()),
        ("WIND_TEST_START_LOG", log.as_str()),
    ];

    let handle = init(options(), bus());
    handle.acquire("r1".to_owned(), with_envs(server_definition("a"), envs));
    handle.acquire("r2".to_owned(), with_envs(server_definition("b"), envs));
    wait_for_start_lines(&log, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        start_log_lines(&log),
        1,
        "the second launch must wait for the startup lock"
    );
    assert!(
        handle.list_tools().await.is_empty(),
        "no catalog is published before the handshake completes"
    );

    fs::write(&gate, "go").expect("open the handshake gate");
    wait_started(&handle, "a").await;
    wait_started(&handle, "b").await;
    assert_eq!(start_log_lines(&log), 2, "both launches must start");
    handle.shutdown().await;
}

#[tokio::test]
async fn call_after_stop_returns_failure_instead_of_hanging() {
    let handle = init(options(), bus());
    handle.acquire("r1".to_owned(), server_definition("demo"));
    wait_started(&handle, "demo").await;
    let tool = find_tool(&handle, "demo", "echo").await;
    handle.release("r1".to_owned());
    wait_gone(&handle, "demo").await;

    let started = Instant::now();
    loop {
        let outcome = call_tool(&handle, &tool, json!({"text": "x"})).await;
        if matches!(outcome, CallOutcome::Failed(_)) {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "a stopped service must reject calls: {outcome:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.shutdown().await;
}

#[tokio::test]
async fn shutdown_stops_services_and_returns() {
    let handle = init(options(), bus());
    handle.acquire("a".to_owned(), server_definition("a"));
    handle.acquire("b".to_owned(), server_definition("b"));
    wait_started(&handle, "a").await;
    wait_started(&handle, "b").await;
    tokio::time::timeout(Duration::from_secs(15), handle.shutdown())
        .await
        .expect("shutdown must return");
    assert!(handle.list_tools().await.is_empty());
    tokio::time::timeout(Duration::from_secs(5), handle.shutdown())
        .await
        .expect("a repeated shutdown must return");
}

#[tokio::test]
async fn builtin_fs_instances_are_isolated_by_workspace() {
    let ws_a = temporary_directory();
    let ws_b = temporary_directory();
    let notes_a = ws_a.join("notes.md");
    let notes_b = ws_b.join("notes.md");
    fs::write(&notes_a, "notes from a").expect("write the notes of a");
    fs::write(&notes_b, "notes from b").expect("write the notes of b");

    let handle = init(options(), bus());
    handle
        .acquire_builtin(
            "t1".to_owned(),
            FsServer::new("fs:topic:1", workspace_info(&ws_a)),
        )
        .await
        .expect("the first builtin fs must connect");
    handle
        .acquire_builtin(
            "t2".to_owned(),
            FsServer::new("fs:topic:2", workspace_info(&ws_b)),
        )
        .await
        .expect("the second builtin fs must connect");
    wait_started(&handle, "fs:topic:1").await;
    wait_started(&handle, "fs:topic:2").await;

    let tool_a = find_tool(&handle, "fs:topic:1", "read_file").await;
    let read_a = read_file(&handle, &tool_a, &notes_a).await;
    assert!(
        read_a["content"]
            .as_str()
            .is_some_and(|content| content.contains("notes from a")),
        "the first instance must read its own workspace: {read_a}"
    );
    let denied_b = read_file(&handle, &tool_a, &notes_b).await;
    assert_eq!(
        denied_b["bytes"],
        json!(0),
        "the first instance must not see the second workspace: {denied_b}"
    );

    let tool_b = find_tool(&handle, "fs:topic:2", "read_file").await;
    let read_b = read_file(&handle, &tool_b, &notes_b).await;
    assert!(
        read_b["content"]
            .as_str()
            .is_some_and(|content| content.contains("notes from b")),
        "the second instance must read its own workspace: {read_b}"
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn a_duplicate_builtin_is_discarded() {
    let bus = RecordingBus::new();
    let workspace = temporary_directory();
    let handle = init(options(), bus.dispatcher());
    handle
        .acquire_builtin(
            "t1".to_owned(),
            FsServer::new("fs:topic:1", workspace_info(&workspace)),
        )
        .await
        .expect("the first builtin fs must connect");
    handle
        .acquire_builtin(
            "t2".to_owned(),
            FsServer::new("fs:topic:1", workspace_info(&workspace)),
        )
        .await
        .expect("the second builtin fs must connect");
    wait_started(&handle, "fs:topic:1").await;
    assert_eq!(
        bus.payloads("core/mcp.started").len(),
        1,
        "a duplicate builtin must not publish a second start"
    );

    handle.release("t1".to_owned());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        tool_names(&handle).await.contains(&"read_file".to_owned()),
        "the second reference must keep the builtin alive"
    );
    handle.release("t2".to_owned());
    wait_gone(&handle, "fs:topic:1").await;
    handle.shutdown().await;
}

#[tokio::test]
async fn builtin_skills_lists_every_configured_root() {
    let tmp = TempDir::new().expect("create a temp dir");
    let public = tmp.path().join("skills");
    let module = tmp.path().join("mods/one/skills");
    write_skill(&public.join("a"), "demo", "demo skill");
    write_skill(&public.join("a/nested"), "nested", "nested skill");
    write_skill(&public.join("c"), "second", "second skill");
    let plain = public.join("plain");
    fs::create_dir_all(&plain).expect("create the plain directory");
    fs::write(plain.join("readme.md"), "readme").expect("write the readme");
    write_skill(&module.join("b"), "mod", "mod skill");

    let handle = init(options(), bus());
    handle
        .acquire_builtin(
            "t1".to_owned(),
            SkillsServer::new(
                "skills:topic:1",
                Arc::new(RwLock::new(WorkspaceInfo::new(
                    1,
                    tmp.path().to_path_buf(),
                    vec![
                        SkillRoot {
                            source: None,
                            root: public.clone(),
                        },
                        SkillRoot {
                            source: Some("s1".to_owned()),
                            root: module,
                        },
                        SkillRoot {
                            source: Some("gone".to_owned()),
                            root: tmp.path().join("missing"),
                        },
                    ],
                    Vec::new(),
                ))),
            ),
        )
        .await
        .expect("the builtin skills server must connect");
    let tool = find_tool(&handle, "skills:topic:1", "skills_list").await;
    let listed = payload(call_tool(&handle, &tool, json!({})).await);
    let found: Vec<(String, Option<String>)> = listed["skills"]
        .as_array()
        .expect("skills must be an array")
        .iter()
        .map(|skill| {
            (
                skill["name"]
                    .as_str()
                    .expect("every skill has a name")
                    .to_owned(),
                skill["source"].as_str().map(str::to_owned),
            )
        })
        .collect();
    assert_eq!(
        found,
        vec![
            ("demo".to_owned(), None),
            ("second".to_owned(), None),
            ("mod".to_owned(), Some("s1".to_owned())),
        ],
        "every root must be scanned and a nested SKILL.md must stay a resource: {listed}"
    );
    assert_eq!(listed["errors"], json!([]));
    handle.shutdown().await;
}

#[tokio::test]
async fn builtin_workspace_reports_its_own_paths() {
    let workspace_a = fs::canonicalize(temporary_directory()).expect("normalize workspace a");
    let workspace_b = fs::canonicalize(temporary_directory()).expect("normalize workspace b");
    let snapshot = |workspace: &Path| {
        Arc::new(RwLock::new(WorkspaceInfo::new(
            0,
            workspace.to_path_buf(),
            vec![SkillRoot {
                source: None,
                root: workspace.join("skills"),
            }],
            vec![ModDir {
                name: "demo".to_owned(),
                version: 1,
                root: workspace.join("mods/demo/1"),
            }],
        )))
    };

    let handle = init(options(), bus());
    handle
        .acquire_builtin(
            "w1".to_owned(),
            WorkspaceServer::new("workspace:topic:1", snapshot(&workspace_a)),
        )
        .await
        .expect("the first builtin workspace must connect");
    handle
        .acquire_builtin(
            "w2".to_owned(),
            WorkspaceServer::new("workspace:topic:2", snapshot(&workspace_b)),
        )
        .await
        .expect("the second builtin workspace must connect");

    let tool = find_tool(&handle, "workspace:topic:1", "workspace_info").await;
    let facts = payload(call_tool(&handle, &tool, json!({})).await);
    assert_eq!(facts["workspace"], json!(workspace_a.to_string_lossy()));
    assert_eq!(
        facts["mods"][0]["root"],
        json!(workspace_a.join("mods/demo/1").to_string_lossy())
    );
    assert_eq!(facts["topic_id"], json!(0));
    assert_eq!(facts["skills"][0]["source"], Value::Null);
    assert_eq!(
        facts["skills"][0]["root"],
        json!(workspace_a.join("skills"))
    );
    assert_eq!(facts["mods"][0]["version"], json!(1));
    let other = workspace_b.to_string_lossy().into_owned();
    assert!(
        !facts.to_string().contains(&other),
        "the first topic must not see the second workspace: {facts}"
    );

    let tool = find_tool(&handle, "workspace:topic:2", "workspace_info").await;
    let facts = payload(call_tool(&handle, &tool, json!({})).await);
    assert_eq!(facts["workspace"], json!(workspace_b.to_string_lossy()));
    handle.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn releasing_reaps_the_process_group() {
    let tmp = TempDir::new().expect("create a temp dir");
    let pid_file = tmp.path().join("grandchild.pid");
    let definition = with_env(
        server_definition("demo"),
        "WIND_TEST_GRANDCHILD_FILE",
        pid_file.to_str().expect("utf-8 path"),
    );
    let handle = init(options(), bus());
    handle.acquire("r1".to_owned(), definition);
    wait_started(&handle, "demo").await;
    let pid = wait_for_pid(&pid_file).await;
    assert!(process_exists(pid), "the grandchild must start alive");

    handle.release("r1".to_owned());
    wait_gone(&handle, "demo").await;
    let started = Instant::now();
    while process_exists(pid) {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the process group must be reaped after release"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.shutdown().await;
}

async fn wait_mark(handle: &RegistryHandle, server: &str, expected: &str) {
    let started = Instant::now();
    loop {
        let tool = find_tool(handle, server, "context").await;
        if let CallOutcome::Ok(value) = call_tool(handle, &tool, json!({})).await
            && value["mark"] == json!(expected)
        {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the service {server} must report the mark {expected}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn payload(outcome: CallOutcome) -> Value {
    match outcome {
        CallOutcome::Ok(value) => value,
        CallOutcome::Failed(error) => panic!("the tool call must return a result: {error}"),
    }
}

async fn read_file(handle: &RegistryHandle, tool: &BoundTool, path: &Path) -> Value {
    payload(call_tool(handle, tool, json!({"path": path.to_string_lossy()})).await)
}

fn start_log_lines(path: &str) -> usize {
    fs::read_to_string(path)
        .map(|text| text.lines().count())
        .unwrap_or(0)
}

async fn wait_for_start_lines(path: &str, expected: usize) {
    let started = Instant::now();
    while start_log_lines(path) < expected {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the start log must reach {expected} lines"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_path(path: &Path) {
    let started = Instant::now();
    while !path.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the path {} must appear",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_pid(path: &Path) -> u32 {
    let started = Instant::now();
    loop {
        if let Ok(text) = fs::read_to_string(path)
            && let Ok(pid) = text.trim().parse::<u32>()
        {
            return pid;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the grandchild pid file must appear"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(unix)]
fn process_exists(pid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn write_skill(dir: &Path, name: &str, description: &str) {
    fs::create_dir_all(dir).expect("create the skill directory");
    let content = format!("---\nname: {name}\ndescription: {description}\n---\n");
    fs::write(dir.join("SKILL.md"), content).expect("write SKILL.md");
}

fn workspace_info(workspace: &Path) -> Arc<RwLock<WorkspaceInfo>> {
    Arc::new(RwLock::new(WorkspaceInfo::new(
        1,
        workspace.to_path_buf(),
        Vec::new(),
        Vec::new(),
    )))
}

#[tokio::test]
async fn builtin_services_read_shared_updates_without_restarting() {
    let tmp = TempDir::new().expect("create a temp dir");
    let old_workspace = tmp.path().join("old");
    let new_workspace = tmp.path().join("new");
    let old_skills = tmp.path().join("old_skills");
    let new_skills = tmp.path().join("new_skills");
    for workspace in [&old_workspace, &new_workspace] {
        fs::create_dir_all(workspace).expect("create the workspace");
        fs::write(workspace.join("notes.md"), "notes").expect("write notes");
    }
    write_skill(&old_skills.join("demo"), "old", "old skill");
    write_skill(&new_skills.join("demo"), "new", "new skill");
    let info = Arc::new(RwLock::new(WorkspaceInfo::new(
        1,
        old_workspace.clone(),
        vec![SkillRoot {
            source: Some("old_mod".to_owned()),
            root: old_skills.clone(),
        }],
        Vec::new(),
    )));
    let bus = RecordingBus::new();
    let handle = init(options(), bus.dispatcher());
    builtin::init(&handle, BuiltinScope::Topic(1), info.clone()).await;
    let read = find_tool(&handle, "wd_fs_1", "read_file").await;
    let skills = find_tool(&handle, "wd_skills_1", "skills_list").await;
    let workspace = find_tool(&handle, "wd_workspace_1", "workspace_info").await;
    assert_eq!(
        read_file(&handle, &read, &old_workspace.join("notes.md")).await["content"],
        json!("notes")
    );
    let denied = read_file(&handle, &read, &new_workspace.join("notes.md")).await;
    assert_eq!(denied["bytes"], json!(0));
    assert!(
        denied["content"]
            .as_str()
            .is_some_and(|text| text.contains("operation not allowed")),
        "{denied}"
    );
    let listed = payload(call_tool(&handle, &skills, json!({})).await);
    assert_eq!(listed["skills"][0]["name"], json!("old"));
    assert_eq!(listed["skills"][0]["source"], json!("old_mod"));

    *info.write().expect("replace workspace info") = WorkspaceInfo::new(
        1,
        new_workspace.clone(),
        vec![SkillRoot {
            source: None,
            root: new_skills.clone(),
        }],
        vec![ModDir {
            name: "demo".to_owned(),
            version: 2,
            root: tmp.path().join("mod_v2"),
        }],
    );

    assert_eq!(
        read_file(&handle, &read, &new_workspace.join("notes.md")).await["content"],
        json!("notes")
    );
    let denied = read_file(&handle, &read, &old_workspace.join("notes.md")).await;
    assert_eq!(denied["bytes"], json!(0));
    assert!(
        denied["content"]
            .as_str()
            .is_some_and(|text| text.contains("operation not allowed")),
        "{denied}"
    );
    let listed = payload(call_tool(&handle, &skills, json!({})).await);
    assert_eq!(listed["skills"][0]["name"], json!("new"));
    assert_eq!(listed["skills"][0]["source"], Value::Null);
    assert_eq!(
        listed["skills"].as_array().map(Vec::len),
        Some(1),
        "the replaced roots must drop the old skill: {listed}"
    );
    let facts = payload(call_tool(&handle, &workspace, json!({})).await);
    assert_eq!(
        facts["workspace"],
        json!(fs::canonicalize(new_workspace).expect("normalize the workspace"))
    );
    assert_eq!(facts["mods"][0]["version"], json!(2));
    assert_eq!(bus.payloads("core/mcp.started").len(), 3);
    assert!(bus.payloads("core/mcp.stopped").is_empty());
    handle.shutdown().await;
}
