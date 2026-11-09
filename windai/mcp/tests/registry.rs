use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use wind_mcp::builtin::fs::FsServer;
use wind_mcp::builtin::skills::{SkillRoot, SkillsServer};
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
    assert!(!first.is_empty(), "the first reference must expose its tools");
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
            FsServer::new("fs:topic:1", vec![ws_a.clone()]),
        )
        .await
        .expect("the first builtin fs must connect");
    handle
        .acquire_builtin(
            "t2".to_owned(),
            FsServer::new("fs:topic:2", vec![ws_b.clone()]),
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
            FsServer::new("fs:topic:1", vec![workspace.clone()]),
        )
        .await
        .expect("the first builtin fs must connect");
    handle
        .acquire_builtin(
            "t2".to_owned(),
            FsServer::new("fs:topic:1", vec![workspace]),
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
async fn builtin_skills_lists_skills_within_its_roots() {
    let tmp = TempDir::new().expect("create a temp dir");
    let root = tmp.path().join("skills");
    write_skill(&root.join("a"), "demo", "demo skill");
    let outside = tmp.path().join("outside");
    fs::create_dir_all(&outside).expect("create the outside directory");

    let handle = init(options(), bus());
    handle
        .acquire_builtin(
            "t1".to_owned(),
            SkillsServer::new(
                "skills:topic:1",
                vec![SkillRoot {
                    source: "s1".to_owned(),
                    path: root.clone(),
                }],
            ),
        )
        .await
        .expect("the builtin skills server must connect");
    let tool = find_tool(&handle, "skills:topic:1", "skills_list").await;
    let listed = structured(call_tool(&handle, &tool, json!({"dir": root.to_string_lossy()})).await);
    assert_eq!(listed["skills"].as_array().map(Vec::len), Some(1));
    assert_eq!(listed["skills"][0]["name"], json!("demo"));
    assert_eq!(listed["skills"][0]["source"], json!("s1"));

    let denied = call_tool(&handle, &tool, json!({"dir": outside.to_string_lossy()})).await;
    assert!(
        matches!(denied, CallOutcome::Failed(_)),
        "a directory outside the skill roots must be rejected: {denied:?}"
    );
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
            && value["structuredContent"]["mark"] == json!(expected)
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

fn structured(outcome: CallOutcome) -> Value {
    match outcome {
        CallOutcome::Ok(value) => value["structuredContent"].clone(),
        CallOutcome::Failed(error) => panic!("the tool call must return a result: {error}"),
    }
}

async fn read_file(handle: &RegistryHandle, tool: &BoundTool, path: &Path) -> Value {
    structured(call_tool(handle, tool, json!({"path": path.to_string_lossy()})).await)
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
