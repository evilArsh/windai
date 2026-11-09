#[path = "support/mod.rs"]
mod support;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use support::{
    RecordingBus, ScriptedLauncher, bus, call_tool, entry_generation, find_tool, first_tool,
    options, roots, scope_roots, spec, spec_with_env, spec_with_envs, stderr_spec, tool_names,
    wait_generation, wait_gone, wait_started,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use wind_mcp::connector::CallOutcome;
use wind_mcp::registry::{BoundTool, BuiltinKind, spawn, spawn_with};
use wind_mcp::roots::ScopeId;

#[tokio::test]
async fn stale_report_does_not_revive_a_stopped_entry() {
    let launcher = ScriptedLauncher::default();
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus());
    handle.acquire("r1".to_owned(), spec("demo"));
    launcher.complete("demo", 1);
    wait_started(&handle, "demo").await;

    handle.acquire("r2".to_owned(), spec_with_env("demo", "V", "2"));
    handle.acquire("r3".to_owned(), spec_with_env("demo", "V", "3"));
    launcher.complete("demo", 2);
    assert_eq!(entry_generation(&handle, "demo").await, 3);
    launcher.complete("demo", 3);
    wait_started(&handle, "demo").await;
}

#[tokio::test]
async fn acquire_returns_before_the_service_is_ready() {
    let launcher = ScriptedLauncher::default();
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus());
    handle.acquire("r1".to_owned(), spec("demo"));
    assert!(handle.list_tools().await.is_empty());
    launcher.complete("demo", 1);
    wait_started(&handle, "demo").await;
    assert_eq!(tool_names(&handle).await, vec!["echo"]);
}

#[tokio::test]
async fn same_name_with_a_new_definition_restarts() {
    let launcher = ScriptedLauncher::default();
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus());
    handle.acquire("mod:demo@1".to_owned(), spec("demo"));
    launcher.complete("demo", 1);
    wait_started(&handle, "demo").await;
    handle.acquire("mod:demo@2".to_owned(), spec_with_env("demo", "V", "2"));
    assert_eq!(entry_generation(&handle, "demo").await, 2);
}

#[tokio::test]
async fn release_stops_a_running_service() {
    let launcher = ScriptedLauncher::default();
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus());
    handle.acquire("r1".to_owned(), spec("demo"));
    launcher.complete("demo", 1);
    wait_started(&handle, "demo").await;
    handle.release("r1".to_owned());
    wait_gone(&handle, "demo").await;
    assert!(handle.list_tools().await.is_empty());
}

#[tokio::test]
async fn release_during_start_discards_the_late_result() {
    let launcher = ScriptedLauncher::default();
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus());
    handle.acquire("r1".to_owned(), spec("demo"));
    handle.release("r1".to_owned());
    wait_generation(&handle, "demo", 2).await;
    launcher.complete("demo", 1);
    wait_gone(&handle, "demo").await;
    assert!(handle.list_tools().await.is_empty());
}

#[tokio::test]
async fn acquire_during_stopping_starts_the_new_definition() {
    let launcher = ScriptedLauncher::default();
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus());
    handle.acquire("r1".to_owned(), spec("demo"));
    handle.release("r1".to_owned());
    wait_generation(&handle, "demo", 2).await;
    handle.acquire("r2".to_owned(), spec_with_env("demo", "V", "2"));
    launcher.complete("demo", 1);
    wait_generation(&handle, "demo", 3).await;
    launcher.complete("demo", 3);
    wait_started(&handle, "demo").await;
    handle.release("r2".to_owned());
    wait_gone(&handle, "demo").await;
}

#[tokio::test]
async fn stale_start_does_not_attach_to_a_rebuilt_entry() {
    let launcher = ScriptedLauncher::default();
    launcher.serve_tools_for("demo", 1, &["old"]);
    launcher.serve_tools("demo", &["new"]);
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus());
    handle.acquire("r1".to_owned(), spec("demo"));
    handle.acquire("r2".to_owned(), spec_with_env("demo", "V", "2"));
    handle.release("r1".to_owned());
    handle.release("r2".to_owned());
    wait_generation(&handle, "demo", 3).await;
    launcher.fail("demo", 2, "boom");
    wait_gone(&handle, "demo").await;

    handle.acquire("r3".to_owned(), spec_with_env("demo", "V", "3"));
    launcher.complete("demo", 1);
    launcher.complete("demo", 4);
    let started = Instant::now();
    loop {
        if tool_names(&handle).await == vec!["new".to_owned()] {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the rebuilt entry must use the new definition"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(handle.entry_generation("demo").await, Some(4));
}

#[tokio::test]
async fn failed_start_is_retried_on_the_next_acquire() {
    let launcher = ScriptedLauncher::default();
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus());
    let service = spec("demo");
    handle.acquire("r1".to_owned(), service.clone());
    launcher.fail("demo", 1, "boom");
    let started = Instant::now();
    loop {
        handle.acquire("r1".to_owned(), service.clone());
        if handle.entry_generation("demo").await == Some(2) {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the failed start must be retried"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    launcher.complete("demo", 2);
    wait_started(&handle, "demo").await;
}

#[tokio::test]
async fn a_panicking_launch_is_reported_as_a_failure() {
    let launcher = ScriptedLauncher::default();
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus());
    let service = spec("demo");
    handle.acquire("r1".to_owned(), service.clone());
    launcher.panic_on_start("demo", 1);
    let started = Instant::now();
    loop {
        handle.acquire("r1".to_owned(), service.clone());
        if handle.entry_generation("demo").await == Some(2) {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the panicking start must be retried"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    launcher.complete("demo", 2);
    wait_started(&handle, "demo").await;
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("shutdown must return");
}

#[tokio::test]
async fn shutdown_stops_services_and_returns() {
    let launcher = ScriptedLauncher::default();
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus());
    handle.acquire("a".to_owned(), spec("a"));
    handle.acquire("b".to_owned(), spec("b"));
    launcher.complete("a", 1);
    launcher.complete("b", 2);
    wait_started(&handle, "a").await;
    wait_started(&handle, "b").await;
    tokio::time::timeout(Duration::from_secs(15), handle.shutdown())
        .await
        .expect("shutdown must return");
    assert!(handle.list_tools().await.is_empty());
}

#[tokio::test]
async fn launch_spec_can_be_rewritten_before_connecting() {
    let bus = RecordingBus::new();
    bus.rewrite("core/mcp.launch", |mut payload| {
        payload["spec"]["transport"]["Stdio"]["env"]["TOKEN"] = json!("injected");
        payload
    });
    let handle = spawn(options(), roots(), bus.dispatcher());
    handle.acquire("r1".to_owned(), spec("demo"));
    wait_started(&handle, "demo").await;
    assert!(bus.seen("core/mcp.launch"));
    let payloads = bus.payloads("core/mcp.launch");
    assert_eq!(
        payloads[0]["spec"]["transport"]["Stdio"]["env"]["TOKEN"],
        json!("injected")
    );
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("shutdown must return");
}

#[tokio::test]
async fn a_rewritten_command_reaches_the_launch() {
    let bus = RecordingBus::new();
    let missing = roots().mods.join("missing-mcp-server");
    bus.rewrite("core/mcp.launch", move |mut payload| {
        payload["spec"]["transport"]["Stdio"]["command"] = json!(missing.to_string_lossy());
        payload
    });
    let handle = spawn(options(), roots(), bus.dispatcher());
    handle.acquire("r1".to_owned(), spec("demo"));
    bus.poll_seen("core/mcp.failed").await;
    assert!(!bus.seen("core/mcp.started"));
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("shutdown must return");
}

#[tokio::test]
async fn lifecycle_events_follow_acquire_start_release() {
    let launcher = ScriptedLauncher::default();
    let bus = RecordingBus::new();
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus.dispatcher());
    handle.acquire("r1".to_owned(), spec("demo"));
    bus.poll_seen("core/mcp.starting").await;
    assert_eq!(bus.payloads("core/mcp.starting")[0]["refs"], json!(["r1"]));
    launcher.complete("demo", 1);
    wait_started(&handle, "demo").await;
    bus.poll_seen("core/mcp.started").await;
    let started = bus.payloads("core/mcp.started");
    assert_eq!(started[0]["tools"], json!(["echo"]));
    handle.release("r1".to_owned());
    wait_gone(&handle, "demo").await;
    bus.poll_seen("core/mcp.stopped").await;
    let stopped = bus.payloads("core/mcp.stopped");
    assert_eq!(stopped[0]["reason"], json!("released"));
    assert_eq!(stopped[0]["refs"], json!(["r1"]));
}

#[tokio::test]
async fn definition_change_publishes_replaced() {
    let launcher = ScriptedLauncher::default();
    let bus = RecordingBus::new();
    bus.rewrite("core/mcp.replaced", |mut payload| {
        payload["new"]["name"] = json!("hijacked");
        payload
    });
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus.dispatcher());
    handle.acquire("r1".to_owned(), spec_with_env("demo", "V", "1"));
    launcher.complete("demo", 1);
    wait_started(&handle, "demo").await;
    handle.acquire("r2".to_owned(), spec_with_env("demo", "V", "2"));
    bus.poll_seen("core/mcp.replaced").await;
    let replaced = bus.payloads("core/mcp.replaced");
    assert_eq!(
        replaced[0]["old"]["transport"]["Stdio"]["env"]["V"],
        json!("1")
    );
    assert_eq!(
        replaced[0]["new"]["transport"]["Stdio"]["env"]["V"],
        json!("2")
    );
    bus.poll_seen("core/mcp.stopped").await;
    let stopped = bus.payloads("core/mcp.stopped");
    assert_eq!(stopped[0]["reason"], json!("replaced"));
    assert_eq!(stopped[0]["refs"], json!(["r1", "r2"]));
    let launches = bus.payloads("core/mcp.launch");
    assert_eq!(
        launches.last().expect("a launch must follow")["spec"]["name"],
        json!("demo")
    );
    launcher.complete("demo", 2);
    wait_started(&handle, "demo").await;
    tokio::time::timeout(Duration::from_secs(15), handle.shutdown())
        .await
        .expect("shutdown must return");
}

#[tokio::test]
async fn shutdown_publishes_stopped_with_shutdown_reason() {
    let launcher = ScriptedLauncher::default();
    let bus = RecordingBus::new();
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus.dispatcher());
    handle.acquire("r1".to_owned(), spec("demo"));
    launcher.complete("demo", 1);
    wait_started(&handle, "demo").await;
    tokio::time::timeout(Duration::from_secs(15), handle.shutdown())
        .await
        .expect("shutdown must return");
    bus.poll_seen("core/mcp.stopped").await;
    let stopped = bus.payloads("core/mcp.stopped");
    assert_eq!(stopped[0]["reason"], json!("shutdown"));
    assert_eq!(stopped[0]["refs"], json!(["r1"]));
}

#[tokio::test]
async fn a_slow_launch_rewrite_still_starts_the_service() {
    let launcher = ScriptedLauncher::default();
    let bus = RecordingBus::new();
    bus.rewrite("core/mcp.launch", |payload| {
        std::thread::sleep(Duration::from_millis(1500));
        payload
    });
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus.dispatcher());
    handle.acquire("r1".to_owned(), spec("demo"));
    launcher.complete("demo", 1);
    wait_started(&handle, "demo").await;
    bus.poll_seen("core/mcp.launch").await;
    assert!(bus.seen("core/mcp.started"));
    handle.release("r1".to_owned());
    wait_gone(&handle, "demo").await;
}

#[tokio::test]
async fn same_startup_key_serializes_external_launches() {
    let tmp = TempDir::new().expect("a temporary directory is required");
    let gate = tmp.path().join("handshake.gate");
    let log = tmp.path().join("start.log");
    let gate = gate.to_string_lossy().into_owned();
    let log = log.to_string_lossy().into_owned();
    let envs = [
        ("WIND_TEST_HANDSHAKE_GATE", gate.as_str()),
        ("WIND_TEST_START_LOG", log.as_str()),
    ];

    let handle = spawn(options(), roots(), bus());
    handle.acquire("r1".to_owned(), spec_with_envs("a", &envs));
    handle.acquire("r2".to_owned(), spec_with_envs("b", &envs));
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

    std::fs::write(&gate, "go").expect("the handshake gate must be written");
    wait_started(&handle, "a").await;
    wait_started(&handle, "b").await;
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("shutdown must return");
    assert_eq!(
        start_log_lines(&log),
        2,
        "both launches must eventually start"
    );
}

fn start_log_lines(path: &str) -> usize {
    std::fs::read_to_string(path)
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

#[tokio::test]
async fn stderr_lines_are_published_and_forwarded() {
    let bus = RecordingBus::new();
    bus.rewrite("core/mcp.launch", |mut payload| {
        payload["spec"]["name"] = json!("hijacked");
        payload
    });
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let mut settings = options();
    settings.log = Some(Arc::new(move |line| {
        sink.lock()
            .expect("lock the captured lines")
            .push(line.to_owned());
    }));
    let handle = spawn(settings, roots(), bus.dispatcher());
    handle.acquire("r1".to_owned(), stderr_spec("demo", "the log line"));
    bus.poll_seen("core/mcp.log").await;
    let logs = bus.payloads("core/mcp.log");
    assert_eq!(logs[0]["stream"], json!("stderr"));
    assert_eq!(logs[0]["line"], json!("the log line"));
    assert_eq!(logs[0]["name"], json!("demo"));
    {
        let lines = captured.lock().expect("lock the captured lines");
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], "the log line");
    }
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("shutdown must return");
}

#[tokio::test]
async fn call_after_stop_returns_failure_instead_of_hanging() {
    let launcher = ScriptedLauncher::default();
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus());
    handle.acquire("r1".to_owned(), spec("demo"));
    launcher.complete("demo", 1);
    wait_started(&handle, "demo").await;
    let tool = first_tool(&handle, "demo").await;
    handle.release("r1".to_owned());
    wait_gone(&handle, "demo").await;
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        tool.call.clone().execute(
            json!({"text": "x"}),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(1),
        ),
    )
    .await
    .expect("a call after stop must not hang");
    assert!(
        matches!(outcome, CallOutcome::Failed(_)),
        "a stopped service must reject calls: {outcome:?}"
    );
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("shutdown must return");
}

#[tokio::test]
async fn refresh_replaces_the_cached_catalog() {
    let launcher = ScriptedLauncher::default();
    launcher.serve_tools("demo", &["echo"]);
    let handle = spawn_with(Arc::new(launcher.clone()), roots(), bus());
    handle.acquire("r1".to_owned(), spec("demo"));
    launcher.complete("demo", 1);
    wait_started(&handle, "demo").await;
    assert_eq!(tool_names(&handle).await, vec!["echo".to_owned()]);
    launcher.serve_tools("demo", &["echo", "wait"]);
    handle.refresh("demo");
    let started = Instant::now();
    loop {
        if tool_names(&handle).await == vec!["echo".to_owned(), "wait".to_owned()] {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the refreshed catalog must replace the cached one"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        tool_names(&handle).await,
        vec!["echo".to_owned(), "wait".to_owned()]
    );
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("shutdown must return");
}

#[tokio::test]
async fn a_tool_call_succeeds_through_the_public_handle() {
    let handle = spawn(options(), roots(), bus());
    handle.acquire("r1".to_owned(), spec("demo"));
    wait_started(&handle, "demo").await;
    let tool = first_tool(&handle, "demo").await;
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        tool.call.execute(
            json!({"text": "hi"}),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
        ),
    )
    .await
    .expect("the tool call must not hang");
    let CallOutcome::Ok(value) = outcome else {
        panic!("the echo tool must succeed: {outcome:?}");
    };
    assert!(
        value.to_string().contains("hi"),
        "the echo tool must return its argument: {value}"
    );
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("shutdown must return");
}

fn structured(outcome: CallOutcome) -> Value {
    match outcome {
        CallOutcome::Ok(value) => value["structuredContent"].clone(),
        CallOutcome::Failed(error) => panic!("the fs tool call must return a result: {error}"),
    }
}

async fn read_file(tool: &BoundTool, path: &Path) -> Value {
    structured(call_tool(tool, json!({"path": path.to_string_lossy()})).await)
}

#[tokio::test]
async fn two_scopes_are_two_isolated_instances() {
    let tmp = TempDir::new().expect("a temporary directory is required");
    let ws_a = tmp.path().join("ws-a");
    let ws_b = tmp.path().join("ws-b");
    std::fs::create_dir_all(&ws_a).expect("workspace a must be created");
    std::fs::create_dir_all(&ws_b).expect("workspace b must be created");
    let notes_a = ws_a.join("notes.md");
    let notes_b = ws_b.join("notes.md");
    std::fs::write(&notes_a, "notes from a").expect("the notes file of a must be written");
    std::fs::write(&notes_b, "notes from b").expect("the notes file of b must be written");

    let handle = spawn(options(), roots(), bus());
    let topic_1 = ScopeId::new("topic:1");
    let topic_2 = ScopeId::new("topic:2");
    handle.set_scope_roots(topic_1.clone(), scope_roots(&[ws_a]));
    handle.set_scope_roots(topic_2.clone(), scope_roots(&[ws_b]));
    handle.acquire_builtin("t1".to_owned(), topic_1, BuiltinKind::Fs);
    handle.acquire_builtin("t2".to_owned(), topic_2, BuiltinKind::Fs);
    wait_started(&handle, "fs:topic:1").await;
    wait_started(&handle, "fs:topic:2").await;

    let tool_a = find_tool(&handle, "fs:topic:1", "read_file").await;
    let read_a = read_file(&tool_a, &notes_a).await;
    assert!(
        read_a["bytes"].as_u64().is_some_and(|bytes| bytes > 0),
        "the first scope must read its own workspace: {read_a}"
    );
    assert!(
        read_a["content"]
            .as_str()
            .is_some_and(|content| content.contains("notes from a")),
        "the first scope must read its own content: {read_a}"
    );
    let denied_b = read_file(&tool_a, &notes_b).await;
    assert_eq!(
        denied_b["bytes"],
        json!(0),
        "the first scope must not see the workspace of the second: {denied_b}"
    );

    let tool_b = find_tool(&handle, "fs:topic:2", "read_file").await;
    let read_b = read_file(&tool_b, &notes_b).await;
    assert!(
        read_b["bytes"].as_u64().is_some_and(|bytes| bytes > 0),
        "the second scope must read its own workspace: {read_b}"
    );
    assert!(
        read_b["content"]
            .as_str()
            .is_some_and(|content| content.contains("notes from b")),
        "the second scope must read its own content: {read_b}"
    );
    let denied_a = read_file(&tool_b, &notes_a).await;
    assert_eq!(
        denied_a["bytes"],
        json!(0),
        "the second scope must not see the workspace of the first: {denied_a}"
    );

    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("shutdown must return");
}

#[tokio::test]
async fn root_changes_do_not_restart_the_builtin() {
    let tmp = TempDir::new().expect("a temporary directory is required");
    let ws_a = tmp.path().join("ws-a");
    let ws_b = tmp.path().join("ws-b");
    std::fs::create_dir_all(&ws_a).expect("workspace a must be created");
    std::fs::create_dir_all(&ws_b).expect("workspace b must be created");
    let notes_a = ws_a.join("notes.md");
    let notes_b = ws_b.join("notes.md");
    std::fs::write(&notes_a, "notes from a").expect("the notes file of a must be written");
    std::fs::write(&notes_b, "notes from b").expect("the notes file of b must be written");

    let handle = spawn(options(), roots(), bus());
    let scope = ScopeId::new("topic:1");
    handle.set_scope_roots(scope.clone(), scope_roots(std::slice::from_ref(&ws_a)));
    handle.acquire_builtin("t1".to_owned(), scope.clone(), BuiltinKind::Fs);
    wait_started(&handle, "fs:topic:1").await;
    let before = entry_generation(&handle, "fs:topic:1").await;
    let tool = find_tool(&handle, "fs:topic:1", "read_file").await;
    let read_a = read_file(&tool, &notes_a).await;
    assert!(
        read_a["bytes"].as_u64().is_some_and(|bytes| bytes > 0),
        "the instance must start with the first root: {read_a}"
    );

    handle.set_scope_roots(scope, scope_roots(&[ws_a, ws_b]));
    assert_eq!(
        entry_generation(&handle, "fs:topic:1").await,
        before,
        "a root change must not restart the builtin instance"
    );

    let started = Instant::now();
    loop {
        let read = read_file(&tool, &notes_b).await;
        if read["bytes"].as_u64().is_some_and(|bytes| bytes > 0) {
            assert!(
                read["content"]
                    .as_str()
                    .is_some_and(|content| content.contains("notes from b")),
                "the added root must serve its content: {read}"
            );
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the added root must take effect without a restart: {read}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("shutdown must return");
}

#[tokio::test]
async fn builtin_fs_without_scope_roots_is_rejected() {
    let bus = RecordingBus::new();
    let handle = spawn(options(), roots(), bus.dispatcher());
    handle.acquire_builtin("r1".to_owned(), ScopeId::new("topic:1"), BuiltinKind::Fs);
    bus.poll_seen("core/mcp.failed").await;
    let failed = bus.payloads("core/mcp.failed");
    assert_eq!(failed[0]["name"], json!("fs:topic:1"));
    assert_eq!(failed[0]["refs"], json!(["r1"]));
    assert!(
        !bus.seen("core/mcp.started"),
        "a builtin fs without scope roots must not start"
    );
    assert!(handle.list_tools().await.is_empty());
    handle.release("r1".to_owned());
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("shutdown must return");
}

#[tokio::test]
async fn builtin_fs_with_an_empty_workspace_is_rejected() {
    let bus = RecordingBus::new();
    let handle = spawn(options(), roots(), bus.dispatcher());
    let scope = ScopeId::new("topic:1");
    handle.set_scope_roots(scope.clone(), scope_roots(&[PathBuf::new()]));
    handle.acquire_builtin("r1".to_owned(), scope, BuiltinKind::Fs);
    bus.poll_seen("core/mcp.failed").await;
    let failed = bus.payloads("core/mcp.failed");
    assert_eq!(failed[0]["name"], json!("fs:topic:1"));
    assert_eq!(failed[0]["refs"], json!(["r1"]));
    assert!(
        !bus.seen("core/mcp.started"),
        "a builtin fs with an empty workspace must not start"
    );
    assert!(handle.list_tools().await.is_empty());
    handle.release("r1".to_owned());
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("shutdown must return");
}
