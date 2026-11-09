#[path = "support/mod.rs"]
mod support;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;
use support::{
    RecordingBus, ScriptedLauncher, bus, entry_generation, first_tool, options, roots, spec,
    spec_with_env, stderr_spec, tool_names, wait_generation, wait_gone, wait_started,
};
use tokio_util::sync::CancellationToken;
use wind_mcp::connector::CallOutcome;
use wind_mcp::registry::{spawn, spawn_with};

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
    let lines = captured.lock().expect("lock the captured lines");
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0], "the log line");
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
