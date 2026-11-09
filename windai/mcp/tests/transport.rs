#[path = "support/mod.rs"]
mod support;

use std::time::{Duration, Instant};

use serde_json::json;
use tokio_util::sync::CancellationToken;
use wind_mcp::connector::CallOutcome;
use wind_mcp::options::McpOptions;

#[tokio::test]
async fn stdio_server_lists_and_answers() {
    let conn = support::open_test_server().await;
    let tools = conn.list_tools().await.expect("the tools must list");
    assert!(
        tools.iter().any(|tool| tool.name == "echo"),
        "the server must expose the echo tool"
    );
    let out = conn
        .call(
            "echo",
            json!({"text": "hi"}),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
        )
        .await;
    let CallOutcome::Ok(value) = out else {
        panic!("the echo tool must succeed: {out:?}");
    };
    assert!(
        value.to_string().contains("hi"),
        "the echo tool must return its argument: {value}"
    );
    conn.close().await;
}

#[tokio::test]
async fn call_after_close_fails_fast() {
    let conn = support::open_test_server().await;
    conn.close().await;
    let out = tokio::time::timeout(
        Duration::from_secs(1),
        conn.call(
            "echo",
            json!({"text": "hi"}),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
        ),
    )
    .await
    .expect("a call after close must not wait");
    assert_eq!(
        out,
        CallOutcome::Failed("the connection is closed".to_owned()),
        "a closed connection must reject calls"
    );
}

#[tokio::test]
async fn dropping_a_call_releases_the_inflight_slot() {
    let conn = support::open_test_server_with(McpOptions {
        shutdown_grace: Duration::from_secs(8),
        ..McpOptions::default()
    })
    .await;
    let call = conn.call(
        "wait",
        json!({"millis": 300}),
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(30),
    );
    let pending = tokio::time::timeout(Duration::from_millis(50), call).await;
    assert!(pending.is_err(), "the wait tool must still be pending");
    tokio::time::timeout(Duration::from_secs(3), conn.close())
        .await
        .expect("close must not wait out the shutdown grace");
}

#[cfg(unix)]
#[tokio::test]
async fn closing_the_connection_reaps_the_process_group() {
    let tmp = tempfile::TempDir::new().expect("a temporary directory is required");
    let pid_file = tmp.path().join("grandchild.pid");
    let pid_file_text = pid_file.to_string_lossy().into_owned();
    let conn = support::open_test_server_with_envs(&[(
        "WIND_TEST_GRANDCHILD_FILE",
        pid_file_text.as_str(),
    )])
    .await;
    let pid = wait_for_grandchild_pid(&pid_file).await;
    assert!(process_exists(pid), "the grandchild must start alive");

    conn.close().await;

    let started = Instant::now();
    while process_exists(pid) {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the process group must be reaped after close"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(unix)]
async fn wait_for_grandchild_pid(path: &std::path::Path) -> u32 {
    let started = Instant::now();
    loop {
        if let Ok(text) = std::fs::read_to_string(path)
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
