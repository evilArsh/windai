#[path = "support/mod.rs"]
mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use support::{
    ScriptedLauncher, bus, entry_generation, roots, spec, spec_with_env, tool_names,
    wait_generation, wait_gone, wait_started,
};
use wind_mcp::registry::spawn_with;

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
