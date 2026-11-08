#[path = "common/lib.rs"]
mod common;
#[path = "common/registry_support.rs"]
mod support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use wind_event::Dispatcher;
use wind_mcp::events::{CORE_MCP_FAILED, CORE_MCP_STARTED, CORE_MCP_STOPPED};
use wind_mcp::registry::{BindingSpec, BindingState, Registry, SOURCE_CONFIGURED, SOURCE_MODS};
use wind_mcp::{Connector, McpError, McpOptions};
use wind_wire::{McpConfig, McpDefinition};

fn definition(name: &str, cwd: &Path, env: &[(&str, &str)]) -> McpDefinition {
    McpDefinition {
        name: name.to_string(),
        description: None,
        config: McpConfig::Stdio {
            command: env!("CARGO_BIN_EXE_mcp_test_server").to_string(),
            args: Vec::new(),
            env: env
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            cwd: cwd.to_path_buf(),
        },
    }
}

fn with_env(mut definition: McpDefinition, key: &str, value: &str) -> McpDefinition {
    if let McpConfig::Stdio { env, .. } = &mut definition.config {
        env.insert(key.to_string(), value.to_string());
    }
    definition
}

fn spec(key: &str, revision: u32, definition: McpDefinition) -> BindingSpec {
    BindingSpec {
        key: key.to_string(),
        revision,
        definition,
    }
}

fn harness() -> (TempDir, Registry, Arc<support::RecordingOutlet>) {
    let dir = TempDir::new().expect("a temporary directory is required");
    let outlet = Arc::new(support::RecordingOutlet::default());
    let bus: Arc<Dispatcher> = support::dispatcher(outlet.clone());
    let registry = Registry::new(Connector::from_host(McpOptions::default()), bus);
    (dir, registry, outlet)
}

fn start_log(dir: &TempDir) -> PathBuf {
    dir.path().join("starts")
}

fn starts(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .map(|text| text.lines().filter(|line| !line.is_empty()).count())
        .unwrap_or(0)
}

async fn wait_for_starts(path: &Path, expected: usize) {
    let started = Instant::now();
    while starts(path) < expected {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the test server must start"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_file(path: &Path) {
    let started = Instant::now();
    while !path.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the file {} must appear",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_event(outlet: &support::RecordingOutlet, name: &str) {
    let started = Instant::now();
    while !outlet.names().iter().any(|event| event == name) {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the event {name} must be delivered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn same_topic_equal_configs_share_connection() {
    let (dir, registry, outlet) = harness();
    let log = start_log(&dir);
    let server = definition(
        "shared",
        dir.path(),
        &[("WIND_TEST_START_LOG", &log.to_string_lossy())],
    );
    let report = registry
        .sync(
            1,
            SOURCE_MODS,
            vec![spec("a", 1, server.clone()), spec("b", 1, server)],
        )
        .await
        .expect("the bindings must sync");
    assert_eq!(report.bindings.len(), 2);
    assert!(
        report
            .bindings
            .iter()
            .all(|binding| binding.state == BindingState::Ready),
        "both bindings must be ready: {report:?}"
    );
    assert_eq!(starts(&log), 1, "equal configs must reuse one handshake");

    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("tools must list");
    assert_eq!(
        tools
            .iter()
            .filter(|tool| tool.target.tool == "echo")
            .count(),
        2,
        "one shared connection must be projected onto both bindings"
    );
    let started = outlet
        .names()
        .iter()
        .filter(|name| name.as_str() == CORE_MCP_STARTED.name)
        .count();
    assert_eq!(started, 1, "one connection must emit one start event");
    registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn different_environment_or_cwd_does_not_share() {
    let (dir, registry, _outlet) = harness();
    let log = start_log(&dir);
    let nested = dir.path().join("nested");
    std::fs::create_dir_all(&nested).expect("the nested directory must be created");
    let base = definition(
        "base",
        dir.path(),
        &[("WIND_TEST_START_LOG", &log.to_string_lossy())],
    );
    let other_environment = with_env(base.clone(), "WIND_TEST_MARK", "other");
    let other_cwd = definition(
        "cwd",
        &nested,
        &[("WIND_TEST_START_LOG", &log.to_string_lossy())],
    );
    registry
        .sync(
            1,
            SOURCE_MODS,
            vec![
                spec("base", 1, base),
                spec("environment", 1, other_environment),
                spec("cwd", 1, other_cwd),
            ],
        )
        .await
        .expect("the bindings must sync");
    assert_eq!(
        starts(&log),
        3,
        "environment or cwd must separate connections"
    );
    registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn different_topics_never_share() {
    let (dir, registry, _outlet) = harness();
    let log = start_log(&dir);
    let server = definition(
        "shared",
        dir.path(),
        &[("WIND_TEST_START_LOG", &log.to_string_lossy())],
    );
    registry
        .sync(1, SOURCE_MODS, vec![spec("a", 1, server.clone())])
        .await
        .expect("the first topic must sync");
    registry
        .sync(2, SOURCE_MODS, vec![spec("a", 1, server)])
        .await
        .expect("the second topic must sync");
    assert_eq!(starts(&log), 2, "topics must not share connections");
    let first = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("the first topic must list");
    let second = registry
        .tools(2, CancellationToken::new())
        .await
        .expect("the second topic must list");
    assert_eq!(first.len(), 5);
    assert_eq!(second.len(), 5);
    registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn sync_only_replaces_its_source() {
    let (dir, registry, outlet) = harness();
    let log = start_log(&dir);
    let mods_server = definition(
        "mods",
        dir.path(),
        &[("WIND_TEST_START_LOG", &log.to_string_lossy())],
    );
    let configured_server = definition("configured", dir.path(), &[]);
    registry
        .sync(1, SOURCE_MODS, vec![spec("a", 1, mods_server)])
        .await
        .expect("the mods source must sync");
    registry
        .sync(1, SOURCE_CONFIGURED, vec![spec("b", 1, configured_server)])
        .await
        .expect("the configured source must sync");

    let report = registry
        .sync(1, SOURCE_MODS, Vec::new())
        .await
        .expect("the empty source must sync");
    assert_eq!(report.removed, vec!["a".to_string()]);
    let statuses = registry.status(1);
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].source, SOURCE_CONFIGURED);
    assert_eq!(statuses[0].key, "b");
    assert_eq!(statuses[0].state, BindingState::Ready);
    wait_for_event(&outlet, CORE_MCP_STOPPED.name).await;
    registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn failed_replacement_does_not_use_old_binding() {
    let (dir, registry, _outlet) = harness();
    let log = start_log(&dir);
    let exit_file = dir.path().join("exit");
    let server = definition(
        "healthy",
        dir.path(),
        &[
            ("WIND_TEST_START_LOG", &log.to_string_lossy()),
            ("WIND_TEST_EXIT_FILE", &exit_file.to_string_lossy()),
        ],
    );
    registry
        .sync(1, SOURCE_MODS, vec![spec("a", 1, server)])
        .await
        .expect("the first binding must sync");
    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("the first binding must list");
    let old = tools
        .iter()
        .find(|tool| tool.target.tool == "echo")
        .expect("the echo tool must be listed")
        .target
        .clone();

    let missing = McpDefinition {
        name: "broken".to_string(),
        description: None,
        config: McpConfig::Stdio {
            command: dir
                .path()
                .join("missing-server")
                .to_string_lossy()
                .into_owned(),
            args: Vec::new(),
            env: BTreeMap::new(),
            cwd: dir.path().to_path_buf(),
        },
    };
    let report = registry
        .sync(1, SOURCE_MODS, vec![spec("a", 2, missing)])
        .await
        .expect("a failed replacement is reported, not raised");
    assert_eq!(report.bindings[0].state, BindingState::Failed);
    assert!(
        report.bindings[0].error.is_some(),
        "the failure must be diagnosable"
    );
    assert!(
        registry
            .tools(1, CancellationToken::new())
            .await
            .expect("tools must list")
            .is_empty(),
        "a failed binding must not expose the old tool list"
    );
    assert!(
        matches!(registry.admit(&old), Err(McpError::ToolUnavailable)),
        "an old target must not reach a replaced binding"
    );
    assert_eq!(starts(&log), 1, "a failed replacement must not reconnect");
    wait_for_file(&exit_file).await;
    registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn last_binding_waits_for_inflight_call() {
    let (dir, registry, _outlet) = harness();
    let log = start_log(&dir);
    let exit_file = dir.path().join("exit");
    let server = definition(
        "healthy",
        dir.path(),
        &[
            ("WIND_TEST_START_LOG", &log.to_string_lossy()),
            ("WIND_TEST_EXIT_FILE", &exit_file.to_string_lossy()),
        ],
    );
    registry
        .sync(1, SOURCE_MODS, vec![spec("a", 1, server)])
        .await
        .expect("the binding must sync");
    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("tools must list");
    let target = tools
        .iter()
        .find(|tool| tool.target.tool == "wait")
        .expect("the wait tool must be listed")
        .target
        .clone();
    let call = registry.admit(&target).expect("the call must be admitted");
    let executing = tokio::spawn(async move {
        call.execute(
            json!({"millis": 300}),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
    });

    let report = registry
        .sync(1, SOURCE_MODS, Vec::new())
        .await
        .expect("the binding must be removed");
    assert_eq!(report.removed, vec!["a".to_string()]);
    assert!(
        matches!(registry.admit(&target), Err(McpError::ToolUnavailable)),
        "a removed binding must reject new calls"
    );
    let result = executing.await.expect("the call task must finish");
    assert!(
        result.is_ok(),
        "the in-flight call must complete before the connection closes: {result:?}"
    );
    wait_for_file(&exit_file).await;
    registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn cancel_connect_wakes_waiters() {
    let (dir, registry, _outlet) = harness();
    let registry = Arc::new(registry);
    let log = start_log(&dir);
    let gate = dir.path().join("gate");
    let server = definition(
        "gated",
        dir.path(),
        &[
            ("WIND_TEST_START_LOG", &log.to_string_lossy()),
            ("WIND_TEST_HANDSHAKE_GATE", &gate.to_string_lossy()),
        ],
    );
    let syncing = tokio::spawn({
        let registry = Arc::clone(&registry);
        async move {
            registry
                .sync(1, SOURCE_MODS, vec![spec("a", 1, server)])
                .await
        }
    });
    wait_for_starts(&log, 1).await;

    let cancel = CancellationToken::new();
    let listing = tokio::spawn({
        let registry = Arc::clone(&registry);
        let cancel = cancel.clone();
        async move { registry.tools(1, cancel).await }
    });
    cancel.cancel();
    let result = listing.await.expect("the listing task must finish");
    assert!(
        matches!(result, Err(McpError::Cancelled)),
        "a cancelled waiter must end: {result:?}"
    );

    std::fs::write(&gate, "go").expect("the gate must open");
    let report = syncing
        .await
        .expect("the sync task must finish")
        .expect("the sync must succeed");
    assert_eq!(report.bindings[0].state, BindingState::Ready);
    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("the connection must be usable after a cancelled waiter");
    assert_eq!(tools.len(), 5);
    registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn late_connect_cannot_restore_removed_binding() {
    let (dir, registry, _outlet) = harness();
    let registry = Arc::new(registry);
    let log = start_log(&dir);
    let gate = dir.path().join("gate");
    let server = definition(
        "gated",
        dir.path(),
        &[
            ("WIND_TEST_START_LOG", &log.to_string_lossy()),
            ("WIND_TEST_HANDSHAKE_GATE", &gate.to_string_lossy()),
        ],
    );
    let later = server.clone();
    let syncing = tokio::spawn({
        let registry = Arc::clone(&registry);
        async move {
            registry
                .sync(1, SOURCE_MODS, vec![spec("a", 1, server)])
                .await
        }
    });
    wait_for_starts(&log, 1).await;
    registry
        .release_topic(1)
        .await
        .expect("the topic must be released");

    std::fs::write(&gate, "go").expect("the gate must open");
    let report = syncing
        .await
        .expect("the sync task must finish")
        .expect("the sync must succeed");
    assert!(
        report.bindings.is_empty(),
        "a late connection must not report bindings for a released topic"
    );
    assert!(registry.status(1).is_empty());
    assert!(
        registry
            .tools(1, CancellationToken::new())
            .await
            .expect("tools must list")
            .is_empty()
    );
    registry
        .sync(1, SOURCE_MODS, vec![spec("a", 1, later)])
        .await
        .expect("a released topic must accept a fresh binding");
    wait_for_starts(&log, 2).await;
    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("the fresh connection must list");
    assert_eq!(tools.len(), 5, "the fresh binding must connect");
    registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn reconnect_invalidates_old_generation() {
    let (dir, registry, _outlet) = harness();
    let first = definition("first", dir.path(), &[]);
    let second = with_env(
        definition("second", dir.path(), &[]),
        "WIND_TEST_MARK",
        "v2",
    );
    registry
        .sync(1, SOURCE_MODS, vec![spec("a", 1, first)])
        .await
        .expect("the first binding must sync");
    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("tools must list");
    let old = tools
        .iter()
        .find(|tool| tool.target.tool == "echo")
        .expect("the echo tool must be listed")
        .target
        .clone();

    let report = registry
        .sync(1, SOURCE_MODS, vec![spec("a", 2, second)])
        .await
        .expect("the replacement must sync");
    assert_eq!(report.bindings[0].state, BindingState::Ready);
    assert!(
        matches!(registry.admit(&old), Err(McpError::ToolUnavailable)),
        "an old generation must not reach the new connection"
    );
    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("tools must list");
    let echo = tools
        .iter()
        .find(|tool| tool.target.tool == "echo")
        .expect("the echo tool must be listed")
        .target
        .clone();
    let context = tools
        .iter()
        .find(|tool| tool.target.tool == "context")
        .expect("the context tool must be listed")
        .target
        .clone();
    assert!(
        echo.generation > old.generation,
        "a new connection must get a new generation"
    );
    let value = registry
        .admit(&context)
        .expect("the new target must be admitted")
        .execute(
            json!({}),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .expect("the context tool must answer");
    assert_eq!(
        value["structuredContent"]["mark"],
        json!("v2"),
        "the replacement must use the new environment"
    );
    registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn tool_list_failure_keeps_other_services_visible() {
    let (dir, registry, outlet) = harness();
    let failing = with_env(
        definition("failing", dir.path(), &[]),
        "WIND_TEST_LIST_FAIL",
        "1",
    );
    let healthy = definition("healthy", dir.path(), &[]);
    registry
        .sync(
            1,
            SOURCE_MODS,
            vec![spec("bad", 1, failing), spec("good", 1, healthy)],
        )
        .await
        .expect("the bindings must sync");
    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("the healthy service must remain visible");
    assert_eq!(tools.len(), 5);
    assert!(
        tools.iter().all(|tool| tool.target.key == "good"),
        "only the healthy service must be projected"
    );
    let statuses = registry.status(1);
    let bad = statuses
        .iter()
        .find(|status| status.key == "bad")
        .expect("the failing binding must be reported");
    assert_eq!(bad.state, BindingState::Failed);
    assert!(
        bad.error.as_deref().is_some_and(|error| !error.is_empty()),
        "the list failure must stay diagnosable"
    );
    assert!(
        outlet
            .names()
            .iter()
            .any(|name| name.as_str() == CORE_MCP_FAILED.name),
        "a failed binding must emit a failure event"
    );
    registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn cancelled_tool_listing_keeps_the_connection_ready() {
    let (dir, registry, outlet) = harness();
    let registry = Arc::new(registry);
    let gate = dir.path().join("list-gate");
    let listed = dir.path().join("list-log");
    let server = definition(
        "slow",
        dir.path(),
        &[
            ("WIND_TEST_LIST_GATE", &gate.to_string_lossy()),
            ("WIND_TEST_LIST_LOG", &listed.to_string_lossy()),
        ],
    );
    registry
        .sync(1, SOURCE_MODS, vec![spec("a", 1, server)])
        .await
        .expect("the binding must sync");

    let cancel = CancellationToken::new();
    let listing = tokio::spawn({
        let registry = Arc::clone(&registry);
        let cancel = cancel.clone();
        async move { registry.tools(1, cancel).await }
    });
    wait_for_file(&listed).await;
    cancel.cancel();
    let result = listing.await.expect("the listing task must finish");
    assert!(
        matches!(result, Err(McpError::Cancelled)),
        "a cancelled listing must end: {result:?}"
    );
    let statuses = registry.status(1);
    assert_eq!(
        statuses[0].state,
        BindingState::Ready,
        "cancellation must not fail the shared connection"
    );
    assert!(
        !outlet
            .names()
            .iter()
            .any(|name| name.as_str() == CORE_MCP_FAILED.name),
        "cancellation must not emit a failure event"
    );

    std::fs::write(&gate, "go").expect("the list gate must open");
    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("the connection must list after cancellation");
    assert_eq!(tools.len(), 5);
    registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn failed_connect_is_retried_on_the_next_listing() {
    let dir = TempDir::new().expect("a temporary directory is required");
    let outlet = Arc::new(support::RecordingOutlet::default());
    let bus: Arc<Dispatcher> = support::dispatcher(outlet);
    let options = McpOptions {
        connect_timeout: Duration::from_millis(300),
        ..McpOptions::default()
    };
    let registry = Registry::new(Connector::from_host(options), bus);
    let gate = dir.path().join("handshake-gate");
    let server = definition(
        "slow",
        dir.path(),
        &[("WIND_TEST_HANDSHAKE_GATE", &gate.to_string_lossy())],
    );
    let report = registry
        .sync(1, SOURCE_MODS, vec![spec("a", 1, server)])
        .await
        .expect("a failed connect is reported, not raised");
    assert_eq!(report.bindings[0].state, BindingState::Failed);

    std::fs::write(&gate, "go").expect("the handshake gate must open");
    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("the next listing must retry the connection");
    assert_eq!(tools.len(), 5);
    assert_eq!(
        registry.status(1)[0].state,
        BindingState::Ready,
        "a successful retry must clear the failure"
    );
    registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn failed_tool_listing_is_retried_on_the_next_listing() {
    let (dir, registry, _outlet) = harness();
    let fail = dir.path().join("list-fail");
    std::fs::write(&fail, "fail").expect("the failure marker must be written");
    let server = definition(
        "flaky",
        dir.path(),
        &[("WIND_TEST_LIST_FAIL_FILE", &fail.to_string_lossy())],
    );
    registry
        .sync(1, SOURCE_MODS, vec![spec("a", 1, server)])
        .await
        .expect("the binding must sync");
    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("a failed listing must not abort the call");
    assert!(tools.is_empty());
    assert_eq!(registry.status(1)[0].state, BindingState::Failed);

    std::fs::remove_file(&fail).expect("the failure marker must be removed");
    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("the next listing must retry the tool list");
    assert_eq!(tools.len(), 5);
    assert_eq!(
        registry.status(1)[0].state,
        BindingState::Ready,
        "a successful retry must clear the failure"
    );
    registry.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn unchanged_connection_is_reused_across_revisions() {
    let (dir, registry, _outlet) = harness();
    let log = start_log(&dir);
    let server = definition(
        "first",
        dir.path(),
        &[("WIND_TEST_START_LOG", &log.to_string_lossy())],
    );
    registry
        .sync(1, SOURCE_MODS, vec![spec("a", 1, server)])
        .await
        .expect("the binding must sync");
    wait_for_starts(&log, 1).await;
    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("tools must list");
    let old = tools
        .iter()
        .find(|tool| tool.target.tool == "echo")
        .expect("the echo tool must be listed")
        .target
        .clone();

    let mut renamed = definition(
        "second",
        dir.path(),
        &[("WIND_TEST_START_LOG", &log.to_string_lossy())],
    );
    renamed.description = Some("a renamed server".to_string());
    let report = registry
        .sync(1, SOURCE_MODS, vec![spec("a", 2, renamed)])
        .await
        .expect("the revision must sync");
    assert_eq!(report.bindings[0].state, BindingState::Ready);
    let tools = registry
        .tools(1, CancellationToken::new())
        .await
        .expect("tools must list");
    let current = tools
        .iter()
        .find(|tool| tool.target.tool == "echo")
        .expect("the echo tool must be listed")
        .target
        .clone();
    assert_eq!(
        current.generation, old.generation,
        "an unchanged connection must keep its generation"
    );
    assert_eq!(starts(&log), 1, "an unchanged connection must not restart");
    assert!(
        matches!(registry.admit(&old), Err(McpError::ToolUnavailable)),
        "an old revision must not reach the reused connection"
    );
    registry
        .admit(&current)
        .expect("the current revision must be admitted");
    registry.shutdown().await.expect("shutdown");
}
