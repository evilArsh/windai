#[path = "common/lib.rs"]
mod common;
#[path = "common/registry_support.rs"]
mod support;

use std::path::Path;
use std::sync::Arc;

use tempfile::TempDir;
use wind_mcp::registry::{BindingSpec, BindingState, Registry, SOURCE_CONFIGURED, SOURCE_MODS};
use wind_mcp::service::McpService;
use wind_mcp::{Connector, McpError, McpOptions};
use wind_storage::Storage;
use wind_storage::error::StorageError;
use wind_wire::{McpConfig, McpDefinition};

async fn service() -> (TempDir, Storage, McpService) {
    let dir = TempDir::new().expect("a temporary directory is required");
    let storage = Storage::open(&dir.path().join("wind.db"))
        .await
        .expect("the database must open");
    let outlet = Arc::new(support::RecordingOutlet::default());
    let registry = Arc::new(Registry::new(
        Connector::from_host(McpOptions::default()),
        support::dispatcher(outlet),
    ));
    let service = McpService::new(storage.clone(), registry);
    (dir, storage, service)
}

fn stdio(name: &str, cwd: &Path) -> McpDefinition {
    McpDefinition {
        name: name.to_string(),
        description: Some(format!("{name} server")),
        config: McpConfig::Stdio {
            command: "./server".to_string(),
            args: vec!["--root".to_string(), "work".to_string()],
            env: Default::default(),
            cwd: cwd.to_path_buf(),
        },
    }
}

fn missing_command(dir: &TempDir, name: &str) -> McpDefinition {
    McpDefinition {
        name: name.to_string(),
        description: None,
        config: McpConfig::Stdio {
            command: dir
                .path()
                .join("missing-server")
                .to_string_lossy()
                .into_owned(),
            args: Vec::new(),
            env: Default::default(),
            cwd: dir.path().to_path_buf(),
        },
    }
}

async fn topic(storage: &Storage, label: &str) -> i64 {
    storage
        .create_topic(None, label, None, None)
        .await
        .expect("the topic must be created")
}

#[tokio::test]
async fn create_and_list_do_not_enable() {
    let (dir, storage, service) = service().await;
    let definition = stdio("files", dir.path());
    let change = service
        .create(&definition)
        .await
        .expect("the server must be created");
    let record = change.record.expect("create must return the record");
    assert_eq!(record.revision, 1);
    assert_eq!(record.definition, definition);
    assert!(
        change.affected_topics.is_empty(),
        "create must not enable or start the server"
    );
    assert_eq!(service.list().await.expect("list"), vec![record.clone()]);
    let topic_id = topic(&storage, "one").await;
    assert!(service.enabled(topic_id).await.expect("enabled").is_empty());

    let mut nameless = stdio("unused", dir.path());
    nameless.name.clear();
    assert!(service.create(&nameless).await.is_err());
    let mut commandless = stdio("unused", dir.path());
    if let McpConfig::Stdio { command, .. } = &mut commandless.config {
        command.clear();
    }
    assert!(service.create(&commandless).await.is_err());
    assert_eq!(
        service.list().await.expect("list"),
        vec![record],
        "invalid definitions must not be persisted"
    );
}

#[tokio::test]
async fn update_checks_revision_and_reports_topics() {
    let (dir, storage, service) = service().await;
    let created = service
        .create(&stdio("files", dir.path()))
        .await
        .expect("create")
        .record
        .expect("record");
    let first = topic(&storage, "first").await;
    let second = topic(&storage, "second").await;
    service
        .enable(first, created.id)
        .await
        .expect("the first topic must enable");
    service
        .enable(second, created.id)
        .await
        .expect("the second topic must enable");

    let updated = stdio("files_v2", dir.path());
    let change = service
        .update(created.id, 1, &updated)
        .await
        .expect("the update must succeed");
    let record = change.record.expect("update must return the record");
    assert_eq!(record.revision, 2);
    assert_eq!(record.definition, updated);
    assert_eq!(change.affected_topics, vec![first, second]);

    let stale = service
        .update(created.id, 1, &stdio("stale", dir.path()))
        .await;
    assert!(
        matches!(stale, Err(McpError::Storage(StorageError::Conflict { .. }))),
        "a stale revision must be rejected: {stale:?}"
    );
    assert_eq!(
        service.get(created.id).await.expect("get"),
        Some(record),
        "a stale update must not overwrite the current revision"
    );

    let missing = service.update(999, 1, &stdio("missing", dir.path())).await;
    assert!(matches!(
        missing,
        Err(McpError::Storage(StorageError::NotFound { .. }))
    ));
}

#[tokio::test]
async fn enable_disable_and_delete() {
    let (dir, storage, service) = service().await;
    let record = service
        .create(&stdio("files", dir.path()))
        .await
        .expect("create")
        .record
        .expect("record");
    let topic_id = topic(&storage, "one").await;

    let enabled = service.enable(topic_id, record.id).await.expect("enable");
    assert_eq!(enabled.affected_topics, vec![topic_id]);
    assert_eq!(
        service.enabled(topic_id).await.expect("enabled"),
        vec![record.clone()]
    );
    assert_eq!(
        service
            .enable(topic_id, record.id)
            .await
            .expect("enable twice")
            .affected_topics,
        vec![topic_id],
        "enabling twice must still report the topic"
    );

    let disabled = service.disable(topic_id, record.id).await.expect("disable");
    assert_eq!(disabled.affected_topics, vec![topic_id]);
    assert!(service.enabled(topic_id).await.expect("enabled").is_empty());
    service
        .disable(topic_id, record.id)
        .await
        .expect("disabling twice must be idempotent");

    service
        .enable(topic_id, record.id)
        .await
        .expect("enable again");
    let deleted = service.delete(record.id).await.expect("delete");
    assert!(deleted.record.is_none());
    assert_eq!(deleted.affected_topics, vec![topic_id]);
    assert!(service.list().await.expect("list").is_empty());
    assert_eq!(service.get(record.id).await.expect("get"), None);
    assert!(service.enabled(topic_id).await.expect("enabled").is_empty());

    let missing_topic = service.enable(999, record.id).await;
    assert!(matches!(
        missing_topic,
        Err(McpError::Storage(StorageError::NotFound { .. }))
    ));
}

#[tokio::test]
async fn configured_binding_failure_is_queryable() {
    let (dir, storage, service) = service().await;
    let record = service
        .create(&missing_command(&dir, "broken"))
        .await
        .expect("create")
        .record
        .expect("record");
    let topic_id = topic(&storage, "one").await;
    service.enable(topic_id, record.id).await.expect("enable");

    let report = service
        .registry()
        .sync(
            topic_id,
            SOURCE_CONFIGURED,
            vec![BindingSpec {
                key: record.id.to_string(),
                revision: record.revision,
                definition: record.definition.clone(),
            }],
        )
        .await
        .expect("the failed binding must be reported");
    assert_eq!(report.bindings[0].state, BindingState::Failed);
    let statuses = service.status(topic_id);
    let status = statuses
        .iter()
        .find(|status| status.source == SOURCE_CONFIGURED)
        .expect("the configured binding must be visible");
    assert_eq!(status.state, BindingState::Failed);
    assert!(status.error.is_some());
    assert_eq!(
        service.enabled(topic_id).await.expect("enabled"),
        vec![record.clone()],
        "runtime failure must not remove the persistent enablement"
    );

    service
        .registry()
        .sync(topic_id, SOURCE_MODS, Vec::new())
        .await
        .expect("a mod source must sync independently");
    assert_eq!(
        service.list().await.expect("list"),
        vec![record],
        "mod derived bindings must not appear in the configured list"
    );
}
