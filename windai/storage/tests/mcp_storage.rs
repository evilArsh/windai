use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use sqlx::Connection;
use tempfile::TempDir;
use wind_storage::Storage;
use wind_storage::error::StorageError;
use wind_wire::{McpConfig, McpDefinition};

async fn database() -> (TempDir, Storage) {
    let dir = TempDir::new().expect("temp dir");
    let storage = Storage::open(&dir.path().join("windai.db"))
        .await
        .expect("open database");
    (dir, storage)
}

fn stdio(name: &str) -> McpDefinition {
    McpDefinition {
        name: name.to_string(),
        description: Some(format!("{name} server")),
        config: McpConfig::Stdio {
            command: "./server".to_string(),
            args: vec!["--root".to_string(), "work".to_string()],
            env: BTreeMap::from([
                ("ALPHA".to_string(), "1".to_string()),
                ("BETA".to_string(), "two".to_string()),
            ]),
            cwd: PathBuf::from("/tmp/wind"),
        },
    }
}

fn http(name: &str, url: &str) -> McpDefinition {
    McpDefinition {
        name: name.to_string(),
        description: None,
        config: McpConfig::StreamableHttp {
            url: url.to_string(),
        },
    }
}

async fn create(storage: &Storage, definition: &McpDefinition) -> i64 {
    storage
        .create_mcp_server(definition)
        .await
        .expect("create server")
        .id
}

async fn topic(storage: &Storage, label: &str) -> i64 {
    storage
        .create_topic(None, label, None, None)
        .await
        .expect("create topic")
}

#[tokio::test]
async fn mcp_config_round_trip() {
    let (_dir, storage) = database().await;
    let definition = stdio("files");
    let created = storage
        .create_mcp_server(&definition)
        .await
        .expect("create server");
    assert_eq!(created.revision, 1);
    assert_eq!(created.definition, definition);
    assert_eq!(created.created_at, created.updated_at);

    let updated_definition = http("files", "http://127.0.0.1:9/mcp?token=a&mode=b");
    let updated = storage
        .update_mcp_server(created.id, 1, &updated_definition)
        .await
        .expect("update server");
    assert_eq!(updated.id, created.id);
    assert_eq!(updated.revision, 2);
    assert_eq!(updated.definition, updated_definition);
    assert!(updated.updated_at >= created.updated_at);

    assert_eq!(
        storage.mcp_server(created.id).await.expect("read"),
        Some(updated.clone())
    );
    assert_eq!(
        storage.list_mcp_servers().await.expect("list"),
        vec![updated]
    );
}

#[tokio::test]
async fn mcp_revision_conflict_does_not_overwrite() {
    let (_dir, storage) = database().await;
    let created = storage
        .create_mcp_server(&stdio("alpha"))
        .await
        .expect("create");
    let second = storage
        .update_mcp_server(created.id, 1, &stdio("alpha-v2"))
        .await
        .expect("update");
    assert_eq!(second.revision, 2);

    let error = storage
        .update_mcp_server(created.id, 1, &stdio("stale"))
        .await
        .expect_err("stale revision");
    assert!(matches!(error, StorageError::Conflict { .. }));
    assert_eq!(
        storage.mcp_server(created.id).await.expect("read"),
        Some(second)
    );

    let error = storage
        .update_mcp_server(999, 1, &stdio("missing"))
        .await
        .expect_err("unknown server");
    assert!(matches!(error, StorageError::NotFound { .. }));
}

#[tokio::test]
async fn mcp_name_conflicts_are_reported() {
    let (_dir, storage) = database().await;
    create(&storage, &stdio("alpha")).await;
    let error = storage
        .create_mcp_server(&stdio("alpha"))
        .await
        .expect_err("duplicate name");
    assert!(matches!(error, StorageError::Conflict { .. }));

    let beta = storage
        .create_mcp_server(&stdio("beta"))
        .await
        .expect("create beta");
    let error = storage
        .update_mcp_server(beta.id, 1, &stdio("alpha"))
        .await
        .expect_err("duplicate name on update");
    assert!(matches!(error, StorageError::Conflict { .. }));
    let unchanged = storage
        .mcp_server(beta.id)
        .await
        .expect("read")
        .expect("server exists");
    assert_eq!(unchanged.revision, 1);
    assert_eq!(unchanged.definition.name, "beta");
}

#[tokio::test]
async fn mcp_enablement_survives_reopen() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("windai.db");
    let storage = Storage::open(&path).await.expect("open database");
    let alpha = create(&storage, &stdio("alpha")).await;
    let beta = create(&storage, &stdio("beta")).await;
    let first = topic(&storage, "first").await;
    let second = topic(&storage, "second").await;

    assert!(
        storage
            .enable_mcp_server(first, alpha)
            .await
            .expect("enable")
    );
    assert!(
        !storage
            .enable_mcp_server(first, alpha)
            .await
            .expect("enable again")
    );
    assert!(
        storage
            .enable_mcp_server(first, beta)
            .await
            .expect("enable beta")
    );
    assert!(
        storage
            .enable_mcp_server(second, beta)
            .await
            .expect("enable second")
    );
    assert!(
        storage
            .disable_mcp_server(first, alpha)
            .await
            .expect("disable")
    );
    assert!(
        !storage
            .disable_mcp_server(first, alpha)
            .await
            .expect("disable again")
    );
    assert!(
        storage
            .enable_mcp_server(first, alpha)
            .await
            .expect("re-enable")
    );

    let enabled: Vec<i64> = storage
        .enabled_mcp_servers(first)
        .await
        .expect("enabled")
        .into_iter()
        .map(|record| record.id)
        .collect();
    assert_eq!(enabled, vec![alpha, beta]);
    assert_eq!(
        storage.topics_with_mcp_server(beta).await.expect("topics"),
        vec![first, second]
    );

    let error = storage
        .enable_mcp_server(999, alpha)
        .await
        .expect_err("unknown topic");
    assert!(matches!(error, StorageError::NotFound { .. }));
    let error = storage
        .enable_mcp_server(first, 999)
        .await
        .expect_err("unknown server");
    assert!(matches!(error, StorageError::NotFound { .. }));
    let error = storage
        .disable_mcp_server(first, 999)
        .await
        .expect_err("unknown server");
    assert!(matches!(error, StorageError::NotFound { .. }));

    drop(storage);
    let storage = Storage::open(&path).await.expect("reopen database");
    let enabled: Vec<i64> = storage
        .enabled_mcp_servers(first)
        .await
        .expect("enabled")
        .into_iter()
        .map(|record| record.id)
        .collect();
    assert_eq!(enabled, vec![alpha, beta]);
    assert_eq!(
        storage.topics_with_mcp_server(beta).await.expect("topics"),
        vec![first, second]
    );
    let listed: Vec<i64> = storage
        .list_mcp_servers()
        .await
        .expect("list")
        .into_iter()
        .map(|record| record.id)
        .collect();
    assert_eq!(listed, vec![alpha, beta]);
}

#[tokio::test]
async fn delete_mcp_preserves_mod_records() {
    let (_dir, storage) = database().await;
    let module = storage
        .install_mod("math", r#"{"name":"math","version":1}"#, None)
        .await
        .expect("install mod");
    let server = create(&storage, &stdio("alpha")).await;
    let first = topic(&storage, "first").await;
    let second = topic(&storage, "second").await;
    storage
        .enable_mcp_server(first, server)
        .await
        .expect("enable first");
    storage
        .enable_mcp_server(second, server)
        .await
        .expect("enable second");
    storage.enable_mod(first, module).await.expect("enable mod");

    let affected = storage.delete_mcp_server(server).await.expect("delete");
    assert_eq!(affected, vec![first, second]);
    assert!(storage.mcp_server(server).await.expect("read").is_none());
    assert!(
        storage
            .enabled_mcp_servers(first)
            .await
            .expect("enabled")
            .is_empty()
    );
    assert!(
        storage
            .topics_with_mcp_server(server)
            .await
            .expect("topics")
            .is_empty()
    );
    assert!(storage.mod_by_name("math").await.expect("mod").is_some());
    assert_eq!(
        storage
            .enabled_mods(first)
            .await
            .expect("enabled mods")
            .len(),
        1
    );
    assert!(
        storage
            .get_topic_by_id(first)
            .await
            .expect("topic")
            .is_some()
    );

    let error = storage
        .delete_mcp_server(server)
        .await
        .expect_err("already deleted");
    assert!(matches!(error, StorageError::NotFound { .. }));
}

#[tokio::test]
async fn mcp_writes_roll_back_with_the_outer_transaction() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("windai.db");
    let storage = Storage::open(&path).await.expect("open database");
    let definition = stdio("alpha");
    let result: Result<(), StorageError> = storage
        .with_tx(async move |tx| {
            tx.create_mcp_server(&definition).await?;
            Err(StorageError::Internal("stop after create".into()))
        })
        .await;
    assert!(result.is_err());

    drop(storage);
    let storage = Storage::open(&path).await.expect("reopen database");
    assert!(storage.list_mcp_servers().await.expect("list").is_empty());
}

async fn raw_connection(path: &Path) -> sqlx::SqliteConnection {
    use sqlx::sqlite::SqliteConnectOptions;
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    <sqlx::SqliteConnection as Connection>::connect_with(&options)
        .await
        .expect("raw connection")
}

#[tokio::test]
async fn invalid_mcp_rows_are_not_silently_decoded() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("windai.db");
    let storage = Storage::open(&path).await.expect("open database");
    create(&storage, &stdio("valid")).await;

    let mut connection = raw_connection(&path).await;
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut connection)
        .await
        .expect("ignore checks");
    sqlx::query(
        "INSERT INTO mcp_servers (name, description, config, revision, created_at, updated_at)
         VALUES ('broken-config', NULL, '[]', 1, 0, 0)",
    )
    .execute(&mut connection)
    .await
    .expect("insert malformed config");
    sqlx::query(
        "INSERT INTO mcp_servers (name, description, config, revision, created_at, updated_at)
         VALUES ('broken-revision', NULL, '{}', 5000000000, 0, 0)",
    )
    .execute(&mut connection)
    .await
    .expect("insert out of range revision");
    let broken: i64 = sqlx::query_scalar("SELECT id FROM mcp_servers WHERE name = 'broken-config'")
        .fetch_one(&mut connection)
        .await
        .expect("read id");
    connection.close().await.expect("close raw");

    assert!(
        storage.list_mcp_servers().await.is_err(),
        "a malformed config must not decode"
    );
    assert!(
        storage.mcp_server(broken).await.is_err(),
        "a config array must not decode as a server definition"
    );
}

#[tokio::test]
async fn unknown_mcp_queries_are_empty() {
    let (_dir, storage) = database().await;
    assert!(storage.mcp_server(999).await.expect("read").is_none());
    assert!(storage.list_mcp_servers().await.expect("list").is_empty());
    assert!(
        storage
            .enabled_mcp_servers(999)
            .await
            .expect("enabled")
            .is_empty()
    );
    assert!(
        storage
            .topics_with_mcp_server(999)
            .await
            .expect("topics")
            .is_empty()
    );
}
