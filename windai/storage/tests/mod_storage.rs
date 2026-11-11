use std::path::Path;

use sqlx::Connection;
use tempfile::TempDir;
use wind_storage::Storage;
use wind_storage::error::StorageError;

async fn database() -> (TempDir, Storage) {
    let dir = TempDir::new().expect("temp dir");
    let storage = Storage::open(&dir.path().join("windai.db"))
        .await
        .expect("open database");
    (dir, storage)
}

fn manifest(name: &str, version: u32) -> String {
    serde_json::json!({ "name": name, "version": version }).to_string()
}

async fn install(storage: &Storage, name: &str) -> i64 {
    storage
        .install_mod(name, &manifest(name, 1))
        .await
        .expect("install mod")
}

async fn topic(storage: &Storage, label: &str) -> i64 {
    storage
        .create_topic(None, label, None, None, None)
        .await
        .expect("create topic")
}

#[tokio::test]
async fn install_and_update_round_trip() {
    let (_dir, storage) = database().await;
    let id = install(&storage, "math").await;

    let installed = storage
        .mod_by_name("math")
        .await
        .expect("read mod")
        .expect("mod exists");
    assert_eq!(installed.id, id);
    assert_eq!(installed.version, 1);
    assert_eq!(installed.manifest, manifest("math", 1));
    assert_eq!(installed.created_at, installed.updated_at);

    storage
        .update_mod(id, 1, 3, &manifest("math", 3))
        .await
        .expect("update to version 3");

    let installed = storage
        .mod_by_name("math")
        .await
        .expect("read mod")
        .expect("mod exists");
    assert_eq!(installed.version, 3);
    assert_eq!(installed.manifest, manifest("math", 3));
    assert!(installed.updated_at >= installed.created_at);

    let manifests = storage
        .list_mods()
        .await
        .expect("list mods")
        .into_iter()
        .map(|module| (module.id, module.manifest))
        .collect::<Vec<_>>();
    assert_eq!(manifests, vec![(id, manifest("math", 3))]);
}

#[tokio::test]
async fn install_rejects_inconsistent_or_duplicate_identity() {
    let (_dir, storage) = database().await;
    let error = storage
        .install_mod("math", &manifest("other", 1))
        .await
        .expect_err("name must match");
    assert!(matches!(error, StorageError::Conflict { .. }));

    let error = storage
        .install_mod("math", &manifest("math", 2))
        .await
        .expect_err("a first install accepts version 1 only");
    assert!(matches!(error, StorageError::Conflict { .. }));

    install(&storage, "math").await;
    let error = storage
        .install_mod("math", &manifest("math", 1))
        .await
        .expect_err("duplicate name");
    assert!(matches!(error, StorageError::Conflict { .. }));
}

#[tokio::test]
async fn failed_update_rolls_back() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("windai.db");
    let storage = Storage::open(&path).await.expect("open database");
    let id = install(&storage, "math").await;

    let result: Result<(), StorageError> = storage
        .with_tx(|tx| async move {
            tx.update_mod(id, 1, 2, &manifest("math", 2)).await?;
            Err(StorageError::Internal("stop after update".into()))
        })
        .await;
    assert!(result.is_err());

    drop(storage);
    let storage = Storage::open(&path).await.expect("reopen database");
    let installed = storage
        .mod_by_name("math")
        .await
        .expect("read mod")
        .expect("mod exists");
    assert_eq!(installed.version, 1);
    assert_eq!(installed.manifest, manifest("math", 1));
}

#[tokio::test]
async fn concurrent_update_has_one_winner() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("windai.db");
    let first = Storage::open(&path).await.expect("open first");
    let second = Storage::open(&path).await.expect("open second");
    let id = install(&first, "math").await;

    let second_manifest = manifest("math", 2);
    let third_manifest = manifest("math", 3);
    let (left, right) = tokio::join!(
        first.update_mod(id, 1, 2, &second_manifest),
        second.update_mod(id, 1, 3, &third_manifest),
    );
    let (winner, loser) = match (left, right) {
        (Ok(()), Err(error)) => (2, error),
        (Err(error), Ok(())) => (3, error),
        (Ok(()), Ok(())) => panic!("both updates committed"),
        (Err(left), Err(right)) => panic!("no update committed: {left}; {right}"),
    };
    assert!(matches!(loser, StorageError::Conflict { .. }), "{loser:?}");

    let installed = first
        .mod_by_name("math")
        .await
        .expect("read mod")
        .expect("mod exists");
    assert_eq!(installed.version, winner);
    assert_eq!(installed.manifest, manifest("math", winner));
}

#[tokio::test]
async fn update_rejects_backwards_and_stale_versions() {
    let (_dir, storage) = database().await;
    let id = install(&storage, "math").await;

    let error = storage
        .update_mod(id, 1, 1, &manifest("math", 1))
        .await
        .expect_err("version must increase");
    assert!(matches!(error, StorageError::Conflict { .. }));

    storage
        .update_mod(id, 1, 2, &manifest("math", 2))
        .await
        .expect("update to version 2");

    let error = storage
        .update_mod(id, 1, 3, &manifest("math", 3))
        .await
        .expect_err("expected version is stale");
    assert!(matches!(error, StorageError::Conflict { .. }));
    let installed = storage
        .mod_by_name("math")
        .await
        .expect("read mod")
        .expect("mod exists");
    assert_eq!(installed.version, 2, "the stale update must not write");
    assert_eq!(installed.manifest, manifest("math", 2));
}

#[tokio::test]
async fn enablement_is_idempotent_and_topic_local() {
    let (_dir, storage) = database().await;
    let subject = install(&storage, "math").await;
    let other = install(&storage, "other").await;
    let first_topic = topic(&storage, "first").await;
    let second_topic = topic(&storage, "second").await;

    assert!(
        storage
            .enable_mod(first_topic, subject)
            .await
            .expect("enable")
    );
    assert!(
        !storage
            .enable_mod(first_topic, subject)
            .await
            .expect("enable again")
    );
    assert!(
        storage
            .enable_mod(second_topic, subject)
            .await
            .expect("enable second")
    );
    assert!(
        storage
            .enable_mod(second_topic, other)
            .await
            .expect("enable other")
    );

    let enabled = storage
        .enabled_mods(second_topic)
        .await
        .expect("enabled mods");
    let names: Vec<&str> = enabled.iter().map(|module| module.name.as_str()).collect();
    assert_eq!(names, vec!["math", "other"]);
    assert!(enabled.iter().all(|module| module.version == 1));

    assert!(
        storage
            .disable_mod(first_topic, subject)
            .await
            .expect("disable")
    );
    assert!(
        !storage
            .disable_mod(first_topic, subject)
            .await
            .expect("disable again")
    );
    assert!(
        storage
            .enabled_mods(first_topic)
            .await
            .expect("enabled")
            .is_empty()
    );
    assert_eq!(
        storage
            .enabled_mods(second_topic)
            .await
            .expect("enabled")
            .len(),
        2
    );

    let all = storage.all_enabled_mods().await.expect("all enabled");
    assert_eq!(
        all,
        vec![
            (second_topic, "math".to_string()),
            (second_topic, "other".to_string())
        ]
    );

    let error = storage
        .enable_mod(999, subject)
        .await
        .expect_err("unknown topic");
    assert!(matches!(error, StorageError::NotFound { .. }));
    let error = storage
        .enable_mod(first_topic, 999)
        .await
        .expect_err("unknown mod");
    assert!(matches!(error, StorageError::NotFound { .. }));
    let error = storage
        .disable_mod(first_topic, 999)
        .await
        .expect_err("unknown mod");
    assert!(matches!(error, StorageError::NotFound { .. }));
}

#[tokio::test]
async fn delete_cascades_and_returns_topics() {
    let (_dir, storage) = database().await;
    let doomed = install(&storage, "math").await;
    let kept = install(&storage, "kept").await;
    let first = topic(&storage, "first").await;
    let second = topic(&storage, "second").await;
    storage
        .enable_mod(first, doomed)
        .await
        .expect("enable first");
    storage
        .enable_mod(second, doomed)
        .await
        .expect("enable second");
    storage.enable_mod(first, kept).await.expect("enable kept");

    let affected = storage.delete_mod(doomed).await.expect("delete");
    assert_eq!(affected, vec![first, second]);

    assert!(storage.mod_by_name("math").await.expect("read").is_none());
    assert!(
        storage
            .enabled_mods(first)
            .await
            .expect("enabled")
            .iter()
            .all(|module| module.id == kept)
    );
    assert_eq!(
        storage.topics_with_mod(kept).await.expect("topics"),
        vec![first]
    );
    assert!(
        storage
            .get_topic_by_id(first)
            .await
            .expect("topic")
            .is_some()
    );
    assert!(
        storage
            .get_topic_by_id(second)
            .await
            .expect("topic")
            .is_some()
    );

    let error = storage
        .delete_mod(doomed)
        .await
        .expect_err("already deleted");
    assert!(matches!(error, StorageError::NotFound { .. }));
}

#[tokio::test]
async fn ordering_is_stable() {
    let (_dir, storage) = database().await;
    let zulu = install(&storage, "zulu").await;
    let alpha = install(&storage, "alpha").await;
    let topic_id = topic(&storage, "topic").await;
    storage
        .enable_mod(topic_id, zulu)
        .await
        .expect("enable zulu");
    storage
        .enable_mod(topic_id, alpha)
        .await
        .expect("enable alpha");

    let names: Vec<String> = storage
        .list_mods()
        .await
        .expect("list mods")
        .into_iter()
        .map(|module| module.name)
        .collect();
    assert_eq!(names, vec!["alpha", "zulu"]);
    let enabled = storage.enabled_mods(topic_id).await.expect("enabled");
    assert_eq!(enabled[0].name, "alpha");
    assert_eq!(enabled[1].name, "zulu");
    assert_eq!(
        storage.topics_with_mod(zulu).await.expect("topics"),
        vec![topic_id]
    );
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
async fn invalid_rows_are_not_silently_decoded() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("windai.db");
    let storage = Storage::open(&path).await.expect("open database");
    install(&storage, "math").await;

    let mut connection = raw_connection(&path).await;
    sqlx::query(
        "INSERT INTO mods (name, version, manifest, created_at, updated_at)
         VALUES ('broken-version', 5000000000, '{}', 0, 0)",
    )
    .execute(&mut connection)
    .await
    .expect("insert out of range version");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mods")
        .fetch_one(&mut connection)
        .await
        .expect("count rows");
    connection.close().await.expect("close raw");
    assert_eq!(count, 2);

    assert!(
        storage.list_mods().await.is_err(),
        "an out of range version must not decode"
    );
    assert!(
        storage.mod_by_name("broken-version").await.is_err(),
        "an out of range version must not decode"
    );
}

#[tokio::test]
async fn unknown_mod_queries_are_empty() {
    let (_dir, storage) = database().await;
    assert!(
        storage
            .mod_by_name("missing")
            .await
            .expect("read")
            .is_none()
    );
    assert!(storage.list_mods().await.expect("list").is_empty());
    assert!(
        storage
            .topics_with_mod(999)
            .await
            .expect("topics")
            .is_empty()
    );
    assert!(
        storage
            .all_enabled_mods()
            .await
            .expect("enabled")
            .is_empty()
    );
}

#[tokio::test]
async fn update_requires_an_installed_mod() {
    let (_dir, storage) = database().await;
    let error = storage
        .update_mod(999, 1, 2, &manifest("math", 2))
        .await
        .expect_err("unknown mod");
    assert!(matches!(error, StorageError::NotFound { .. }));
}

#[tokio::test]
async fn create_topic_writes_the_default_workspace() {
    let (_dir, storage) = database().await;
    let id = storage
        .create_topic(None, "topic", None, None, None)
        .await
        .expect("topic");
    let topic = storage
        .get_topic_by_id(id)
        .await
        .expect("read")
        .expect("exists");
    let workspace = topic
        .workspace
        .expect("the default workspace must be written");
    assert!(
        workspace.ends_with(&format!("topics/{id}")),
        "unexpected workspace: {workspace}"
    );
}

#[tokio::test]
async fn create_topic_keeps_an_explicit_workspace() {
    let (_dir, storage) = database().await;
    let id = storage
        .create_topic(None, "topic", None, None, Some("/tmp/ws"))
        .await
        .expect("topic");
    let topic = storage
        .get_topic_by_id(id)
        .await
        .expect("read")
        .expect("exists");
    assert_eq!(topic.workspace.as_deref(), Some("/tmp/ws"));
}
