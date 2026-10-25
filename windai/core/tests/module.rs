//! 验证 mod 的安装、启用、版本与状态键的读写。

#[path = "common/lib.rs"]
mod common;

use common::store;

#[tokio::test]
async fn the_same_mod_state_key_is_isolated_per_scope() {
    let (_dir, store) = store().await;
    let id = store
        .mod_install("research-kit", "{}", None)
        .await
        .expect("install");
    store
        .mod_put_state(id, "topic", 1, "storage", "cursor", serde_json::json!("a"))
        .await
        .expect("write");
    store
        .mod_put_state(id, "topic", 2, "storage", "cursor", serde_json::json!("b"))
        .await
        .expect("write");

    let first = store
        .mod_get_state(id, "topic", 1, "storage", "cursor")
        .await
        .expect("query");
    let second = store
        .mod_get_state(id, "topic", 2, "storage", "cursor")
        .await
        .expect("query");
    assert_eq!(first.expect("must exist").value, serde_json::json!("a"));
    assert_eq!(second.expect("must exist").value, serde_json::json!("b"));
}

#[tokio::test]
async fn writing_the_same_key_twice_replaces_it() {
    let (_dir, store) = store().await;
    let id = store.mod_install("m", "{}", None).await.expect("install");
    for value in ["a", "b"] {
        store
            .mod_put_state(id, "global", 0, "state", "k", serde_json::json!(value))
            .await
            .expect("write");
    }
    let entry = store
        .mod_get_state(id, "global", 0, "state", "k")
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(entry.value, serde_json::json!("b"));
}

#[tokio::test]
async fn the_enabled_set_is_per_topic() {
    let (_dir, store) = store().await;
    let a = store.mod_install("a", "{}", None).await.expect("install");
    let b = store.mod_install("b", "{}", None).await.expect("install");
    store.mod_enable(1, a).await.expect("enable");
    store.mod_enable(2, b).await.expect("enable");

    let in_one = store.mod_enabled_in(1).await.expect("query");
    assert_eq!(in_one, vec!["a"], "topic one has only a");
    let in_two = store.mod_enabled_in(2).await.expect("query");
    assert_eq!(in_two, vec!["b"], "topic two has only b");
}

#[tokio::test]
async fn enabling_the_same_mod_twice_changes_nothing() {
    let (_dir, store) = store().await;
    let a = store.mod_install("a", "{}", None).await.expect("install");
    store.mod_enable(1, a).await.expect("enable");
    store.mod_enable(1, a).await.expect("enable mod twice");
    assert_eq!(store.mod_enabled_in(1).await.expect("query").len(), 1);
}

#[tokio::test]
async fn disabling_a_mod_that_is_not_enabled_reports_no_rows() {
    let (_dir, store) = store().await;
    let a = store.mod_install("a", "{}", None).await.expect("install");
    assert!(
        store.mod_disable(1, a).await.is_err(),
        "disabling a mod that is not enabled must report an error, not silently succeed"
    );
}

#[tokio::test]
async fn a_mod_name_is_unique() {
    let (_dir, store) = store().await;
    store
        .mod_install("same", "{}", None)
        .await
        .expect("install");
    assert!(
        store.mod_install("same", "{}", None).await.is_err(),
        "a mod name is unique; the second one must be rejected"
    );
}

#[tokio::test]
async fn publishing_a_version_keeps_the_previous_ones() {
    let (_dir, store) = store().await;
    let id = store
        .mod_install("m", "{\"v\":1}", None)
        .await
        .expect("install");
    store
        .mod_publish(id, 2, "{\"v\":2}", None)
        .await
        .expect("publish");
    store
        .mod_set_current_version(id, 2)
        .await
        .expect("switch current version");

    let versions = store.mod_list_versions(id).await.expect("query");
    assert_eq!(versions.len(), 2, "version history is retained");
    let current = store
        .mod_get_by_name("m")
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(current.current_version, 2);
}

#[tokio::test]
async fn the_topics_a_mod_was_enabled_on_are_listed() {
    let (_dir, store) = store().await;
    let a = store.mod_install("a", "{}", None).await.expect("install");
    let b = store.mod_install("b", "{}", None).await.expect("install");
    store.mod_enable(1, a).await.expect("enable");
    store.mod_enable(3, a).await.expect("enable");
    store.mod_enable(1, b).await.expect("enable");

    assert_eq!(
        store.mod_topics(a).await.expect("query"),
        vec![1, 3],
        "sorted by topic id"
    );
    assert_eq!(
        store.mod_topics(b).await.expect("query"),
        vec![1],
        "only this mod's own enablement records are considered"
    );
    assert!(
        store.mod_topics(999).await.expect("query").is_empty(),
        "never enabled means an empty set"
    );
}

#[tokio::test]
async fn deleting_a_state_key_removes_it_and_a_missing_key_is_not_an_error() {
    let (_dir, store) = store().await;
    let id = store.mod_install("m", "{}", None).await.expect("install");
    store
        .mod_put_state(id, "global", 0, "state", "k", serde_json::json!(1))
        .await
        .expect("write");
    store
        .mod_delete_state(id, "global", 0, "state", "k")
        .await
        .expect("delete");
    assert!(
        store.mod_list_state(id).await.expect("query").is_empty(),
        "a deleted key is no longer in the store"
    );
    store
        .mod_delete_state(id, "global", 0, "state", "k")
        .await
        .expect("delete mod twice");
}

#[tokio::test]
async fn a_refused_version_does_not_leave_a_half_installed_mod() {
    let (_dir, store) = store().await;
    sqlx::query(
        "CREATE TRIGGER refuse_mod_version BEFORE INSERT ON mod_versions \
         BEGIN SELECT RAISE(ABORT, 'refused'); END",
    )
    .execute(store.pool())
    .await
    .expect("install the refusing trigger");

    assert!(
        store.mod_install("m", "{}", None).await.is_err(),
        "the refused version must fail the install"
    );
    assert!(
        store.mod_get_by_name("m").await.expect("query").is_none(),
        "a failed install must not leave a mod row that blocks every retry"
    );
}

#[tokio::test]
async fn a_refused_version_switch_does_not_leave_the_new_version_published() {
    let (_dir, store) = store().await;
    let id = store
        .mod_install("m", r#"{"v":1}"#, None)
        .await
        .expect("install");
    sqlx::query(
        "CREATE TRIGGER refuse_mod_switch BEFORE UPDATE OF current_version ON mods \
         BEGIN SELECT RAISE(ABORT, 'refused'); END",
    )
    .execute(store.pool())
    .await
    .expect("install the refusing trigger");

    assert!(
        store
            .mod_publish_current(id, 2, r#"{"v":2}"#, None)
            .await
            .is_err(),
        "the refused switch must fail the publish"
    );
    assert!(
        store.mod_get_version(id, 2).await.expect("query").is_none(),
        "a version whose switch failed must not stay published"
    );
    let current = store
        .mod_get_by_name("m")
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(current.current_version, 1);
}

#[tokio::test]
async fn deleting_a_mod_clears_its_state() {
    let (_dir, store) = store().await;
    let id = store.mod_install("m", "{}", None).await.expect("install");
    store
        .mod_put_state(id, "global", 0, "storage", "k", serde_json::json!(1))
        .await
        .expect("write");
    store.mod_delete(id).await.expect("delete");

    assert!(store.mod_get_by_name("m").await.expect("query").is_none());
    assert!(
        store.mod_list_state(id).await.expect("query").is_empty(),
        "state is cascaded clear by mod"
    );
}

#[tokio::test]
async fn deleting_a_mod_that_has_no_state_still_removes_it() {
    let (_dir, store) = store().await;
    let id = store.mod_install("m", "{}", None).await.expect("install");
    store
        .mod_delete(id)
        .await
        .expect("delete mod without state");
    assert!(store.mod_get_by_name("m").await.expect("query").is_none());
}

#[tokio::test]
async fn deleting_an_unknown_mod_reports_no_rows() {
    let (_dir, store) = store().await;
    assert!(
        store.mod_delete(999).await.is_err(),
        "deleting a nonexistent mod must report an error, not silently succeed"
    );
}
