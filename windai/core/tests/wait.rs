//! 验证外部等待记录的读写。

#[path = "common/lib.rs"]
mod common;

use common::store;
use wind_core::models::WaitOpen;

#[tokio::test]
async fn an_open_wait_is_answerable_once() {
    let (_dir, store) = store().await;
    store
        .wait_open(WaitOpen {
            handle: "h1".to_string(),
            instance_id: 7,
            kind: "approval".to_string(),
            message_id: 1,
            payload: serde_json::json!({}).clone(),
        })
        .await
        .expect("start");
    let open = store
        .wait_get("h1")
        .await
        .expect("query")
        .expect("must exist");
    assert!(!open.is_answered());

    store
        .wait_answer("h1", serde_json::json!({ "allow": ["a"] }))
        .await
        .expect("answer");
    let answered = store
        .wait_get("h1")
        .await
        .expect("query")
        .expect("must exist");
    assert!(answered.is_answered());
    assert_eq!(answered.decision.expect("read answer")["allow"][0], "a");
}

#[tokio::test]
async fn answering_twice_is_rejected() {
    let (_dir, store) = store().await;
    store
        .wait_open(WaitOpen {
            handle: "h1".to_string(),
            instance_id: 7,
            kind: "approval".to_string(),
            message_id: 1,
            payload: serde_json::json!({}).clone(),
        })
        .await
        .expect("start");
    store
        .wait_answer("h1", serde_json::json!({}))
        .await
        .expect("answer first time");
    let second = store.wait_answer("h1", serde_json::json!({})).await;
    assert!(second.is_err(), "the same wait must not be answered twice");
}

#[tokio::test]
async fn answering_an_unknown_handle_reports_no_rows() {
    let (_dir, store) = store().await;
    assert!(
        store
            .wait_answer("nope", serde_json::json!({}))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn only_unanswered_waits_are_listed_as_open() {
    let (_dir, store) = store().await;
    store
        .wait_open(WaitOpen {
            handle: "h1".to_string(),
            instance_id: 7,
            kind: "approval".to_string(),
            message_id: 1,
            payload: serde_json::json!({}).clone(),
        })
        .await
        .expect("start");
    store
        .wait_open(WaitOpen {
            handle: "h2".to_string(),
            instance_id: 7,
            kind: "approval".to_string(),
            message_id: 1,
            payload: serde_json::json!({}).clone(),
        })
        .await
        .expect("start");
    store
        .wait_answer("h1", serde_json::json!({}))
        .await
        .expect("answer");

    let open = store.wait_list_open_by_instance(7).await.expect("query");
    let handles: Vec<&str> = open.iter().map(|w| w.handle.as_str()).collect();
    assert_eq!(
        handles,
        vec!["h2"],
        "answered waits are not counted as open"
    );
}
