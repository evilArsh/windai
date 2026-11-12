use tempfile::TempDir;
use wind_storage::Storage;
use wind_wire::{AgentMode, AgentRole, Content, PendingKind};

async fn database() -> (TempDir, Storage, i64) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(&dir.path().join("test.db")).await.unwrap();
    let topic = storage
        .create_topic(None, "runtime", None, None, None)
        .await
        .unwrap();
    (dir, storage, topic)
}

async fn child(storage: &Storage, topic: i64, parent: i64, mode: AgentMode, phase: &str) -> i64 {
    storage
        .create_agent_instance(
            topic,
            Some(parent),
            AgentRole::Child,
            Some(mode),
            phase,
            None,
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn main_is_unique_and_topic_reads_are_isolated_and_ordered() {
    let (_dir, s, topic) = database().await;
    let main = s.ensure_main(topic, AgentMode::Sync).await.unwrap();
    assert_eq!(main, s.ensure_main(topic, AgentMode::Sync).await.unwrap());
    let other = s
        .create_topic(None, "other", None, None, None)
        .await
        .unwrap();
    let other_main = s.ensure_main(other, AgentMode::Sync).await.unwrap();
    let sub = child(&s, topic, main, AgentMode::Sync, "running").await;
    let first = s
        .create_pending(
            sub,
            PendingKind::User,
            vec![Content::new_text("first".into())],
            None,
        )
        .await
        .unwrap();
    let second = s
        .create_pending(main, PendingKind::User, vec![], None)
        .await
        .unwrap();
    s.create_pending(other_main, PendingKind::User, vec![], None)
        .await
        .unwrap();
    assert_eq!(
        s.list_instances_by_topic(topic)
            .await
            .unwrap()
            .iter()
            .map(|r| r.id)
            .collect::<Vec<_>>(),
        [main, sub]
    );
    assert_eq!(
        s.list_pending_by_topic(topic)
            .await
            .unwrap()
            .iter()
            .map(|r| r.id)
            .collect::<Vec<_>>(),
        [first, second]
    );
}

#[tokio::test]
async fn closing_pending_only_deletes_the_target_subtree() {
    let (_dir, s, topic) = database().await;
    let root = s.ensure_main(topic, AgentMode::Sync).await.unwrap();
    let sub = child(&s, topic, root, AgentMode::Sync, "running").await;
    let grandchild = child(&s, topic, sub, AgentMode::Sync, "running").await;
    for id in [root, sub, grandchild] {
        s.create_pending(id, PendingKind::User, vec![], None)
            .await
            .unwrap();
    }
    s.close_pending(sub).await.unwrap();
    let rows = s.list_pending_by_topic(topic).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].instance_id, root);
    s.close_pending(-1).await.unwrap();
    assert_eq!(s.list_pending_by_topic(topic).await.unwrap().len(), 1);
}

#[tokio::test]
async fn result_claim_and_scan_exclude_delivered_live_and_sync_children() {
    let (_dir, s, topic) = database().await;
    let root = s.ensure_main(topic, AgentMode::Sync).await.unwrap();
    let mut terminal = vec![];
    for (n, (mode, phase)) in [
        (AgentMode::Background, "done"),
        (AgentMode::Background, "failed"),
        (AgentMode::Background, "cancelled"),
        (AgentMode::Background, "aborted"),
        (AgentMode::Background, "running"),
        (AgentMode::Sync, "done"),
    ]
    .into_iter()
    .enumerate()
    {
        let id = child(&s, topic, root, mode, phase).await;
        s.create_spawn_call_map(root, &format!("call-{n}"), id)
            .await
            .unwrap();
        if n < 4 {
            terminal.push(id);
        }
    }
    assert_eq!(
        s.list_undelivered_background(topic).await.unwrap(),
        terminal
    );
    let call = s.spawn_call_of_child(terminal[0]).await.unwrap().unwrap();
    assert_eq!(call.parent_id, root);
    assert_eq!(call.call_id, "call-0");
    assert_eq!(call.child_id, terminal[0]);
    assert!(call.result_delivered_at.is_none());
    assert_eq!(
        s.claim_result_delivery(terminal[0]).await.unwrap(),
        Some(terminal[0])
    );
    assert_eq!(s.claim_result_delivery(terminal[0]).await.unwrap(), None);
    assert!(
        s.spawn_call_of_child(terminal[0])
            .await
            .unwrap()
            .unwrap()
            .result_delivered_at
            .is_some()
    );
    assert_eq!(
        s.list_undelivered_background(topic).await.unwrap(),
        terminal[1..]
    );
    assert!(s.spawn_call_of_child(-1).await.unwrap().is_none());
}

#[tokio::test]
async fn background_input_is_atomic_idempotent_and_checks_parent_phase() {
    let (_dir, s, topic) = database().await;
    let root = s.ensure_main(topic, AgentMode::Sync).await.unwrap();
    let sub = child(&s, topic, root, AgentMode::Background, "done").await;
    s.create_spawn_call_map(root, "call", sub).await.unwrap();
    let tx = s.begin().await.unwrap();
    let db = tx.storage();
    db.claim_result_delivery(sub).await.unwrap();
    db.insert_background_result(root, vec![], sub)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(
        s.spawn_call_of_child(sub)
            .await
            .unwrap()
            .unwrap()
            .result_delivered_at
            .is_none()
    );
    assert!(s.list_pending_by_topic(topic).await.unwrap().is_empty());
    for phase in [
        "running",
        "waiting_children",
        "suspended",
        "done",
        "failed",
        "cancelled",
        "aborted",
    ] {
        s.update_instance_phase(root, phase, None).await.unwrap();
        let inserted = s
            .insert_background_result(root, vec![Content::new_text("reply".into())], sub)
            .await
            .unwrap();
        assert_eq!(
            inserted.is_some(),
            matches!(phase, "running" | "waiting_children" | "suspended" | "done"),
            "{phase}"
        );
        assert!(
            s.insert_background_result(root, vec![], sub)
                .await
                .unwrap()
                .is_none()
        );
        s.close_pending(root).await.unwrap();
    }
    assert!(
        s.insert_background_result(-1, vec![], sub)
            .await
            .unwrap()
            .is_none()
    );
}
