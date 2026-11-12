#[path = "common/lib.rs"]
mod common;
use common::{Fixture, event_until, eventually};
use wind_core::agent::{CancelOrigin, Phase};
use wind_core::llm::domain::LlmOutcome;
use wind_wire::{AgentMode, Block, Body, Role};

async fn seed(parent_cancelled: bool, mode: AgentMode, running: bool) -> (Fixture, i64, i64, i64) {
    let f = Fixture::new().await;
    let phase = if parent_cancelled {
        Phase::Cancelled(CancelOrigin::External)
    } else {
        Phase::Done(LlmOutcome { message_id: 0 })
    };
    let root = f.instance(None, AgentMode::Sync, phase).await;
    let child = f.instance(Some(root), mode, Phase::Running).await;
    let trigger = f
        .instance(Some(root), AgentMode::Sync, Phase::Running)
        .await;
    if !running {
        let turn = f.storage.create_turn(f.topic, child, None).await.unwrap();
        let message = f
            .storage
            .create_message(turn, Role::Assistant, None)
            .await
            .unwrap();
        let blocks = [
            Block {
                role: Role::Assistant,
                seq: 0,
                revision: 0,
                body: Body::Text {
                    content: "background ".into(),
                },
            },
            Block {
                role: Role::Assistant,
                seq: 1,
                revision: 0,
                body: Body::Text {
                    content: "reply".into(),
                },
            },
        ];
        f.storage.create_blocks(message, &blocks).await.unwrap();
        let (name, payload) = Phase::<wind_core::llm::domain::LlmDomain>::Done(LlmOutcome {
            message_id: message,
        })
        .columns()
        .unwrap();
        f.storage
            .update_instance_phase(child, &name, payload)
            .await
            .unwrap();
    }
    f.storage
        .create_spawn_call_map(root, "spawn-call", child)
        .await
        .unwrap();
    (f, root, child, trigger)
}

async fn delivered(f: &Fixture, child: i64) -> bool {
    f.storage
        .spawn_call_of_child(child)
        .await
        .unwrap()
        .unwrap()
        .result_delivered_at
        .is_some()
}

async fn user_text(f: &Fixture, root: i64) -> String {
    f.storage
        .list_instance_context(root)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|row| {
            if row.block.role != Role::User {
                return None;
            }
            match row.block.body {
                Body::Text { content } => Some(content),
                _ => None,
            }
        })
        .collect()
}

#[tokio::test]
async fn startup_scan_delivers_reply_once_and_survives_reopen() {
    let (f, root, child, trigger) = seed(false, AgentMode::Background, false).await;
    let runtime = f.open().await;
    runtime.cancel(trigger).await.unwrap();
    eventually(|| async { delivered(&f, child).await && f.phase(root).await == "failed" }).await;
    assert_eq!(user_text(&f, root).await, "background reply");
    assert!(
        f.storage
            .list_pending_by_topic(f.topic)
            .await
            .unwrap()
            .is_empty()
    );
    runtime.shutdown().await.unwrap();
    let runtime = f.open().await;
    runtime.cancel(trigger).await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(user_text(&f, root).await, "background reply");
}

#[tokio::test]
async fn terminal_status_observer_delivers_fallback_and_clears_delivery_mark() {
    let (f, root, child, trigger) = seed(false, AgentMode::Background, true).await;
    let runtime = f.open().await;
    let mut sub = runtime.subscribe().await.unwrap().subscription;
    runtime.cancel(trigger).await.unwrap();
    event_until(&mut sub, |event| event.envelope.name == "core/agent.sealed").await;
    eventually(|| async { delivered(&f, child).await && f.phase(root).await == "failed" }).await;
    assert_eq!(
        user_text(&f, root).await,
        format!("background child {child} ended with phase aborted")
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_parent_is_not_reopened_by_background_result() {
    let (f, root, child, trigger) = seed(true, AgentMode::Background, false).await;
    let runtime = f.open().await;
    runtime.cancel(trigger).await.unwrap();
    eventually(|| delivered(&f, child)).await;
    runtime.shutdown().await.unwrap();
    assert_eq!(f.phase(root).await, "cancelled");
    assert!(
        f.storage
            .list_pending_by_topic(f.topic)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(user_text(&f, root).await.is_empty());
}

#[tokio::test]
async fn synchronous_children_are_not_delivered_as_background_input() {
    let (f, root, child, trigger) = seed(false, AgentMode::Sync, false).await;
    let runtime = f.open().await;
    runtime.cancel(trigger).await.unwrap();
    runtime.shutdown().await.unwrap();
    assert!(!delivered(&f, child).await);
    assert_eq!(f.phase(root).await, "done");
    assert!(user_text(&f, root).await.is_empty());
}
