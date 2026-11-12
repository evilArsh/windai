#[path = "common/lib.rs"]
mod common;
use common::{Fixture, event_until, eventually};
use std::collections::BTreeSet;
use wind_core::agent::CancelOrigin;
use wind_core::agent::Phase;
use wind_core::llm::domain::{LlmDomain, Suspended, WaitAnswer};
use wind_runtime::RuntimeError;
use wind_wire::{AgentMode, Content, Role};

#[tokio::test]
async fn submit_emits_acceptance_terminal_status_and_sealed() {
    let f = Fixture::new().await;
    let runtime = f.open().await;
    let mut sub = runtime.subscribe().await.unwrap().subscription;
    let ack = runtime
        .submit(f.topic, vec![Content::new_text("hello".into())])
        .await
        .unwrap();
    assert_eq!(ack.topic_id, f.topic);
    let mut seen = BTreeSet::new();
    while seen.len() < 3 {
        let event = event_until(&mut sub, |event| {
            let name = event.envelope.name.as_str();
            name == "core/input.accepted"
                || name == "core/agent.sealed"
                || (name == "core/agent.status"
                    && event.envelope.payload.as_ref().unwrap()["phase"] == "failed")
        })
        .await;
        let payload = event.envelope.payload.as_ref().unwrap();
        assert_eq!(payload["instance_id"], ack.instance_id);
        if event.envelope.name == "core/input.accepted" {
            assert_eq!(payload["pending_id"], ack.pending_id);
        }
        seen.insert(event.envelope.name.clone());
    }
    assert!(
        f.storage
            .list_pending_by_topic(f.topic)
            .await
            .unwrap()
            .is_empty()
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_first_submits_share_main_and_consume_every_input() {
    let f = Fixture::new().await;
    let runtime = f.open().await;
    let mut tasks = vec![];
    for n in 0..12 {
        let rt = runtime.clone();
        let topic = f.topic;
        tasks.push(tokio::spawn(async move {
            rt.submit(topic, vec![Content::new_text(format!("[input-{n}]"))])
                .await
                .unwrap()
        }));
    }
    let mut ids = BTreeSet::new();
    let mut pending = BTreeSet::new();
    for task in tasks {
        let ack = task.await.unwrap();
        ids.insert(ack.instance_id);
        pending.insert(ack.pending_id);
    }
    assert_eq!(ids.len(), 1);
    assert_eq!(pending.len(), 12);
    eventually(|| async {
        f.storage
            .list_pending_by_topic(f.topic)
            .await
            .unwrap()
            .is_empty()
    })
    .await;
    let rows = f.storage.list_instances_by_topic(f.topic).await.unwrap();
    assert_eq!(rows.len(), 1);
    let context = f.storage.list_instance_context(rows[0].id).await.unwrap();
    let text: String = context
        .iter()
        .filter_map(|r| match &r.block.body {
            wind_wire::Body::Text { content } => Some(content.as_str()),
            _ => None,
        })
        .collect();
    for n in 0..12 {
        assert_eq!(text.matches(&format!("[input-{n}]")).count(), 1);
    }
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovery_consumes_running_pending_but_preserves_cancelled_pending() {
    for cancelled in [false, true] {
        let f = Fixture::new().await;
        let phase = if cancelled {
            Phase::Cancelled(CancelOrigin::External)
        } else {
            Phase::Running
        };
        let root = f.instance(None, AgentMode::Sync, phase).await;
        let trigger = f
            .instance(Some(root), AgentMode::Sync, Phase::Running)
            .await;
        f.pending(root, "recovered").await;
        let runtime = f.open().await;
        assert_eq!(
            f.storage
                .list_pending_by_topic(f.topic)
                .await
                .unwrap()
                .len(),
            1
        );
        runtime.cancel(trigger).await.unwrap();
        if cancelled {
            runtime.shutdown().await.unwrap();
            assert_eq!(f.phase(root).await, "cancelled");
            assert_eq!(
                f.storage
                    .list_pending_by_topic(f.topic)
                    .await
                    .unwrap()
                    .len(),
                1
            );
        } else {
            eventually(|| async { f.phase(root).await == "failed" }).await;
            assert!(
                f.storage
                    .list_pending_by_topic(f.topic)
                    .await
                    .unwrap()
                    .is_empty()
            );
            runtime.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn cancelling_a_suspended_subtree_clears_all_pending() {
    let f = Fixture::new().await;
    let phase = Phase::Suspended(Suspended {
        external_wait_id: 7,
    });
    let root = f.instance(None, AgentMode::Sync, phase.clone()).await;
    let child = f.instance(Some(root), AgentMode::Sync, phase).await;
    f.pending(root, "root").await;
    f.pending(child, "child").await;
    let runtime = f.open().await;
    runtime.cancel(root).await.unwrap();
    eventually(|| async {
        f.phase(root).await == "cancelled" && f.phase(child).await == "cancelled"
    })
    .await;
    assert!(
        f.storage
            .list_pending_by_topic(f.topic)
            .await
            .unwrap()
            .is_empty()
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn resume_rejects_missing_mismatched_and_answered_waits() {
    let f = Fixture::new().await;
    let root = f
        .instance(
            None,
            AgentMode::Sync,
            Phase::Suspended(Suspended {
                external_wait_id: 999,
            }),
        )
        .await;
    let turn = f.storage.create_turn(f.topic, root, None).await.unwrap();
    let message = f
        .storage
        .create_message(turn, Role::Assistant, None)
        .await
        .unwrap();
    let wait = f
        .storage
        .create_external_wait(root, message, serde_json::json!([]))
        .await
        .unwrap();
    let runtime = f.open().await;
    for id in [-1, wait] {
        assert!(matches!(
            runtime
                .resume(WaitAnswer {
                    external_wait_id: id,
                    decisions: vec![]
                })
                .await,
            Err(RuntimeError::Denied(_))
        ));
    }
    f.storage
        .answer_external_wait(wait, serde_json::json!([]))
        .await
        .unwrap();
    let (phase, payload) = Phase::<LlmDomain>::Suspended(Suspended {
        external_wait_id: wait,
    })
    .columns()
    .unwrap();
    f.storage
        .update_instance_phase(root, &phase, payload)
        .await
        .unwrap();
    assert!(matches!(
        runtime
            .resume(WaitAnswer {
                external_wait_id: wait,
                decisions: vec![]
            })
            .await,
        Err(RuntimeError::Denied(_))
    ));
    assert_eq!(f.phase(root).await, "suspended");
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_targets_are_denied_and_shutdown_rejects_input() {
    let f = Fixture::new().await;
    let runtime = f.open().await;
    assert!(matches!(
        runtime.submit(-1, vec![]).await,
        Err(RuntimeError::Denied(_))
    ));
    assert!(matches!(
        runtime.cancel(-1).await,
        Err(RuntimeError::Denied(_))
    ));
    runtime.shutdown().await.unwrap();
    assert!(matches!(
        runtime.submit(f.topic, vec![]).await,
        Err(RuntimeError::Stopped)
    ));
    assert!(matches!(
        runtime.cancel(-1).await,
        Err(RuntimeError::Stopped)
    ));
    assert!(matches!(
        runtime
            .resume(WaitAnswer {
                external_wait_id: -1,
                decisions: vec![]
            })
            .await,
        Err(RuntimeError::Stopped)
    ));
}

#[tokio::test]
async fn mod_lifecycle_reports_topics_and_emits_events() {
    let f = Fixture::new().await;
    let runtime = f.open().await;
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sample");
    let package = f.dirs.mod_version("runtime-sample", 1);
    std::fs::create_dir_all(package.join("prompts")).unwrap();
    std::fs::copy(source.join("mod.json"), package.join("mod.json")).unwrap();
    std::fs::copy(
        source.join("prompts/default.md"),
        package.join("prompts/default.md"),
    )
    .unwrap();
    let mut sub = runtime.subscribe().await.unwrap().subscription;
    assert!(
        runtime
            .install(&package)
            .await
            .unwrap()
            .affected_topics
            .is_empty()
    );
    event_until(&mut sub, |event| {
        event.envelope.name == "core/mod.installed"
    })
    .await;
    assert_eq!(
        runtime
            .enable(f.topic, "runtime-sample")
            .await
            .unwrap()
            .affected_topics,
        [f.topic]
    );
    let second = f.dirs.mod_version("runtime-sample", 2);
    std::fs::create_dir_all(&second).unwrap();
    std::fs::write(
        second.join("mod.json"),
        r#"{"name":"runtime-sample","version":2}"#,
    )
    .unwrap();
    assert_eq!(
        runtime.update(&second).await.unwrap().affected_topics,
        [f.topic]
    );
    event_until(&mut sub, |event| event.envelope.name == "core/mod.updated").await;
    assert_eq!(
        runtime
            .disable(f.topic, "runtime-sample")
            .await
            .unwrap()
            .affected_topics,
        [f.topic]
    );
    assert!(
        runtime
            .delete("runtime-sample")
            .await
            .unwrap()
            .affected_topics
            .is_empty()
    );
    event_until(&mut sub, |event| event.envelope.name == "core/mod.deleted").await;
    assert!(
        runtime
            .storage()
            .mod_by_name("runtime-sample")
            .await
            .unwrap()
            .is_none()
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn matching_wait_resumes_after_restart_and_records_the_answer() {
    let f = Fixture::new().await;
    let root = f.instance(None, AgentMode::Sync, Phase::Running).await;
    let turn = f.storage.create_turn(f.topic, root, None).await.unwrap();
    let message = f
        .storage
        .create_message(turn, Role::Assistant, None)
        .await
        .unwrap();
    let wait = f
        .storage
        .create_external_wait(root, message, serde_json::json!([]))
        .await
        .unwrap();
    let (phase, payload) = Phase::<LlmDomain>::Suspended(Suspended {
        external_wait_id: wait,
    })
    .columns()
    .unwrap();
    f.storage
        .update_instance_phase(root, &phase, payload)
        .await
        .unwrap();
    let runtime = f.open().await;
    assert_eq!(f.phase(root).await, "suspended");
    runtime
        .resume(WaitAnswer {
            external_wait_id: wait,
            decisions: vec![],
        })
        .await
        .unwrap();
    eventually(|| async {
        f.storage
            .get_external_wait(wait)
            .await
            .unwrap()
            .unwrap()
            .answered_at
            .is_some()
            && f.phase(root).await == "failed"
    })
    .await;
    assert_eq!(
        f.storage
            .get_external_wait(wait)
            .await
            .unwrap()
            .unwrap()
            .decision,
        Some(serde_json::json!([]))
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_phase_does_not_reopen_its_pending_inputs() {
    let f = Fixture::new().await;
    let root = f.instance(None, AgentMode::Sync, Phase::Running).await;
    let trigger = f
        .instance(Some(root), AgentMode::Sync, Phase::Running)
        .await;
    f.storage
        .update_instance_phase(root, "suspended", Some("not-json".into()))
        .await
        .unwrap();
    f.pending(root, "must remain pending").await;
    let runtime = f.open().await;
    runtime.cancel(trigger).await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(
        f.storage
            .list_pending_by_topic(f.topic)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        f.storage
            .list_instance_context(root)
            .await
            .unwrap()
            .is_empty()
    );
}
