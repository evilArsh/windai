use serde_json::{Value, json};
use tempfile::TempDir;
use wind_storage::{Storage, Topic};
use wind_tui::app::{MessageView, ModView, Session};
use wind_tui::fetch::load_session;
use wind_tui::reduce::apply;
use wind_wire::event::{Envelope, EventKey, FeedEvent, Origin, Scope, Shape, Source, Target};
use wind_wire::{AgentMode, AgentRole, Block, Body, Role, Usage};

const CURRENT_TOPIC: i64 = 7;

fn topic(id: i64) -> Topic {
    Topic {
        id,
        parent_id: None,
        label: "topic".to_owned(),
        model_id: None,
        icon: None,
        workspace: None,
    }
}

fn session(topic_id: i64, main: Option<i64>) -> Session {
    Session::new(topic(topic_id), main)
}

fn scope(topic_id: i64) -> Scope {
    Scope::Topic {
        topic_id,
        instance_id: 1,
    }
}

fn event(seq: i64, name: &str, scope: Scope, payload: Value) -> FeedEvent {
    FeedEvent {
        key: EventKey::new(0, seq).expect("build an event key"),
        envelope: Envelope::new(
            name,
            scope,
            Origin::Internal,
            Source::core("test"),
            Some(payload),
            Shape::Observe,
        ),
        target: Target::Core,
    }
}

fn delta_in(scope: Scope, message_id: i64, seq: i64, revision: i64, content: &str) -> FeedEvent {
    event(
        1,
        "core/block.delta",
        scope,
        json!({
            "message_id": message_id,
            "role": "assistant",
            "seq": seq,
            "revision": revision,
            "kind": "text",
            "content": content
        }),
    )
}

fn delta(message_id: i64, seq: i64, revision: i64, content: &str) -> FeedEvent {
    delta_in(scope(CURRENT_TOPIC), message_id, seq, revision, content)
}

#[test]
fn delta_keeps_the_highest_revision() {
    let mut session = session(CURRENT_TOPIC, None);
    assert!(apply(&mut session, &delta(10, 0, 5, "newest")));
    assert!(!apply(&mut session, &delta(10, 0, 3, "older")));
    let message = &session.messages[0];
    assert_eq!(message.blocks.len(), 1);
    assert_eq!(message.blocks[0].revision, 5);
    assert_eq!(
        message.blocks[0].body,
        Body::Text {
            content: "newest".to_owned()
        }
    );
}

#[test]
fn delta_replaces_an_older_block_when_the_revision_grows() {
    let mut session = session(CURRENT_TOPIC, None);
    assert!(apply(&mut session, &delta(10, 0, 1, "first")));
    assert!(apply(&mut session, &delta(10, 0, 2, "second")));
    assert_eq!(session.messages[0].blocks.len(), 1);
    assert_eq!(session.messages[0].blocks[0].revision, 2);
    assert_eq!(
        session.messages[0].blocks[0].body,
        Body::Text {
            content: "second".to_owned()
        }
    );
}

#[test]
fn duplicate_delta_does_not_change_state() {
    let mut session = session(CURRENT_TOPIC, None);
    assert!(apply(&mut session, &delta(10, 0, 2, "kept")));
    assert!(!apply(&mut session, &delta(10, 0, 2, "ignored")));
    assert_eq!(session.messages[0].blocks.len(), 1);
    assert_eq!(
        session.messages[0].blocks[0].body,
        Body::Text {
            content: "kept".to_owned()
        }
    );
}

#[test]
fn blocks_are_kept_in_seq_order() {
    let mut session = session(CURRENT_TOPIC, None);
    assert!(apply(&mut session, &delta(11, 2, 1, "third")));
    assert!(apply(&mut session, &delta(11, 0, 1, "first")));
    assert!(apply(&mut session, &delta(11, 1, 1, "second")));
    let seqs: Vec<i64> = session.messages[0]
        .blocks
        .iter()
        .map(|block| block.seq)
        .collect();
    assert_eq!(seqs, [0, 1, 2]);
}

#[test]
fn committed_with_a_block_inserts_it_as_committed() {
    let mut session = session(CURRENT_TOPIC, None);
    let committed = event(
        2,
        "core/block.committed",
        scope(CURRENT_TOPIC),
        json!({
            "message_id": 12,
            "seq": 4,
            "block_id": 99,
            "block": {
                "role": "assistant",
                "seq": 4,
                "revision": 2,
                "kind": "text",
                "content": "done"
            }
        }),
    );
    assert!(apply(&mut session, &committed));
    let message = &session.messages[0];
    assert_eq!(message.id, 12);
    assert_eq!(message.role, Role::Assistant);
    assert_eq!(message.blocks.len(), 1);
    assert!(message.blocks[0].committed);
    assert_eq!(message.blocks[0].seq, 4);
    assert_eq!(message.blocks[0].revision, 2);
}

#[test]
fn committed_without_a_block_marks_the_existing_block() {
    let mut session = session(CURRENT_TOPIC, None);
    assert!(apply(&mut session, &delta(13, 3, 1, "partial")));
    let committed = event(
        2,
        "core/block.committed",
        scope(CURRENT_TOPIC),
        json!({"message_id": 13, "seq": 3, "block_id": 41, "block": null}),
    );
    assert!(apply(&mut session, &committed));
    assert!(session.messages[0].blocks[0].committed);
}

#[test]
fn committed_without_a_block_ignores_unknown_messages() {
    let mut session = session(CURRENT_TOPIC, None);
    let committed = event(
        2,
        "core/block.committed",
        scope(CURRENT_TOPIC),
        json!({"message_id": 404, "seq": 0, "block_id": 1, "block": null}),
    );
    assert!(!apply(&mut session, &committed));
    assert!(session.messages.is_empty());
}

#[test]
fn seal_marks_the_message_and_sums_usage() {
    let mut session = session(CURRENT_TOPIC, None);
    assert!(apply(&mut session, &delta(20, 0, 1, "answer")));
    let seal = event(
        2,
        "core/message.seal",
        scope(CURRENT_TOPIC),
        json!({
            "message_id": 20,
            "finish_reason": "stop",
            "usage": {"input_tokens": 11, "output_tokens": 3}
        }),
    );
    assert!(apply(&mut session, &seal));
    assert_eq!(session.messages[0].sealed.as_deref(), Some("stop"));
    assert_eq!(
        session.usage,
        Usage {
            input_tokens: 11,
            output_tokens: 3
        }
    );
}

#[test]
fn seal_without_a_message_still_sums_usage() {
    let mut session = session(CURRENT_TOPIC, None);
    let seal = event(
        2,
        "core/message.seal",
        scope(CURRENT_TOPIC),
        json!({
            "message_id": 404,
            "finish_reason": "stop",
            "usage": {"input_tokens": 5, "output_tokens": 7}
        }),
    );
    assert!(apply(&mut session, &seal));
    assert!(session.messages.is_empty());
    assert_eq!(
        session.usage,
        Usage {
            input_tokens: 5,
            output_tokens: 7
        }
    );
}

#[test]
fn interrupted_marks_the_message() {
    let mut session = session(CURRENT_TOPIC, None);
    assert!(apply(&mut session, &delta(21, 0, 1, "partial")));
    let interrupted = event(
        2,
        "core/message.interrupted",
        scope(CURRENT_TOPIC),
        json!({"message_id": 21, "reason": "cancelled"}),
    );
    assert!(apply(&mut session, &interrupted));
    assert_eq!(
        session.messages[0].interrupted.as_deref(),
        Some("cancelled")
    );
}

#[test]
fn interrupted_ignores_unknown_messages() {
    let mut session = session(CURRENT_TOPIC, None);
    let interrupted = event(
        2,
        "core/message.interrupted",
        scope(CURRENT_TOPIC),
        json!({"message_id": 404, "reason": "failed"}),
    );
    assert!(!apply(&mut session, &interrupted));
}

#[test]
fn global_events_only_append_to_the_log() {
    let mut session = session(CURRENT_TOPIC, None);
    let global = event(1, "core/unknown", Scope::Global, json!({"value": 1}));
    assert!(!apply(&mut session, &global));
    assert_eq!(session.log.entries().count(), 1);
}

#[test]
fn other_topic_events_are_dropped_entirely() {
    let mut session = session(CURRENT_TOPIC, None);
    let other = delta_in(scope(CURRENT_TOPIC + 1), 30, 0, 1, "hidden");
    assert!(!apply(&mut session, &other));
    assert!(session.messages.is_empty());
    assert_eq!(session.log.entries().count(), 0);
}

#[test]
fn unknown_topic_events_only_append_to_the_log() {
    let mut session = session(CURRENT_TOPIC, None);
    let unknown = event(1, "core/unknown", scope(CURRENT_TOPIC), json!({"value": 1}));
    assert!(!apply(&mut session, &unknown));
    assert_eq!(session.log.entries().count(), 1);
}

#[test]
fn malformed_payload_appends_a_local_log_and_keeps_state() {
    let mut session = session(CURRENT_TOPIC, None);
    let malformed = event(
        1,
        "core/block.delta",
        scope(CURRENT_TOPIC),
        json!({"message_id": "not-a-number"}),
    );
    assert!(!apply(&mut session, &malformed));
    assert!(session.messages.is_empty());
    assert_eq!(session.log.entries().count(), 2);
}

async fn database() -> (TempDir, Storage) {
    let dir = TempDir::new().expect("create a temp dir");
    let storage = Storage::open(&dir.path().join("windai.db"))
        .await
        .expect("open the database");
    (dir, storage)
}

#[tokio::test]
async fn load_session_groups_blocks_and_sums_usage() {
    let (_dir, storage) = database().await;
    let topic_id = storage
        .create_topic(None, "topic", None, None, None)
        .await
        .expect("create the topic");
    let main = storage
        .create_agent_instance(
            topic_id,
            None,
            AgentRole::Main,
            Some(AgentMode::Sync),
            "running",
            None,
        )
        .await
        .expect("create the main instance");
    storage
        .create_agent_instance(
            topic_id,
            Some(main),
            AgentRole::Child,
            None,
            "running",
            None,
        )
        .await
        .expect("create a child instance");
    let turn = storage
        .create_turn(topic_id, main, None)
        .await
        .expect("create the turn");
    let first = storage
        .create_message(turn, Role::User, None)
        .await
        .expect("create the first message");
    let second = storage
        .create_message(turn, Role::Assistant, None)
        .await
        .expect("create the second message");
    storage
        .seal_message(first, "stop", 4, 1)
        .await
        .expect("seal the first message");
    storage
        .seal_message(second, "stop", 6, 2)
        .await
        .expect("seal the second message");
    storage
        .create_blocks(
            first,
            &[
                Block::new(
                    Role::User,
                    1,
                    1,
                    Body::Text {
                        content: "second".to_owned(),
                    },
                ),
                Block::new(
                    Role::User,
                    0,
                    1,
                    Body::Text {
                        content: "first".to_owned(),
                    },
                ),
            ],
        )
        .await
        .expect("create the first message blocks");
    storage
        .create_blocks(
            second,
            &[Block::new(
                Role::Assistant,
                0,
                3,
                Body::Text {
                    content: "answer".to_owned(),
                },
            )],
        )
        .await
        .expect("create the second message blocks");

    let loaded = load_session(&storage, topic_id)
        .await
        .expect("load the session");

    assert_eq!(loaded.topic.id, topic_id);
    assert_eq!(loaded.main, Some(main));
    assert_eq!(loaded.messages.len(), 2);
    assert_eq!(loaded.messages[0].id, first);
    assert_eq!(loaded.messages[0].role, Role::User);
    assert_eq!(
        loaded.messages[0]
            .blocks
            .iter()
            .map(|block| block.seq)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert!(
        loaded.messages[0]
            .blocks
            .iter()
            .all(|block| block.committed)
    );
    assert_eq!(loaded.messages[1].id, second);
    assert_eq!(loaded.messages[1].role, Role::Assistant);
    assert_eq!(
        loaded.usage,
        Usage {
            input_tokens: 14,
            output_tokens: 4
        }
    );
}

#[tokio::test]
async fn load_session_without_a_main_instance_has_no_messages() {
    let (_dir, storage) = database().await;
    let topic_id = storage
        .create_topic(None, "idle", None, None, None)
        .await
        .expect("create the topic");
    let loaded = load_session(&storage, topic_id)
        .await
        .expect("load the session");
    assert_eq!(loaded.main, None);
    assert!(loaded.messages.is_empty());
    assert_eq!(
        loaded.usage,
        Usage {
            input_tokens: 0,
            output_tokens: 0
        }
    );
}

fn spawn(seq: i64, instance_id: i64, parent_id: Option<i64>) -> FeedEvent {
    event(
        seq,
        "core/agent.spawn",
        scope(CURRENT_TOPIC),
        json!({
            "instance_id": instance_id,
            "parent_id": parent_id,
            "detached": false
        }),
    )
}

fn status(seq: i64, instance_id: i64, phase: &str, payload: Option<Value>) -> FeedEvent {
    event(
        seq,
        "core/agent.status",
        scope(CURRENT_TOPIC),
        json!({
            "instance_id": instance_id,
            "phase": phase,
            "phase_payload": payload
        }),
    )
}

fn sealed(seq: i64, instance_id: i64) -> FeedEvent {
    event(
        seq,
        "core/agent.sealed",
        scope(CURRENT_TOPIC),
        json!({"instance_id": instance_id}),
    )
}

fn manifest(name: &str, version: u32) -> String {
    json!({"name": name, "version": version}).to_string()
}

#[tokio::test]
async fn load_session_loads_nodes_and_mods() {
    let (_dir, storage) = database().await;
    let topic_id = storage
        .create_topic(None, "topic", None, None, None)
        .await
        .expect("create the topic");
    let main = storage
        .create_agent_instance(
            topic_id,
            None,
            AgentRole::Main,
            Some(AgentMode::Sync),
            "running",
            None,
        )
        .await
        .expect("create the main instance");
    let waiting = storage
        .create_agent_instance(
            topic_id,
            Some(main),
            AgentRole::Child,
            None,
            "suspended",
            Some(json!({"external_wait_id": 42}).to_string()),
        )
        .await
        .expect("create a suspended instance");
    let failed = storage
        .create_agent_instance(
            topic_id,
            Some(main),
            AgentRole::Child,
            None,
            "failed",
            Some(json!({"message_id": null, "error": "boom\nstack"}).to_string()),
        )
        .await
        .expect("create a failed instance");
    let weather = storage
        .install_mod("weather", &manifest("weather", 1))
        .await
        .expect("install weather");
    let alerts = storage
        .install_mod("alerts", &manifest("alerts", 1))
        .await
        .expect("install alerts");
    storage
        .enable_mod(topic_id, weather)
        .await
        .expect("enable weather");
    storage
        .enable_mod(topic_id, alerts)
        .await
        .expect("enable alerts");

    let loaded = load_session(&storage, topic_id)
        .await
        .expect("load the session");

    assert_eq!(loaded.main, Some(main));
    let ids: Vec<i64> = loaded.nodes.iter().map(|node| node.id).collect();
    assert_eq!(ids, [main, waiting, failed]);
    assert_eq!(loaded.nodes[0].role, AgentRole::Main);
    assert_eq!(loaded.nodes[0].phase, "running");
    assert_eq!(loaded.nodes[1].parent_id, Some(main));
    assert_eq!(loaded.nodes[1].phase, "suspended");
    assert_eq!(loaded.nodes[1].suspended_wait, Some(42));
    assert!(loaded.nodes[1].reason.is_none());
    assert_eq!(loaded.nodes[2].phase, "failed");
    assert_eq!(loaded.nodes[2].reason.as_deref(), Some("boom"));
    assert_eq!(
        loaded.mods,
        [
            ModView {
                name: "alerts".to_owned(),
                version: 1
            },
            ModView {
                name: "weather".to_owned(),
                version: 1
            }
        ]
    );
}

#[test]
fn agent_spawn_inserts_a_child_and_backfills_the_main_role() {
    let mut session = session(CURRENT_TOPIC, Some(50));
    assert!(apply(&mut session, &spawn(1, 50, None)));
    assert_eq!(session.nodes.len(), 1);
    assert_eq!(session.nodes[0].role, AgentRole::Main);
    assert_eq!(session.nodes[0].phase, "running");
    assert!(apply(&mut session, &spawn(2, 60, Some(50))));
    assert_eq!(session.nodes.len(), 2);
    assert_eq!(session.nodes[1].role, AgentRole::Child);
    assert_eq!(session.nodes[1].parent_id, Some(50));
    assert!(!apply(&mut session, &spawn(3, 60, Some(50))));
    assert_eq!(session.nodes.len(), 2);
}

#[test]
fn agent_spawn_does_not_guess_main() {
    let mut session = session(CURRENT_TOPIC, None);
    assert!(apply(&mut session, &spawn(1, 70, None)));
    assert_eq!(session.main, None);
    assert_eq!(session.nodes[0].role, AgentRole::Child);
}

#[test]
fn agent_status_updates_the_phase_and_syncs_a_terminal_message() {
    let mut session = session(CURRENT_TOPIC, Some(5));
    let mut done = MessageView::new(101, Role::Assistant);
    done.sealed = Some("stop".to_owned());
    session.messages.push(done);
    session
        .messages
        .push(MessageView::new(102, Role::Assistant));
    session.messages.push(MessageView::new(103, Role::User));
    assert!(apply(&mut session, &spawn(1, 5, None)));
    assert!(apply(
        &mut session,
        &status(
            2,
            5,
            "failed",
            Some(json!({"message_id": null, "error": "boom\nstack"}))
        )
    ));
    let node = &session.nodes[0];
    assert_eq!(node.phase, "failed");
    assert_eq!(node.reason.as_deref(), Some("boom"));
    assert_eq!(session.messages[0].interrupted, None);
    assert_eq!(session.messages[1].interrupted, None);
    assert_eq!(session.messages[2].interrupted.as_deref(), Some("failed"));
}

#[test]
fn agent_status_inserts_a_missing_node_and_decodes_the_payload() {
    let mut session = session(CURRENT_TOPIC, Some(9));
    assert!(apply(
        &mut session,
        &status(1, 9, "suspended", Some(json!({"external_wait_id": 77})))
    ));
    assert_eq!(session.nodes.len(), 1);
    assert_eq!(session.nodes[0].id, 9);
    assert_eq!(session.nodes[0].phase, "suspended");
    assert_eq!(session.nodes[0].suspended_wait, Some(77));
}

#[test]
fn agent_status_keeps_the_phase_on_a_bad_payload() {
    let mut session = session(CURRENT_TOPIC, Some(5));
    assert!(apply(&mut session, &spawn(1, 5, None)));
    let log_before = session.log.entries().count();
    assert!(!apply(
        &mut session,
        &status(2, 5, "suspended", Some(json!({"unexpected": 1})))
    ));
    assert_eq!(session.nodes[0].phase, "running");
    assert_eq!(session.log.entries().count(), log_before + 2);
}

#[test]
fn agent_sealed_marks_the_main_node() {
    let mut session = session(CURRENT_TOPIC, Some(5));
    assert!(apply(&mut session, &spawn(1, 5, None)));
    assert!(apply(&mut session, &spawn(2, 6, Some(5))));
    assert!(apply(&mut session, &sealed(3, 5)));
    assert!(session.nodes[0].sealed);
    assert!(!session.nodes[1].sealed);
    assert!(!apply(&mut session, &sealed(4, 5)));
    assert!(!apply(&mut session, &sealed(5, 6)));
}

#[test]
fn mod_events_request_a_reload() {
    let mut session = session(CURRENT_TOPIC, None);
    for (seq, name) in [
        (1, "core/mod.installed"),
        (2, "core/mod.updated"),
        (3, "core/mod.deleted"),
    ] {
        let payload = json!({
            "name": "weather",
            "version": 1,
            "from_version": 1,
            "to_version": 2
        });
        assert!(apply(
            &mut session,
            &event(seq, name, scope(CURRENT_TOPIC), payload)
        ));
        assert!(session.reload_mods);
        session.reload_mods = false;
    }
}
