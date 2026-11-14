//! 后台结果重复交付只写入一条待消费输入

#[path = "common/lib.rs"]
mod common;

use common::{Bus, Fixture};
use wind_core::agent::Deliver;
use wind_core::llm::LlmRunner;
use wind_wire::{AgentMode, AgentRole};

#[tokio::test]
async fn duplicate_delivery_keeps_one_pending_result() {
    let fixture = Fixture::open("delivery").await;
    let bus = Bus::open(&fixture).await;
    let storage = &fixture.storage;
    let topic = storage
        .create_topic(None, "duplicate", None, None, None)
        .await
        .unwrap();
    let root = storage
        .create_agent_instance(
            topic,
            None,
            AgentRole::Main,
            Some(AgentMode::Sync),
            "suspended",
            Some(r#"{"external_wait_id":1}"#.into()),
        )
        .await
        .unwrap();
    let child = storage
        .create_agent_instance(
            topic,
            Some(root),
            AgentRole::Child,
            Some(AgentMode::Background),
            "aborted",
            None,
        )
        .await
        .unwrap();
    storage
        .create_spawn_call_map(root, "duplicate-call", child)
        .await
        .unwrap();
    let runner = LlmRunner::new(
        fixture.storage.clone(),
        bus.dispatcher.clone(),
        bus.services.clone(),
        bus.services.clone(),
    );
    runner.deliver(child, root).await.unwrap();
    runner.deliver(child, root).await.unwrap();
    let pending = storage.list_pending_by_topic(topic).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].source_id, Some(child));
    assert_eq!(pending[0].instance_id, root);
    assert!(
        storage
            .spawn_call_of_child(child)
            .await
            .unwrap()
            .unwrap()
            .result_delivered_at
            .is_some()
    );
    bus.close().await;
}
