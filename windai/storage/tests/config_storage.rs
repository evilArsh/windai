use tempfile::TempDir;
use wind_storage::Storage;
use wind_wire::{AdapterType, AgentMode, AgentRole, Block, Role};

async fn database() -> (TempDir, Storage) {
    let dir = TempDir::new().expect("temp dir");
    let storage = Storage::open(&dir.path().join("test.db"))
        .await
        .expect("open database");
    (dir, storage)
}

async fn provider(storage: &Storage, name: &str) -> i64 {
    storage
        .create_provider(name, "https://example.test")
        .await
        .expect("create provider")
}

async fn model(storage: &Storage, provider_id: i64, name: &str) -> i64 {
    storage
        .create_model(provider_id, name, AdapterType::OpenAICompletion, None, None)
        .await
        .expect("create model")
}

#[tokio::test]
async fn topics_are_listed_newest_first() {
    let (_dir, storage) = database().await;
    let first = storage
        .create_topic(None, "first", None, None, None)
        .await
        .expect("create first topic");
    let second = storage
        .create_topic(None, "second", None, None, None)
        .await
        .expect("create second topic");
    let third = storage
        .create_topic(None, "third", None, None, None)
        .await
        .expect("create third topic");

    let topics = storage.list_topics().await.expect("list topics");
    let ids: Vec<i64> = topics.iter().map(|topic| topic.id).collect();
    assert_eq!(ids, [third, second, first]);
    assert_eq!(
        topics
            .iter()
            .map(|topic| topic.label.as_str())
            .collect::<Vec<_>>(),
        ["third", "second", "first"]
    );
}

#[tokio::test]
async fn providers_are_listed_in_id_order() {
    let (_dir, storage) = database().await;
    let first = provider(&storage, "first").await;
    let second = provider(&storage, "second").await;
    let third = provider(&storage, "third").await;

    let providers = storage.list_providers().await.expect("list providers");
    let ids: Vec<i64> = providers.iter().map(|provider| provider.id).collect();
    assert_eq!(ids, [first, second, third]);
    assert_eq!(
        providers
            .iter()
            .map(|provider| provider.name.as_str())
            .collect::<Vec<_>>(),
        ["first", "second", "third"]
    );
    assert!(
        providers
            .iter()
            .all(|provider| provider.base_url == "https://example.test" && provider.active)
    );
}

#[tokio::test]
async fn models_are_filtered_by_provider_and_ordered() {
    let (_dir, storage) = database().await;
    let first_provider = provider(&storage, "first").await;
    let second_provider = provider(&storage, "second").await;
    let first = model(&storage, first_provider, "first").await;
    let other = model(&storage, second_provider, "other").await;
    let second = model(&storage, first_provider, "second").await;

    let all = storage.list_models(None).await.expect("list all models");
    let ids: Vec<i64> = all.iter().map(|model| model.id).collect();
    assert_eq!(ids, [first, second, other]);
    assert_eq!(
        all.iter()
            .map(|model| model.provider_id)
            .collect::<Vec<_>>(),
        [first_provider, first_provider, second_provider]
    );

    let filtered = storage
        .list_models(Some(first_provider))
        .await
        .expect("list provider models");
    let ids: Vec<i64> = filtered.iter().map(|model| model.id).collect();
    assert_eq!(ids, [first, second]);
    assert!(
        filtered
            .iter()
            .all(|model| model.provider_id == first_provider)
    );

    let empty = storage
        .list_models(Some(-1))
        .await
        .expect("list unknown provider models");
    assert!(empty.is_empty());
}

#[tokio::test]
async fn context_rows_carry_the_message_id() {
    let (_dir, storage) = database().await;
    let topic = storage
        .create_topic(None, "topic", None, None, None)
        .await
        .expect("create topic");
    let instance = storage
        .create_agent_instance(
            topic,
            None,
            AgentRole::Main,
            Some(AgentMode::Sync),
            "running",
            None,
        )
        .await
        .expect("create instance");
    let turn = storage
        .create_turn(topic, instance, None)
        .await
        .expect("create turn");
    let first = storage
        .create_message(turn, Role::User, None)
        .await
        .expect("create first message");
    let second = storage
        .create_message(turn, Role::Assistant, None)
        .await
        .expect("create second message");
    storage
        .create_blocks(
            first,
            &[
                Block::new_text(Role::User, 0),
                Block::new_text(Role::User, 1),
            ],
        )
        .await
        .expect("create first message blocks");
    storage
        .create_blocks(second, &[Block::new_text(Role::Assistant, 0)])
        .await
        .expect("create second message blocks");

    let rows = storage
        .list_instance_context(instance)
        .await
        .expect("list context");
    let message_ids: Vec<i64> = rows.iter().map(|row| row.message_id).collect();
    assert_eq!(message_ids, [first, first, second]);
    assert!(
        rows.iter()
            .all(|row| row.message_id == row.block.message_id)
    );
}
