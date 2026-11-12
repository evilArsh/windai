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
async fn providers_and_models_can_be_updated() {
    let (_dir, storage) = database().await;
    let provider_id = provider(&storage, "alpha").await;
    let model_id = model(&storage, provider_id, "alpha-model").await;

    storage
        .update_provider(provider_id, "beta", "https://beta.example")
        .await
        .expect("update the provider");
    storage
        .update_model(
            model_id,
            "beta-model",
            AdapterType::OpenAIResponse,
            Some("https://beta.example/v1"),
            Some(&serde_json::json!({"stream": true})),
        )
        .await
        .expect("update the model");

    let providers = storage.list_providers().await.expect("list providers");
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0].name, "beta");
    assert_eq!(providers[0].base_url, "https://beta.example");

    let models = storage
        .list_models(Some(provider_id))
        .await
        .expect("list models");
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].name, "beta-model");
    assert_eq!(models[0].adapter, AdapterType::OpenAIResponse);
    assert_eq!(
        models[0].endpoint.as_deref(),
        Some("https://beta.example/v1")
    );
    assert_eq!(models[0].config, Some(serde_json::json!({"stream": true})));
}

#[tokio::test]
async fn updating_a_missing_row_fails() {
    let (_dir, storage) = database().await;
    let provider = storage
        .update_provider(999, "missing", "https://missing.example")
        .await;
    assert!(provider.is_err());
}

#[tokio::test]
async fn replacing_a_credential_leaves_only_the_new_key_active() {
    let (_dir, storage) = database().await;
    let provider_id = provider(&storage, "alpha").await;
    let model_id = model(&storage, provider_id, "alpha-model").await;
    storage
        .create_credential(provider_id, "sk-old")
        .await
        .expect("create the old credential");

    storage
        .replace_credential(provider_id, "sk-new")
        .await
        .expect("replace the credential");

    let config = storage
        .get_model_config(model_id)
        .await
        .expect("read the model config")
        .expect("the model config exists");
    assert_eq!(config.api_key.as_deref(), Some("sk-new"));
}

#[tokio::test]
async fn updating_the_topic_model_changes_the_stored_model() {
    let (_dir, storage) = database().await;
    let provider_id = provider(&storage, "alpha").await;
    let first = model(&storage, provider_id, "first").await;
    let second = model(&storage, provider_id, "second").await;
    let topic_id = storage
        .create_topic(None, "topic", Some(first), None, None)
        .await
        .expect("create the topic");

    storage
        .update_topic_model(topic_id, Some(second))
        .await
        .expect("update the topic model");

    let topic = storage
        .get_topic_by_id(topic_id)
        .await
        .expect("read the topic")
        .expect("the topic exists");
    assert_eq!(topic.model_id, Some(second));
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
