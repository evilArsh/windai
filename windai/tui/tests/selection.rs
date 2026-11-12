use serde_json::json;
use tempfile::TempDir;
use wind_storage::{Model, Provider, Storage, Topic};
use wind_tui::config::Selection;
use wind_tui::fetch::load_startup;
use wind_wire::AdapterType;

async fn database() -> (TempDir, Storage) {
    let dir = TempDir::new().expect("create a temp dir");
    let storage = Storage::open(&dir.path().join("windai.db"))
        .await
        .expect("open the database");
    (dir, storage)
}

async fn provider(storage: &Storage, name: &str) -> i64 {
    storage
        .create_provider(name, &format!("https://{name}.example"))
        .await
        .expect("create the provider")
}

async fn model(storage: &Storage, provider_id: i64, name: &str, config: Option<&str>) -> i64 {
    let config = config.map(|text| serde_json::from_str(text).expect("parse the config"));
    storage
        .create_model(
            provider_id,
            name,
            AdapterType::OpenAICompletion,
            None,
            config.as_ref(),
        )
        .await
        .expect("create the model")
}

fn find_provider(providers: &[Provider], id: i64) -> &Provider {
    providers
        .iter()
        .find(|provider| provider.id == id)
        .expect("the provider exists")
}

fn find_model(models: &[Model], id: i64) -> &Model {
    models
        .iter()
        .find(|model| model.id == id)
        .expect("the model exists")
}

#[tokio::test]
async fn startup_reports_an_empty_database() {
    let (_dir, storage) = database().await;
    let startup = load_startup(&storage)
        .await
        .expect("load the startup state");
    assert!(startup.selection.provider.is_none());
    assert!(startup.selection.model.is_none());
    assert_eq!(startup.notice.as_deref(), Some("no providers configured"));
    assert!(startup.providers.is_empty());
    assert!(startup.models.is_empty());
}

#[tokio::test]
async fn startup_falls_back_to_the_first_provider_and_model() {
    let (_dir, storage) = database().await;
    let alpha = provider(&storage, "alpha").await;
    let alpha_first = model(&storage, alpha, "alpha-first", None).await;
    model(&storage, alpha, "alpha-second", None).await;
    let beta = provider(&storage, "beta").await;
    model(&storage, beta, "beta-first", None).await;

    let startup = load_startup(&storage)
        .await
        .expect("load the startup state");
    assert_eq!(
        startup
            .selection
            .provider
            .as_ref()
            .map(|provider| provider.id),
        Some(alpha)
    );
    assert_eq!(
        startup.selection.model.as_ref().map(|model| model.id),
        Some(alpha_first)
    );
    assert_eq!(startup.providers.len(), 2);
    assert_eq!(
        startup
            .models
            .iter()
            .map(|model| model.name.as_str())
            .collect::<Vec<_>>(),
        ["alpha-first", "alpha-second"]
    );
    assert!(startup.notice.is_none());
}

#[tokio::test]
async fn startup_prefers_the_model_of_the_latest_topic() {
    let (_dir, storage) = database().await;
    let alpha = provider(&storage, "alpha").await;
    let alpha_model = model(&storage, alpha, "alpha-model", None).await;
    let beta = provider(&storage, "beta").await;
    let beta_model = model(&storage, beta, "beta-model", None).await;

    storage
        .create_topic(None, "older", Some(alpha_model), None, None)
        .await
        .expect("create the older topic");
    storage
        .create_topic(None, "newer", Some(beta_model), None, None)
        .await
        .expect("create the newer topic");

    let startup = load_startup(&storage)
        .await
        .expect("load the startup state");
    assert_eq!(
        startup
            .selection
            .provider
            .as_ref()
            .map(|provider| provider.id),
        Some(beta)
    );
    assert_eq!(
        startup.selection.model.as_ref().map(|model| model.id),
        Some(beta_model)
    );
    assert_eq!(
        startup
            .models
            .iter()
            .map(|model| model.id)
            .collect::<Vec<_>>(),
        [beta_model]
    );
}

#[tokio::test]
async fn switching_providers_cascades_to_the_smallest_model_id() {
    let (_dir, storage) = database().await;
    let alpha = provider(&storage, "alpha").await;
    let alpha_model = model(&storage, alpha, "alpha-model", None).await;
    let beta = provider(&storage, "beta").await;
    let beta_first = model(&storage, beta, "beta-first", None).await;
    model(&storage, beta, "beta-second", None).await;

    let providers = storage.list_providers().await.expect("list providers");
    let models = storage.list_models(None).await.expect("list models");
    let mut selection = Selection::new(
        Some(find_provider(&providers, alpha).clone()),
        Some(find_model(&models, alpha_model).clone()),
    );

    let notice = selection.select_provider(&providers, &models, beta);
    assert!(notice.is_none());
    assert_eq!(
        selection.provider.as_ref().map(|provider| provider.id),
        Some(beta)
    );
    assert_eq!(
        selection.model.as_ref().map(|model| model.id),
        Some(beta_first)
    );

    let notice = selection.select_provider(&providers, &models, alpha);
    assert!(notice.is_none());
    assert_eq!(
        selection.provider.as_ref().map(|provider| provider.id),
        Some(alpha)
    );
    assert_eq!(
        selection.model.as_ref().map(|model| model.id),
        Some(alpha_model)
    );
}

#[tokio::test]
async fn a_provider_without_models_reports_a_notice_and_clears_the_model() {
    let (_dir, storage) = database().await;
    let alpha = provider(&storage, "alpha").await;
    let alpha_model = model(&storage, alpha, "alpha-model", None).await;
    let empty = provider(&storage, "empty").await;

    let providers = storage.list_providers().await.expect("list providers");
    let models = storage.list_models(None).await.expect("list models");
    let mut selection = Selection::new(
        Some(find_provider(&providers, alpha).clone()),
        Some(find_model(&models, alpha_model).clone()),
    );

    let notice = selection.select_provider(&providers, &models, empty);
    assert_eq!(notice.as_deref(), Some("this provider has no models"));
    assert_eq!(
        selection.provider.as_ref().map(|provider| provider.id),
        Some(empty)
    );
    assert!(selection.model.is_none());
    assert!(selection.reasoning_effort.is_none());
}

#[tokio::test]
async fn selecting_a_model_outside_the_current_provider_changes_nothing() {
    let (_dir, storage) = database().await;
    let alpha = provider(&storage, "alpha").await;
    let alpha_model = model(&storage, alpha, "alpha-model", None).await;
    let beta = provider(&storage, "beta").await;
    let beta_model = model(&storage, beta, "beta-model", None).await;

    let providers = storage.list_providers().await.expect("list providers");
    let models = storage.list_models(None).await.expect("list models");
    let mut selection = Selection::new(
        Some(find_provider(&providers, alpha).clone()),
        Some(find_model(&models, alpha_model).clone()),
    );

    let notice = selection.select_model(&models, beta_model);
    assert!(notice.is_some());
    assert_eq!(
        selection.model.as_ref().map(|model| model.id),
        Some(alpha_model)
    );
    assert_eq!(
        selection.provider.as_ref().map(|provider| provider.id),
        Some(alpha)
    );

    let alpha_models = storage
        .list_models(Some(alpha))
        .await
        .expect("list the provider models");
    let notice = selection.select_model(&alpha_models, alpha_model);
    assert!(notice.is_none());
    assert_eq!(
        selection.model.as_ref().map(|model| model.id),
        Some(alpha_model)
    );
}

#[tokio::test]
async fn switching_models_updates_the_reasoning_effort() {
    let (_dir, storage) = database().await;
    let alpha = provider(&storage, "alpha").await;
    let plain = model(&storage, alpha, "plain", None).await;
    let thinking = model(
        &storage,
        alpha,
        "thinking",
        Some(r#"{"reasoning_effort":"high"}"#),
    )
    .await;

    let providers = storage.list_providers().await.expect("list providers");
    let models = storage.list_models(Some(alpha)).await.expect("list models");
    let mut selection = Selection::new(
        Some(find_provider(&providers, alpha).clone()),
        Some(find_model(&models, plain).clone()),
    );
    assert!(selection.reasoning_effort.is_none());

    let notice = selection.select_model(&models, thinking);
    assert!(notice.is_none());
    assert_eq!(selection.reasoning_effort.as_deref(), Some("high"));

    let notice = selection.select_model(&models, plain);
    assert!(notice.is_none());
    assert!(selection.reasoning_effort.is_none());
}

#[tokio::test]
async fn a_model_selection_only_changes_topics_created_afterwards() {
    let (_dir, storage) = database().await;
    let alpha = provider(&storage, "alpha").await;
    let first = model(&storage, alpha, "first", None).await;
    let second = model(&storage, alpha, "second", None).await;

    let old_topic = storage
        .create_topic(None, "old", Some(first), None, None)
        .await
        .expect("create the old topic");
    let providers = storage.list_providers().await.expect("list providers");
    let models = storage.list_models(Some(alpha)).await.expect("list models");
    let mut selection = Selection::new(
        Some(find_provider(&providers, alpha).clone()),
        Some(find_model(&models, first).clone()),
    );
    assert!(selection.select_model(&models, second).is_none());

    let new_topic = storage
        .create_topic(
            None,
            "new",
            selection.model.as_ref().map(|model| model.id),
            None,
            None,
        )
        .await
        .expect("create the new topic");

    let old: Topic = storage
        .get_topic_by_id(old_topic)
        .await
        .expect("load the old topic")
        .expect("the old topic exists");
    let new: Topic = storage
        .get_topic_by_id(new_topic)
        .await
        .expect("load the new topic")
        .expect("the new topic exists");
    assert_eq!(old.model_id, Some(first));
    assert_eq!(new.model_id, Some(second));
}

#[tokio::test]
async fn reasoning_values_cover_the_base_list_and_the_current_value() {
    let values = Selection::new(None, None).reasoning_values();
    assert_eq!(values, ["low", "medium", "high", "max"]);

    let model: Model = Model {
        id: 1,
        provider_id: 1,
        name: "custom".to_owned(),
        alias: None,
        adapter: AdapterType::OpenAICompletion,
        modalities: None,
        active: true,
        icon: None,
        endpoint: None,
        config: Some(json!({"reasoning_effort": "extreme"})),
        frequency: None,
        created_at: 0,
        updated_at: 0,
    };
    let mut selection = Selection::new(None, Some(model));
    assert_eq!(selection.reasoning_effort.as_deref(), Some("extreme"));
    assert_eq!(
        selection.reasoning_values(),
        ["low", "medium", "high", "max", "extreme"]
    );

    selection.cycle_reasoning();
    assert_eq!(selection.reasoning_effort.as_deref(), Some("low"));
    selection.cycle_reasoning();
    assert_eq!(selection.reasoning_effort.as_deref(), Some("medium"));
    selection.cycle_reasoning_back();
    assert_eq!(selection.reasoning_effort.as_deref(), Some("low"));
    selection.cycle_reasoning_back();
    assert_eq!(selection.reasoning_effort.as_deref(), Some("max"));
}

#[tokio::test]
async fn cycling_reasoning_starts_at_the_first_value() {
    let mut selection = Selection::new(None, None);
    selection.cycle_reasoning();
    assert_eq!(selection.reasoning_effort.as_deref(), Some("low"));
    for _ in 0..4 {
        selection.cycle_reasoning();
    }
    assert_eq!(selection.reasoning_effort.as_deref(), Some("low"));
}
