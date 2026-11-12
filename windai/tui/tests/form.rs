use serde_json::json;
use tempfile::TempDir;
use wind_storage::error::StorageError;
use wind_storage::{Model, Provider, Storage};
use wind_tui::config::{Selection, refresh, save_model, save_provider};
use wind_tui::form::{Form, ModelFields, ModelForm, ProviderFields, ProviderForm};
use wind_wire::AdapterType;

fn provider(id: i64, name: &str) -> Provider {
    Provider {
        id,
        name: name.to_owned(),
        alias: None,
        description: None,
        base_url: format!("https://{name}.example"),
        doc: None,
        active: true,
        created_at: 0,
        updated_at: 0,
    }
}

fn model(id: i64, provider_id: i64, name: &str) -> Model {
    Model {
        id,
        provider_id,
        name: name.to_owned(),
        alias: None,
        adapter: AdapterType::OpenAICompletion,
        modalities: None,
        active: true,
        icon: None,
        endpoint: None,
        config: None,
        frequency: None,
        created_at: 0,
        updated_at: 0,
    }
}

fn model_form(form: &Form) -> &ModelForm {
    match form {
        Form::Model(model) => model,
        Form::Provider(_) => panic!("expected a model form"),
    }
}

async fn database() -> (TempDir, Storage) {
    let dir = TempDir::new().expect("create a temp dir");
    let storage = Storage::open(&dir.path().join("windai.db"))
        .await
        .expect("open the database");
    (dir, storage)
}

#[test]
fn provider_validation_rejects_an_empty_name() {
    let form = ProviderForm::default();
    let error = form.validate(&[]).expect_err("the name is missing");
    assert_eq!(error.0, 0);
    assert!(!error.1.is_empty());
}

#[test]
fn provider_validation_rejects_an_invalid_base_url() {
    let providers = [provider(1, "alpha")];
    let mut form = ProviderForm {
        name: "beta".to_owned(),
        ..ProviderForm::default()
    };
    for base_url in ["", "ftp://beta.example", "beta.example"] {
        form.base_url = base_url.to_owned();
        let error = form
            .validate(&providers)
            .expect_err("the base_url is invalid");
        assert_eq!(error.0, 1, "base_url {base_url}");
        assert!(!error.1.is_empty());
    }
}

#[test]
fn provider_validation_rejects_a_duplicate_name() {
    let providers = [provider(1, "alpha")];
    let form = ProviderForm {
        name: "alpha".to_owned(),
        base_url: "https://alpha.example".to_owned(),
        ..ProviderForm::default()
    };
    let error = form.validate(&providers).expect_err("the name is taken");
    assert_eq!(error.0, 0);
}

#[test]
fn provider_validation_accepts_optional_api_keys() {
    let providers = [provider(1, "alpha")];
    let mut form = ProviderForm {
        name: "  beta  ".to_owned(),
        base_url: " https://beta.example ".to_owned(),
        ..ProviderForm::default()
    };
    let fields = form.validate(&providers).expect("the form is valid");
    assert_eq!(
        fields,
        ProviderFields {
            name: "beta".to_owned(),
            base_url: "https://beta.example".to_owned(),
            api_key: None,
        }
    );

    form.api_key = "  sk-beta  ".to_owned();
    let fields = form.validate(&providers).expect("the form is valid");
    assert_eq!(fields.api_key.as_deref(), Some("sk-beta"));
}

#[test]
fn model_validation_requires_a_provider() {
    let form = ModelForm::default();
    let error = form.validate(&[], &[]).expect_err("there is no provider");
    assert_eq!(error.0, 0);
}

#[test]
fn model_validation_rejects_an_empty_or_duplicate_name() {
    let providers = [provider(1, "alpha")];
    let models = [model(1, 1, "alpha-model")];
    let form = ModelForm::default();
    let error = form
        .validate(&providers, &models)
        .expect_err("the name is missing");
    assert_eq!(error.0, 1);

    let form = ModelForm {
        name: "alpha-model".to_owned(),
        ..ModelForm::default()
    };
    let error = form
        .validate(&providers, &models)
        .expect_err("the name is taken");
    assert_eq!(error.0, 1);
}

#[test]
fn model_validation_cycles_the_adapter_enum() {
    let providers = [provider(1, "alpha")];
    let form = ModelForm {
        name: "alpha-model".to_owned(),
        focus: 2,
        ..ModelForm::default()
    };
    let mut wrapper = Form::Model(form);
    wrapper.move_right(&providers);
    let Form::Model(form) = wrapper else {
        panic!("the form is a model form");
    };
    let fields = form.validate(&providers, &[]).expect("the form is valid");
    assert_eq!(fields.adapter, AdapterType::OpenAIResponse);
}

#[test]
fn model_validation_rejects_an_invalid_endpoint() {
    let providers = [provider(1, "alpha")];
    let form = ModelForm {
        name: "alpha-model".to_owned(),
        endpoint: "ftp://alpha.example".to_owned(),
        ..ModelForm::default()
    };
    let error = form
        .validate(&providers, &[])
        .expect_err("the endpoint is invalid");
    assert_eq!(error.0, 3);
}

#[test]
fn model_validation_accepts_optional_endpoints_and_configs() {
    let providers = [provider(1, "alpha")];
    let mut form = ModelForm {
        name: "alpha-model".to_owned(),
        endpoint: "  https://alpha.example/v1  ".to_owned(),
        config: r#"{"reasoning_effort":"high"}"#.to_owned(),
        ..ModelForm::default()
    };
    let fields = form.validate(&providers, &[]).expect("the form is valid");
    assert_eq!(
        fields,
        ModelFields {
            provider_id: 1,
            name: "alpha-model".to_owned(),
            adapter: AdapterType::OpenAICompletion,
            endpoint: Some("https://alpha.example/v1".to_owned()),
            config: Some(json!({"reasoning_effort": "high"})),
        }
    );

    form.endpoint = String::new();
    form.config = String::new();
    let fields = form.validate(&providers, &[]).expect("the form is valid");
    assert!(fields.endpoint.is_none());
    assert!(fields.config.is_none());
}

#[test]
fn model_validation_rejects_bad_configs() {
    let providers = [provider(1, "alpha")];
    let mut form = ModelForm {
        name: "alpha-model".to_owned(),
        ..ModelForm::default()
    };
    for config in ["{bad", "[]", "\"text\"", "42"] {
        form.config = config.to_owned();
        let error = form
            .validate(&providers, &[])
            .expect_err("the config is invalid");
        assert_eq!(error.0, 4, "config {config}");
    }
}

#[test]
fn the_provider_enum_cycles_between_providers() {
    let providers = [
        provider(1, "alpha"),
        provider(2, "beta"),
        provider(3, "gamma"),
    ];
    let mut form = Form::Model(ModelForm::default());
    form.move_right(&providers);
    assert_eq!(model_form(&form).provider_index, 1);
    form.move_right(&providers);
    assert_eq!(model_form(&form).provider_index, 2);
    form.move_right(&providers);
    assert_eq!(model_form(&form).provider_index, 0);
    form.move_left(&providers);
    assert_eq!(model_form(&form).provider_index, 2);
}

#[test]
fn text_fields_move_the_cursor_and_insert_and_delete() {
    let mut form = ProviderForm::default();
    form.insert_char('a');
    form.insert_char('b');
    form.insert_char('c');
    assert_eq!(form.name, "abc");
    assert_eq!(form.cursor, 3);
    form.move_left();
    form.move_left();
    assert_eq!(form.cursor, 1);
    form.insert_char('X');
    assert_eq!(form.name, "aXbc");
    assert_eq!(form.cursor, 2);
    form.backspace();
    assert_eq!(form.name, "abc");
    assert_eq!(form.cursor, 1);
    form.home();
    assert_eq!(form.cursor, 0);
    form.delete();
    assert_eq!(form.name, "bc");
    form.end();
    assert_eq!(form.cursor, 2);
    form.delete();
    assert_eq!(form.name, "bc");
}

#[test]
fn tab_moves_between_fields_and_lands_at_the_end_of_text() {
    let mut form = Form::Model(ModelForm::default());
    form.insert_char('x');
    assert!(model_form(&form).name.is_empty(), "enum fields ignore text");
    form.next_field();
    form.insert_char('m');
    form.next_field();
    form.insert_char('y');
    assert!(
        model_form(&form).config.is_empty(),
        "enum fields ignore text"
    );
    form.previous_field();
    assert!(
        model_form(&form).endpoint.is_empty(),
        "enum fields ignore text"
    );
    form.next_field();
    form.next_field();
    form.next_field();
    form.insert_char('c');
    assert!(model_form(&form).endpoint.is_empty());
    assert_eq!(model_form(&form).name, "m");
    assert_eq!(model_form(&form).config, "c");
}

#[tokio::test]
async fn saving_a_provider_persists_the_row_and_the_credential() {
    let (_dir, storage) = database().await;
    let provider_id = save_provider(
        &storage,
        ProviderFields {
            name: "alpha".to_owned(),
            base_url: "https://alpha.example".to_owned(),
            api_key: Some("sk-alpha".to_owned()),
        },
    )
    .await
    .expect("save the provider");

    let providers = storage.list_providers().await.expect("list providers");
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0].id, provider_id);
    assert_eq!(providers[0].name, "alpha");
    assert_eq!(providers[0].base_url, "https://alpha.example");

    let model_id = storage
        .create_model(
            provider_id,
            "alpha-model",
            AdapterType::OpenAICompletion,
            None,
            None,
        )
        .await
        .expect("create the model");
    let config = storage
        .get_model_config(model_id)
        .await
        .expect("read the model config")
        .expect("the model config exists");
    assert_eq!(config.api_key.as_deref(), Some("sk-alpha"));
}

#[tokio::test]
async fn saving_a_provider_without_a_key_creates_no_credential() {
    let (_dir, storage) = database().await;
    let provider_id = save_provider(
        &storage,
        ProviderFields {
            name: "alpha".to_owned(),
            base_url: "https://alpha.example".to_owned(),
            api_key: None,
        },
    )
    .await
    .expect("save the provider");
    let model_id = storage
        .create_model(
            provider_id,
            "alpha-model",
            AdapterType::OpenAICompletion,
            None,
            None,
        )
        .await
        .expect("create the model");
    let config = storage
        .get_model_config(model_id)
        .await
        .expect("read the model config")
        .expect("the model config exists");
    assert!(config.api_key.is_none());
}

#[tokio::test]
async fn saving_a_model_persists_the_json_config() {
    let (_dir, storage) = database().await;
    let provider_id = storage
        .create_provider("alpha", "https://alpha.example")
        .await
        .expect("create the provider");
    let model_id = save_model(
        &storage,
        ModelFields {
            provider_id,
            name: "alpha-model".to_owned(),
            adapter: AdapterType::OpenAIResponse,
            endpoint: Some("https://alpha.example/v1".to_owned()),
            config: Some(json!({"reasoning_effort": "max"})),
        },
    )
    .await
    .expect("save the model");

    let models = storage
        .list_models(Some(provider_id))
        .await
        .expect("list the models");
    let saved = models
        .iter()
        .find(|model| model.id == model_id)
        .expect("the saved model exists");
    assert_eq!(saved.name, "alpha-model");
    assert_eq!(saved.adapter, AdapterType::OpenAIResponse);
    assert_eq!(saved.endpoint.as_deref(), Some("https://alpha.example/v1"));
    assert_eq!(saved.config, Some(json!({"reasoning_effort": "max"})));
}

#[tokio::test]
async fn saving_moves_the_selection_to_the_new_records() {
    let (_dir, storage) = database().await;
    let old_provider = storage
        .create_provider("old", "https://old.example")
        .await
        .expect("create the old provider");
    let old_model = storage
        .create_model(
            old_provider,
            "old-model",
            AdapterType::OpenAICompletion,
            None,
            None,
        )
        .await
        .expect("create the old model");
    let providers = storage.list_providers().await.expect("list providers");
    let models = storage.list_models(None).await.expect("list models");
    let mut selection = Selection::new(
        providers
            .iter()
            .find(|provider| provider.id == old_provider)
            .cloned(),
        models.iter().find(|model| model.id == old_model).cloned(),
    );

    let new_provider = save_provider(
        &storage,
        ProviderFields {
            name: "new".to_owned(),
            base_url: "https://new.example".to_owned(),
            api_key: None,
        },
    )
    .await
    .expect("save the provider");
    let first = save_model(
        &storage,
        ModelFields {
            provider_id: new_provider,
            name: "first".to_owned(),
            adapter: AdapterType::OpenAICompletion,
            endpoint: None,
            config: None,
        },
    )
    .await
    .expect("save the first model");
    let second = save_model(
        &storage,
        ModelFields {
            provider_id: new_provider,
            name: "second".to_owned(),
            adapter: AdapterType::OpenAICompletion,
            endpoint: None,
            config: None,
        },
    )
    .await
    .expect("save the second model");

    let providers = storage.list_providers().await.expect("list providers");
    let models = storage
        .list_models(Some(new_provider))
        .await
        .expect("list the provider models");
    let notice = selection.select_provider(&providers, &models, new_provider);
    assert!(notice.is_none());
    assert_eq!(
        selection.provider.as_ref().map(|provider| provider.id),
        Some(new_provider)
    );
    assert_eq!(selection.model.as_ref().map(|model| model.id), Some(first));

    let notice = selection.select_model(&models, second);
    assert!(notice.is_none());
    assert_eq!(selection.model.as_ref().map(|model| model.id), Some(second));
}

#[tokio::test]
async fn a_failed_save_leaves_the_selection_and_the_form_unchanged() {
    let (_dir, storage) = database().await;
    let provider_id = storage
        .create_provider("alpha", "https://alpha.example")
        .await
        .expect("create the provider");
    let providers = storage.list_providers().await.expect("list providers");
    let selection = Selection::new(providers.first().cloned(), None);
    let mut form = ProviderForm {
        name: "alpha".to_owned(),
        base_url: "https://alpha.example".to_owned(),
        ..ProviderForm::default()
    };
    let before = form.clone();

    let result = save_provider(
        &storage,
        ProviderFields {
            name: "alpha".to_owned(),
            base_url: "https://alpha.example".to_owned(),
            api_key: Some("sk-duplicate".to_owned()),
        },
    )
    .await;
    assert!(result.is_err());
    form.error = Some("the provider already exists".to_owned());
    assert_eq!(form.name, before.name);
    assert_eq!(form.base_url, before.base_url);
    assert_eq!(form.api_key, before.api_key);
    assert!(form.error.is_some());
    assert_eq!(
        selection.provider.as_ref().map(|provider| provider.id),
        Some(provider_id)
    );
}

#[tokio::test]
async fn a_failed_transaction_rolls_back_the_provider_row() {
    let (_dir, storage) = database().await;
    let result: Result<(), StorageError> = storage
        .with_tx(|tx| async move {
            let provider_id = tx
                .create_provider("atomic", "https://atomic.example")
                .await?;
            tx.create_credential(provider_id, "sk-atomic").await?;
            Err(StorageError::Internal("forced failure".to_owned()))
        })
        .await;
    assert!(result.is_err());
    let providers = storage.list_providers().await.expect("list providers");
    assert!(
        providers.iter().all(|provider| provider.name != "atomic"),
        "the rolled-back provider must not exist"
    );
}

#[tokio::test]
async fn refresh_falls_back_to_the_first_provider_and_model() {
    let (_dir, storage) = database().await;
    let provider_id = storage
        .create_provider("alpha", "https://alpha.example")
        .await
        .expect("create the provider");
    let model_id = storage
        .create_model(
            provider_id,
            "alpha-model",
            AdapterType::OpenAICompletion,
            None,
            None,
        )
        .await
        .expect("create the model");
    let stale = Model {
        id: 999,
        provider_id: 999,
        name: "stale".to_owned(),
        alias: None,
        adapter: AdapterType::OpenAICompletion,
        modalities: None,
        active: true,
        icon: None,
        endpoint: None,
        config: None,
        frequency: None,
        created_at: 0,
        updated_at: 0,
    };
    let mut selection = Selection::new(None, Some(stale));

    refresh(&storage, &mut selection)
        .await
        .expect("refresh the selection");
    assert_eq!(
        selection.provider.as_ref().map(|provider| provider.id),
        Some(provider_id)
    );
    assert_eq!(
        selection.model.as_ref().map(|model| model.id),
        Some(model_id)
    );
}
