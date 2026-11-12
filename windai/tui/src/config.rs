use crate::form::{ModelFields, ProviderFields};
use serde_json::Value;
use wind_storage::error::StorageError;
use wind_storage::{Model, Provider, Storage};

const REASONING_VALUES: [&str; 4] = ["low", "medium", "high", "max"];

#[derive(Debug, Clone, PartialEq)]
pub struct Selection {
    pub provider: Option<Provider>,
    pub model: Option<Model>,
    pub reasoning_effort: Option<String>,
    pub stream: Option<bool>,
}

impl Selection {
    pub fn new(provider: Option<Provider>, model: Option<Model>) -> Self {
        let reasoning_effort = model.as_ref().and_then(reasoning_effort);
        let stream = model.as_ref().and_then(stream_enabled);
        Self {
            provider,
            model,
            reasoning_effort,
            stream,
        }
    }

    pub fn stream_label(&self) -> &'static str {
        match self.stream {
            Some(true) => "true",
            Some(false) => "false",
            None => "-",
        }
    }

    pub fn select_provider(
        &mut self,
        providers: &[Provider],
        models: &[Model],
        provider_id: i64,
    ) -> Option<String> {
        let provider = providers
            .iter()
            .find(|provider| provider.id == provider_id)?
            .clone();
        let model = models
            .iter()
            .filter(|model| model.provider_id == provider_id)
            .min_by_key(|model| model.id)
            .cloned();
        self.provider = Some(provider);
        self.model = model.clone();
        self.reasoning_effort = model.as_ref().and_then(reasoning_effort);
        self.stream = model.as_ref().and_then(stream_enabled);
        match model {
            Some(_) => None,
            None => Some("this provider has no models".to_owned()),
        }
    }

    pub fn select_model(&mut self, models: &[Model], model_id: i64) -> Option<String> {
        let Some(provider) = self.provider.as_ref() else {
            return Some("no provider selected".to_owned());
        };
        let Some(model) = models
            .iter()
            .find(|model| model.id == model_id && model.provider_id == provider.id)
            .cloned()
        else {
            return Some("unknown model".to_owned());
        };
        self.reasoning_effort = reasoning_effort(&model);
        self.stream = stream_enabled(&model);
        self.model = Some(model);
        None
    }

    pub fn reasoning_values(&self) -> Vec<String> {
        let mut values: Vec<String> = REASONING_VALUES
            .iter()
            .map(|value| (*value).to_owned())
            .collect();
        if let Some(current) = &self.reasoning_effort
            && !values.iter().any(|value| value == current)
        {
            values.push(current.clone());
        }
        values
    }

    pub fn cycle_reasoning(&mut self) {
        self.cycle_reasoning_by(1);
    }

    pub fn cycle_reasoning_back(&mut self) {
        self.cycle_reasoning_by(-1);
    }

    pub fn refresh(&mut self, providers: &[Provider], models: &[Model]) -> Option<String> {
        let provider = self
            .provider
            .as_ref()
            .and_then(|current| {
                providers
                    .iter()
                    .find(|provider| provider.id == current.id)
                    .cloned()
            })
            .or_else(|| providers.first().cloned());
        let Some(provider) = provider else {
            *self = Selection::new(None, None);
            return None;
        };
        let model = self
            .model
            .as_ref()
            .and_then(|current| {
                models
                    .iter()
                    .find(|model| model.id == current.id && model.provider_id == provider.id)
                    .cloned()
            })
            .or_else(|| {
                models
                    .iter()
                    .filter(|model| model.provider_id == provider.id)
                    .min_by_key(|model| model.id)
                    .cloned()
            });
        let missing = model.is_none();
        self.reasoning_effort = model.as_ref().and_then(reasoning_effort);
        self.stream = model.as_ref().and_then(stream_enabled);
        self.provider = Some(provider);
        self.model = model;
        missing.then(|| "this provider has no models".to_owned())
    }

    fn cycle_reasoning_by(&mut self, step: isize) {
        let values = self.reasoning_values();
        if values.is_empty() {
            return;
        }
        let current = self
            .reasoning_effort
            .as_ref()
            .and_then(|current| values.iter().position(|value| value == current));
        let index = match current {
            Some(index) => (index as isize + step).rem_euclid(values.len() as isize) as usize,
            None => 0,
        };
        self.reasoning_effort = Some(values[index].clone());
    }
}

fn reasoning_effort(model: &Model) -> Option<String> {
    model
        .config
        .as_ref()?
        .get("reasoning_effort")?
        .as_str()
        .map(str::to_owned)
}

fn stream_enabled(model: &Model) -> Option<bool> {
    model.config.as_ref()?.get("stream")?.as_bool()
}

pub async fn save_provider(storage: &Storage, fields: ProviderFields) -> Result<i64, StorageError> {
    storage
        .with_tx(|tx| async move {
            let provider_id = tx.create_provider(&fields.name, &fields.base_url).await?;
            if let Some(api_key) = fields.api_key.as_deref() {
                tx.create_credential(provider_id, api_key).await?;
            }
            Ok(provider_id)
        })
        .await
}

pub async fn save_model(storage: &Storage, fields: ModelFields) -> Result<i64, StorageError> {
    storage
        .create_model(
            fields.provider_id,
            &fields.name,
            fields.adapter,
            fields.endpoint.as_deref(),
            fields.config.as_ref(),
        )
        .await
}

pub async fn update_provider(
    storage: &Storage,
    provider_id: i64,
    fields: ProviderFields,
) -> Result<i64, StorageError> {
    storage
        .with_tx(|tx| async move {
            tx.update_provider(provider_id, &fields.name, &fields.base_url)
                .await?;
            if let Some(api_key) = fields.api_key.as_deref() {
                tx.replace_credential(provider_id, api_key).await?;
            }
            Ok(provider_id)
        })
        .await
}

pub async fn update_model(
    storage: &Storage,
    model_id: i64,
    fields: ModelFields,
) -> Result<i64, StorageError> {
    storage
        .update_model(
            model_id,
            &fields.name,
            fields.adapter,
            fields.endpoint.as_deref(),
            fields.config.as_ref(),
        )
        .await?;
    Ok(model_id)
}

pub async fn update_reasoning(
    storage: &Storage,
    model: &Model,
    level: &str,
) -> Result<(), StorageError> {
    update_model_setting(
        storage,
        model,
        "reasoning_effort",
        Some(Value::String(level.to_owned())),
    )
    .await
}

pub async fn update_stream(storage: &Storage, model: &Model, on: bool) -> Result<(), StorageError> {
    update_model_setting(storage, model, "stream", Some(Value::Bool(on))).await
}

pub async fn update_model_setting(
    storage: &Storage,
    model: &Model,
    key: &str,
    value: Option<Value>,
) -> Result<(), StorageError> {
    let mut config = model
        .config
        .as_ref()
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    match value {
        Some(value) => config.insert(key.to_owned(), value),
        None => config.remove(key),
    };
    storage
        .update_model_config(model.id, Some(&Value::Object(config)))
        .await
}

pub async fn refresh(storage: &Storage, selection: &mut Selection) -> Result<(), StorageError> {
    let providers = storage.list_providers().await?;
    let provider = selection
        .provider
        .as_ref()
        .and_then(|current| {
            providers
                .iter()
                .find(|provider| provider.id == current.id)
                .cloned()
        })
        .or_else(|| providers.first().cloned());
    let Some(provider) = provider else {
        *selection = Selection::new(None, None);
        return Ok(());
    };
    let models = storage.list_models(Some(provider.id)).await?;
    selection.refresh(&providers, &models);
    Ok(())
}
