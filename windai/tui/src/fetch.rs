use wind_storage::Storage;
use wind_storage::error::StorageError;

use crate::config::Selection;

pub struct Startup {
    pub selection: Selection,
    pub notice: Option<String>,
}

pub async fn load_startup(storage: &Storage) -> Result<Startup, StorageError> {
    let providers = storage.list_providers().await?;
    let Some(first_provider) = providers.first() else {
        return Ok(Startup {
            selection: Selection::new(None, None),
            notice: Some("no providers configured".to_owned()),
        });
    };
    let models = storage.list_models(None).await?;
    let topics = storage.list_topics().await?;
    let latest = topics
        .first()
        .and_then(|topic| topic.model_id)
        .and_then(|model_id| models.iter().find(|model| model.id == model_id));
    if let Some(model) = latest
        && let Some(provider) = providers
            .iter()
            .find(|provider| provider.id == model.provider_id)
    {
        return Ok(Startup {
            selection: Selection::new(Some(provider.clone()), Some(model.clone())),
            notice: None,
        });
    }
    let model = models
        .iter()
        .find(|model| model.provider_id == first_provider.id)
        .cloned();
    Ok(Startup {
        selection: Selection::new(Some(first_provider.clone()), model),
        notice: None,
    })
}
