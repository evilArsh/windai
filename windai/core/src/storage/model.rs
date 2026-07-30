use super::{
    executor::StorageExecutor,
    utils::{self, ensure_affected},
};
use crate::{
    delete_by_id,
    error::{CoreError, Result},
    get_by_id, insert,
    models::{CreateModel, Model, UpdateModel},
    select_fields,
    storage::{TableName, next_id},
    update,
};
#[derive(Clone)]
pub struct ModelStorage {
    executor: StorageExecutor,
}

impl ModelStorage {
    pub(crate) fn new(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    pub async fn create(&self, data: CreateModel) -> Result<Model> {
        if data.name.is_empty() {
            return Err(CoreError::Validation("model name cannot be empty".into()));
        }
        let id = next_id();
        let now = crate::storage::now_ts();
        let active = data.active.unwrap_or(true);
        let modalities = utils::vec_to_str_default(data.modalities.as_deref())?;
        let mut qb = insert!(
            TableName::MODELS,
            ("id", id),
            ("name", data.name.clone()),
            ("provider_id", data.provider_id),
            ("alias", data.alias.clone()),
            ("adapter", data.adapter.to_string()),
            ("modalities", modalities),
            ("active", active),
            ("icon", data.icon.clone()),
            ("endpoint", data.endpoint.clone()),
            ("created_at", now),
        );
        self.executor.execute(qb.build()).await?;
        Ok(Model {
            id,
            name: data.name,
            provider_id: data.provider_id,
            alias: data.alias,
            adapter: data.adapter,
            modalities: data.modalities,
            active,
            icon: data.icon,
            endpoint: data.endpoint,
            frequency: Some(0),
            created_at: now,
        })
    }

    pub async fn update(&self, id: i64, data: UpdateModel) -> Result<()> {
        let mut qb = update!(
            TableName::MODELS,
            id,
            ("name", Some(data.name)),
            ("alias", data.alias),
            ("adapter", data.adapter.map(|a| a.to_string())),
            (
                "modalities",
                utils::vec_to_str_optional(data.modalities.as_deref())?
            ),
            ("active", data.active),
            ("icon", data.icon),
            ("endpoint", data.endpoint),
            ("frequency", data.frequency),
        );
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!(TableName::MODELS, id);
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn get(&self, id: i64) -> Result<Option<Model>> {
        let mut qb = get_by_id!(
            TableName::MODELS,
            id,
            (
                "id",
                "name",
                "provider_id",
                "alias",
                "adapter",
                "modalities",
                "active",
                "icon",
                "endpoint",
                "frequency",
                "created_at",
            )
        );
        let row = self
            .executor
            .fetch_optional(qb.build_query_as::<Model>())
            .await?;

        Ok(row)
    }

    pub async fn list_by_provider(&self) -> Result<Vec<Model>> {
        let mut qb = select_fields!(
            TableName::MODELS,
            (
                "id",
                "name",
                "provider_id",
                "alias",
                "adapter",
                "modalities",
                "active",
                "icon",
                "endpoint",
                "frequency",
                "created_at",
            )
        );
        qb.push(" ORDER BY id DESC ");
        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<Model>())
            .await?;

        Ok(rows)
    }
}
