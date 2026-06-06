use super::utils;
use crate::{
    db::DbPool,
    delete_by_id,
    error::{CoreError, Result},
    get_by_id, insert,
    models::{CreateModel, Model, UpdateModel},
    select_fields,
    storage::next_id,
    update,
};
pub struct ModelStorage {
    db: DbPool,
}

impl ModelStorage {
    pub fn new(db: DbPool) -> Self {
        Self { db }
    }

    pub async fn create(&self, data: CreateModel) -> Result<i64> {
        if data.name.is_empty() {
            return Err(CoreError::Validation("model name cannot be empty".into()));
        }
        let id = next_id();
        let mut qb = insert!(
            "models",
            ("id", id),
            ("name", data.name),
            ("provider_id", data.provider_id),
            ("alias", data.alias),
            ("adaptor", data.adaptor.to_string()),
            (
                "modalities",
                utils::vec_to_str_default(data.modalities.as_deref())?
            ),
            ("active", data.active),
            ("icon", data.icon),
            ("endpoint", data.endpoint),
        );
        qb.build().execute(&self.db).await?;
        Ok(id)
    }

    pub async fn update(&self, id: i64, data: UpdateModel) -> Result<()> {
        let mut qb = update!(
            "models",
            id,
            ("name", Some(data.name)),
            ("alias", data.alias),
            ("adaptor", data.adaptor.map(|a| a.to_string())),
            (
                "modalities",
                utils::vec_to_str_optional(data.modalities.as_deref())?
            ),
            ("active", data.active),
            ("icon", data.icon),
            ("endpoint", data.endpoint),
            ("frequency", data.frequency),
        );
        qb.build().execute(&self.db).await?;
        Ok(())
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!("models", id);
        qb.build().execute(&self.db).await?;
        Ok(())
    }

    pub async fn get(&self, id: i64) -> Result<Option<Model>> {
        let mut qb = get_by_id!(
            "models",
            id,
            (
                "id",
                "name",
                "provider_id",
                "alias",
                "adaptor",
                "modalities",
                "active",
                "icon",
                "endpoint",
                "frequency",
                "created_at",
            )
        );
        let row = qb
            .build_query_as::<Model>()
            .fetch_optional(&self.db)
            .await?;

        Ok(row)
    }

    pub async fn list_by_provider(&self) -> Result<Vec<Model>> {
        let mut qb = select_fields!(
            "models",
            (
                "id",
                "name",
                "provider_id",
                "alias",
                "adaptor",
                "modalities",
                "active",
                "icon",
                "endpoint",
                "frequency",
                "created_at",
            )
        );
        qb.push(" ORDER BY id DESC ");
        let rows = qb.build_query_as::<Model>().fetch_all(&self.db).await?;

        Ok(rows)
    }
}
