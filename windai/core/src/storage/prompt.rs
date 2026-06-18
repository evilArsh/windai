use crate::{
    db::DbPool,
    delete_by_id,
    error::{CoreError, Result},
    get_by_id, insert,
    models::agent::{CreatePromptModule, PromptModule, UpdatePromptModule},
    select_fields,
    storage::next_id,
    update,
};

use super::utils::{self, ensure_affected};

pub struct PromptStorage {
    db: DbPool,
}

impl PromptStorage {
    pub fn new(db: DbPool) -> Self {
        Self { db }
    }

    pub async fn create(&self, data: CreatePromptModule) -> Result<i64> {
        if data.key.trim().is_empty() {
            return Err(CoreError::Validation("prompt key cannot be empty".into()));
        }
        if data.name.trim().is_empty() {
            return Err(CoreError::Validation("prompt name cannot be empty".into()));
        }

        let id = next_id();
        insert!(
            "prompt_modules",
            ("id", id),
            ("key", data.key),
            ("name", data.name),
            ("description", data.description),
            ("module_type", data.module_type.to_string()),
            ("content", data.content),
            ("active", data.active.unwrap_or(true)),
            ("data", utils::map_to_str_default(Some(&data.data))?)
        )
        .build()
        .execute(&self.db)
        .await?;

        Ok(id)
    }

    pub async fn update(&self, id: i64, data: UpdatePromptModule) -> Result<()> {
        let mut qb = update!(
            "prompt_modules",
            id,
            ("key", data.key),
            ("name", data.name),
            ("description", data.description),
            ("module_type", data.module_type.map(|v| v.to_string())),
            ("content", data.content),
            ("active", data.active),
            ("data", utils::map_to_str_optional(data.data.as_ref())?)
        );
        ensure_affected(&qb.build().execute(&self.db).await?)?;
        Ok(())
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!("prompt_modules", id);
        qb.build().execute(&self.db).await?;
        Ok(())
    }

    pub async fn get(&self, id: i64) -> Result<Option<PromptModule>> {
        let row = get_by_id!("prompt_modules", id)
            .build_query_as::<PromptModule>()
            .fetch_optional(&self.db)
            .await?;
        Ok(row)
    }

    pub async fn get_by_key(&self, key: &str) -> Result<Option<PromptModule>> {
        let row = select_fields!(
            "prompt_modules",
            (
                "id",
                "key",
                "name",
                "description",
                "module_type",
                "content",
                "active",
                "data",
                "created_at"
            )
        )
        .push(" WHERE key = ")
        .push_bind(key)
        .build_query_as::<PromptModule>()
        .fetch_optional(&self.db)
        .await?;
        Ok(row)
    }

    pub async fn list(&self) -> Result<Vec<PromptModule>> {
        let rows = select_fields!(
            "prompt_modules",
            (
                "id",
                "key",
                "name",
                "description",
                "module_type",
                "content",
                "active",
                "data",
                "created_at"
            )
        )
        .push(" ORDER BY id DESC ")
        .build_query_as::<PromptModule>()
        .fetch_all(&self.db)
        .await?;
        Ok(rows)
    }
}
