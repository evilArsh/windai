use crate::{
    db::DbDriver,
    delete_by_id,
    error::{CoreError, Result},
    get_by_id, insert,
    models::agent::{CreatePromptModule, PromptModule, UpdatePromptModule},
    select_fields,
    storage::{TableName, next_id},
    update,
};
use sqlx::QueryBuilder;

use super::{executor::StorageExecutor, now_ts, utils::ensure_affected};

#[derive(Clone)]
pub struct PromptStorage {
    executor: StorageExecutor,
}

impl PromptStorage {
    pub(crate) fn new(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    pub async fn create(&self, data: CreatePromptModule) -> Result<PromptModule> {
        if data.alias.trim().is_empty() {
            return Err(CoreError::Validation("prompt name cannot be empty".into()));
        }

        let id = next_id();
        let now = now_ts();
        let active = data.active.unwrap_or(true);
        let mut qb = insert!(
            TableName::PROMPT_MODULES,
            ("id", id),
            ("name", data.alias.clone()),
            ("description", data.description.clone()),
            ("content", data.content.clone()),
            ("active", active),
            ("created_at", now)
        );
        self.executor.execute(qb.build()).await?;

        Ok(PromptModule {
            id,
            alias: data.alias,
            description: data.description,
            content: data.content,
            active,
            created_at: now,
        })
    }

    pub async fn update(&self, id: i64, data: UpdatePromptModule) -> Result<()> {
        let mut qb = update!(
            TableName::PROMPT_MODULES,
            id,
            ("name", data.alias),
            ("description", data.description),
            ("content", data.content),
            ("active", data.active)
        );
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!(TableName::PROMPT_MODULES, id);
        self.executor.execute(qb.build()).await?;
        Ok(())
    }

    pub async fn get(&self, id: i64) -> Result<Option<PromptModule>> {
        let row = self
            .executor
            .fetch_optional(
                get_by_id!(TableName::PROMPT_MODULES, id).build_query_as::<PromptModule>(),
            )
            .await?;
        Ok(row)
    }

    pub async fn batch_get(&self, ids: &[i64]) -> Result<Vec<PromptModule>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = Self::common_select();
        qb.push(" WHERE id IN (");
        let mut separated = qb.separated(", ");
        for id in ids {
            separated.push_bind(*id);
        }
        separated.push_unseparated(") ");
        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<PromptModule>())
            .await?;
        Ok(rows)
    }

    pub async fn list(&self) -> Result<Vec<PromptModule>> {
        let rows = self
            .executor
            .fetch_all(
                Self::common_select()
                    .push(" ORDER BY id DESC ")
                    .build_query_as::<PromptModule>(),
            )
            .await?;
        Ok(rows)
    }

    fn common_select<'a>() -> QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::PROMPT_MODULES,
            (
                "id",
                "name",
                "description",
                "content",
                "active",
                "created_at"
            )
        )
    }
}
