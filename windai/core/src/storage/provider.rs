use wind_ai::model::AdapterType;

use super::{executor::StorageExecutor, now_ts, utils::ensure_affected};
use crate::{
    delete_by_id,
    error::{CoreError, Result},
    get_by_id, insert,
    models::{
        CreateCredentials, CreateJsonRule, CreateProvider, Credentials, JsonRule, Provider,
        UpdateJsonRule, UpdateProvider,
    },
    select_fields,
    storage::{TableName, next_id},
    update,
};
use sqlx::QueryBuilder;
#[derive(Clone)]
pub struct ProviderStorage {
    executor: StorageExecutor,
}

impl ProviderStorage {
    pub(crate) fn new(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    async fn delete_by_column(
        executor: &StorageExecutor,
        table: &str,
        column: &str,
        value: i64,
    ) -> Result<()> {
        let mut qb = QueryBuilder::new("DELETE FROM ");
        qb.push(table)
            .push(" WHERE ")
            .push(column)
            .push(" = ")
            .push_bind(value);
        executor.execute(qb.build()).await?;
        Ok(())
    }

    pub async fn get_by_name(&self, name: &str) -> Result<Option<Provider>> {
        let mut qb = select_fields!(
            TableName::PROVIDERS,
            (
                "id",
                "name",
                "alias",
                "description",
                "base_url",
                "doc",
                "active",
                "created_at"
            )
        );

        let row = self
            .executor
            .fetch_optional(
                qb.push(" WHERE name = ")
                    .push_bind(name)
                    .build_query_as::<Provider>(),
            )
            .await?;

        Ok(row)
    }

    /// 创建提供商，提供商名称必须唯一
    pub async fn create(&self, data: CreateProvider) -> Result<Provider> {
        self.executor
            .transaction_required(
                |executor| async move { Self::new(executor).create_inner(data).await },
            )
            .await
    }

    async fn create_inner(&self, data: CreateProvider) -> Result<Provider> {
        if data.name.is_empty() {
            return Err(CoreError::Validation(
                "provider name cannot be empty".into(),
            ));
        }
        if self.get_by_name(data.name.trim()).await?.is_some() {
            return Err(CoreError::Validation("provider name already exists".into()));
        }

        let id = next_id();
        let now = now_ts();
        let mut qb = insert!(
            TableName::PROVIDERS,
            ("id", id),
            ("name", data.name.clone()),
            ("alias", data.alias.clone()),
            ("description", data.description.clone()),
            ("base_url", data.base_url.clone()),
            ("doc", data.doc.clone()),
            ("active", true),
            ("created_at", now),
        );
        self.executor.execute(qb.build()).await?;
        Ok(Provider {
            id,
            name: data.name,
            base_url: data.base_url,
            description: data.description,
            doc: data.doc,
            alias: data.alias,
            active: true,
            created_at: now,
        })
    }

    pub async fn update(&self, id: i64, data: UpdateProvider) -> Result<()> {
        self.executor
            .transaction_required(|executor| async move {
                Self::new(executor).update_inner(id, data).await
            })
            .await
    }

    async fn update_inner(&self, id: i64, data: UpdateProvider) -> Result<()> {
        if let Some(name) = &data.name {
            if name.is_empty() {
                return Err(CoreError::Validation("name cannot be empty".to_string()));
            }
            if let Some(exists) = self.get_by_name(&name.trim()).await?
                && exists.id != id
            {
                return Err(CoreError::Validation("provider name already exists".into()));
            }
        }

        let mut qb = update!(
            TableName::PROVIDERS,
            id,
            ("name", data.name),
            ("alias", data.alias),
            ("description", data.description),
            ("base_url", data.base_url),
            ("doc", data.doc),
            ("active", data.active),
        );
        self.executor.execute(qb.build()).await?;
        Ok(())
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        self.executor
            .transaction_required(|executor| async move {
                Self::delete_by_column(&executor, TableName::PROVIDERS, "id", id).await?;
                Self::delete_by_column(&executor, "credentials", "provider_id", id).await?;
                Self::delete_by_column(&executor, "json_rule", "provider_id", id).await?;
                Ok(())
            })
            .await
    }

    pub async fn get(&self, id: i64) -> Result<Option<Provider>> {
        let mut qb = get_by_id!(
            TableName::PROVIDERS,
            id,
            (
                "id",
                "name",
                "alias",
                "description",
                "base_url",
                "doc",
                "active",
                "created_at"
            )
        );
        let row = self
            .executor
            .fetch_optional(qb.build_query_as::<Provider>())
            .await?;

        Ok(row)
    }

    pub async fn list_all(&self) -> Result<Vec<Provider>> {
        let mut qb = select_fields!(
            TableName::PROVIDERS,
            (
                "id",
                "name",
                "alias",
                "description",
                "base_url",
                "doc",
                "active",
                "created_at"
            )
        );
        qb.push(" ORDER BY id DESC ");
        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<Provider>())
            .await?;

        Ok(rows)
    }

    // --- Credentials ---

    /// 创建一条提供商凭证
    pub async fn create_credentials(&self, data: CreateCredentials) -> Result<Credentials> {
        let id = next_id();
        let now = now_ts();
        let mut qb = insert!(
            "credentials",
            ("id", id),
            ("provider_id", data.provider_id),
            ("key", data.key.clone()),
            ("active", true),
            ("created_at", now),
        );
        self.executor.execute(qb.build()).await?;
        Ok(Credentials {
            id,
            provider_id: data.provider_id,
            key: data.key,
            active: true,
            created_at: now,
        })
    }

    pub async fn get_provider_credentials(&self, provider_id: i64) -> Result<Vec<Credentials>> {
        let mut qb = select_fields!(
            "credentials",
            ("id", "provider_id", "key", "active", "created_at")
        );
        qb.push(" WHERE provider_id = ")
            .push_bind(provider_id)
            .push(" ORDER BY active DESC ");

        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<Credentials>())
            .await?;

        Ok(rows)
    }

    pub async fn delete_credentials(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!("credentials", id);
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn create_json_rule(&self, data: CreateJsonRule) -> Result<JsonRule> {
        let id = next_id();
        let now = now_ts();
        let mut qb = insert!(
            "json_rule",
            ("id", id),
            ("provider_id", data.provider_id),
            ("adapter", data.adapter.to_string()),
            ("active", true),
            ("json_rule", data.json_rule.clone()),
            ("created_at", now),
        );
        self.executor.execute(qb.build()).await?;
        Ok(JsonRule {
            id,
            provider_id: data.provider_id,
            adapter: data.adapter,
            json_rule: data.json_rule,
            active: true,
            created_at: now,
        })
    }

    pub async fn update_json_rule(&self, id: i64, data: UpdateJsonRule) -> Result<()> {
        let mut qb = update!(
            "json_rule",
            id,
            ("provider_id", data.provider_id),
            ("adapter", data.adapter.map(|a| a.to_string())),
            ("active", data.active),
            ("json_rule", data.json_rule),
        );
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn list_json_rules(&self, provider_id: i64) -> Result<Vec<JsonRule>> {
        let mut qb = select_fields!(
            "json_rule",
            (
                "id",
                "provider_id",
                "adapter",
                "json_rule",
                "active",
                "created_at"
            )
        );
        let row = self
            .executor
            .fetch_all(
                qb.push(" WHERE provider_id = ")
                    .push_bind(provider_id)
                    .push(" ORDER BY id DESC")
                    .build_query_as::<JsonRule>(),
            )
            .await?;

        Ok(row)
    }

    /// 通过 provider id 和 adapter 类型获取 json_rule
    pub async fn get_json_rule(
        &self,
        provider_id: i64,
        adapter: AdapterType,
    ) -> Result<Option<JsonRule>> {
        let mut qb = select_fields!(
            "json_rule",
            (
                "id",
                "provider_id",
                "adapter",
                "json_rule",
                "active",
                "created_at"
            )
        );
        let row = self
            .executor
            .fetch_optional(
                qb.push(" WHERE provider_id = ")
                    .push_bind(provider_id)
                    .push(" AND adapter = ")
                    .push_bind(adapter.to_string())
                    .build_query_as::<JsonRule>(),
            )
            .await?;

        Ok(row)
    }

    pub async fn get_json_rule_by_id(&self, id: i64) -> Result<Option<JsonRule>> {
        let mut qb = get_by_id!(
            "json_rule",
            id,
            (
                "id",
                "provider_id",
                "adapter",
                "json_rule",
                "active",
                "created_at"
            )
        );
        let row = self
            .executor
            .fetch_optional(qb.build_query_as::<JsonRule>())
            .await?;

        Ok(row)
    }

    pub async fn delete_json_rule(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!("json_rule", id);
        ensure_affected(self.executor.execute(qb.build()).await?)
    }
}
