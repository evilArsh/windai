use wind_ai::model::AdapterType;

use crate::{
    db::{DbDriver, DbPool},
    delete_by_id,
    error::{CoreError, Result},
    get_by_id, insert,
    models::{
        CreateCredentials, CreateJsonRule, CreateProvider, Credentials, JsonRule, Provider,
        UpdateJsonRule, UpdateProvider,
    },
    select_fields,
    storage::next_id,
    update,
};

use super::utils::ensure_affected;
pub struct ProviderStorage {
    db: DbPool,
}

impl ProviderStorage {
    pub fn new(db: DbPool) -> Self {
        Self { db }
    }

    async fn get_by_name_inner<'e, E>(executor: E, name: &str) -> Result<Option<Provider>>
    where
        E: sqlx::Executor<'e, Database = DbDriver>,
    {
        let mut qb = select_fields!(
            "providers",
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

        let row = qb
            .push(" WHERE name = ")
            .push_bind(name)
            .build_query_as::<Provider>()
            .fetch_optional(executor)
            .await?;

        Ok(row)
    }
    pub async fn get_by_name(&self, name: &str) -> Result<Option<Provider>> {
        Self::get_by_name_inner(&self.db, name).await
    }

    /// 创建提供商，提供商名称必须唯一
    pub async fn create(&self, data: CreateProvider) -> Result<i64> {
        let mut tx = self.db.begin().await?;

        if data.name.is_empty() {
            return Err(CoreError::Validation(
                "provider name cannot be empty".into(),
            ));
        }
        if Self::get_by_name_inner(&mut *tx, data.name.trim())
            .await?
            .is_some()
        {
            return Err(CoreError::Validation("provider name already exists".into()));
        }

        let id = next_id();
        let mut qb = insert!(
            "providers",
            ("id", id),
            ("name", data.name),
            ("alias", data.alias),
            ("description", data.description),
            ("base_url", data.base_url),
            ("doc", data.doc),
            ("active", true),
        );
        qb.build().execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(id)
    }

    pub async fn update(&self, id: i64, data: UpdateProvider) -> Result<()> {
        let mut tx = self.db.begin().await?;

        if let Some(name) = &data.name {
            if name.is_empty() {
                return Err(CoreError::Validation("name cannot be empty".to_string()));
            }
            if let Some(exists) = Self::get_by_name_inner(&mut *tx, &name.trim()).await?
                && exists.id != id
            {
                return Err(CoreError::Validation("provider name already exists".into()));
            }
        }

        let mut qb = update!(
            "providers",
            id,
            ("name", data.name),
            ("alias", data.alias),
            ("description", data.description),
            ("base_url", data.base_url),
            ("doc", data.doc),
            ("active", data.active),
        );
        qb.build().execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut tx = self.db.begin().await?;
        sqlx::query("DELETE FROM providers WHERE id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM credentials WHERE provider_id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM json_rule WHERE provider_id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn get(&self, id: i64) -> Result<Option<Provider>> {
        let mut qb = get_by_id!(
            "providers",
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
        let row = qb
            .build_query_as::<Provider>()
            .fetch_optional(&self.db)
            .await?;

        Ok(row)
    }

    pub async fn list_all(&self) -> Result<Vec<Provider>> {
        let mut qb = select_fields!(
            "providers",
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
        let rows = qb.build_query_as::<Provider>().fetch_all(&self.db).await?;

        Ok(rows)
    }

    // --- Credentials ---

    /// 创建一条提供商凭证
    pub async fn create_credentials(&self, data: CreateCredentials) -> Result<i64> {
        let id = next_id();
        let mut qb = insert!(
            "credentials",
            ("id", id),
            ("provider_id", data.provider_id),
            ("key", data.key),
            ("active", true),
        );
        qb.build().execute(&self.db).await?;
        Ok(id)
    }

    pub async fn get_provider_credentials(&self, provider_id: i64) -> Result<Vec<Credentials>> {
        let mut qb = select_fields!(
            "credentials",
            ("id", "provider_id", "key", "active", "created_at")
        );
        qb.push(" WHERE provider_id = ")
            .push_bind(provider_id)
            .push(" ORDER BY active DESC ");

        let rows = qb
            .build_query_as::<Credentials>()
            .fetch_all(&self.db)
            .await?;

        Ok(rows)
    }

    pub async fn delete_credentials(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!("credentials", id);
        qb.build().execute(&self.db).await?;
        Ok(())
    }

    pub async fn create_json_rule(&self, data: CreateJsonRule) -> Result<i64> {
        let id = next_id();
        let mut qb = insert!(
            "json_rule",
            ("id", id),
            ("provider_id", data.provider_id),
            ("adapter", data.adapter.to_string()),
            ("active", true),
            ("json_rule", data.json_rule),
        );
        qb.build().execute(&self.db).await?;
        Ok(id)
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
        ensure_affected(&qb.build().execute(&self.db).await?)?;
        Ok(())
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
        let row = qb
            .push(" WHERE provider_id = ")
            .push_bind(provider_id)
            .push(" ORDER BY id DESC")
            .build_query_as::<JsonRule>()
            .fetch_all(&self.db)
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
        let row = qb
            .push(" WHERE provider_id = ")
            .push_bind(provider_id)
            .push(" AND adapter = ")
            .push_bind(adapter.to_string())
            .build_query_as::<JsonRule>()
            .fetch_optional(&self.db)
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
        let row = qb
            .build_query_as::<JsonRule>()
            .fetch_optional(&self.db)
            .await?;

        Ok(row)
    }

    pub async fn delete_json_rule(&self, id: i64) -> Result<()> {
        let mut qb = delete_by_id!("json_rule", id);
        qb.build().execute(&self.db).await?;
        Ok(())
    }
}
