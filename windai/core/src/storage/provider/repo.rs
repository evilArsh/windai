use crate::error::Result;
use crate::models::{CreateCredentials, CreateJsonRule, Credentials, JsonRule, Provider};
use sqlx::{Row, SqlitePool, Transaction};
use wind_ai::model::AdaptorType;

pub struct ProviderRepo {
    pub(crate) db: SqlitePool,
}

impl ProviderRepo {
    pub fn new(db: SqlitePool) -> Self {
        Self { db }
    }

    fn row_to_provider(row: sqlx::sqlite::SqliteRow) -> Provider {
        Provider {
            id: row.get(0),
            name: row.get(1),
            alias: row.get(2),
            description: row.get(3),
            base_url: row.get(4),
            doc: row.get(5),
            active: row.get(6),
            created_at: row.get(7),
        }
    }

    pub async fn create(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        name: &str,
        description: Option<&str>,
        base_url: &str,
        doc: Option<&str>,
        alias: Option<&str>,
        active: bool,
    ) -> Result<i64> {
        let row = sqlx::query(
            r#"INSERT INTO providers 
            (name, alias, description, base_url, doc, active)
            VALUES (?, ?, ?, ?, ?, ?)"#,
        )
        .bind(name)
        .bind(alias)
        .bind(description)
        .bind(base_url)
        .bind(doc)
        .bind(active)
        .execute(&mut **tx)
        .await?;

        Ok(row.last_insert_rowid())
    }

    pub async fn update(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        id: i64,
        name: &str,
        alias: Option<&str>,
        description: Option<&str>,
        base_url: &str,
        doc: Option<&str>,
        active: bool,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE providers SET
            name = ?, alias = ?, description = ?, base_url = ?, doc = ?, active = ?, 
            updated_at = strftime('%s', 'now')
            WHERE id = ?"#,
        )
        .bind(name)
        .bind(alias)
        .bind(description)
        .bind(base_url)
        .bind(doc)
        .bind(active)
        .bind(id)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    pub async fn get(&self, id: i64) -> Result<Option<Provider>> {
        let row = sqlx::query(
            r#"SELECT
            id, name, alias, description, base_url, doc, active,
            created_at
            FROM providers WHERE id = ?"#,
        )
        .bind(id)
        .map(Self::row_to_provider)
        .fetch_optional(&self.db)
        .await?;

        Ok(row)
    }

    pub async fn get_by_name(&self, name: &str) -> Result<Option<Provider>> {
        let row = sqlx::query(
            r#"SELECT
            id, name, alias, description, base_url, doc, active,
            created_at
            FROM providers WHERE name = ?"#,
        )
        .bind(name)
        .map(Self::row_to_provider)
        .fetch_optional(&self.db)
        .await?;

        Ok(row)
    }

    pub async fn list_all(&self) -> Result<Vec<Provider>> {
        let rows = sqlx::query(
            r#"SELECT
            id, name, alias, description, base_url, doc, active,
            created_at
            FROM providers ORDER BY id DESC"#,
        )
        .map(Self::row_to_provider)
        .fetch_all(&self.db)
        .await?;

        Ok(rows)
    }

    pub async fn delete(&self, tx: &mut Transaction<'_, sqlx::Sqlite>, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM providers WHERE id = ?")
            .bind(id)
            .execute(&mut **tx)
            .await?;

        sqlx::query("DELETE FROM credentials WHERE provider_id = ?")
            .bind(id)
            .execute(&mut **tx)
            .await?;

        sqlx::query("DELETE FROM json_rule WHERE provider_id = ?")
            .bind(id)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    // --- Credentials ---

    pub async fn create_credentials(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        data: CreateCredentials,
    ) -> Result<i64> {
        let row = sqlx::query(
            r#"INSERT INTO credentials 
            (provider_id, api_key, active)
            VALUES (?, ?, 1)"#,
        )
        .bind(data.provider_id)
        .bind(data.key)
        .execute(&mut **tx)
        .await?;

        Ok(row.last_insert_rowid())
    }

    pub async fn get_credentials_by_provider(&self, provider_id: i64) -> Result<Vec<Credentials>> {
        let rows = sqlx::query(
            r#"SELECT
            id, provider_id, api_key, active,
            created_at
            FROM credentials
            WHERE provider_id = ? ORDER BY active DESC"#,
        )
        .bind(provider_id)
        .map(|row: sqlx::sqlite::SqliteRow| Credentials {
            id: row.get(0),
            provider_id: row.get(1),
            key: row.get(2),
            created_at: row.get(3),
            active: row.get(4),
        })
        .fetch_all(&self.db)
        .await?;

        Ok(rows)
    }

    pub async fn get_credentials(&self, id: i64) -> Result<Option<Credentials>> {
        let row = sqlx::query(
            r#"SELECT
            id, provider_id, api_key, active,
            created_at
            FROM credentials
            WHERE id = ?"#,
        )
        .bind(id)
        .map(|row: sqlx::sqlite::SqliteRow| Credentials {
            id: row.get(0),
            provider_id: row.get(1),
            key: row.get(2),
            created_at: row.get(3),
            active: row.get(4),
        })
        .fetch_optional(&self.db)
        .await?;

        Ok(row)
    }

    pub async fn delete_credentials(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        id: i64,
    ) -> Result<()> {
        sqlx::query("DELETE FROM credentials WHERE id = ?")
            .bind(id)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    pub async fn create_json_rule(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        data: CreateJsonRule,
    ) -> Result<i64> {
        let row = sqlx::query(
            "INSERT INTO json_rule
            (provider_id, adaptor, json_rule, active)
            VALUES (?, ?, ?, ?)",
        )
        .bind(data.provider_id)
        .bind(data.adaptor.to_string())
        .bind(data.json_rule)
        .bind(data.active)
        .execute(&mut **tx)
        .await?;

        Ok(row.last_insert_rowid())
    }
    pub async fn update_json_rule(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        id: i64,
        provider_id: i64,
        adaptor: &str,
        json_rule: &str,
        active: bool,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE json_rule SET
            provider_id = ?, adaptor = ?, json_rule = ?, active = ?, 
            updated_at = strftime('%s', 'now')
            WHERE id = ?"#,
        )
        .bind(provider_id)
        .bind(adaptor)
        .bind(json_rule)
        .bind(active)
        .bind(id)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    fn row_to_json_rule(row: sqlx::sqlite::SqliteRow) -> Result<JsonRule> {
        let adaptor_str: String = row.get(2);
        let adaptor: AdaptorType = adaptor_str.parse()?;
        Ok(JsonRule {
            id: row.get(0),
            provider_id: row.get(1),
            adaptor,
            json_rule: row.get(3),
            active: row.get(4),
            created_at: row.get(5),
        })
    }

    pub async fn get_json_rule(
        &self,
        provider_id: i64,
        adaptor: AdaptorType,
    ) -> Result<Option<JsonRule>> {
        let row = sqlx::query(
            "SELECT
            id, provider_id, adaptor, json_rule, active,
            created_at 
            FROM json_rule WHERE provider_id = ? AND adaptor = ?",
        )
        .bind(provider_id)
        .bind(adaptor.to_string())
        .map(Self::row_to_json_rule)
        .fetch_optional(&self.db)
        .await?;

        row.map(|r| r.map_err(Into::into)).transpose()
    }

    pub async fn get_json_rule_by_id(&self, id: i64) -> Result<Option<JsonRule>> {
        let row = sqlx::query(
            "SELECT
            id, provider_id, adaptor, json_rule, active,
            created_at
            FROM json_rule WHERE id = ?",
        )
        .bind(id)
        .map(Self::row_to_json_rule)
        .fetch_optional(&self.db)
        .await?;

        row.map(|r| r.map_err(Into::into)).transpose()
    }

    pub async fn list_json_rules(&self, provider_id: i64) -> Result<Vec<JsonRule>> {
        let rows = sqlx::query(
            "SELECT
            id, provider_id, adaptor, json_rule, active,
            created_at
            FROM json_rule WHERE provider_id = ? ORDER BY id DESC",
        )
        .bind(provider_id)
        .map(Self::row_to_json_rule)
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .flatten();

        Ok(rows.collect())
    }

    pub async fn delete_json_rule(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        id: i64,
    ) -> Result<()> {
        sqlx::query("DELETE FROM json_rule WHERE id = ?")
            .bind(id)
            .execute(&mut **tx)
            .await?;

        Ok(())
    }
}
