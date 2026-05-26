use crate::error::Result;
use crate::models::{Model, ModelType};
use sqlx::{Row, SqlitePool, Transaction};
use wind_ai::model::AdaptorType;

pub struct ModelRepo {
    pub(crate) db: SqlitePool,
}

impl ModelRepo {
    pub fn new(db: SqlitePool) -> Self {
        Self { db }
    }

    pub async fn create(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        name: &str,
        provider_id: i64,
        alias: Option<&str>,
        adaptor: &str,
        modalities_json: &str,
        active: bool,
        icon: Option<&str>,
        endpoint: Option<&str>,
    ) -> Result<i64> {
        let row = sqlx::query(
            r#"INSERT INTO models 
            (name, provider_id, alias, adaptor, modalities, active, icon, endpoint, frequency)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)"#,
        )
        .bind(name)
        .bind(provider_id)
        .bind(alias)
        .bind(adaptor)
        .bind(modalities_json)
        .bind(active)
        .bind(icon)
        .bind(endpoint)
        .execute(&mut **tx)
        .await?;

        Ok(row.last_insert_rowid())
    }

    pub async fn get(&self, id: i64) -> Result<Option<Model>> {
        let row = sqlx::query(
            r#"SELECT
            id, name, provider_id, alias, adaptor, modalities, active, icon, endpoint, frequency,
            created_at
            FROM models WHERE id = ?"#,
        )
        .bind(id)
        .map(Self::row_to_model)
        .fetch_optional(&self.db)
        .await?;

        row.map(|r| r.map_err(Into::into)).transpose()
    }

    pub async fn list_by_provider(&self, provider_id: i64) -> Result<Vec<Model>> {
        let rows = sqlx::query(
            r#"SELECT
            id, name, provider_id, alias, adaptor, modalities, active, icon, endpoint, frequency,
            created_at
            FROM models WHERE provider_id = ? ORDER BY id DESC"#,
        )
        .bind(provider_id)
        .map(Self::row_to_model)
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .collect::<Result<Vec<Model>>>()?;

        Ok(rows)
    }

    pub async fn update(
        &self,
        tx: &mut Transaction<'_, sqlx::Sqlite>,
        id: i64,
        name: &str,
        alias: Option<&str>,
        adaptor: &str,
        modalities_json: &str,
        active: bool,
        icon: Option<&str>,
        endpoint: Option<&str>,
        frequency: Option<i32>,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE models SET
            name = ?, alias = ?, adaptor = ?, modalities = ?,
            active = ?, icon = ?, endpoint = ?, frequency = ?,
            updated_at = strftime('%s', 'now')
            WHERE id = ?"#,
        )
        .bind(name)
        .bind(alias)
        .bind(adaptor)
        .bind(modalities_json)
        .bind(active)
        .bind(icon)
        .bind(endpoint)
        .bind(frequency)
        .bind(id)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    pub async fn delete(&self, tx: &mut Transaction<'_, sqlx::Sqlite>, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM models WHERE id = ?")
            .bind(id)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    fn row_to_model(row: sqlx::sqlite::SqliteRow) -> Result<Model> {
        let adaptor_str: String = row.get(4);
        let modalities_str: String = row.get(5);
        let adaptor: AdaptorType = adaptor_str.parse()?;
        let modalities: Option<Vec<ModelType>> =
            Some(serde_json::from_str(&modalities_str).unwrap_or(vec![]));

        Ok(Model {
            id: row.get(0),
            name: row.get(1),
            provider_id: row.get(2),
            alias: row.get(3),
            adaptor,
            modalities,
            active: row.get(6),
            icon: row.get(7),
            endpoint: row.get(8),
            frequency: row.get(9),
            created_at: row.get(10),
        })
    }
}
