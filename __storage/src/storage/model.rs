use crate::storage::utils::value_or_none;

use super::{Storage, StorageError, lock_db};
use wind_domain::{
    adaptor::AdaptorType,
    model::{Model, ModelType},
};

fn row_to_model(row: &rusqlite::Row<'_>) -> Result<Model, rusqlite::Error> {
    use std::str::FromStr;
    let adaptor: AdaptorType = AdaptorType::from_str(&row.get::<_, String>(4)?).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(e))
    })?;

    let modalities_str: String = row.get(5)?;
    let modalities: Vec<ModelType> = serde_json::from_str(&modalities_str).unwrap_or_default();

    Ok(Model {
        id: row.get(0)?,
        name: row.get(1)?,
        alias: row.get(2)?,
        provider_id: row.get(3)?,
        adaptor,
        modalities,
        active: row.get(6)?,
        icon: row.get(7)?,
        endpoint: row.get(8)?,
        frequency: row.get(9)?,
    })
}
impl Storage {
    /// 创建模型
    /// - 创建成功后将 id 设置到 model 中
    pub fn create_model(&self, model: &mut Model) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        let modalities = serde_json::to_string(&model.modalities)?;
        let row_count = conn.execute(
            "INSERT INTO models (name, alias, provider_id, adaptor, modalities, active, icon, endpoint, frequency)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            (
                &model.name,
                &model.alias,
                model.provider_id,
                model.adaptor.to_string(),
                modalities,
                model.active,
                &model.icon,
                &model.endpoint,
                model.frequency,
            ),
        )?;
        model.id = conn.last_insert_rowid();
        Ok(row_count)
    }

    /// 根据 id 查询模型
    pub fn get_model(&self, id: i64) -> Result<Option<Model>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, name, alias, provider_id, adaptor, modalities, active, icon, endpoint, frequency
            FROM models WHERE id = ?1",
        )?;
        let result = stmt.query_row([id], row_to_model);
        value_or_none(result)
    }

    /// 根据 name 查询模型
    pub fn get_model_by_name(&self, name: &str) -> Result<Option<Model>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, name, alias, provider_id, adaptor, modalities, active, icon, endpoint, frequency
            FROM models WHERE name = ?1",
        )?;
        let result = stmt.query_row([name], row_to_model);
        value_or_none(result)
    }

    /// 根据 provider_id 查询该提供商下的所有模型
    pub fn list_models_by_provider(&self, provider_id: i64) -> Result<Vec<Model>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, name, alias, provider_id, adaptor, modalities, active, icon, endpoint, frequency
            FROM models WHERE provider_id = ?1 ORDER BY created_at DESC",
        )?;
        let models = stmt
            .query_map([provider_id], row_to_model)?
            // .filter_map(|r| r.ok())
            .collect::<Result<Vec<Model>, rusqlite::Error>>()?;
        Ok(models)
    }

    /// 查询所有模型
    pub fn list_all_models(&self) -> Result<Vec<Model>, StorageError> {
        let conn = lock_db!(&self);
        let mut stmt = conn.prepare(
            "SELECT id, name, alias, provider_id, adaptor, modalities, active, icon, endpoint, frequency
            FROM models ORDER BY created_at DESC",
        )?;
        let models = stmt
            .query_map([], row_to_model)?
            // .filter_map(|r| r.ok())
            .collect::<Result<Vec<Model>, rusqlite::Error>>()?;
        Ok(models)
    }

    /// 更新模型
    pub fn update_model(&self, model: &Model) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        let modalities = serde_json::to_string(&model.modalities)?;

        Ok(conn.execute(
            "UPDATE models SET name = ?1, alias = ?2, provider_id = ?3, adaptor = ?4,
            modalities = ?5, active = ?6, icon = ?7, endpoint = ?8, frequency = ?9,
            updated_at = strftime('%s', 'now') WHERE id = ?10",
            (
                &model.name,
                &model.alias,
                model.provider_id,
                model.adaptor.to_string(),
                modalities,
                model.active,
                &model.icon,
                &model.endpoint,
                model.frequency,
                model.id,
            ),
        )?)
    }

    /// 增加模型使用次数
    pub fn increment_model_frequency(&self, id: i64) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);

        Ok(conn.execute(
            "UPDATE models SET frequency = COALESCE(frequency, 0) + 1,
            updated_at = strftime('%s', 'now') WHERE id = ?1",
            [id],
        )?)
    }

    /// 根据 id 删除模型
    pub fn delete_model(&self, id: i64) -> Result<usize, StorageError> {
        let conn = lock_db!(&self);
        Ok(conn.execute("DELETE FROM models WHERE id = ?1", [id])?)
    }
}
