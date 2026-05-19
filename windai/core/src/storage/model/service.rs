use super::repo::ModelRepo;
use crate::db;
use crate::error::{CoreError, Result};
use crate::models::{CreateModel, Model, UpdateModel};
use sqlx::SqlitePool;

pub struct ModelService {
    repo: ModelRepo,
}

impl ModelService {
    pub fn new(db: SqlitePool) -> Self {
        Self {
            repo: ModelRepo::new(db),
        }
    }

    /// 创建一个模型
    pub async fn create(&self, data: CreateModel) -> Result<Model> {
        if data.name.is_empty() {
            return Err(CoreError::Validation("model name cannot be empty".into()));
        }

        let modalities_json =
            serde_json::to_string(&data.modalities).unwrap_or_else(|_| "[]".into());

        let mut tx = db::begin_tx(&self.repo.db).await?;
        let id = self
            .repo
            .create(
                &mut tx,
                &data.name,
                data.provider_id,
                data.alias.as_deref(),
                &data.adaptor.to_string(),
                &modalities_json,
                data.active.unwrap_or(true),
                data.icon.as_deref(),
                data.endpoint.as_deref(),
            )
            .await?;
        tx.commit().await?;

        self.repo
            .get(id)
            .await?
            .ok_or(CoreError::NotFound("created model".into()))
    }

    /// 更新模型
    pub async fn update(&self, id: i64, data: UpdateModel) -> Result<()> {
        let current = self
            .get(id)
            .await?
            .ok_or(CoreError::NotFound(format!("model {id}")))?;

        let name = data.name.as_deref().unwrap_or(&current.name);
        let alias = data.alias.as_deref().or(current.alias.as_deref());
        let adaptor = data
            .adaptor
            .map(|a| a.to_string())
            .unwrap_or_else(|| current.adaptor.to_string());
        let modalities_json = data
            .modalities
            .as_ref()
            .map(|m| serde_json::to_string(m).unwrap_or_else(|_| "[]".into()))
            .unwrap_or_else(|| {
                serde_json::to_string(&current.modalities).unwrap_or_else(|_| "[]".into())
            });
        let active = data.active.unwrap_or(current.active);
        let icon = data.icon.as_deref().or(current.icon.as_deref());
        let endpoint = data.endpoint.as_deref().or(current.endpoint.as_deref());
        let frequency = data.frequency.or(current.frequency);

        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo
            .update(
                &mut tx,
                id,
                name,
                alias,
                &adaptor,
                &modalities_json,
                active,
                icon,
                endpoint,
                frequency,
            )
            .await?;
        tx.commit().await?;

        Ok(())
    }

    /// 查询指定提供商的所有模型
    pub async fn list_by_provider(&self, provider_id: i64) -> Result<Vec<Model>> {
        self.repo.list_by_provider(provider_id).await
    }

    /// 获取模型详情
    pub async fn get(&self, id: i64) -> Result<Option<Model>> {
        self.repo.get(id).await
    }

    /// 删除一个模型
    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo.delete(&mut tx, id).await?;
        tx.commit().await?;
        Ok(())
    }
}
