use super::repo::ProviderRepo;
use crate::db;
use crate::error::{CoreError, Result};
use crate::models::{
    CreateCredentials, CreateJsHookCode, CreateProvider, Credentials, JsHookCode, Provider,
    UpdateJsHookCode, UpdateProvider,
};
use sqlx::SqlitePool;
use wind_ai::model::AdaptorType;

pub struct ProviderService {
    repo: ProviderRepo,
}

impl ProviderService {
    pub fn new(db: SqlitePool) -> Self {
        Self {
            repo: ProviderRepo::new(db),
        }
    }

    /// 创建提供商，提供商名称必须唯一
    pub async fn create(&self, data: CreateProvider) -> Result<Provider> {
        if data.name.is_empty() {
            return Err(CoreError::Validation(
                "provider name cannot be empty".into(),
            ));
        }

        if self.repo.get_by_name(data.name.trim()).await?.is_some() {
            return Err(CoreError::Validation("provider name already exists".into()));
        }

        let mut tx = db::begin_tx(&self.repo.db).await?;
        let id = self
            .repo
            .create(
                &mut tx,
                &data.name,
                data.description.as_deref(),
                data.base_url.as_ref(),
                data.doc.as_deref(),
                data.alias.as_deref(),
                data.active.unwrap_or(true),
            )
            .await?;
        tx.commit().await?;

        self.repo
            .get(id)
            .await?
            .ok_or(CoreError::NotFound("created provider".into()))
    }

    /// 列出所有提供商
    pub async fn list(&self) -> Result<Vec<Provider>> {
        self.repo.list_all().await
    }

    /// 通过 id 获取提供商
    pub async fn get(&self, id: i64) -> Result<Option<Provider>> {
        self.repo.get(id).await
    }

    /// 通过 提供商名字 获取提供商
    pub async fn get_by_name(&self, name: &str) -> Result<Option<Provider>> {
        self.repo.get_by_name(name).await
    }

    /// 更新提供商
    pub async fn update(&self, id: i64, data: UpdateProvider) -> Result<()> {
        let current = self
            .get(id)
            .await?
            .ok_or(CoreError::NotFound(format!("provider {id}")))?;

        if let Some(name) = &data.name {
            if name.is_empty() {
                return Err(CoreError::Validation("name cannot be empty".to_string()));
            }
            if self.repo.get_by_name(name.trim()).await?.is_some() {
                return Err(CoreError::Validation("provider name already exists".into()));
            }
        }

        let name = data.name.as_deref().unwrap_or(&current.name);
        let alias = data.alias.as_deref().or(current.alias.as_deref());
        let description = data
            .description
            .as_deref()
            .or(current.description.as_deref());
        let base_url = data
            .base_url
            .as_deref()
            .unwrap_or(current.base_url.as_ref());
        let doc = data.doc.as_deref().or(current.doc.as_deref());
        let active = data.active.unwrap_or(current.active);

        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo
            .update(&mut tx, id, name, alias, description, base_url, doc, active)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// 删除一条提供商记录
    pub async fn delete(&self, id: i64) -> Result<()> {
        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo.delete(&mut tx, id).await?;
        tx.commit().await?;
        Ok(())
    }

    // --- Credentials ---

    /// 创建一条提供商凭证
    pub async fn create_credentials(&self, data: CreateCredentials) -> Result<Credentials> {
        let mut tx = db::begin_tx(&self.repo.db).await?;
        let id = self.repo.create_credentials(&mut tx, data).await?;
        tx.commit().await?;

        self.repo
            .get_credentials(id)
            .await?
            .ok_or(CoreError::NotFound("created credentials".into()))
    }

    /// 列出所有指定提供商的凭证
    pub async fn list_credentials(&self, provider_id: i64) -> Result<Vec<Credentials>> {
        self.repo.get_credentials_by_provider(provider_id).await
    }

    /// 删除一条凭证
    pub async fn delete_credentials(&self, id: i64) -> Result<()> {
        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo.delete_credentials(&mut tx, id).await?;
        tx.commit().await?;
        Ok(())
    }

    // --- JsHookCode ---

    pub async fn create_js_hook_code(&self, data: CreateJsHookCode) -> Result<JsHookCode> {
        let mut tx = db::begin_tx(&self.repo.db).await?;
        let id = self.repo.create_js_hook_code(&mut tx, data).await?;
        tx.commit().await?;

        self.repo
            .get_js_hook_code_by_id(id)
            .await?
            .ok_or(CoreError::NotFound("created js_hook_code".into()))
    }

    pub async fn update_js_hook_code(&self, id: i64, data: UpdateJsHookCode) -> Result<()> {
        let current = self
            .get_js_hook_code_by_id(id)
            .await?
            .ok_or(CoreError::NotFound(format!("js_hook_code {id}")))?;

        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo
            .update_js_hook_code(
                &mut tx,
                id,
                data.provider_id.unwrap_or(current.provider_id),
                &data
                    .adaptor
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| current.adaptor.to_string()),
                data.js_code.as_deref().unwrap_or(&current.js_code),
                data.active.unwrap_or(current.active),
            )
            .await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn list_js_hook_codes(&self, provider_id: i64) -> Result<Vec<JsHookCode>> {
        self.repo.list_js_hook_codes(provider_id).await
    }

    /// 通过 provider id 和 adaptor 类型获取 js_hook_code
    pub async fn get_js_hook_code(
        &self,
        provider_id: i64,
        adaptor: AdaptorType,
    ) -> Result<Option<JsHookCode>> {
        self.repo.get_js_hook_code(provider_id, adaptor).await
    }

    pub async fn get_js_hook_code_by_id(&self, id: i64) -> Result<Option<JsHookCode>> {
        self.repo.get_js_hook_code_by_id(id).await
    }

    pub async fn delete_js_hook_code(&self, id: i64) -> Result<()> {
        let mut tx = db::begin_tx(&self.repo.db).await?;
        self.repo.delete_js_hook_code(&mut tx, id).await?;
        tx.commit().await?;
        Ok(())
    }
}
