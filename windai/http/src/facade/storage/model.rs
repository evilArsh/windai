use crate::dto::{ApiResponse, map_core_error};
use std::sync::Arc;
use wind_core::WindCore;
use wind_core::models::{CreateModel, Model, UpdateModel};

pub struct ModelStorageFacade {
    core: Arc<WindCore>,
}

impl ModelStorageFacade {
    pub fn new(core: Arc<WindCore>) -> Self {
        Self { core }
    }

    pub async fn list_models(&self, provider_id: i64) -> ApiResponse<Vec<Model>> {
        match self
            .core
            .storage()
            .model()
            .list_by_provider(provider_id)
            .await
        {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn create_model(&self, input: CreateModel) -> ApiResponse<Model> {
        match self.core.storage().model().create(input).await {
            Ok(m) => ApiResponse::ok(m),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_model(&self, id: i64) -> ApiResponse<Model> {
        match self.core.storage().model().get(id).await {
            Ok(Some(m)) => ApiResponse::ok(m),
            Ok(None) => ApiResponse::not_found("model not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn update_model(&self, id: i64, input: UpdateModel) -> ApiResponse<Model> {
        if let Err(e) = self.core.storage().model().update(id, input).await {
            return map_core_error(e);
        }
        self.get_model(id).await
    }

    pub async fn delete_model(&self, id: i64) -> ApiResponse<()> {
        match self.core.storage().model().delete(id).await {
            Ok(()) => ApiResponse::ok(()),
            Err(e) => map_core_error(e),
        }
    }
}
