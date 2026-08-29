use std::sync::Arc;
use wind_core::WindCore;
use wind_core::models::{CreatePromptModule, PromptModule, UpdatePromptModule};

use crate::dto::envelope::{ApiResponse, map_core_error};

pub struct PromptStorageFacade {
    core: Arc<WindCore>,
}

impl PromptStorageFacade {
    pub fn new(core: Arc<WindCore>) -> Self {
        Self { core }
    }

    pub async fn list_prompt_modules(&self) -> ApiResponse<Vec<PromptModule>> {
        match self.core.storage().prompt().list().await {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn create_prompt_module(
        &self,
        input: CreatePromptModule,
    ) -> ApiResponse<PromptModule> {
        match self.core.storage().prompt().create(input).await {
            Ok(p) => ApiResponse::ok(p),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_prompt_module(&self, id: i64) -> ApiResponse<PromptModule> {
        match self.core.storage().prompt().get(id).await {
            Ok(Some(p)) => ApiResponse::ok(p),
            Ok(None) => ApiResponse::not_found("prompt module not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn update_prompt_module(
        &self,
        id: i64,
        input: UpdatePromptModule,
    ) -> ApiResponse<PromptModule> {
        if let Err(e) = self.core.storage().prompt().update(id, input).await {
            return map_core_error(e);
        }
        self.get_prompt_module(id).await
    }

    pub async fn delete_prompt_module(&self, id: i64) -> ApiResponse<()> {
        match self.core.storage().prompt().get(id).await {
            Ok(None) => return ApiResponse::not_found("prompt module not found"),
            Ok(Some(_)) => {}
            Err(e) => return map_core_error(e),
        }
        match self.core.storage().prompt().delete(id).await {
            Ok(()) => ApiResponse::ok(()),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_prompt_module_by_key(&self, key: String) -> ApiResponse<PromptModule> {
        match self.core.storage().prompt().get_by_key(&key).await {
            Ok(Some(p)) => ApiResponse::ok(p),
            Ok(None) => ApiResponse::not_found("prompt module not found"),
            Err(e) => map_core_error(e),
        }
    }
}
