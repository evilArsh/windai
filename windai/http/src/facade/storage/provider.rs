use std::sync::Arc;
use wind_ai::model::AdapterType;
use wind_core::WindCore;
use wind_core::models::{
    CreateCredentials, CreateJsonRule, CreateProvider, Credentials, JsonRule, Provider,
    UpdateJsonRule, UpdateProvider,
};

use crate::dto::envelope::{ApiResponse, map_core_error};

pub struct ProviderStorageFacade {
    core: Arc<WindCore>,
}

impl ProviderStorageFacade {
    pub fn new(core: Arc<WindCore>) -> Self {
        Self { core }
    }

    pub async fn list_providers(&self) -> ApiResponse<Vec<Provider>> {
        match self.core.storage().provider().list_all().await {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn create_provider(&self, input: CreateProvider) -> ApiResponse<Provider> {
        match self.core.storage().provider().create(input).await {
            Ok(p) => ApiResponse::ok(p),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_provider(&self, id: i64) -> ApiResponse<Provider> {
        match self.core.storage().provider().get(id).await {
            Ok(Some(p)) => ApiResponse::ok(p),
            Ok(None) => ApiResponse::not_found("provider not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn update_provider(&self, id: i64, input: UpdateProvider) -> ApiResponse<Provider> {
        if let Err(e) = self.core.storage().provider().update(id, input).await {
            return map_core_error(e);
        }
        self.get_provider(id).await
    }

    pub async fn delete_provider(&self, id: i64) -> ApiResponse<()> {
        match self.core.storage().provider().get(id).await {
            Ok(None) => return ApiResponse::not_found("provider not found"),
            Ok(Some(_)) => {}
            Err(e) => return map_core_error(e),
        }
        match self.core.storage().provider().delete(id).await {
            Ok(()) => ApiResponse::ok(()),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_provider_by_name(&self, name: String) -> ApiResponse<Provider> {
        match self.core.storage().provider().get_by_name(&name).await {
            Ok(Some(p)) => ApiResponse::ok(p),
            Ok(None) => ApiResponse::not_found("provider not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn list_credentials(&self, provider_id: i64) -> ApiResponse<Vec<Credentials>> {
        match self
            .core
            .storage()
            .provider()
            .get_provider_credentials(provider_id)
            .await
        {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn create_credentials(&self, input: CreateCredentials) -> ApiResponse<Credentials> {
        match self
            .core
            .storage()
            .provider()
            .create_credentials(input)
            .await
        {
            Ok(c) => ApiResponse::ok(c),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn delete_credentials(&self, id: i64) -> ApiResponse<()> {
        match self.core.storage().provider().delete_credentials(id).await {
            Ok(()) => ApiResponse::ok(()),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn list_json_rules(&self, provider_id: i64) -> ApiResponse<Vec<JsonRule>> {
        match self
            .core
            .storage()
            .provider()
            .list_json_rules(provider_id)
            .await
        {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn create_json_rule(&self, input: CreateJsonRule) -> ApiResponse<JsonRule> {
        match self.core.storage().provider().create_json_rule(input).await {
            Ok(r) => ApiResponse::ok(r),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_json_rule(&self, id: i64) -> ApiResponse<JsonRule> {
        match self.core.storage().provider().get_json_rule_by_id(id).await {
            Ok(Some(r)) => ApiResponse::ok(r),
            Ok(None) => ApiResponse::not_found("json rule not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn update_json_rule(&self, id: i64, input: UpdateJsonRule) -> ApiResponse<JsonRule> {
        if let Err(e) = self
            .core
            .storage()
            .provider()
            .update_json_rule(id, input)
            .await
        {
            return map_core_error(e);
        }
        self.get_json_rule(id).await
    }

    pub async fn delete_json_rule(&self, id: i64) -> ApiResponse<()> {
        match self.core.storage().provider().delete_json_rule(id).await {
            Ok(()) => ApiResponse::ok(()),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_json_rule_by_adapter(
        &self,
        provider_id: i64,
        adapter: AdapterType,
    ) -> ApiResponse<JsonRule> {
        match self
            .core
            .storage()
            .provider()
            .get_json_rule(provider_id, adapter)
            .await
        {
            Ok(Some(r)) => ApiResponse::ok(r),
            Ok(None) => ApiResponse::not_found("json rule not found"),
            Err(e) => map_core_error(e),
        }
    }
}
