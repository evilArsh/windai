use super::UseCases;
use super::error::UseCaseError;
use super::schema;
use crate::models::{self, Provider, ProviderCreate};
use serde::{Deserialize, Serialize};
use wind_event::context::DispatchCx;
use wind_event::def::{EventDef, Mount, Shape};

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProviderUpdate {
    pub provider_id: i64,
    pub data: models::ProviderUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProviderDelete {
    pub provider_id: i64,
}

pub const CORE_PROVIDER_CREATE: EventDef<ProviderCreate> = EventDef::new(
    "core/provider.create",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<ProviderCreate>,
);

pub const CORE_PROVIDER_UPDATE: EventDef<ProviderUpdate> = EventDef::new(
    "core/provider.update",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<ProviderUpdate>,
);

pub const CORE_PROVIDER_DELETE: EventDef<ProviderDelete> = EventDef::new(
    "core/provider.delete",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<ProviderDelete>,
);

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProviderCreated {
    pub provider_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProviderUpdated {
    pub provider_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProviderDeleted {
    pub provider_id: i64,
}

pub const CORE_PROVIDER_CREATED: EventDef<ProviderCreated> = EventDef::new(
    "core/provider.created",
    Shape::Observe,
    true,
    Mount::UseCase,
    schema::<ProviderCreated>,
);

pub const CORE_PROVIDER_UPDATED: EventDef<ProviderUpdated> = EventDef::new(
    "core/provider.updated",
    Shape::Observe,
    true,
    Mount::UseCase,
    schema::<ProviderUpdated>,
);

pub const CORE_PROVIDER_DELETED: EventDef<ProviderDeleted> = EventDef::new(
    "core/provider.deleted",
    Shape::Observe,
    true,
    Mount::UseCase,
    schema::<ProviderDeleted>,
);

impl UseCases {
    pub async fn create_provider(
        &self,
        cx: &DispatchCx,
        input: ProviderCreate,
    ) -> Result<i64, UseCaseError> {
        let payload = self.intercept(&CORE_PROVIDER_CREATE, cx, input).await;
        let id = self.storage().provider_create(payload).await?;
        self.notify(
            &CORE_PROVIDER_CREATED,
            cx,
            ProviderCreated { provider_id: id },
        )
        .await;
        Ok(id)
    }

    pub async fn update_provider(
        &self,
        cx: &DispatchCx,
        id: i64,
        data: models::ProviderUpdate,
    ) -> Result<(), UseCaseError> {
        self.get_provider(id).await?.ok_or(UseCaseError::NotFound {
            entity: "provider",
            id,
        })?;
        let payload = ProviderUpdate {
            provider_id: id,
            data,
        };
        let payload = self.intercept(&CORE_PROVIDER_UPDATE, cx, payload).await;
        self.storage().provider_update(id, payload.data).await?;
        self.notify(
            &CORE_PROVIDER_UPDATED,
            cx,
            ProviderUpdated { provider_id: id },
        )
        .await;
        Ok(())
    }

    pub async fn delete_provider(&self, cx: &DispatchCx, id: i64) -> Result<(), UseCaseError> {
        self.get_provider(id).await?.ok_or(UseCaseError::NotFound {
            entity: "provider",
            id,
        })?;
        let payload = ProviderDelete { provider_id: id };
        let payload = self.intercept(&CORE_PROVIDER_DELETE, cx, payload).await;
        self.storage().provider_delete(payload.provider_id).await?;
        self.notify(
            &CORE_PROVIDER_DELETED,
            cx,
            ProviderDeleted {
                provider_id: payload.provider_id,
            },
        )
        .await;
        Ok(())
    }

    pub async fn get_provider(&self, id: i64) -> Result<Option<Provider>, UseCaseError> {
        self.storage().provider_get(id).await.map_err(Into::into)
    }

    pub async fn list_providers(&self) -> Result<Vec<Provider>, UseCaseError> {
        self.storage().provider_list().await.map_err(Into::into)
    }
}
