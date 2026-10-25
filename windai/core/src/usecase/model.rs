use super::UseCases;
use super::error::UseCaseError;
use super::schema;
use crate::models::{self, Model, ModelCreate};
use serde::{Deserialize, Serialize};
use wind_event::context::DispatchCx;
use wind_event::def::{EventDef, Mount, Shape};

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModelUpdate {
    pub model_id: i64,
    pub data: models::ModelUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModelDelete {
    pub model_id: i64,
}

pub const CORE_MODEL_CREATE: EventDef<ModelCreate> = EventDef::new(
    "core/model.create",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<ModelCreate>,
);

pub const CORE_MODEL_UPDATE: EventDef<ModelUpdate> = EventDef::new(
    "core/model.update",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<ModelUpdate>,
);

pub const CORE_MODEL_DELETE: EventDef<ModelDelete> = EventDef::new(
    "core/model.delete",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<ModelDelete>,
);

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModelCreated {
    pub model_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModelUpdated {
    pub model_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModelDeleted {
    pub model_id: i64,
}

pub const CORE_MODEL_CREATED: EventDef<ModelCreated> = EventDef::new(
    "core/model.created",
    Shape::Observe,
    true,
    Mount::UseCase,
    schema::<ModelCreated>,
);

pub const CORE_MODEL_UPDATED: EventDef<ModelUpdated> = EventDef::new(
    "core/model.updated",
    Shape::Observe,
    true,
    Mount::UseCase,
    schema::<ModelUpdated>,
);

pub const CORE_MODEL_DELETED: EventDef<ModelDeleted> = EventDef::new(
    "core/model.deleted",
    Shape::Observe,
    true,
    Mount::UseCase,
    schema::<ModelDeleted>,
);

impl UseCases {
    pub async fn create_model(
        &self,
        cx: &DispatchCx,
        input: ModelCreate,
    ) -> Result<i64, UseCaseError> {
        let payload = self.intercept(&CORE_MODEL_CREATE, cx, input).await;
        let id = self.storage().model_create(payload).await?;
        self.notify(&CORE_MODEL_CREATED, cx, ModelCreated { model_id: id })
            .await;
        Ok(id)
    }

    pub async fn update_model(
        &self,
        cx: &DispatchCx,
        id: i64,
        data: models::ModelUpdate,
    ) -> Result<(), UseCaseError> {
        self.get_model(id).await?.ok_or(UseCaseError::NotFound {
            entity: "model",
            id,
        })?;
        let payload = ModelUpdate { model_id: id, data };
        let payload = self.intercept(&CORE_MODEL_UPDATE, cx, payload).await;
        self.storage().model_update(id, payload.data).await?;
        self.notify(&CORE_MODEL_UPDATED, cx, ModelUpdated { model_id: id })
            .await;
        Ok(())
    }

    pub async fn delete_model(&self, cx: &DispatchCx, id: i64) -> Result<(), UseCaseError> {
        self.get_model(id).await?.ok_or(UseCaseError::NotFound {
            entity: "model",
            id,
        })?;
        let payload = ModelDelete { model_id: id };
        let payload = self.intercept(&CORE_MODEL_DELETE, cx, payload).await;
        self.storage().model_delete(payload.model_id).await?;
        self.notify(
            &CORE_MODEL_DELETED,
            cx,
            ModelDeleted {
                model_id: payload.model_id,
            },
        )
        .await;
        Ok(())
    }

    pub async fn get_model(&self, id: i64) -> Result<Option<Model>, UseCaseError> {
        self.storage().model_get(id).await.map_err(Into::into)
    }

    pub async fn list_models_by_provider(
        &self,
        provider_id: i64,
    ) -> Result<Vec<Model>, UseCaseError> {
        self.storage()
            .model_list_by_provider(provider_id)
            .await
            .map_err(Into::into)
    }
}
