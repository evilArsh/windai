use super::UseCases;
use super::error::UseCaseError;
use super::schema;
use crate::models::{self, Credentials};
use serde::{Deserialize, Serialize};
use wind_event::context::DispatchCx;
use wind_event::def::{EventDef, Mount, Shape};

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CredentialUpdate {
    pub credential_id: i64,
    pub data: models::CredentialUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CredentialDelete {
    pub credential_id: i64,
}

pub const CORE_CREDENTIAL_CREATE: EventDef<Credentials> = EventDef::new(
    "core/credential.create",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<Credentials>,
);

pub const CORE_CREDENTIAL_UPDATE: EventDef<CredentialUpdate> = EventDef::new(
    "core/credential.update",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<CredentialUpdate>,
);

pub const CORE_CREDENTIAL_DELETE: EventDef<CredentialDelete> = EventDef::new(
    "core/credential.delete",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<CredentialDelete>,
);

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CredentialCreated {
    pub credential_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CredentialUpdated {
    pub credential_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CredentialDeleted {
    pub credential_id: i64,
}

pub const CORE_CREDENTIAL_CREATED: EventDef<CredentialCreated> = EventDef::new(
    "core/credential.created",
    Shape::Observe,
    true,
    Mount::UseCase,
    schema::<CredentialCreated>,
);

pub const CORE_CREDENTIAL_UPDATED: EventDef<CredentialUpdated> = EventDef::new(
    "core/credential.updated",
    Shape::Observe,
    true,
    Mount::UseCase,
    schema::<CredentialUpdated>,
);

pub const CORE_CREDENTIAL_DELETED: EventDef<CredentialDeleted> = EventDef::new(
    "core/credential.deleted",
    Shape::Observe,
    true,
    Mount::UseCase,
    schema::<CredentialDeleted>,
);

impl UseCases {
    pub async fn create_credential(
        &self,
        cx: &DispatchCx,
        provider_id: i64,
        secret: &str,
    ) -> Result<i64, UseCaseError> {
        let payload = Credentials {
            id: 0,
            provider_id,
            key: secret.to_string(),
            created_at: 0,
            active: true,
        };
        let payload = self.intercept(&CORE_CREDENTIAL_CREATE, cx, payload).await;
        let id = self
            .storage()
            .credential_create(payload.provider_id, secret)
            .await?;
        self.notify(
            &CORE_CREDENTIAL_CREATED,
            cx,
            CredentialCreated { credential_id: id },
        )
        .await;
        Ok(id)
    }

    pub async fn update_credential(
        &self,
        cx: &DispatchCx,
        id: i64,
        data: models::CredentialUpdate,
    ) -> Result<(), UseCaseError> {
        self.get_credential(id)
            .await?
            .ok_or(UseCaseError::NotFound {
                entity: "credential",
                id,
            })?;
        let payload = CredentialUpdate {
            credential_id: id,
            data,
        };
        let payload = self.intercept(&CORE_CREDENTIAL_UPDATE, cx, payload).await;
        self.storage().credential_update(id, payload.data).await?;
        self.notify(
            &CORE_CREDENTIAL_UPDATED,
            cx,
            CredentialUpdated { credential_id: id },
        )
        .await;
        Ok(())
    }

    pub async fn delete_credential(&self, cx: &DispatchCx, id: i64) -> Result<(), UseCaseError> {
        self.get_credential(id)
            .await?
            .ok_or(UseCaseError::NotFound {
                entity: "credential",
                id,
            })?;
        let payload = CredentialDelete { credential_id: id };
        let payload = self.intercept(&CORE_CREDENTIAL_DELETE, cx, payload).await;
        self.storage()
            .credential_delete(payload.credential_id)
            .await?;
        self.notify(
            &CORE_CREDENTIAL_DELETED,
            cx,
            CredentialDeleted {
                credential_id: payload.credential_id,
            },
        )
        .await;
        Ok(())
    }

    pub async fn get_credential(&self, id: i64) -> Result<Option<Credentials>, UseCaseError> {
        self.storage().credential_get(id).await.map_err(Into::into)
    }

    pub async fn list_credentials_by_provider(
        &self,
        provider_id: i64,
    ) -> Result<Vec<Credentials>, UseCaseError> {
        self.storage()
            .credential_list_by_provider(provider_id)
            .await
            .map_err(Into::into)
    }
}
