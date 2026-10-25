use super::UseCases;
use super::error::UseCaseError;
use super::schema;
use crate::models::{self, Topic, TopicCreate};
use serde::{Deserialize, Serialize};
use wind_event::context::DispatchCx;
use wind_event::def::{EventDef, Mount, Shape};

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TopicUpdate {
    pub topic_id: i64,
    pub data: models::TopicUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TopicDelete {
    pub topic_id: i64,
}

pub const CORE_TOPIC_CREATE: EventDef<TopicCreate> = EventDef::new(
    "core/topic.create",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<TopicCreate>,
);

pub const CORE_TOPIC_UPDATE: EventDef<TopicUpdate> = EventDef::new(
    "core/topic.update",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<TopicUpdate>,
);

pub const CORE_TOPIC_DELETE: EventDef<TopicDelete> = EventDef::new(
    "core/topic.delete",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<TopicDelete>,
);

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TopicCreated {
    pub topic_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TopicUpdated {
    pub topic_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TopicDeleted {
    pub topic_id: i64,
}

pub const CORE_TOPIC_CREATED: EventDef<TopicCreated> = EventDef::new(
    "core/topic.created",
    Shape::Observe,
    true,
    Mount::UseCase,
    schema::<TopicCreated>,
);

pub const CORE_TOPIC_UPDATED: EventDef<TopicUpdated> = EventDef::new(
    "core/topic.updated",
    Shape::Observe,
    true,
    Mount::UseCase,
    schema::<TopicUpdated>,
);

pub const CORE_TOPIC_DELETED: EventDef<TopicDeleted> = EventDef::new(
    "core/topic.deleted",
    Shape::Observe,
    true,
    Mount::UseCase,
    schema::<TopicDeleted>,
);

impl UseCases {
    pub async fn create_topic(
        &self,
        cx: &DispatchCx,
        input: TopicCreate,
    ) -> Result<i64, UseCaseError> {
        let payload = self.intercept(&CORE_TOPIC_CREATE, cx, input).await;
        let id = self.storage().topic_create(payload).await?;
        self.notify(&CORE_TOPIC_CREATED, cx, TopicCreated { topic_id: id })
            .await;
        Ok(id)
    }

    pub async fn update_topic(
        &self,
        cx: &DispatchCx,
        id: i64,
        data: models::TopicUpdate,
    ) -> Result<(), UseCaseError> {
        self.get_topic(id).await?.ok_or(UseCaseError::NotFound {
            entity: "topic",
            id,
        })?;
        let payload = TopicUpdate { topic_id: id, data };
        let payload = self.intercept(&CORE_TOPIC_UPDATE, cx, payload).await;
        self.storage().topic_update(id, payload.data).await?;
        self.notify(&CORE_TOPIC_UPDATED, cx, TopicUpdated { topic_id: id })
            .await;
        Ok(())
    }

    pub async fn delete_topic(&self, cx: &DispatchCx, id: i64) -> Result<(), UseCaseError> {
        self.get_topic(id).await?.ok_or(UseCaseError::NotFound {
            entity: "topic",
            id,
        })?;
        let payload = TopicDelete { topic_id: id };
        let payload = self.intercept(&CORE_TOPIC_DELETE, cx, payload).await;
        self.storage().topic_delete(payload.topic_id).await?;
        self.notify(
            &CORE_TOPIC_DELETED,
            cx,
            TopicDeleted {
                topic_id: payload.topic_id,
            },
        )
        .await;
        Ok(())
    }

    pub async fn get_topic(&self, id: i64) -> Result<Option<Topic>, UseCaseError> {
        self.storage().topic_get(id).await.map_err(Into::into)
    }

    pub async fn list_topics(&self) -> Result<Vec<Topic>, UseCaseError> {
        self.storage().topic_list().await.map_err(Into::into)
    }
}
