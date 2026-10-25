//! 配置实体的唯一变更入口

pub mod credential;
pub mod error;
pub mod model;
pub mod provider;
pub mod topic;

use self::credential::{
    CORE_CREDENTIAL_CREATE, CORE_CREDENTIAL_CREATED, CORE_CREDENTIAL_DELETE,
    CORE_CREDENTIAL_DELETED, CORE_CREDENTIAL_UPDATE, CORE_CREDENTIAL_UPDATED,
};
use self::model::{
    CORE_MODEL_CREATE, CORE_MODEL_CREATED, CORE_MODEL_DELETE, CORE_MODEL_DELETED,
    CORE_MODEL_UPDATE, CORE_MODEL_UPDATED,
};
use self::provider::{
    CORE_PROVIDER_CREATE, CORE_PROVIDER_CREATED, CORE_PROVIDER_DELETE, CORE_PROVIDER_DELETED,
    CORE_PROVIDER_UPDATE, CORE_PROVIDER_UPDATED,
};
use self::topic::{
    CORE_TOPIC_CREATE, CORE_TOPIC_CREATED, CORE_TOPIC_DELETE, CORE_TOPIC_DELETED,
    CORE_TOPIC_UPDATE, CORE_TOPIC_UPDATED,
};
use super::storage::Storage;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use wind_event::context::DispatchCx;
use wind_event::def::{Directory, EventDef};
use wind_event::dispatch::Dispatcher;

#[derive(Clone)]
pub struct UseCases {
    storage: Storage,
    bus: Arc<Dispatcher>,
}

impl UseCases {
    pub fn new(storage: Storage, bus: Arc<Dispatcher>) -> Self {
        Self { storage, bus }
    }

    pub(super) fn storage(&self) -> &Storage {
        &self.storage
    }

    pub(super) fn bus(&self) -> &Dispatcher {
        &self.bus
    }

    pub(super) async fn intercept<P>(&self, def: &EventDef<P>, cx: &DispatchCx, payload: P) -> P
    where
        P: Serialize + DeserializeOwned + Send + Sync,
    {
        self.bus().decide(def, cx, payload).await
    }

    pub(super) async fn notify<P>(&self, def: &EventDef<P>, cx: &DispatchCx, payload: P)
    where
        P: Serialize + Send + Sync,
    {
        self.bus().notify(def, cx, payload).await;
    }
}

pub(super) fn schema<T: schemars::JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("the generated schema is valid JSON")
}

pub fn directory() -> Directory {
    Directory::from_metas([
        CORE_TOPIC_CREATE.meta(),
        CORE_TOPIC_CREATED.meta(),
        CORE_TOPIC_UPDATE.meta(),
        CORE_TOPIC_UPDATED.meta(),
        CORE_TOPIC_DELETE.meta(),
        CORE_TOPIC_DELETED.meta(),
        CORE_PROVIDER_CREATE.meta(),
        CORE_PROVIDER_CREATED.meta(),
        CORE_PROVIDER_UPDATE.meta(),
        CORE_PROVIDER_UPDATED.meta(),
        CORE_PROVIDER_DELETE.meta(),
        CORE_PROVIDER_DELETED.meta(),
        CORE_CREDENTIAL_CREATE.meta(),
        CORE_CREDENTIAL_CREATED.meta(),
        CORE_CREDENTIAL_UPDATE.meta(),
        CORE_CREDENTIAL_UPDATED.meta(),
        CORE_CREDENTIAL_DELETE.meta(),
        CORE_CREDENTIAL_DELETED.meta(),
        CORE_MODEL_CREATE.meta(),
        CORE_MODEL_CREATED.meta(),
        CORE_MODEL_UPDATE.meta(),
        CORE_MODEL_UPDATED.meta(),
        CORE_MODEL_DELETE.meta(),
        CORE_MODEL_DELETED.meta(),
    ])
}
