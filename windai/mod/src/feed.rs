//! mod 的数据面（`ModFeed`）

use super::STATE_KIND;
use super::manifest::{Delivery, Manifest};
use super::split_scope;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use wind_core::error::CoreError;
use wind_core::feed::hub::{BaselineProvider, FeedHub};
use wind_core::feed::queue::Channel;
use wind_core::storage::Storage;
use wind_event::context::DispatchCx;
use wind_event::dispatch::Dispatcher;
use wind_event::envelope::{EventEnvelope, Scope, Source};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StateKey {
    mod_id: i64,
    scope_kind: String,
    scope_id: i64,
    event: String,
}

#[derive(Debug, thiserror::Error)]
pub enum FeedError {
    #[error("channel {name} is not declared by {mod_name}")]
    Undeclared { mod_name: String, name: String },
    #[error("payload for {name} is rejected by its declared shape: {reason}")]
    Payload { name: String, reason: String },
    #[error(transparent)]
    Storage(#[from] CoreError),
}

/// Mod 事件总线
pub struct ModFeed {
    hub: Arc<FeedHub>,
    bus: Arc<Dispatcher>,
    storage: Storage,
    states: Mutex<HashMap<StateKey, serde_json::Value>>,
}

impl ModFeed {
    pub async fn open(
        hub: Arc<FeedHub>,
        bus: Arc<Dispatcher>,
        storage: Storage,
    ) -> Result<Self, CoreError> {
        let feed = Self {
            hub,
            bus,
            storage,
            states: Mutex::new(HashMap::new()),
        };
        feed.reload().await?;
        Ok(feed)
    }

    async fn reload(&self) -> Result<(), CoreError> {
        let declared = self.declared_channels().await?;
        let rows = self.storage.mod_list_state_kind(STATE_KIND).await?;
        let mut states = self
            .states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        states.clear();
        for row in rows {
            let key = StateKey {
                mod_id: row.mod_id,
                scope_kind: row.scope_kind,
                scope_id: row.scope_id,
                event: row.key,
            };
            if declared.contains(&(key.mod_id, key.event.clone())) {
                states.insert(key, row.value);
            }
        }
        Ok(())
    }

    async fn declared_channels(&self) -> Result<HashSet<(i64, String)>, CoreError> {
        let mut declared = HashSet::new();
        for (mod_id, raw) in self.storage.mod_current_manifests().await? {
            let manifest: Manifest = match serde_json::from_str(&raw) {
                Ok(manifest) => manifest,
                Err(error) => {
                    log::warn!("[mod] mod {mod_id} carries an unreadable manifest: {error}");
                    continue;
                }
            };
            for (_, key) in manifest.stateful_channels() {
                declared.insert((mod_id, key));
            }
        }
        Ok(declared)
    }

    pub fn forget(&self, mod_id: i64) {
        self.states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|key, _| key.mod_id != mod_id);
    }

    pub async fn emit(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
        manifest: &Manifest,
        name: &str,
        payload: serde_json::Value,
    ) -> Result<(), FeedError> {
        let decl = manifest
            .events
            .emit
            .get(name)
            .ok_or_else(|| FeedError::Undeclared {
                mod_name: manifest.name.clone(),
                name: name.to_string(),
            })?;
        if let Some(schema) = &decl.payload {
            check_payload(name, schema, &payload)?;
        }
        let event_name = manifest.event_name(name);
        let scope = Scope::Topic { topic_id };
        let (scope_kind, scope_id) = split_scope(&scope);

        if decl.delivery == Delivery::Stateful {
            let row = self
                .storage
                .mod_get_by_name(&manifest.name)
                .await?
                .ok_or_else(|| {
                    FeedError::Storage(CoreError::Internal(format!(
                        "{} has no installed row",
                        manifest.name
                    )))
                })?;
            self.storage
                .mod_put_state(
                    row.id,
                    scope_kind,
                    scope_id,
                    STATE_KIND,
                    &event_name,
                    payload.clone(),
                )
                .await?;
            self.states
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(
                    StateKey {
                        mod_id: row.id,
                        scope_kind: scope_kind.to_string(),
                        scope_id,
                        event: event_name.clone(),
                    },
                    payload.clone(),
                );
        }

        if !self.bus.allow_mod_emit(&manifest.name) {
            log::warn!("[mod] emitting {event_name} was rejected by the emit rate limit");
            return Ok(());
        }

        let seq = self.bus.emit_seq(cx).map_err(CoreError::from)?;
        let version = source_version(manifest)?;
        let envelope = EventEnvelope {
            seq,
            time: EventEnvelope::<()>::now_ms(),
            name: event_name.clone(),
            scope,
            origin: cx.origin,
            source: Source::mod_(manifest.name.clone(), version),
            cause: cx.cause,
            payload,
        };
        let envelope = serde_json::to_value(envelope).map_err(|error| FeedError::Payload {
            name: name.to_string(),
            reason: error.to_string(),
        })?;
        self.hub
            .publish(&event_name, channel_of(decl.delivery), true, envelope);
        Ok(())
    }

    pub async fn retract(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
        manifest: &Manifest,
        name: &str,
    ) -> Result<(), FeedError> {
        let decl = manifest
            .events
            .emit
            .get(name)
            .ok_or_else(|| FeedError::Undeclared {
                mod_name: manifest.name.clone(),
                name: name.to_string(),
            })?;
        if decl.delivery != Delivery::Stateful {
            return Ok(());
        }
        let event_name = manifest.event_name(name);
        let (scope_kind, scope_id) = split_scope(&Scope::Topic { topic_id });
        let row = self
            .storage
            .mod_get_by_name(&manifest.name)
            .await?
            .ok_or_else(|| {
                FeedError::Storage(CoreError::Internal(format!(
                    "{} has no installed row",
                    manifest.name
                )))
            })?;
        self.storage
            .mod_delete_state(row.id, scope_kind, scope_id, STATE_KIND, &event_name)
            .await?;
        self.states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&StateKey {
                mod_id: row.id,
                scope_kind: scope_kind.to_string(),
                scope_id,
                event: event_name.clone(),
            });

        if !self.bus.allow_mod_emit(&manifest.name) {
            log::warn!("[mod] retracting {event_name} was rejected by the emit rate limit");
            return Ok(());
        }
        let seq = self.bus.emit_seq(cx).map_err(CoreError::from)?;
        let version = source_version(manifest)?;
        let envelope = EventEnvelope {
            seq,
            time: EventEnvelope::<()>::now_ms(),
            name: event_name.clone(),
            scope: Scope::Topic { topic_id },
            origin: cx.origin,
            source: Source::mod_(manifest.name.clone(), version),
            cause: cx.cause,
            payload: serde_json::Value::Null,
        };
        let envelope = serde_json::to_value(envelope).map_err(|error| FeedError::Payload {
            name: name.to_string(),
            reason: error.to_string(),
        })?;
        self.hub
            .publish(&event_name, Channel::Stateful, true, envelope);
        Ok(())
    }

    pub fn stateful_baselines(&self, scope: &Scope) -> Vec<(String, serde_json::Value)> {
        let (scope_kind, scope_id) = split_scope(scope);
        let states = self
            .states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        states
            .iter()
            .filter(|(key, _)| key.scope_kind == scope_kind && key.scope_id == scope_id)
            .map(|(key, value)| (key.event.clone(), value.clone()))
            .collect()
    }
}

impl BaselineProvider for ModFeed {
    fn stateful(&self, scope: &Scope) -> Vec<(String, serde_json::Value)> {
        self.stateful_baselines(scope)
    }
}

fn source_version(manifest: &Manifest) -> Result<u32, FeedError> {
    u32::try_from(manifest.version).map_err(|_| {
        FeedError::Storage(CoreError::Internal(format!(
            "version {} does not fit a source version",
            manifest.version
        )))
    })
}

fn channel_of(delivery: Delivery) -> Channel {
    match delivery {
        Delivery::Stateful => Channel::Stateful,
        Delivery::Notice => Channel::Journal,
    }
}

fn check_payload(
    name: &str,
    schema: &serde_json::Value,
    payload: &serde_json::Value,
) -> Result<(), FeedError> {
    if schema.get("type").and_then(|t| t.as_str()) == Some("array") && !payload.is_array() {
        return Err(FeedError::Payload {
            name: name.to_string(),
            reason: "declared an array".to_string(),
        });
    }
    if schema.get("type").and_then(|t| t.as_str()) == Some("object") && !payload.is_object() {
        return Err(FeedError::Payload {
            name: name.to_string(),
            reason: "declared an object".to_string(),
        });
    }
    Ok(())
}
