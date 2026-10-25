//! 一次调用期间 mod 的状态视图

use super::STATE_KIND;
use super::split_scope;
use std::collections::{BTreeMap, BTreeSet};
use wind_core::error::CoreError;
use wind_core::storage::Storage;
use wind_event::envelope::Scope;

/// 私有键在存储里的种类
const STORAGE_KIND: &str = "storage";

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Flush {
    /// 对外状态里发生变化的键，删除以空值表示
    pub changed_state: Vec<(String, serde_json::Value)>,
    pub changed_storage: Vec<(String, serde_json::Value)>,
}

/// js 执行时产生的状态视图
pub struct StateSnapshot {
    storage: Storage,
    mod_id: i64,
    scope_kind: &'static str,
    scope_id: i64,
    state: BTreeMap<String, serde_json::Value>,
    private: BTreeMap<String, serde_json::Value>,
    dirty_state: BTreeSet<String>,
    dirty_private: BTreeSet<String>,
}

impl StateSnapshot {
    pub async fn open(storage: Storage, mod_id: i64, scope: Scope) -> Result<Self, CoreError> {
        let (scope_kind, scope_id) = split_scope(&scope);
        let rows = storage.mod_list_state(mod_id).await?;
        let mut state = BTreeMap::new();
        let mut private = BTreeMap::new();
        for row in rows {
            if row.scope_kind != scope_kind || row.scope_id != scope_id {
                continue;
            }
            match row.kind.as_str() {
                STATE_KIND => {
                    state.insert(row.key, row.value);
                }
                STORAGE_KIND => {
                    private.insert(row.key, row.value);
                }
                _ => {}
            }
        }
        Ok(Self {
            storage,
            mod_id,
            scope_kind,
            scope_id,
            state,
            private,
            dirty_state: BTreeSet::new(),
            dirty_private: BTreeSet::new(),
        })
    }

    pub fn state(&self, key: &str) -> Option<&serde_json::Value> {
        self.state.get(key)
    }

    pub fn storage(&self, key: &str) -> Option<&serde_json::Value> {
        self.private.get(key)
    }

    pub fn put_state(&mut self, key: &str, value: serde_json::Value) {
        self.state.insert(key.to_string(), value);
        self.dirty_state.insert(key.to_string());
    }

    pub fn put_storage(&mut self, key: &str, value: serde_json::Value) {
        self.private.insert(key.to_string(), value);
        self.dirty_private.insert(key.to_string());
    }

    pub fn remove_state(&mut self, key: &str) {
        self.state.remove(key);
        self.dirty_state.insert(key.to_string());
    }

    pub fn remove_storage(&mut self, key: &str) {
        self.private.remove(key);
        self.dirty_private.insert(key.to_string());
    }

    pub fn keys_state(&self) -> Vec<&str> {
        self.state.keys().map(String::as_str).collect()
    }

    pub fn keys_storage(&self) -> Vec<&str> {
        self.private.keys().map(String::as_str).collect()
    }

    pub async fn flush(mut self) -> Result<Flush, CoreError> {
        let mut upserts = Vec::new();
        let mut deletes = Vec::new();
        let mut changed_state = Vec::new();
        let mut changed_storage = Vec::new();

        for key in std::mem::take(&mut self.dirty_state) {
            match self.state.get(&key) {
                Some(value) => {
                    upserts.push((STATE_KIND, key.clone(), value.clone()));
                    changed_state.push((key, value.clone()));
                }
                None => {
                    deletes.push((STATE_KIND, key.clone()));
                    changed_state.push((key, serde_json::Value::Null));
                }
            }
        }
        for key in std::mem::take(&mut self.dirty_private) {
            match self.private.get(&key) {
                Some(value) => {
                    upserts.push((STORAGE_KIND, key.clone(), value.clone()));
                    changed_storage.push((key, value.clone()));
                }
                None => {
                    deletes.push((STORAGE_KIND, key.clone()));
                    changed_storage.push((key, serde_json::Value::Null));
                }
            }
        }

        // TODO: 独立的 sqlite db
        self.storage
            .mod_write_state(
                self.mod_id,
                self.scope_kind,
                self.scope_id,
                &upserts,
                &deletes,
            )
            .await?;

        Ok(Flush {
            changed_state,
            changed_storage,
        })
    }
}
