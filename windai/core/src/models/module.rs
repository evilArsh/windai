use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// mod 基础信息
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Mod {
    pub id: i64,
    pub name: String,
    pub current_version: i64,
    pub created_at: i64,
}

impl_display!(Mod {
    id,
    name,
    current_version,
    created_at
});

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModVersion {
    pub mod_id: i64,
    pub version: i64,
    pub manifest: String,
    pub compiled: Option<serde_json::Value>,
    pub engine_fingerprint: Option<String>,
    pub created_at: i64,
}

impl_display!(ModVersion {
    mod_id,
    version,
    manifest,
    compiled,
    engine_fingerprint,
    created_at
});

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledScripts {
    pub modules: BTreeMap<String, Vec<u8>>,
    pub fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModStateEntry {
    pub mod_id: i64,
    pub scope_kind: String,
    /// 全局作用域为 0
    pub scope_id: i64,
    /// 区分对外状态与私有存储
    pub kind: String,
    pub key: String,
    pub value: serde_json::Value,
    pub updated_at: i64,
}

impl_display!(ModStateEntry {
    mod_id,
    scope_kind,
    scope_id,
    kind,
    key,
    value,
    updated_at
});
