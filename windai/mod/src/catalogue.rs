//! 话题启用过的 mod 在请求链上的投影

use super::manifest::{CallableBy, Manifest, parse};
use super::prompt::declared_fragments;
use wind_core::error::CoreError;
use wind_core::llm::ports::{DeclaredTool, FunctionTool, ToolOrigin, Tools, TopicMods};
use wind_core::storage::Storage;
use wind_event::def::Directory;

/// 一个话题启用过的 mod
pub struct Catalogue {
    storage: Storage,
    directory: Directory,
}

impl Catalogue {
    pub fn new(storage: Storage, directory: Directory) -> Self {
        Self { storage, directory }
    }

    async fn manifest_of(&self, name: &str) -> Result<Manifest, CoreError> {
        let row = self
            .storage
            .mod_get_by_name(name)
            .await?
            .ok_or_else(|| CoreError::Internal(format!("mod {name} has no installed row")))?;
        let current = row.current_version;
        let stored = self
            .storage
            .mod_get_version(row.id, current)
            .await?
            .ok_or_else(|| {
                CoreError::Internal(format!(
                    "mod {name} gives version {current} as its current one, but that version is absent"
                ))
            })?;
        parse(&stored.manifest, &self.directory).map_err(|error| {
            CoreError::Internal(format!(
                "mod {name} carries an unreadable manifest: {error}"
            ))
        })
    }

    async fn manifests_of(&self, topic_id: i64) -> Result<Vec<Manifest>, CoreError> {
        let names = self.storage.mod_enabled_in(topic_id).await?;
        let mut manifests = Vec::with_capacity(names.len());
        for name in names {
            manifests.push(self.manifest_of(&name).await?);
        }
        Ok(manifests)
    }
}

#[async_trait::async_trait]
impl TopicMods for Catalogue {
    async fn prompt_fragments(&self, topic_id: i64) -> Result<Vec<String>, CoreError> {
        let manifests = self.manifests_of(topic_id).await?;
        Ok(declared_fragments(&manifests))
    }

    async fn agent_tools(&self, topic_id: i64) -> Result<Vec<DeclaredTool>, CoreError> {
        let manifests = self.manifests_of(topic_id).await?;
        let mut declared = Vec::new();
        for manifest in manifests {
            for (request_name, request) in &manifest.requests {
                if !request.callable_by.contains(&CallableBy::Agent) {
                    continue;
                }
                let origin = ToolOrigin::ModRequest {
                    mod_name: manifest.name.clone(),
                    request: request_name.clone(),
                };
                declared.push(DeclaredTool {
                    declaration: Tools::Function(FunctionTool {
                        name: origin.exposed(),
                        description: request.summary.clone(),
                        parameters: request.input.clone(),
                        strict: None,
                    }),
                    origin,
                });
            }
        }
        Ok(declared)
    }

    async fn version_of(&self, mod_name: &str) -> Result<i64, CoreError> {
        let row = self
            .storage
            .mod_get_by_name(mod_name)
            .await?
            .ok_or_else(|| CoreError::Internal(format!("mod {mod_name} has no installed row")))?;
        Ok(row.current_version)
    }
}
