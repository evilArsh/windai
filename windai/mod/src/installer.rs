use super::assembly::rebuild_subscriptions;
use super::compiler::{CompiledScripts, Compiler};
use super::dirs::AppDirs;
use super::enablement::Enablement;
use super::events::{
    CORE_MOD_DELETE, CORE_MOD_DELETED, CORE_MOD_INSTALL, CORE_MOD_INSTALLED, CORE_MOD_UPDATE,
    CORE_MOD_UPDATED, ModDelete, ModDeleted, ModInstall, ModInstalled, ModUpdate, ModUpdated,
};
use super::feed::ModFeed;
use super::manifest::{DEFAULT_SUB_TIMEOUT_MS, Manifest, ManifestError, parse};
use super::package::{Package, PackageError};
use serde::Serialize;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use wind_core::error::CoreError;
use wind_core::models::{Mod, ModVersion};
use wind_core::storage::Storage;
use wind_event::context::DispatchCx;
use wind_event::def::EventDef;
use wind_event::dispatch::Dispatcher;

/// 首次安装的版本号
const FIRST_VERSION: i64 = 1;

fn handlers_of(manifest: &Manifest) -> BTreeSet<String> {
    let mut handlers = BTreeSet::new();
    for request in manifest.requests.values() {
        handlers.insert(request.handler.clone());
    }
    for sub in manifest.events.on.values() {
        handlers.insert(sub.handler.clone());
    }
    handlers
}

#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    #[error(transparent)]
    Package(#[from] PackageError),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error("the package at {path} cannot be accepted: {reason}")]
    Mismatch { path: String, reason: String },
    #[error("{name} is already installed")]
    AlreadyInstalled { name: String },
    #[error("{name} is not installed")]
    NotInstalled { name: String },
    #[error("the package at {path} was refused: {reason}")]
    Rejected { path: String, reason: String },
    #[error("event bus error: {0}")]
    Bus(String),
    #[error("topic servers refused: {0}")]
    Servers(String),
    #[error(transparent)]
    Storage(#[from] CoreError),
}

/// mod 安装器接口
#[async_trait::async_trait]
pub trait TopicServers: Send + Sync {
    async fn attach(&self, cx: &DispatchCx, topic_id: i64, mod_name: &str) -> Result<(), String>;
    async fn detach(&self, cx: &DispatchCx, topic_id: i64, mod_name: &str) -> Result<(), String>;
    async fn refresh(&self, cx: &DispatchCx, topic_id: i64) -> Result<(), String>;
}

pub struct NoServers;

#[async_trait::async_trait]
impl TopicServers for NoServers {
    async fn attach(
        &self,
        _cx: &DispatchCx,
        _topic_id: i64,
        _mod_name: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn detach(
        &self,
        _cx: &DispatchCx,
        _topic_id: i64,
        _mod_name: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn refresh(&self, _cx: &DispatchCx, _topic_id: i64) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct Installer {
    storage: Storage,
    bus: Arc<Dispatcher>,
    dirs: AppDirs,
    servers: Arc<dyn TopicServers>,
    enablement: Arc<Enablement>,
    feed: Arc<ModFeed>,
    compiler: Arc<dyn Compiler>,
}

impl Installer {
    pub fn new(
        storage: Storage,
        bus: Arc<Dispatcher>,
        dirs: AppDirs,
        servers: Arc<dyn TopicServers>,
        enablement: Arc<Enablement>,
        feed: Arc<ModFeed>,
        compiler: Arc<dyn Compiler>,
    ) -> Self {
        Self {
            storage,
            bus,
            dirs,
            servers,
            enablement,
            feed,
            compiler,
        }
    }

    pub async fn get(&self, name: &str) -> Result<Option<Mod>, InstallError> {
        Ok(self.storage.mod_get_by_name(name).await?)
    }

    pub async fn versions(&self, name: &str) -> Result<Vec<ModVersion>, InstallError> {
        let row = self.require(name).await?;
        Ok(self.storage.mod_list_versions(row.id).await?)
    }

    pub async fn install(&self, cx: &DispatchCx, package: &Path) -> Result<i64, InstallError> {
        let package = Package::open(package)?;
        let manifest = parse(package.manifest_raw(), self.bus.directory())?;
        self.check_location(&package, &manifest)?;
        if manifest.version != FIRST_VERSION {
            return Err(InstallError::Mismatch {
                path: package.root.display().to_string(),
                reason: format!("a first install accepts version {FIRST_VERSION} only"),
            });
        }
        if self
            .storage
            .mod_get_by_name(&manifest.name)
            .await?
            .is_some()
        {
            return Err(InstallError::AlreadyInstalled {
                name: manifest.name.clone(),
            });
        }
        let name = manifest.name.clone();
        let version = manifest.version;
        self.notify(
            cx,
            &CORE_MOD_INSTALL,
            ModInstall {
                name: name.clone(),
                version,
                manifest: serde_json::json!(&manifest),
            },
        )
        .await;

        let compiled = self.compile(&package, &manifest)?;
        let id = self
            .storage
            .mod_install(&name, package.manifest_raw(), compiled.as_ref())
            .await?;
        self.rebuild_subscriptions().await?;
        self.notify(
            cx,
            &CORE_MOD_INSTALLED,
            ModInstalled {
                name,
                version,
                manifest: serde_json::json!(&manifest),
                ui_entry: self.ui_entry(&package, &manifest),
            },
        )
        .await;
        Ok(id)
    }

    pub async fn update(&self, cx: &DispatchCx, package: &Path) -> Result<(), InstallError> {
        let package = Package::open(package)?;
        let manifest = parse(package.manifest_raw(), self.bus.directory())?;
        self.check_location(&package, &manifest)?;
        let row = self.require(&manifest.name).await?;
        if manifest.version <= row.current_version {
            return Err(InstallError::Mismatch {
                path: package.root.display().to_string(),
                reason: format!(
                    "its version {} must be greater than the current {}",
                    manifest.version, row.current_version
                ),
            });
        }
        self.notify(
            cx,
            &CORE_MOD_UPDATE,
            ModUpdate {
                name: manifest.name.clone(),
                from_version: row.current_version,
                to_version: manifest.version,
            },
        )
        .await;
        let compiled = self.compile(&package, &manifest)?;
        self.storage
            .mod_publish_current(
                row.id,
                manifest.version,
                package.manifest_raw(),
                compiled.as_ref(),
            )
            .await?;
        self.enablement.refresh(&self.storage).await?;
        self.rebuild_subscriptions().await?;
        let topics = self.storage.mod_topics(row.id).await?;
        for topic_id in topics {
            self.servers
                .refresh(cx, topic_id)
                .await
                .map_err(InstallError::Servers)?;
        }
        self.notify(
            cx,
            &CORE_MOD_UPDATED,
            ModUpdated {
                name: manifest.name.clone(),
                from_version: row.current_version,
                to_version: manifest.version,
                ui_entry: self.ui_entry(&package, &manifest),
            },
        )
        .await;
        Ok(())
    }

    pub async fn delete(&self, cx: &DispatchCx, name: &str) -> Result<(), InstallError> {
        let row = self.require(name).await?;
        let topics = self.storage.mod_topics(row.id).await?;
        self.notify(
            cx,
            &CORE_MOD_DELETE,
            ModDelete {
                name: name.to_string(),
                version: row.current_version,
            },
        )
        .await;
        self.storage.mod_delete(row.id).await?;
        self.feed.forget(row.id);
        self.enablement.refresh(&self.storage).await?;
        self.rebuild_subscriptions().await?;
        for topic_id in topics {
            self.servers
                .detach(cx, topic_id, name)
                .await
                .map_err(InstallError::Servers)?;
        }
        self.notify(
            cx,
            &CORE_MOD_DELETED,
            ModDeleted {
                name: name.to_string(),
                version: row.current_version,
            },
        )
        .await;
        Ok(())
    }

    pub async fn enable(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
        name: &str,
    ) -> Result<(), InstallError> {
        let row = self.require(name).await?;
        self.storage.mod_enable(topic_id, row.id).await?;
        self.enablement.refresh(&self.storage).await?;
        self.servers
            .attach(cx, topic_id, name)
            .await
            .map_err(InstallError::Servers)?;
        Ok(())
    }

    pub async fn disable(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
        name: &str,
    ) -> Result<(), InstallError> {
        let row = self.require(name).await?;
        self.storage.mod_disable(topic_id, row.id).await?;
        self.enablement.refresh(&self.storage).await?;
        self.servers
            .detach(cx, topic_id, name)
            .await
            .map_err(InstallError::Servers)?;
        Ok(())
    }

    fn compile(
        &self,
        package: &Package,
        manifest: &Manifest,
    ) -> Result<Option<CompiledScripts>, InstallError> {
        self.compiler
            .compile(&package.root, &handlers_of(manifest))
            .map_err(|reason| InstallError::Rejected {
                path: package.root.display().to_string(),
                reason,
            })
    }

    async fn rebuild_subscriptions(&self) -> Result<(), InstallError> {
        rebuild_subscriptions(&self.storage, &self.bus, DEFAULT_SUB_TIMEOUT_MS).await?;
        Ok(())
    }

    async fn require(&self, name: &str) -> Result<Mod, InstallError> {
        self.storage
            .mod_get_by_name(name)
            .await?
            .ok_or_else(|| InstallError::NotInstalled {
                name: name.to_string(),
            })
    }

    fn ui_entry(&self, package: &Package, manifest: &Manifest) -> Option<String> {
        if !package.ui_entry().is_file() {
            return None;
        }
        Some(format!(
            "windai://mods/{}/{}/ui/index.js",
            manifest.name, manifest.version
        ))
    }

    fn check_location(&self, package: &Package, manifest: &Manifest) -> Result<(), InstallError> {
        let expected = self.dirs.mod_version(&manifest.name, manifest.version);
        let actual = &package.root;
        if *actual != expected {
            return Err(InstallError::Mismatch {
                path: actual.display().to_string(),
                reason: format!("it belongs at {}", expected.display()),
            });
        }
        Ok(())
    }

    async fn notify<P>(&self, cx: &DispatchCx, def: &EventDef<P>, payload: P)
    where
        P: Serialize + Send + Sync,
    {
        self.bus.notify(def, cx, payload).await;
    }
}
