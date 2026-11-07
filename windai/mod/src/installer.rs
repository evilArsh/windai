use crate::{
    error::{ModError, Result},
    events::{
        CORE_MOD_INSTALL, CORE_MOD_INSTALLED, CORE_MOD_UPDATE, CORE_MOD_UPDATED, ModInstall,
        ModInstalled, ModUpdate, ModUpdated,
    },
    manifest::{DEFAULT_SUB_TIMEOUT_MS, Manifest},
    package::{Package, UI_ENTRY},
    roster::Roster,
};
use std::{collections::HashSet, path::Path, sync::Arc};
use wind_event::{DispatchCx, Dispatcher, Subscription};
use wind_storage::{Mod, ModVersion, Storage};
use wind_wire::CompiledScripts;

use crate::{Compiler, dirs::AppDirs};
pub(crate) const FIRST_VERSION: u32 = 1;

/// mod 安装器
#[derive(Clone)]
pub struct Installer {
    storage: Storage,
    bus: Arc<Dispatcher>,
    dirs: AppDirs,
    /// 
    // servers: Arc<dyn TopicServers>,
    roster: Arc<Roster>,
    /// TODO: 暂不实现，等本模块完成后再考虑
    compiler: Arc<dyn Compiler>,
}

impl Installer {
    pub fn new(
        storage: Storage,
        bus: Arc<Dispatcher>,
        roster: Arc<Roster>,
        dirs: AppDirs,
        compiler: Arc<dyn Compiler>,
    ) -> Self {
        Self {
            storage,
            bus,
            dirs,
            roster,
            compiler,
        }
    }

    pub async fn install(&self, cx: &DispatchCx, package: &Path) -> Result<i64> {
        let package = Package::open(package)?;
        let manifest = Manifest::parse(&package.manifest_raw)?;
        if manifest.version != FIRST_VERSION {
            return Err(ModError::Mismatch {
                path: package.root.display().to_string(),
                reason: format!("a first install accepts version {FIRST_VERSION} only"),
            });
        }
        if self.get(&manifest.name).await?.is_some() {
            return Err(ModError::AlreadyInstalled {
                name: manifest.name.clone(),
            });
        }
        let name = manifest.name.clone();
        let version = manifest.version;
        self.bus.notify(
            &CORE_MOD_INSTALL,
            cx,
            ModInstall {
                name: name.clone(),
                version,
                manifest: serde_json::json!(&manifest),
            },
        );

        // TODO: 事务?
        let compiled = self.compile(&package, &manifest)?;
        let id = self
            .storage
            .install_mod(&name, &package.manifest_raw, compiled.as_ref())
            .await?;
        self.rebuild_subscriptions(&self.storage).await?;

        self.bus.notify(
            &CORE_MOD_INSTALLED,
            cx,
            ModInstalled {
                name,
                version,
                manifest: serde_json::json!(&manifest),
                ui_entry: self.ui_entry(&package, &manifest),
            },
        );

        Ok(id)
    }

    pub async fn update(&self, cx: &DispatchCx, package: &Path) -> Result<()> {
        let package = Package::open(package)?;
        let manifest = Manifest::parse(&package.manifest_raw)?;
        self.check_location(&package, &manifest)?;
        let row = self.require(&manifest.name).await?;

        if manifest.version <= row.current_version {
            return Err(ModError::Mismatch {
                path: package.root.display().to_string(),
                reason: format!(
                    "its version {} must be greater than the current {}",
                    manifest.version, row.current_version
                ),
            });
        }

        self.bus.notify(
            &CORE_MOD_UPDATE,
            cx,
            ModUpdate {
                name: manifest.name.clone(),
                from_version: row.current_version,
                to_version: manifest.version,
            },
        );
        let compiled = self.compile(&package, &manifest)?;
        self.storage
            .publish_current_mod(
                row.id,
                manifest.version,
                &package.manifest_raw,
                compiled.as_ref(),
            )
            .await?;
        self.roster.refresh(&self.storage).await?;
        self.rebuild_subscriptions(&self.storage).await?;

        // let topics = self.storage.topics_with_mod(row.id).await?;
        // for topic_id in topics {
        //     self.servers
        //         .refresh(cx, topic_id)
        //         .await
        //         .map_err(InstallError::Servers)?;
        // }

        self.bus.notify(
            &CORE_MOD_UPDATED,
            cx,
            ModUpdated {
                name: manifest.name.clone(),
                from_version: row.current_version,
                to_version: manifest.version,
                ui_entry: self.ui_entry(&package, &manifest),
            },
        );
        Ok(())
    }

    /// TODO: 完善
    // pub async fn delete(&self, cx: &DispatchCx, name: &str) -> Result<(), InstallError> {
    //     let row = self.require(name).await?;
    //     let topics = self.storage.mod_topics(row.id).await?;
    //     self.notify(
    //         cx,
    //         &CORE_MOD_DELETE,
    //         ModDelete {
    //             name: name.to_string(),
    //             version: row.current_version,
    //         },
    //     )
    //     .await;
    //     self.storage.mod_delete(row.id).await?;
    //     self.feed.forget(row.id);
    //     self.enablement.refresh(&self.storage).await?;
    //     self.rebuild_subscriptions().await?;
    //     for topic_id in topics {
    //         self.servers
    //             .detach(cx, topic_id, name)
    //             .await
    //             .map_err(InstallError::Servers)?;
    //     }
    //     self.notify(
    //         cx,
    //         &CORE_MOD_DELETED,
    //         ModDeleted {
    //             name: name.to_string(),
    //             version: row.current_version,
    //         },
    //     )
    //     .await;
    //     Ok(())
    // }

    /// TODO:完善
    // pub async fn enable(
    //     &self,
    //     cx: &DispatchCx,
    //     topic_id: i64,
    //     name: &str,
    // ) -> Result<(), InstallError> {
    //     let row = self.require(name).await?;
    //     self.storage.mod_enable(topic_id, row.id).await?;
    //     self.enablement.refresh(&self.storage).await?;
    //     self.servers
    //         .attach(cx, topic_id, name)
    //         .await
    //         .map_err(InstallError::Servers)?;
    //     Ok(())
    // }

    /// TODO:完善
    // pub async fn disable(
    //     &self,
    //     cx: &DispatchCx,
    //     topic_id: i64,
    //     name: &str,
    // ) -> Result<(), InstallError> {
    //     let row = self.require(name).await?;
    //     self.storage.mod_disable(topic_id, row.id).await?;
    //     self.enablement.refresh(&self.storage).await?;
    //     self.servers
    //         .detach(cx, topic_id, name)
    //         .await
    //         .map_err(InstallError::Servers)?;
    //     Ok(())
    // }

    pub async fn get(&self, name: &str) -> Result<Option<Mod>> {
        Ok(self.storage.mod_by_name(name).await?)
    }
    async fn require(&self, name: &str) -> Result<Mod> {
        match self.get(name).await? {
            Some(m) => Ok(m),
            None => Err(ModError::NotInstalled {
                name: name.to_string(),
            }),
        }
    }

    pub async fn versions(&self, name: &str) -> Result<Vec<ModVersion>> {
        todo!()
    }

    fn compile(&self, package: &Package, manifest: &Manifest) -> Result<Option<CompiledScripts>> {
        self.compiler
            .compile(&package.root, &handlers_of(manifest))
            .map_err(|reason| ModError::Rejected {
                path: package.root.display().to_string(),
                reason,
            })
    }

    fn ui_entry(&self, package: &Package, manifest: &Manifest) -> Option<String> {
        if !package.ui_entry().is_file() {
            return None;
        }
        Some(format!(
            "windai://mods/{}/{}/{}",
            manifest.name, manifest.version, UI_ENTRY
        ))
    }

    fn check_location(&self, package: &Package, manifest: &Manifest) -> Result<()> {
        let expected = self.dirs.mod_version(&manifest.name, manifest.version);
        let actual = &package.root;
        if *actual != expected {
            return Err(ModError::Mismatch {
                path: actual.display().to_string(),
                reason: format!("it belongs at {}", expected.display()),
            });
        }
        Ok(())
    }

    async fn rebuild_subscriptions(&self, tx: &Storage) -> Result<()> {
        let mut subs: Vec<Subscription> = Vec::new();
        for (_, raw) in tx.current_mod_manifests().await? {
            match Manifest::parse(&raw) {
                Ok(manifest) => {
                    subs.extend(
                        manifest.subscriptions(self.bus.directory(), DEFAULT_SUB_TIMEOUT_MS),
                    );
                }
                Err(error) => {
                    log::warn!("[mod] an installed manifest is unreadable: {error}");
                }
            }
        }
        self.bus.rebuild_index(subs);

        Ok(())
    }
}

fn handlers_of(manifest: &Manifest) -> HashSet<String> {
    let mut handlers = HashSet::new();
    for request in manifest.requests.values() {
        handlers.insert(request.handler.clone());
    }
    for sub in manifest.events.on.values() {
        handlers.insert(sub.handler.clone());
    }
    handlers
}
