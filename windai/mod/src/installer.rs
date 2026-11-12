use crate::{
    error::{ModError, Result},
    events::{
        CORE_MOD_DELETED, CORE_MOD_INSTALLED, CORE_MOD_UPDATED, ModDeleted, ModInstalled,
        ModUpdated,
    },
    manifest::{DEFAULT_SUB_TIMEOUT_MS, Manifest},
    package::Package,
    roster::Roster,
};
use serde_json::Value;
use std::{
    io,
    path::{Component, Path},
    sync::Arc,
};
use tokio::sync::{Mutex, OwnedMutexGuard};
use wind_event::{DispatchCx, Dispatcher, Subscription};
use wind_storage::{Mod, Storage, StorageTx, error::StorageError};
use wind_wire::{AppDirs, MOD_UI_ENTRY};

pub(crate) const FIRST_VERSION: u32 = 1;

/// 一次变更影响的话题
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModChange {
    pub affected_topics: Vec<i64>,
}

/// mod 安装器
#[derive(Clone)]
pub struct Installer {
    storage: Storage,
    bus: Arc<Dispatcher>,
    dirs: AppDirs,
    roster: Arc<Roster>,
    write: Arc<Mutex<()>>,
}

impl Installer {
    pub fn new(storage: Storage, bus: Arc<Dispatcher>, roster: Arc<Roster>, dirs: AppDirs) -> Self {
        Self {
            storage,
            bus,
            dirs,
            roster,
            write: Arc::new(Mutex::new(())),
        }
    }

    pub async fn install(&self, cx: &DispatchCx, package: &Path) -> Result<i64> {
        let package = Package::open(package)?;
        self.check_location(&package)?;
        if package.manifest.version != FIRST_VERSION {
            return Err(ModError::Mismatch {
                path: package.root.display().to_string(),
                reason: format!("a first install accepts version {FIRST_VERSION} only"),
            });
        }
        self.validate(&package)?;

        let gate = Arc::clone(&self.write).lock_owned().await;
        let tx = self.storage.begin().await?;
        let storage = tx.storage();
        let prepared = self.prepare_install(&storage, &package).await;
        let (id, snapshot) = match prepared {
            Ok(value) => value,
            Err(error) => {
                let _ = tx.rollback().await;
                return Err(error);
            }
        };
        let completion = Completion::installed(&package)?;
        self.finish(tx, gate, snapshot, Some((cx.clone(), completion)))
            .await?;
        Ok(id)
    }

    pub async fn update(&self, cx: &DispatchCx, package: &Path) -> Result<ModChange> {
        let package = Package::open(package)?;
        self.check_location(&package)?;
        self.validate(&package)?;

        let gate = Arc::clone(&self.write).lock_owned().await;
        let tx = self.storage.begin().await?;
        let storage = tx.storage();
        let prepared = self.prepare_update(&storage, &package).await;
        let (change, completion, snapshot) = match prepared {
            Ok(value) => value,
            Err(error) => {
                let _ = tx.rollback().await;
                return Err(error);
            }
        };
        self.finish(tx, gate, snapshot, Some((cx.clone(), completion)))
            .await?;
        Ok(change)
    }

    pub async fn delete(&self, cx: &DispatchCx, name: &str) -> Result<ModChange> {
        let gate = Arc::clone(&self.write).lock_owned().await;
        let tx = self.storage.begin().await?;
        let storage = tx.storage();
        let prepared = self.prepare_delete(&storage, name).await;
        let (change, completion, snapshot) = match prepared {
            Ok(value) => value,
            Err(error) => {
                let _ = tx.rollback().await;
                return Err(error);
            }
        };
        self.finish(tx, gate, snapshot, Some((cx.clone(), completion)))
            .await?;
        Ok(change)
    }

    pub async fn enable(&self, _cx: &DispatchCx, topic_id: i64, name: &str) -> Result<ModChange> {
        let gate = Arc::clone(&self.write).lock_owned().await;
        let tx = self.storage.begin().await?;
        let storage = tx.storage();
        let prepared = self.prepare_enable(&storage, topic_id, name).await;
        let (change, snapshot) = match prepared {
            Ok(value) => value,
            Err(error) => {
                let _ = tx.rollback().await;
                return Err(error);
            }
        };
        self.finish(tx, gate, snapshot, None).await?;
        Ok(change)
    }

    pub async fn disable(&self, _cx: &DispatchCx, topic_id: i64, name: &str) -> Result<ModChange> {
        let gate = Arc::clone(&self.write).lock_owned().await;
        let tx = self.storage.begin().await?;
        let storage = tx.storage();
        let prepared = self.prepare_disable(&storage, topic_id, name).await;
        let (change, snapshot) = match prepared {
            Ok(value) => value,
            Err(error) => {
                let _ = tx.rollback().await;
                return Err(error);
            }
        };
        self.finish(tx, gate, snapshot, None).await?;
        Ok(change)
    }

    /// 从数据库恢复内存中的启用集合与订阅
    pub async fn restore(&self) -> Result<()> {
        let gate = Arc::clone(&self.write).lock_owned().await;
        let snapshot = self.snapshot(&self.storage).await?;
        self.publish_memory(snapshot.subscriptions, snapshot.roster_rows);
        drop(gate);
        Ok(())
    }

    pub async fn get(&self, name: &str) -> Result<Option<Mod>> {
        Ok(self.storage.mod_by_name(name).await?)
    }

    pub async fn list(&self) -> Result<Vec<Mod>> {
        Ok(self.storage.list_mods().await?)
    }

    async fn prepare_install(&self, tx: &Storage, package: &Package) -> Result<(i64, Snapshot)> {
        let name = &package.manifest.name;
        if tx.mod_by_name(name).await?.is_some() {
            return Err(ModError::AlreadyInstalled { name: name.clone() });
        }
        let id =
            tx.install_mod(name, &package.manifest_raw)
                .await
                .map_err(|error| match error {
                    StorageError::Conflict { .. } => {
                        ModError::AlreadyInstalled { name: name.clone() }
                    }
                    other => other.into(),
                })?;
        let snapshot = self.snapshot(tx).await?;
        Ok((id, snapshot))
    }

    async fn prepare_update(
        &self,
        tx: &Storage,
        package: &Package,
    ) -> Result<(ModChange, Completion, Snapshot)> {
        let manifest = &package.manifest;
        let row = tx
            .mod_by_name(&manifest.name)
            .await?
            .ok_or_else(|| ModError::NotInstalled {
                name: manifest.name.clone(),
            })?;
        if manifest.version <= row.version {
            return Err(ModError::Mismatch {
                path: package.root.display().to_string(),
                reason: format!(
                    "its version {} must be greater than the current {}",
                    manifest.version, row.version
                ),
            });
        }
        tx.update_mod(row.id, row.version, manifest.version, &package.manifest_raw)
            .await
            .map_err(|error| match error {
                StorageError::Conflict { .. } => ModError::Conflict {
                    name: manifest.name.clone(),
                    reason: "the current version changed".to_string(),
                },
                other => other.into(),
            })?;
        let topics = tx.topics_with_mod(row.id).await?;
        let snapshot = self.snapshot(tx).await?;
        let completion = Completion::updated(package, row.version);
        Ok((
            ModChange {
                affected_topics: topics,
            },
            completion,
            snapshot,
        ))
    }

    async fn prepare_delete(
        &self,
        tx: &Storage,
        name: &str,
    ) -> Result<(ModChange, Completion, Snapshot)> {
        let row = tx
            .mod_by_name(name)
            .await?
            .ok_or_else(|| ModError::NotInstalled {
                name: name.to_string(),
            })?;
        let topics = tx.delete_mod(row.id).await?;
        let snapshot = self.snapshot(tx).await?;
        let completion = Completion::Deleted {
            name: row.name,
            version: row.version,
        };
        Ok((
            ModChange {
                affected_topics: topics,
            },
            completion,
            snapshot,
        ))
    }

    async fn prepare_enable(
        &self,
        tx: &Storage,
        topic_id: i64,
        name: &str,
    ) -> Result<(ModChange, Snapshot)> {
        let row = tx
            .mod_by_name(name)
            .await?
            .ok_or_else(|| ModError::NotInstalled {
                name: name.to_string(),
            })?;
        tx.enable_mod(topic_id, row.id).await?;
        let snapshot = self.snapshot(tx).await?;
        Ok((
            ModChange {
                affected_topics: vec![topic_id],
            },
            snapshot,
        ))
    }

    async fn prepare_disable(
        &self,
        tx: &Storage,
        topic_id: i64,
        name: &str,
    ) -> Result<(ModChange, Snapshot)> {
        let row = tx
            .mod_by_name(name)
            .await?
            .ok_or_else(|| ModError::NotInstalled {
                name: name.to_string(),
            })?;
        tx.disable_mod(topic_id, row.id).await?;
        let snapshot = self.snapshot(tx).await?;
        Ok((
            ModChange {
                affected_topics: vec![topic_id],
            },
            snapshot,
        ))
    }

    /// 事务内构造将要发布的内存状态
    async fn snapshot(&self, tx: &Storage) -> Result<Snapshot> {
        let modules = tx.list_mods().await?;
        let mut subscriptions = Vec::new();
        for module in modules {
            let manifest = Manifest::parse(&module.manifest)?;
            subscriptions
                .extend(manifest.subscriptions(self.bus.directory(), DEFAULT_SUB_TIMEOUT_MS)?);
        }
        let roster_rows = tx.all_enabled_mods().await?;
        Ok(Snapshot {
            subscriptions,
            roster_rows,
        })
    }

    fn publish_memory(&self, subscriptions: Vec<Subscription>, roster_rows: Vec<(i64, String)>) {
        self.bus.rebuild_index(subscriptions);
        self.roster.publish(roster_rows);
    }

    fn validate(&self, package: &Package) -> Result<()> {
        package
            .manifest
            .subscriptions(self.bus.directory(), DEFAULT_SUB_TIMEOUT_MS)?;
        Ok(())
    }

    fn check_location(&self, package: &Package) -> Result<()> {
        let expected = self.dirs.mod_version(package.name(), package.version());
        if package.root != expected {
            return Err(ModError::Mismatch {
                path: package.root.display().to_string(),
                reason: format!("it belongs at {}", expected.display()),
            });
        }
        Ok(())
    }

    /// 提交与内存发布都在这个任务里完成，调用方取消只放弃等待
    async fn finish(
        &self,
        tx: StorageTx,
        gate: OwnedMutexGuard<()>,
        snapshot: Snapshot,
        completion: Option<(DispatchCx, Completion)>,
    ) -> Result<()> {
        let bus = Arc::clone(&self.bus);
        let roster = Arc::clone(&self.roster);
        let bytecode = self.dirs.bytecode_root();
        let task: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            let _gate = gate;
            tx.commit().await?;
            bus.rebuild_index(snapshot.subscriptions);
            roster.publish(snapshot.roster_rows);
            if let Some((cx, completion)) = completion {
                if let Completion::Deleted { name, .. } = &completion {
                    let mut components = Path::new(name).components();
                    if matches!(components.next(), Some(Component::Normal(_)))
                        && components.next().is_none()
                        && !name.ends_with(['.', ' '])
                        && !name.contains(['/', '\\', ':'])
                    {
                        let path = bytecode.join(name);
                        if let Err(error) = std::fs::remove_dir_all(&path)
                            && error.kind() != io::ErrorKind::NotFound
                        {
                            log::warn!("[mod] cannot remove {}: {error}", path.display());
                        }
                    }
                }
                completion.notify(&bus, &cx);
            }
            Ok(())
        });
        match task.await {
            Ok(result) => result,
            Err(error) => {
                log::warn!("[mod] the publish task ended unexpectedly: {error}");
                self.restore().await?;
                Err(ModError::Background(error.to_string()))
            }
        }
    }
}

struct Snapshot {
    subscriptions: Vec<Subscription>,
    roster_rows: Vec<(i64, String)>,
}

enum Completion {
    Installed {
        name: String,
        version: u32,
        manifest: Value,
        ui_entry: Option<String>,
    },
    Updated {
        name: String,
        from_version: u32,
        to_version: u32,
        ui_entry: Option<String>,
    },
    Deleted {
        name: String,
        version: u32,
    },
}

impl Completion {
    fn installed(package: &Package) -> Result<Self> {
        Ok(Self::Installed {
            name: package.manifest.name.clone(),
            version: package.manifest.version,
            manifest: serde_json::to_value(&package.manifest)
                .map_err(|error| ModError::Encode(error.to_string()))?,
            ui_entry: ui_entry_of(package),
        })
    }

    fn updated(package: &Package, from_version: u32) -> Self {
        Self::Updated {
            name: package.manifest.name.clone(),
            from_version,
            to_version: package.manifest.version,
            ui_entry: ui_entry_of(package),
        }
    }

    fn notify(self, bus: &Dispatcher, cx: &DispatchCx) {
        match self {
            Self::Installed {
                name,
                version,
                manifest,
                ui_entry,
            } => bus.notify(
                &CORE_MOD_INSTALLED,
                cx,
                ModInstalled {
                    name,
                    version,
                    manifest,
                    ui_entry,
                },
            ),
            Self::Updated {
                name,
                from_version,
                to_version,
                ui_entry,
            } => bus.notify(
                &CORE_MOD_UPDATED,
                cx,
                ModUpdated {
                    name,
                    from_version,
                    to_version,
                    ui_entry,
                },
            ),
            Self::Deleted { name, version } => {
                bus.notify(&CORE_MOD_DELETED, cx, ModDeleted { name, version })
            }
        }
    }
}

fn ui_entry_of(package: &Package) -> Option<String> {
    if !package.ui_entry().is_file() {
        return None;
    }
    Some(format!(
        "windai://mods/{}/{}/{}",
        package.manifest.name, package.manifest.version, MOD_UI_ENTRY
    ))
}
