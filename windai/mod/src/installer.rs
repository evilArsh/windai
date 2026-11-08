use crate::{
    Compiler,
    dirs::AppDirs,
    error::{ModError, Result},
    events::{
        CORE_MOD_DELETED, CORE_MOD_INSTALLED, CORE_MOD_UPDATED, ModDeleted, ModInstalled,
        ModUpdated,
    },
    manifest::{DEFAULT_SUB_TIMEOUT_MS, Manifest},
    package::{Package, UI_ENTRY},
    roster::Roster,
};
use serde_json::Value;
use std::{collections::HashSet, path::Path, sync::Arc};
use tokio::sync::{Mutex, OwnedMutexGuard};
use wind_event::{DispatchCx, Dispatcher, Subscription};
use wind_storage::{Mod, Storage, StorageTx, error::StorageError};
use wind_wire::CompiledScripts;

pub(crate) const FIRST_VERSION: u32 = 1;

/// 一次变更影响的话题
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModChange {
    pub affected_topics: Vec<i64>,
}

/// 因为缺少脚本引擎而没有装载的 mod
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailableMod {
    pub name: String,
    pub version: u32,
    pub reason: String,
}

/// mod 安装器
#[derive(Clone)]
pub struct Installer {
    storage: Storage,
    bus: Arc<Dispatcher>,
    dirs: AppDirs,
    roster: Arc<Roster>,
    compiler: Option<Arc<dyn Compiler>>,
    write: Arc<Mutex<()>>,
}

impl Installer {
    pub fn new(
        storage: Storage,
        bus: Arc<Dispatcher>,
        roster: Arc<Roster>,
        dirs: AppDirs,
        compiler: Option<Arc<dyn Compiler>>,
    ) -> Self {
        Self {
            storage,
            bus,
            dirs,
            roster,
            compiler,
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
        let compiled = self.compile(&package)?;

        let gate = Arc::clone(&self.write).lock_owned().await;
        let tx = self.storage.begin().await?;
        let storage = tx.storage();
        let prepared = self
            .prepare_install(&storage, &package, compiled.as_ref())
            .await;
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
        let compiled = self.compile(&package)?;

        let gate = Arc::clone(&self.write).lock_owned().await;
        let tx = self.storage.begin().await?;
        let storage = tx.storage();
        let prepared = self
            .prepare_update(&storage, &package, compiled.as_ref())
            .await;
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
    pub async fn restore(&self) -> Result<Vec<UnavailableMod>> {
        let gate = Arc::clone(&self.write).lock_owned().await;
        let snapshot = self.snapshot(&self.storage).await?;
        self.publish_memory(snapshot.subscriptions, snapshot.roster_rows);
        drop(gate);
        Ok(snapshot.unavailable)
    }

    pub async fn get(&self, name: &str) -> Result<Option<Mod>> {
        Ok(self.storage.mod_by_name(name).await?)
    }

    pub async fn list(&self) -> Result<Vec<Mod>> {
        Ok(self.storage.list_mods().await?)
    }

    async fn prepare_install(
        &self,
        tx: &Storage,
        package: &Package,
        compiled: Option<&CompiledScripts>,
    ) -> Result<(i64, Snapshot)> {
        let name = &package.manifest.name;
        if tx.mod_by_name(name).await?.is_some() {
            return Err(ModError::AlreadyInstalled { name: name.clone() });
        }
        let id = tx
            .install_mod(name, &package.manifest_raw, compiled)
            .await
            .map_err(|error| match error {
                StorageError::Conflict { .. } => ModError::AlreadyInstalled { name: name.clone() },
                other => other.into(),
            })?;
        let snapshot = self.snapshot(tx).await?;
        Ok((id, snapshot))
    }

    async fn prepare_update(
        &self,
        tx: &Storage,
        package: &Package,
        compiled: Option<&CompiledScripts>,
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
        tx.update_mod(
            row.id,
            row.version,
            manifest.version,
            &package.manifest_raw,
            compiled,
        )
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
        let manifest = Manifest::parse(&row.manifest)?;
        if manifest.requires_scripts() && self.compiler.is_none() {
            return Err(ModError::ScriptsUnavailable {
                name: name.to_string(),
                version: row.version,
            });
        }
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
        let mut unavailable = Vec::new();
        for module in modules {
            let manifest = Manifest::parse(&module.manifest)?;
            if manifest.requires_scripts() && self.compiler.is_none() {
                unavailable.push(UnavailableMod {
                    name: manifest.name.clone(),
                    version: manifest.version,
                    reason: "a script engine is not available".to_string(),
                });
                continue;
            }
            subscriptions
                .extend(manifest.subscriptions(self.bus.directory(), DEFAULT_SUB_TIMEOUT_MS)?);
        }
        let roster_rows = tx.all_enabled_mods().await?;
        Ok(Snapshot {
            subscriptions,
            roster_rows,
            unavailable,
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

    fn compile(&self, package: &Package) -> Result<Option<CompiledScripts>> {
        let requires_scripts = package.manifest.requires_scripts();
        let Some(compiler) = &self.compiler else {
            return if requires_scripts {
                Err(ModError::ScriptsUnavailable {
                    name: package.name().to_string(),
                    version: package.version(),
                })
            } else {
                Ok(None)
            };
        };
        // TODO: wind-scripts 实现验证
        let compiled = compiler
            .compile(&package.root, &handlers_of(&package.manifest))
            .map_err(|reason| ModError::Rejected {
                path: package.root.display().to_string(),
                reason,
            })?;
        if requires_scripts && compiled.is_none() {
            return Err(ModError::ScriptsUnavailable {
                name: package.name().to_string(),
                version: package.version(),
            });
        }
        Ok(compiled)
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
        let task: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            let _gate = gate;
            tx.commit().await?;
            bus.rebuild_index(snapshot.subscriptions);
            roster.publish(snapshot.roster_rows);
            if let Some((cx, completion)) = completion {
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
    unavailable: Vec<UnavailableMod>,
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
        package.manifest.name, package.manifest.version, UI_ENTRY
    ))
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
