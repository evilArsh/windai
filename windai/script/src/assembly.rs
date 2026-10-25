use super::bridge::{self, BridgeError};
use super::bytecode::{compile_scripts, engine_fingerprint};
use super::circuit::Breaker;
use super::deployment::{DeployError, Deployment, NotDeployed};
use super::events::{CORE_INPUT_DRAFT, CORE_MOD_NOTIFY, CoreInputDraft, CoreModNotify};
use super::facade::{FacadeError, HostApi, HttpRequest, ModIdentity, TranscriptEntry};
use super::request::RequestCaller;
use super::runtime::{CallHost, HostFactory, ScriptHost, ScriptHostOptions};
use super::threads::ThreadPool;
use futures::future::join_all;
use log::Level;
use reqwest::{Client, Method, Response};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::future::Future;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;
use wind_core::error::CoreError;
use wind_core::models::{BlockKind, CompiledScripts};
use wind_core::sandbox::{Sandbox, SandboxError};
use wind_core::storage::Storage;
use wind_event::context::DispatchCx;
use wind_event::dispatch::Dispatcher;
use wind_event::envelope::{Origin, Scope, Source};
use wind_event::host::{CallCx, HostError, ModHost};
use wind_event::subscription::Subscription;
use wind_mod::compiler::Compiler;
use wind_mod::dirs::AppDirs;
use wind_mod::feed::{FeedError, ModFeed};
use wind_mod::manifest::{Delivery, Manifest, parse};
use wind_mod::state::StateSnapshot;

/// 解释器线程数的上限
const INTERPRETER_THREADS: usize = 4;
/// 连续超时的阈值
const TIMEOUT_THRESHOLD: u32 = 3;
/// 一次出网的响应体上限，字节
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
/// 私有目录里一次读取的文件上限，字节
const MAX_FILE_BYTES: u64 = 4 * 1024 * 1024;
/// 对外状态里一个键、以及一条事件载荷的字节上限
const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
/// 一次转录没有给出条数时取回的条数
const DEFAULT_TRANSCRIPT: u32 = 20;
/// 一次转录最多取回的条数
const MAX_TRANSCRIPT: u32 = 200;
/// 宿主侧代码里正文块的种类
/// ctx 方法自身派发事件时一次顶层操作的预算
const FACADE_BUDGET: Duration = Duration::from_millis(50);

/// 脚本运行时装配入口
pub struct ScriptRuntime {
    /// 脚本执行线程池，不在 tokio 线程池中执行脚本
    pool: Arc<ThreadPool>,
    /// 熔断器
    breaker: Arc<Breaker>,
    factory: Arc<SystemFactory>,
    feed: Arc<ModFeed>,
}

impl ScriptRuntime {
    pub async fn open(
        storage: Storage,
        bus: Arc<Dispatcher>,
        dirs: AppDirs,
        feed: Arc<ModFeed>,
    ) -> Result<Self, CoreError> {
        Ok(Self {
            pool: Arc::new(ThreadPool::new(INTERPRETER_THREADS)),
            breaker: Arc::new(Breaker::new(TIMEOUT_THRESHOLD)),
            factory: Arc::new(SystemFactory {
                storage,
                bus,
                dirs,
                feed: Arc::clone(&feed),
                http: Arc::new(OnceLock::new()),
            }),
            feed,
        })
    }

    pub fn feed(&self) -> Arc<ModFeed> {
        Arc::clone(&self.feed)
    }

    pub fn host(&self) -> Arc<dyn ModHost> {
        let factory: Arc<dyn HostFactory> = self.factory.clone();
        Arc::new(ScriptHost::new(ScriptHostOptions {
            factory,
            pool: Arc::clone(&self.pool),
            breaker: Arc::clone(&self.breaker),
        }))
    }

    pub fn caller_for_agent(&self) -> RequestCaller {
        let factory: Arc<dyn HostFactory> = self.factory.clone();
        RequestCaller::for_agent(factory, Arc::clone(&self.pool), Arc::clone(&self.breaker))
    }

    pub fn caller_for_ui(&self) -> RequestCaller {
        let factory: Arc<dyn HostFactory> = self.factory.clone();
        RequestCaller::for_ui(factory, Arc::clone(&self.pool), Arc::clone(&self.breaker))
    }
}

#[derive(Default)]
pub struct DeferredHost {
    host: OnceLock<Arc<dyn ModHost>>,
}

impl DeferredHost {
    pub fn new() -> Self {
        Self {
            host: OnceLock::new(),
        }
    }

    pub fn attach(&self, host: Arc<dyn ModHost>) {
        let _ = self.host.set(host);
    }
}

#[async_trait::async_trait]
impl ModHost for DeferredHost {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        let host = self.host.get().cloned().ok_or_else(|| {
            HostError::Internal("the script host is not attached yet".to_string())
        })?;
        host.call(sub, payload, cx).await
    }
}

/// 将 `scripts/` 中的 js 脚本编译成字节码
pub struct ScriptCompiler;

impl Compiler for ScriptCompiler {
    fn compile(
        &self,
        root: &Path,
        handlers: &BTreeSet<String>,
    ) -> Result<Option<CompiledScripts>, String> {
        let compiled = compile_scripts(root, handlers).map_err(|error| error.to_string())?;
        if compiled.modules.is_empty() {
            return Ok(None);
        }
        Ok(Some(compiled))
    }
}

/// 一次调用的 Deployment 与 StateSnapshot 来源
struct SystemFactory {
    storage: Storage,
    bus: Arc<Dispatcher>,
    dirs: AppDirs,
    feed: Arc<ModFeed>,
    http: Arc<OnceLock<Client>>,
}

#[async_trait::async_trait]
impl HostFactory for SystemFactory {
    async fn open(
        &self,
        name: &str,
        version: i64,
        scope: &Scope,
        cx: &CallCx,
    ) -> Result<(Deployment, Box<dyn CallHost>), DeployError> {
        let read = |reason: String| DeployError::Read {
            name: name.to_string(),
            version,
            reason,
        };
        let missing = || NotDeployed {
            name: name.to_string(),
            version,
        };

        let row = self
            .storage
            .mod_get_by_name(name)
            .await
            .map_err(|error| read(error.to_string()))?
            .ok_or_else(missing)?;
        let stored = self
            .storage
            .mod_get_version(row.id, version)
            .await
            .map_err(|error| read(error.to_string()))?
            .ok_or_else(missing)?;
        let manifest = parse(&stored.manifest, self.bus.directory())
            .map_err(|error| read(error.to_string()))?;
        let snapshot = StateSnapshot::open(self.storage.clone(), row.id, scope.clone())
            .await
            .map_err(|error| read(error.to_string()))?;

        let deployment = Deployment {
            name: manifest.name.clone(),
            version,
            root: self.dirs.mod_version(&manifest.name, version),
            compiled: compiled_of(
                stored.compiled.as_ref(),
                stored.engine_fingerprint.as_deref(),
            ),
            manifest: manifest.clone(),
        };
        let identity = ModIdentity {
            name: manifest.name.clone(),
            version,
        };
        let host = SystemHost {
            snapshot: Mutex::new(Some(snapshot)),
            delivery: Arc::new(Settlement {
                bus: Arc::clone(&self.bus),
                feed: Arc::clone(&self.feed),
                channels: stateful_channels(&manifest),
                manifest,
                scope: scope.clone(),
                identity,
            }),
            storage: self.storage.clone(),
            dirs: self.dirs.clone(),
            http: Arc::clone(&self.http),
            cancel: cx.cancel.clone(),
            deadline: Mutex::new(cx.deadline),
        };
        Ok((deployment, Box::new(host)))
    }
}

/// js 调用宿主能力的功能实现
///
/// # HostApi
/// - 给 js 侧使用的 `ctx.xx` 方法集
///
/// # CallHost
/// - 给宿主侧使用的收尾功能
struct SystemHost {
    snapshot: Mutex<Option<StateSnapshot>>,
    delivery: Arc<Settlement>,
    storage: Storage,
    dirs: AppDirs,
    http: Arc<OnceLock<Client>>,
    cancel: CancellationToken,
    deadline: Mutex<TokioInstant>,
}

impl SystemHost {
    fn take_snapshot(&self) -> Option<StateSnapshot> {
        self.snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }

    fn snapshot_read(&self, key: &str, private: bool) -> Option<serde_json::Value> {
        let guard = self
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let snapshot = guard.as_ref()?;
        let value = if private {
            snapshot.storage(key)
        } else {
            snapshot.state(key)
        };
        value.cloned()
    }

    fn snapshot_write(&self, key: &str, value: serde_json::Value, private: bool) {
        let mut guard = self
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.as_mut() {
            Some(snapshot) if private => snapshot.put_storage(key, value),
            Some(snapshot) => snapshot.put_state(key, value),
            None => log::warn!("[script] a state write arrived after the call was closed"),
        }
    }

    fn snapshot_remove(&self, key: &str, private: bool) {
        let mut guard = self
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.as_mut() {
            Some(snapshot) if private => snapshot.remove_storage(key),
            Some(snapshot) => snapshot.remove_state(key),
            None => log::warn!("[script] a state delete arrived after the call was closed"),
        }
    }

    fn snapshot_keys(&self, private: bool) -> Vec<String> {
        let guard = self
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(snapshot) = guard.as_ref() else {
            return Vec::new();
        };
        let keys = if private {
            snapshot.keys_storage()
        } else {
            snapshot.keys_state()
        };
        keys.into_iter().map(str::to_string).collect()
    }

    fn remaining(&self) -> Duration {
        self.deadline
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .saturating_duration_since(TokioInstant::now())
    }

    fn fetch_budget(&self, declared: Option<u64>) -> Duration {
        let remaining = self.remaining();
        declared
            .map_or(remaining, Duration::from_millis)
            .min(remaining)
    }

    fn blocked<U, F>(&self, limit: Duration, work: F) -> Result<U, FacadeError>
    where
        U: Send + 'static,
        F: Future<Output = Result<U, FacadeError>> + Send + 'static,
    {
        let cancel = self.cancel.clone();
        let outcome = bridge::block_on_current(limit, async move {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Ok(Err(FacadeError::Cancelled)),
                result = work => Ok(result),
            }
        });
        match outcome {
            Ok(result) => result,
            Err(BridgeError::Timeout) => Err(FacadeError::Timeout),
            Err(BridgeError::Cancelled) => Err(FacadeError::Cancelled),
            Err(BridgeError::IoPanicked) => Err(FacadeError::Failed(
                "the I/O side stopped before it could answer".to_string(),
            )),
            Err(BridgeError::Failed(reason)) => Err(FacadeError::Failed(reason)),
        }
    }

    fn private_path(&self, name: &str, path: &str, create: bool) -> Result<PathBuf, FacadeError> {
        let root = self.dirs.mod_data(name);
        if create {
            fs::create_dir_all(&root).map_err(|error| {
                FacadeError::Failed(format!("cannot create {}: {error}", root.display()))
            })?;
        }
        Sandbox::new(vec![root.clone()])
            .resolve(&root.join(path))
            .map_err(|error| match error {
                SandboxError::NotAllowed(_) | SandboxError::InvalidPath(_) => FacadeError::Path {
                    path: path.to_string(),
                },
                SandboxError::NotFound(_) => FacadeError::NotFound {
                    path: path.to_string(),
                },
                SandboxError::Io(reason) => FacadeError::Failed(reason),
            })
    }
}

impl Drop for SystemHost {
    fn drop(&mut self) {
        let Some(snapshot) = self.take_snapshot() else {
            return;
        };
        self.delivery.settle_detached(snapshot);
    }
}

/// 把快照里改过的键写回存储，把改过的对外状态推给 ModFeed
struct Settlement {
    bus: Arc<Dispatcher>,
    feed: Arc<ModFeed>,
    channels: BTreeMap<String, String>,
    manifest: Manifest,
    scope: Scope,
    identity: ModIdentity,
}

impl Settlement {
    fn dispatch_cx(&self, budget: Duration) -> DispatchCx {
        let version = u32::try_from(self.identity.version).unwrap_or(u32::MAX);
        DispatchCx::root(
            Origin::Internal,
            self.scope.clone(),
            Source::mod_(self.identity.name.clone(), version),
            budget,
        )
    }

    async fn settle(&self, snapshot: StateSnapshot) -> Result<(), CoreError> {
        let flushed = snapshot.flush().await?;
        self.deliver(flushed.changed_state).await;
        Ok(())
    }

    fn spawn_settle(
        self: &Arc<Self>,
        snapshot: StateSnapshot,
    ) -> Result<JoinHandle<Result<(), CoreError>>, BridgeError> {
        let work = Arc::clone(self);
        Ok(bridge::handle()?.spawn(async move { work.settle(snapshot).await }))
    }

    fn settle_detached(self: &Arc<Self>, snapshot: StateSnapshot) {
        let work = Arc::clone(self);
        let handle = match bridge::handle() {
            Ok(handle) => handle,
            Err(error) => {
                log::error!(
                    "[script] cannot schedule the state write of {}: {error}",
                    work.identity.name
                );
                return;
            }
        };
        handle.spawn(async move {
            if let Err(error) = work.settle(snapshot).await {
                log::error!(
                    "[script] the state of {} could not be written after the call was dropped: {error}",
                    work.identity.name
                );
            }
        });
    }

    async fn deliver(&self, changed: Vec<(String, serde_json::Value)>) {
        if changed.is_empty() {
            return;
        }
        let Some(topic_id) = self.scope.topic_id() else {
            log::warn!(
                "[script] {} changed state in a global scope, which has no subscriber",
                self.identity.name
            );
            return;
        };
        let cx = self.dispatch_cx(FACADE_BUDGET);
        for (key, value) in changed {
            let Some(channel) = self.channels.get(&key) else {
                continue;
            };
            let outcome = if value.is_null() {
                self.feed
                    .retract(&cx, topic_id, &self.manifest, channel)
                    .await
            } else {
                self.feed
                    .emit(&cx, topic_id, &self.manifest, channel, value)
                    .await
            };
            if let Err(error) = outcome {
                log::warn!(
                    "[script] {key} of {} was changed but not delivered: {error}",
                    self.identity.name
                );
            }
        }
    }

    fn read_error(&self, reason: String) -> DeployError {
        DeployError::Read {
            name: self.identity.name.clone(),
            version: self.identity.version,
            reason,
        }
    }
}

fn topic_of(scope: &Scope) -> Result<i64, FacadeError> {
    scope
        .topic_id()
        .ok_or_else(|| FacadeError::Failed("this scope has no topic".to_string()))
}

impl HostApi for SystemHost {
    fn state_read(&self, key: &str) -> Option<serde_json::Value> {
        self.snapshot_read(key, false)
    }

    fn state_write(&self, key: &str, value: serde_json::Value) -> Result<(), FacadeError> {
        let encoded = serde_json::to_string(&value)
            .map_err(|error| FacadeError::Failed(error.to_string()))?;
        if encoded.len() > MAX_PAYLOAD_BYTES {
            return Err(FacadeError::PayloadTooLarge {
                key: key.to_string(),
                limit: MAX_PAYLOAD_BYTES,
            });
        }
        self.snapshot_write(key, value, false);
        Ok(())
    }

    fn state_delete(&self, key: &str) {
        self.snapshot_remove(key, false);
    }

    fn state_keys(&self) -> Vec<String> {
        self.snapshot_keys(false)
    }

    fn storage_read(&self, key: &str) -> Option<serde_json::Value> {
        self.snapshot_read(key, true)
    }

    fn storage_write(&self, key: &str, value: serde_json::Value) {
        self.snapshot_write(key, value, true);
    }

    fn storage_delete(&self, key: &str) {
        self.snapshot_remove(key, true);
    }

    fn storage_keys(&self) -> Vec<String> {
        self.snapshot_keys(true)
    }

    fn http_fetch(&self, init: HttpRequest) -> Result<Vec<u8>, FacadeError> {
        let budget = self.fetch_budget(init.timeout_ms);
        let client = Arc::clone(&self.http);
        self.blocked(budget, async move {
            let client = client.get_or_init(Client::new).clone();
            fetch_one(&client, init).await
        })
    }

    fn http_fetch_many(&self, requests: Vec<HttpRequest>) -> Vec<Result<Vec<u8>, FacadeError>> {
        let count = requests.len();
        if count == 0 {
            return Vec::new();
        }
        let client = Arc::clone(&self.http);
        let batch = self.blocked(self.remaining(), async move {
            let client = client.get_or_init(Client::new).clone();
            let answered = join_all(requests.into_iter().map(|request| {
                let client = client.clone();
                async move {
                    match request.timeout_ms.map(Duration::from_millis) {
                        Some(limit) => {
                            match tokio::time::timeout(limit, fetch_one(&client, request)).await {
                                Ok(answered) => answered,
                                Err(_) => Err(FacadeError::Timeout),
                            }
                        }
                        None => fetch_one(&client, request).await,
                    }
                }
            }))
            .await;
            Ok(answered)
        });
        match batch {
            Ok(answered) => answered,
            Err(error) => (0..count).map(|_| Err(error.clone())).collect(),
        }
    }

    fn agent_prompt(&self, _scope: &Scope, _text: &str) -> Result<(), FacadeError> {
        Err(FacadeError::NoRuntime {
            capability: "agent",
        })
    }

    fn agent_draft(&self, scope: &Scope, text: &str) -> Result<(), FacadeError> {
        let topic_id = topic_of(scope)?;
        let cx = self
            .delivery
            .dispatch_cx(self.remaining().min(FACADE_BUDGET));
        let bus = Arc::clone(&self.delivery.bus);
        let text = text.to_string();
        self.blocked(self.remaining(), async move {
            bus.notify(&CORE_INPUT_DRAFT, &cx, CoreInputDraft { topic_id, text })
                .await;
            Ok(())
        })
    }

    fn agent_spawn(&self, _scope: &Scope, _mode: &str, _task: &str) -> Result<(), FacadeError> {
        Err(FacadeError::NoRuntime {
            capability: "agent",
        })
    }

    fn agent_transcript(&self, scope: &Scope, limit: Option<u32>) -> Vec<TranscriptEntry> {
        let take = limit.unwrap_or(DEFAULT_TRANSCRIPT).min(MAX_TRANSCRIPT);
        let Some(topic_id) = scope.topic_id() else {
            return Vec::new();
        };
        let storage = self.storage.clone();
        match self.blocked(self.remaining(), async move {
            recent_messages(storage, topic_id, take).await
        }) {
            Ok(entries) => entries,
            Err(error) => {
                log::warn!("[script] cannot read the transcript of topic {topic_id}: {error}");
                Vec::new()
            }
        }
    }

    fn fs_dir(&self, name: &str) -> PathBuf {
        self.dirs.mod_data(name)
    }

    fn fs_read(&self, name: &str, path: &str) -> Result<String, FacadeError> {
        let missing = || FacadeError::NotFound {
            path: path.to_string(),
        };
        let root = self.dirs.mod_data(name);
        if !root.is_dir() {
            return Err(missing());
        }
        let target = self.private_path(name, path, false)?;
        let meta = fs::metadata(&target).map_err(|error| match error.kind() {
            ErrorKind::NotFound => missing(),
            _ => FacadeError::Failed(format!("cannot read {}: {error}", target.display())),
        })?;
        if meta.len() > MAX_FILE_BYTES {
            return Err(FacadeError::TooLarge {
                target: path.to_string(),
                limit: MAX_FILE_BYTES as usize,
            });
        }
        fs::read_to_string(&target).map_err(|error| {
            FacadeError::Failed(format!("cannot read {}: {error}", target.display()))
        })
    }

    fn fs_write(&self, name: &str, path: &str, data: &str) -> Result<(), FacadeError> {
        if data.len() as u64 > MAX_FILE_BYTES {
            return Err(FacadeError::TooLarge {
                target: path.to_string(),
                limit: MAX_FILE_BYTES as usize,
            });
        }
        let target = self.private_path(name, path, true)?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                FacadeError::Failed(format!("cannot create {}: {error}", parent.display()))
            })?;
        }
        fs::write(&target, data).map_err(|error| {
            FacadeError::Failed(format!("cannot write {}: {error}", target.display()))
        })
    }

    fn fs_list(&self, name: &str, path: &str) -> Result<Vec<String>, FacadeError> {
        let root = self.dirs.mod_data(name);
        if !root.is_dir() {
            return Ok(Vec::new());
        }
        let target = self.private_path(name, path, false)?;
        let listing = fs::read_dir(&target).map_err(|error| match error.kind() {
            ErrorKind::NotFound => FacadeError::NotFound {
                path: path.to_string(),
            },
            _ => FacadeError::Failed(format!("cannot list {}: {error}", target.display())),
        })?;
        let mut entries = Vec::new();
        for entry in listing {
            let entry = entry.map_err(|error| {
                FacadeError::Failed(format!("cannot list {}: {error}", target.display()))
            })?;
            entries.push(relative_to(&root, &entry.path()));
        }
        entries.sort();
        Ok(entries)
    }

    fn fs_remove(&self, name: &str, path: &str) -> Result<(), FacadeError> {
        let root = self.dirs.mod_data(name);
        if !root.is_dir() {
            return Ok(());
        }
        let target = self.private_path(name, path, false)?;
        let meta = match fs::symlink_metadata(&target) {
            Ok(meta) => meta,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(FacadeError::Failed(format!(
                    "cannot remove {}: {error}",
                    target.display()
                )));
            }
        };
        let removed = if meta.is_dir() {
            fs::remove_dir(&target)
        } else {
            fs::remove_file(&target)
        };
        removed.map_err(|error| {
            FacadeError::Failed(format!("cannot remove {}: {error}", target.display()))
        })
    }

    fn emit(
        &self,
        scope: &Scope,
        name: &str,
        payload: serde_json::Value,
    ) -> Result<(), FacadeError> {
        let encoded = serde_json::to_string(&payload)
            .map_err(|error| FacadeError::Failed(error.to_string()))?;
        if encoded.len() > MAX_PAYLOAD_BYTES {
            return Err(FacadeError::PayloadTooLarge {
                key: name.to_string(),
                limit: MAX_PAYLOAD_BYTES,
            });
        }
        let topic_id = topic_of(scope)?;
        let cx = self
            .delivery
            .dispatch_cx(self.remaining().min(FACADE_BUDGET));
        let feed = Arc::clone(&self.delivery.feed);
        let manifest = self.delivery.manifest.clone();
        let name = name.to_string();
        self.blocked(self.remaining(), async move {
            feed.emit(&cx, topic_id, &manifest, &name, payload)
                .await
                .map_err(|error| match error {
                    FeedError::Undeclared { .. } => FacadeError::Unsupported,
                    other => FacadeError::Failed(other.to_string()),
                })
        })
    }

    fn notify(&self, _scope: &Scope, level: &str, text: &str) -> Result<(), FacadeError> {
        let cx = self
            .delivery
            .dispatch_cx(self.remaining().min(FACADE_BUDGET));
        let bus = Arc::clone(&self.delivery.bus);
        let level = level.to_string();
        let text = text.to_string();
        self.blocked(self.remaining(), async move {
            bus.notify(&CORE_MOD_NOTIFY, &cx, CoreModNotify { level, text })
                .await;
            Ok(())
        })
    }

    fn log(&self, level: &str, message: &str) {
        log::log!(
            target: "mod",
            Level::from_str(level).unwrap_or(Level::Info),
            "{}: {message}",
            self.delivery.identity.name
        );
    }
}

#[async_trait::async_trait]
impl CallHost for SystemHost {
    fn rebase(&self, deadline: TokioInstant) {
        *self
            .deadline
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = deadline;
    }

    async fn finish(&self) -> Result<(), DeployError> {
        let Some(snapshot) = self.take_snapshot() else {
            return Ok(());
        };
        let handle = match self.delivery.spawn_settle(snapshot) {
            Ok(handle) => handle,
            Err(error) => return Err(self.delivery.read_error(error.to_string())),
        };
        match handle.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(self.delivery.read_error(error.to_string())),
            Err(_) => Err(self
                .delivery
                .read_error("the flush task panicked".to_string())),
        }
    }
}

fn compiled_of(
    modules: Option<&serde_json::Value>,
    fingerprint: Option<&str>,
) -> BTreeMap<String, Vec<u8>> {
    if fingerprint != Some(engine_fingerprint().as_str()) {
        return BTreeMap::new();
    }
    let Some(modules) = modules else {
        return BTreeMap::new();
    };
    match serde_json::from_value::<BTreeMap<String, Vec<u8>>>(modules.clone()) {
        Ok(modules) => modules,
        Err(error) => {
            log::warn!("[script] the bytecode stored with a version is unreadable: {error}");
            BTreeMap::new()
        }
    }
}

fn stateful_channels(manifest: &Manifest) -> BTreeMap<String, String> {
    manifest
        .events
        .emit
        .iter()
        .filter_map(|(name, decl)| match decl.delivery {
            Delivery::Stateful => decl
                .state_key
                .as_ref()
                .map(|key| (key.clone(), name.clone())),
            Delivery::Notice => None,
        })
        .collect()
}

async fn fetch_one(client: &Client, init: HttpRequest) -> Result<Vec<u8>, FacadeError> {
    let HttpRequest {
        method,
        url,
        headers,
        body,
        ..
    } = init;
    let method = Method::from_bytes(method.as_bytes()).map_err(|_| FacadeError::Http {
        status: None,
        reason: format!("{method} is not an http method"),
    })?;
    let mut request = client.request(method, &url);
    for (name, value) in &headers {
        request = request.header(name.as_str(), value.as_str());
    }
    if let Some(body) = body {
        request = request.body(body);
    }
    let response = request.send().await.map_err(|error| FacadeError::Http {
        status: None,
        reason: format!("{url} could not be reached: {error}"),
    })?;
    let status = response.status();
    if !status.is_success() {
        return Err(FacadeError::Http {
            status: Some(status.as_u16()),
            reason: format!("{url} answered {status}"),
        });
    }
    read_body(response, &url).await
}

async fn read_body(mut response: Response, url: &str) -> Result<Vec<u8>, FacadeError> {
    let status = response.status().as_u16();
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| FacadeError::Http {
        status: Some(status),
        reason: format!("{url} was cut off: {error}"),
    })? {
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            return Err(FacadeError::TooLarge {
                target: url.to_string(),
                limit: MAX_BODY_BYTES,
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn relative_to(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// 只取话题根实例的对话
async fn recent_messages(
    storage: Storage,
    topic_id: i64,
    take: u32,
) -> Result<Vec<TranscriptEntry>, FacadeError> {
    let fail = |error: CoreError| FacadeError::Failed(error.to_string());
    let Some(main) = storage
        .agent_list_by_topic(topic_id)
        .await
        .map_err(fail)?
        .into_iter()
        .find(|instance| instance.parent_id.is_none())
    else {
        return Ok(Vec::new());
    };
    let turns = storage.turn_list_by_instance(main.id).await.map_err(fail)?;
    let turn_ids: Vec<i64> = turns.iter().map(|turn| turn.id).collect();
    let messages = storage
        .message_list_by_turns(&turn_ids)
        .await
        .map_err(fail)?;
    // 最近的消息在末尾。消息按轮与轮内序号升序给出
    let start = messages.len().saturating_sub(take as usize);
    let recent = &messages[start..];
    let message_ids: Vec<i64> = recent.iter().map(|message| message.id).collect();
    let blocks = storage
        .block_list_by_messages(&message_ids)
        .await
        .map_err(fail)?;
    Ok(recent
        .iter()
        .map(|message| TranscriptEntry {
            message_id: message.id,
            role: message.role.to_string(),
            text: blocks
                .iter()
                .filter(|block| block.message_id == message.id && block.kind == BlockKind::Text)
                .map(|block| block.content.clone())
                .collect::<Vec<String>>()
                .join(" "),
        })
        .collect())
}
