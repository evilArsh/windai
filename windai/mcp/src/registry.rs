use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wind_event::{DispatchCx, Dispatcher, Origin, Scope, Source};
use wind_wire::McpDefinition;

use crate::config;
use crate::connector::{Connection, Connector, RemoteTool};
use crate::error::{McpError, Result};
use crate::events::{
    CORE_MCP_FAILED, CORE_MCP_STARTED, CORE_MCP_STOPPED, McpFailed, McpStarted, McpStopped,
};
use crate::identity::ConnectionKey;

/// 由 mod 清单派生的绑定来源
pub const SOURCE_MODS: &str = "mods";
/// 由独立配置派生的绑定来源
pub const SOURCE_CONFIGURED: &str = "configured";
/// 内建服务的绑定来源
pub const SOURCE_BUILTIN: &str = "builtin";

const EVENT_BUDGET: Duration = Duration::from_secs(1);

/// 一个要同步的绑定
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingSpec {
    /// 来源内的稳定键
    pub key: String,
    /// 配置版本
    pub revision: u32,
    /// 服务声明
    pub definition: McpDefinition,
}

/// 一个绑定的连接状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingState {
    Connecting,
    Ready,
    Failed,
}

/// 一个绑定的当前状态
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingStatus {
    /// 绑定来源
    pub source: String,
    /// 来源内的稳定键
    pub key: String,
    /// 配置版本
    pub revision: u32,
    /// 连接状态
    pub state: BindingState,
    /// 失败原因
    pub error: Option<String>,
}

/// 一次同步的结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncReport {
    /// 来源同步后的绑定
    pub bindings: Vec<BindingStatus>,
    /// 已被移除的键
    pub removed: Vec<String>,
}

/// 目录中一个工具的路由目标
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ToolTarget {
    /// 所属话题
    pub topic_id: i64,
    /// 绑定来源
    pub source: String,
    /// 来源内的稳定键
    pub key: String,
    /// 配置版本
    pub revision: u32,
    /// 连接的进程内编号
    pub generation: u64,
    /// 原始工具名
    pub tool: String,
}

/// 目录中一个工具的完整定义
#[derive(Debug, Clone, PartialEq)]
pub struct BoundTool {
    /// 路由目标
    pub target: ToolTarget,
    /// 远端描述
    pub description: Option<String>,
    /// 远端输入 schema
    pub input_schema: Value,
}

/// 一个技能根
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillRoot {
    /// 技能来源
    pub source: String,
    /// 技能目录
    pub path: PathBuf,
}

/// 内建服务的根目录
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BuiltinRoots {
    /// 文件工具的工作区根，缺省时不注册
    pub workspace: Option<PathBuf>,
    /// 只读技能根，为空时不注册
    pub skills: Vec<SkillRoot>,
}

/// 内建服务的种类
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BuiltinKind {
    Fs,
    Skills,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SlotKey {
    External(ConnectionKey),
    Builtin(BuiltinKind),
    Invalid(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Definition {
    External(McpDefinition),
    Builtin(BuiltinKind, BuiltinRoots),
}

type ConnectionState = (Arc<Connection>, Option<JoinHandle<()>>);

struct Slot {
    topic_id: i64,
    key: SlotKey,
    generation: u64,
    definition: Definition,
    state: Mutex<SlotState>,
    connect: AsyncMutex<()>,
    idle: Notify,
    refs: AtomicUsize,
    in_flight: AtomicUsize,
}

enum SlotState {
    Connecting,
    Ready(ConnectionState),
    Failed {
        error: String,
        connection: Option<ConnectionState>,
    },
    Closed,
}

enum Snapshot {
    Connecting,
    Ready(Arc<Connection>),
    Failed(String),
    Closed,
}

impl Slot {
    fn new(topic_id: i64, key: SlotKey, generation: u64, definition: Definition) -> Self {
        Self {
            topic_id,
            key,
            generation,
            definition,
            state: Mutex::new(SlotState::Connecting),
            connect: AsyncMutex::new(()),
            idle: Notify::new(),
            refs: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
        }
    }

    fn snapshot(&self) -> Snapshot {
        let state = self.lock_state();
        match &*state {
            SlotState::Connecting => Snapshot::Connecting,
            SlotState::Ready((connection, _)) => Snapshot::Ready(Arc::clone(connection)),
            SlotState::Failed { error, .. } => Snapshot::Failed(error.clone()),
            SlotState::Closed => Snapshot::Closed,
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, SlotState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn set_ready(&self, connection: Arc<Connection>, server: Option<JoinHandle<()>>) {
        let mut state = self.lock_state();
        if matches!(*state, SlotState::Connecting) {
            *state = SlotState::Ready((connection, server));
        }
    }

    /// 把连接或连接中的槽标记为失败，保留已有连接等待收尾
    fn fail(&self, error: String) {
        let mut state = self.lock_state();
        match std::mem::replace(
            &mut *state,
            SlotState::Failed {
                error: String::new(),
                connection: None,
            },
        ) {
            SlotState::Connecting => {
                *state = SlotState::Failed {
                    error,
                    connection: None,
                };
            }
            SlotState::Ready(connection) => {
                *state = SlotState::Failed {
                    error,
                    connection: Some(connection),
                };
            }
            SlotState::Failed {
                error: previous,
                connection,
            } => {
                *state = SlotState::Failed {
                    error: if previous.is_empty() { error } else { previous },
                    connection,
                };
            }
            SlotState::Closed => *state = SlotState::Closed,
        }
    }

    fn set_closed(&self) {
        *self.lock_state() = SlotState::Closed;
    }

    /// 已失败但仍保留连接时，取得该连接用于重试
    fn failed_connection(&self) -> Option<Arc<Connection>> {
        match &*self.lock_state() {
            SlotState::Failed {
                connection: Some((connection, _)),
                ..
            } => Some(Arc::clone(connection)),
            _ => None,
        }
    }

    /// 把没有保留连接的失败槽改回连接中，返回是否取得重试权
    fn retry_connect(&self) -> bool {
        let mut state = self.lock_state();
        match &*state {
            SlotState::Failed {
                connection: None, ..
            } => {
                *state = SlotState::Connecting;
                true
            }
            _ => false,
        }
    }

    /// 工具清单重试成功后清除失败原因
    fn mark_ready(&self) {
        let mut state = self.lock_state();
        if !matches!(
            *state,
            SlotState::Failed {
                connection: Some(_),
                ..
            }
        ) {
            return;
        }
        let previous = std::mem::replace(&mut *state, SlotState::Closed);
        if let SlotState::Failed {
            connection: Some(connection),
            ..
        } = previous
        {
            *state = SlotState::Ready(connection);
        }
    }

    /// 取出连接并关闭槽；失败状态里保留的连接也一并取出
    fn take_connection(&self) -> Option<ConnectionState> {
        let mut state = self.lock_state();
        match std::mem::replace(&mut *state, SlotState::Closed) {
            SlotState::Ready(connection) => Some(connection),
            SlotState::Failed {
                connection: Some(connection),
                ..
            } => Some(connection),
            SlotState::Connecting
            | SlotState::Failed {
                connection: None, ..
            } => None,
            SlotState::Closed => None,
        }
    }
}

struct Binding {
    revision: u32,
    definition: Definition,
    slot: Arc<Slot>,
}

#[derive(Default)]
struct Topic {
    sources: BTreeMap<String, BTreeMap<String, Binding>>,
}

#[derive(Default)]
struct BuiltinState {
    roots: BuiltinRoots,
    revision: u32,
}

#[derive(Default)]
struct Inner {
    topics: HashMap<i64, Topic>,
    builtin: HashMap<i64, BuiltinState>,
    closed: bool,
}

/// 话题内 MCP 绑定的连接管理与工具目录
pub struct Registry {
    connector: Connector,
    bus: Arc<Dispatcher>,
    inner: Mutex<Inner>,
    gates: Mutex<HashMap<(i64, String), Arc<AsyncMutex<()>>>>,
    generation: AtomicU64,
    stopping: CancellationToken,
}

impl Registry {
    pub fn new(connector: Connector, bus: Arc<Dispatcher>) -> Self {
        Self {
            connector,
            bus,
            inner: Mutex::new(Inner::default()),
            gates: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
            stopping: CancellationToken::new(),
        }
    }

    /// 用目标集合替换一个来源的全部绑定
    pub async fn sync(
        &self,
        topic_id: i64,
        source: &str,
        desired: Vec<BindingSpec>,
    ) -> Result<SyncReport> {
        if source.is_empty() {
            return Err(McpError::Binding(
                "the source must not be empty".to_string(),
            ));
        }
        if source == SOURCE_BUILTIN {
            return Err(McpError::Binding(
                "the builtin source is reserved".to_string(),
            ));
        }
        let desired = validate_specs(desired)?;
        self.replace(topic_id, source, desired).await
    }

    /// 用根目录集合替换内建服务的绑定
    pub async fn sync_builtin(&self, topic_id: i64, roots: BuiltinRoots) -> Result<SyncReport> {
        let gate = self.gate(topic_id, SOURCE_BUILTIN);
        let _gate = gate.lock_owned().await;
        let revision = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if inner.closed {
                return Err(McpError::Closed);
            }
            let state = inner.builtin.entry(topic_id).or_default();
            if state.revision == 0 || state.roots != roots {
                state.roots = roots.clone();
                state.revision += 1;
            }
            state.revision
        };
        let mut desired = Vec::new();
        if roots.workspace.is_some() {
            desired.push((
                "fs".to_string(),
                revision,
                Definition::Builtin(BuiltinKind::Fs, roots.clone()),
            ));
        }
        if !roots.skills.is_empty() {
            desired.push((
                "skills".to_string(),
                revision,
                Definition::Builtin(BuiltinKind::Skills, roots),
            ));
        }
        self.replace_locked(topic_id, SOURCE_BUILTIN, desired).await
    }

    /// 一个话题的全部绑定状态
    pub fn status(&self, topic_id: i64) -> Vec<BindingStatus> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(topic) = inner.topics.get(&topic_id) else {
            return Vec::new();
        };
        statuses_of(topic)
    }

    /// 汇集一个话题的全部工具
    pub async fn tools(&self, topic_id: i64, cancel: CancellationToken) -> Result<Vec<BoundTool>> {
        let bindings = self.bindings(topic_id)?;
        let mut unique: Vec<Arc<Slot>> = Vec::new();
        for binding in &bindings {
            if !unique.iter().any(|slot| Arc::ptr_eq(slot, &binding.slot)) {
                unique.push(Arc::clone(&binding.slot));
            }
        }
        let mut tools: HashMap<usize, Vec<RemoteTool>> = HashMap::new();
        for slot in &unique {
            let connection = match self.ensure_ready(slot, &cancel).await {
                Snapshot::Ready(connection) => connection,
                Snapshot::Failed(_) => match slot.failed_connection() {
                    Some(connection) => connection,
                    None => continue,
                },
                _ => continue,
            };
            match connection.list_tools(cancel.clone()).await {
                Ok(remote) => {
                    slot.mark_ready();
                    tools.insert(slot_id(slot), remote);
                }
                Err(McpError::Cancelled) => return Err(McpError::Cancelled),
                Err(McpError::Closed) if self.stopping.is_cancelled() => {
                    return Err(McpError::Closed);
                }
                Err(error) => {
                    let message = error.to_string();
                    slot.fail(message.clone());
                    self.notify_failed(topic_id, slot, &message);
                }
            }
        }
        if cancel.is_cancelled() {
            return Err(McpError::Cancelled);
        }
        if self.stopping.is_cancelled() {
            return Err(McpError::Closed);
        }
        let mut catalog = Vec::new();
        for binding in bindings {
            let Some(remote) = tools.get(&slot_id(&binding.slot)) else {
                continue;
            };
            for tool in remote {
                catalog.push(BoundTool {
                    target: ToolTarget {
                        topic_id,
                        source: binding.source.clone(),
                        key: binding.key.clone(),
                        revision: binding.revision,
                        generation: binding.slot.generation,
                        tool: tool.name.clone(),
                    },
                    description: tool.description.clone(),
                    input_schema: tool.input_schema.clone(),
                });
            }
        }
        Ok(catalog)
    }

    /// 接纳一次调用；目标必须仍指向当前连接
    pub fn admit(&self, target: &ToolTarget) -> Result<AdmittedCall> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.closed {
            return Err(McpError::Closed);
        }
        let binding = inner
            .topics
            .get(&target.topic_id)
            .and_then(|topic| topic.sources.get(&target.source))
            .and_then(|source| source.get(&target.key))
            .ok_or(McpError::ToolUnavailable)?;
        if binding.revision != target.revision || binding.slot.generation != target.generation {
            return Err(McpError::ToolUnavailable);
        }
        let Snapshot::Ready(connection) = binding.slot.snapshot() else {
            return Err(McpError::ToolUnavailable);
        };
        binding.slot.in_flight.fetch_add(1, Ordering::SeqCst);
        Ok(AdmittedCall {
            slot: Arc::clone(&binding.slot),
            connection,
            tool: target.tool.clone(),
            grace: self.connector.options().shutdown_grace,
        })
    }

    /// 移除一个话题的全部绑定
    pub async fn release_topic(&self, topic_id: i64) -> Result<()> {
        let (slots, targets) = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(topic) = inner.topics.remove(&topic_id) else {
                return Ok(());
            };
            inner.builtin.remove(&topic_id);
            let mut slots: Vec<Arc<Slot>> = Vec::new();
            let mut targets: HashMap<usize, (Arc<Slot>, Vec<(String, String)>)> = HashMap::new();
            for (source, bindings) in &topic.sources {
                for (key, binding) in bindings {
                    binding.slot.refs.fetch_sub(1, Ordering::SeqCst);
                    let entry = targets
                        .entry(slot_id(&binding.slot))
                        .or_insert_with(|| (Arc::clone(&binding.slot), Vec::new()));
                    entry.1.push((source.clone(), key.clone()));
                    if !slots.iter().any(|slot| Arc::ptr_eq(slot, &binding.slot)) {
                        slots.push(Arc::clone(&binding.slot));
                    }
                }
            }
            (slots, targets)
        };
        for slot in slots {
            let targets = targets
                .get(&slot_id(&slot))
                .map(|(_, targets)| targets.clone())
                .unwrap_or_default();
            self.schedule_close(slot, targets);
        }
        Ok(())
    }

    /// 关闭全部连接并等待清理
    pub async fn shutdown(&self) -> Result<()> {
        let slots = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            inner.closed = true;
            let mut slots: Vec<Arc<Slot>> = Vec::new();
            for topic in inner.topics.values() {
                for bindings in topic.sources.values() {
                    for binding in bindings.values() {
                        if !slots.iter().any(|slot| Arc::ptr_eq(slot, &binding.slot)) {
                            slots.push(Arc::clone(&binding.slot));
                        }
                    }
                }
            }
            slots
        };
        self.stopping.cancel();
        let grace = self.connector.options().shutdown_grace;
        for slot in slots {
            slot.refs.store(0, Ordering::SeqCst);
            close_slot(&slot, grace).await;
        }
        Ok(())
    }

    async fn replace(
        &self,
        topic_id: i64,
        source: &str,
        desired: Vec<(String, u32, Definition)>,
    ) -> Result<SyncReport> {
        let gate = self.gate(topic_id, source);
        let _gate = gate.lock_owned().await;
        self.replace_locked(topic_id, source, desired).await
    }

    async fn replace_locked(
        &self,
        topic_id: i64,
        source: &str,
        desired: Vec<(String, u32, Definition)>,
    ) -> Result<SyncReport> {
        let (established, to_close, removed, stopped_targets, pre_failed) =
            self.install(topic_id, source, desired)?;

        for (slot, error) in &pre_failed {
            self.notify_failed(topic_id, slot, error);
        }
        for slot in &established {
            let before = slot.snapshot();
            let readiness = self.ensure_ready(slot, &CancellationToken::new()).await;
            match (&before, &readiness) {
                (Snapshot::Ready(_), Snapshot::Ready(_)) => {}
                (_, Snapshot::Ready(_)) => self.notify_started(topic_id, source, slot),
                (Snapshot::Failed(_), Snapshot::Failed(_)) => {}
                (_, Snapshot::Failed(error)) => {
                    self.notify_failed(topic_id, slot, error);
                }
                _ => {}
            }
        }
        for slot in to_close {
            let targets = stopped_targets
                .get(&slot_id(&slot))
                .cloned()
                .unwrap_or_default();
            self.schedule_close(slot, targets);
        }
        let bindings = {
            let inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            inner
                .topics
                .get(&topic_id)
                .and_then(|topic| topic.sources.get(source))
                .map(|bindings| bindings_of(source, bindings))
                .unwrap_or_default()
        };
        Ok(SyncReport { bindings, removed })
    }

    fn install(
        &self,
        topic_id: i64,
        source: &str,
        desired: Vec<(String, u32, Definition)>,
    ) -> Result<(
        Vec<Arc<Slot>>,
        Vec<Arc<Slot>>,
        Vec<String>,
        HashMap<usize, Vec<(String, String)>>,
        Vec<(Arc<Slot>, String)>,
    )> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.closed {
            return Err(McpError::Closed);
        }
        let topic = inner.topics.entry(topic_id).or_default();
        let existing = topic.sources.remove(source).unwrap_or_default();
        let desired_keys: HashSet<&str> = desired.iter().map(|(key, _, _)| key.as_str()).collect();
        let removed: Vec<String> = existing
            .keys()
            .filter(|key| !desired_keys.contains(key.as_str()))
            .cloned()
            .collect();

        let mut known: HashMap<SlotKey, Arc<Slot>> = HashMap::new();
        for bindings in topic.sources.values() {
            for binding in bindings.values() {
                known
                    .entry(binding.slot.key.clone())
                    .or_insert_with(|| Arc::clone(&binding.slot));
            }
        }

        let mut next: BTreeMap<String, Binding> = BTreeMap::new();
        let mut established: Vec<Arc<Slot>> = Vec::new();
        let mut pre_failed: Vec<(Arc<Slot>, String)> = Vec::new();
        for (key, revision, definition) in desired {
            let reused = existing
                .get(&key)
                .filter(|binding| same_connection(&binding.definition, &definition));
            let slot = if let Some(binding) = reused {
                binding.slot.refs.fetch_add(1, Ordering::SeqCst);
                Arc::clone(&binding.slot)
            } else {
                match self.slot_key(&definition) {
                    Ok(slot_key) => match known.get(&slot_key) {
                        Some(slot) => {
                            slot.refs.fetch_add(1, Ordering::SeqCst);
                            Arc::clone(slot)
                        }
                        None => {
                            let slot = Arc::new(Slot::new(
                                topic_id,
                                slot_key.clone(),
                                self.next_generation(),
                                definition.clone(),
                            ));
                            slot.refs.fetch_add(1, Ordering::SeqCst);
                            known.insert(slot_key, Arc::clone(&slot));
                            established.push(Arc::clone(&slot));
                            slot
                        }
                    },
                    Err(error) => {
                        let generation = self.next_generation();
                        let slot = Arc::new(Slot::new(
                            topic_id,
                            SlotKey::Invalid(generation),
                            generation,
                            definition.clone(),
                        ));
                        let message = error.to_string();
                        slot.fail(message.clone());
                        slot.refs.fetch_add(1, Ordering::SeqCst);
                        established.push(Arc::clone(&slot));
                        pre_failed.push((Arc::clone(&slot), message));
                        slot
                    }
                }
            };
            if !established
                .iter()
                .any(|candidate| Arc::ptr_eq(candidate, &slot))
            {
                established.push(Arc::clone(&slot));
            }
            next.insert(
                key,
                Binding {
                    revision,
                    definition,
                    slot,
                },
            );
        }

        for (key, binding) in &existing {
            let reused = next
                .get(key)
                .is_some_and(|next| Arc::ptr_eq(&next.slot, &binding.slot));
            if !reused {
                binding.slot.refs.fetch_sub(1, Ordering::SeqCst);
            }
        }
        topic.sources.insert(source.to_string(), next);

        let mut to_close: Vec<Arc<Slot>> = Vec::new();
        for binding in existing.values() {
            if binding.slot.refs.load(Ordering::SeqCst) == 0
                && !to_close.iter().any(|slot| Arc::ptr_eq(slot, &binding.slot))
            {
                to_close.push(Arc::clone(&binding.slot));
            }
        }
        for slot in closeable_slots(topic) {
            if !to_close
                .iter()
                .any(|candidate| Arc::ptr_eq(candidate, &slot))
            {
                to_close.push(slot);
            }
        }
        let mut stopped_targets: HashMap<usize, Vec<(String, String)>> = HashMap::new();
        for (key, binding) in &existing {
            if to_close.iter().any(|slot| Arc::ptr_eq(slot, &binding.slot)) {
                stopped_targets
                    .entry(slot_id(&binding.slot))
                    .or_default()
                    .push((source.to_string(), key.clone()));
            }
        }
        Ok((established, to_close, removed, stopped_targets, pre_failed))
    }

    fn bindings(&self, topic_id: i64) -> Result<Vec<BindingSnapshot>> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.closed {
            return Err(McpError::Closed);
        }
        let Some(topic) = inner.topics.get(&topic_id) else {
            return Ok(Vec::new());
        };
        let mut bindings = Vec::new();
        for (source, source_bindings) in &topic.sources {
            for (key, binding) in source_bindings {
                bindings.push(BindingSnapshot {
                    source: source.clone(),
                    key: key.clone(),
                    revision: binding.revision,
                    slot: Arc::clone(&binding.slot),
                });
            }
        }
        Ok(bindings)
    }

    async fn ensure_ready(&self, slot: &Arc<Slot>, cancel: &CancellationToken) -> Snapshot {
        loop {
            match slot.snapshot() {
                Snapshot::Ready(connection) => return Snapshot::Ready(connection),
                Snapshot::Failed(error) => {
                    if slot.failed_connection().is_some() {
                        return Snapshot::Failed(error);
                    }
                    if !slot.retry_connect() {
                        continue;
                    }
                }
                Snapshot::Closed => return Snapshot::Closed,
                Snapshot::Connecting => {}
            }
            let guard = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Snapshot::Connecting,
                _ = self.stopping.cancelled() => return Snapshot::Closed,
                guard = slot.connect.lock() => guard,
            };
            match slot.snapshot() {
                Snapshot::Ready(connection) => return Snapshot::Ready(connection),
                Snapshot::Failed(error) => return Snapshot::Failed(error),
                Snapshot::Closed => return Snapshot::Closed,
                Snapshot::Connecting => {}
            }
            let opened = tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(McpError::Cancelled),
                _ = self.stopping.cancelled() => Err(McpError::Closed),
                result = open_slot(slot, &self.connector) => result,
            };
            drop(guard);
            match opened {
                Ok((connection, server)) => {
                    let kept = {
                        let inner = self
                            .inner
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        !inner.closed && slot.refs.load(Ordering::SeqCst) > 0
                    };
                    if !kept {
                        slot.set_closed();
                        let connection = Arc::new(connection);
                        let _ = connection.close().await;
                        if let Some(server) = server {
                            server.abort();
                        }
                        return Snapshot::Closed;
                    }
                    let connection = Arc::new(connection);
                    slot.set_ready(Arc::clone(&connection), server);
                    return Snapshot::Ready(connection);
                }
                Err(McpError::Cancelled) => return Snapshot::Connecting,
                Err(McpError::Closed) => {
                    slot.set_closed();
                    return Snapshot::Closed;
                }
                Err(error) => {
                    let message = error.to_string();
                    slot.fail(message.clone());
                    return Snapshot::Failed(message);
                }
            }
        }
    }

    fn slot_key(&self, definition: &Definition) -> Result<SlotKey> {
        match definition {
            Definition::External(definition) => {
                Ok(SlotKey::External(self.connector.key(&definition.config)?))
            }
            Definition::Builtin(kind, _) => Ok(SlotKey::Builtin(*kind)),
        }
    }

    fn next_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn gate(&self, topic_id: i64, source: &str) -> Arc<AsyncMutex<()>> {
        let mut gates = self
            .gates
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(
            gates
                .entry((topic_id, source.to_string()))
                .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
        )
    }

    fn schedule_close(&self, slot: Arc<Slot>, targets: Vec<(String, String)>) {
        let bus = Arc::clone(&self.bus);
        let topic_id = slot.topic_id;
        let grace = self.connector.options().shutdown_grace;
        tokio::spawn(async move {
            close_slot(&slot, grace).await;
            for (source, key) in targets {
                notify_stopped(&bus, topic_id, &source, &key);
            }
        });
    }

    fn notify_started(&self, topic_id: i64, source: &str, slot: &Arc<Slot>) {
        let Some(key) = binding_key_of(&self.inner, topic_id, source, slot) else {
            return;
        };
        self.bus.notify(
            &CORE_MCP_STARTED,
            &event_cx(),
            McpStarted {
                topic_id,
                source: source.to_string(),
                key,
            },
        );
    }

    fn notify_failed(&self, topic_id: i64, slot: &Arc<Slot>, error: &str) {
        for (source, key) in binding_keys_of(&self.inner, topic_id, slot) {
            self.bus.notify(
                &CORE_MCP_FAILED,
                &event_cx(),
                McpFailed {
                    topic_id,
                    source,
                    key,
                    error: error.to_string(),
                },
            );
        }
    }
}

/// 一次已接纳的调用
pub struct AdmittedCall {
    slot: Arc<Slot>,
    connection: Arc<Connection>,
    tool: String,
    grace: Duration,
}

impl AdmittedCall {
    pub async fn execute(
        self,
        arguments: Value,
        cancel: CancellationToken,
        deadline: Instant,
    ) -> Result<Value> {
        self.connection
            .call(&self.tool, arguments, cancel, deadline)
            .await
    }
}

impl Drop for AdmittedCall {
    fn drop(&mut self) {
        self.slot.in_flight.fetch_sub(1, Ordering::SeqCst);
        if self.slot.in_flight.load(Ordering::SeqCst) != 0 {
            return;
        }
        self.slot.idle.notify_waiters();
        if self.slot.refs.load(Ordering::SeqCst) != 0 {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let slot = Arc::clone(&self.slot);
        let grace = self.grace;
        handle.spawn(async move {
            close_slot(&slot, grace).await;
        });
    }
}

struct BindingSnapshot {
    source: String,
    key: String,
    revision: u32,
    slot: Arc<Slot>,
}

/// 决定两次声明是否指向同一次连接
fn same_connection(left: &Definition, right: &Definition) -> bool {
    match (left, right) {
        (Definition::External(left), Definition::External(right)) => left.config == right.config,
        (
            Definition::Builtin(left_kind, left_roots),
            Definition::Builtin(right_kind, right_roots),
        ) => left_kind == right_kind && left_roots == right_roots,
        _ => false,
    }
}

fn validate_specs(desired: Vec<BindingSpec>) -> Result<Vec<(String, u32, Definition)>> {
    let mut seen = HashSet::new();
    let mut validated = Vec::with_capacity(desired.len());
    for spec in desired {
        if spec.key.is_empty() {
            return Err(McpError::Binding("the key must not be empty".to_string()));
        }
        if !seen.insert(spec.key.clone()) {
            return Err(McpError::Binding(format!(
                "the key {} appears twice",
                spec.key
            )));
        }
        config::validate_definition(&spec.definition).map_err(|error| match error {
            McpError::InvalidConfig(message) => McpError::Binding(format!(
                "the definition of {} is invalid: {message}",
                spec.key
            )),
            other => other,
        })?;
        validated.push((
            spec.key,
            spec.revision,
            Definition::External(spec.definition),
        ));
    }
    Ok(validated)
}

fn statuses_of(topic: &Topic) -> Vec<BindingStatus> {
    let mut statuses = Vec::new();
    for (source, bindings) in &topic.sources {
        statuses.extend(bindings_of(source, bindings));
    }
    statuses
}

fn bindings_of(source: &str, bindings: &BTreeMap<String, Binding>) -> Vec<BindingStatus> {
    bindings
        .iter()
        .map(|(key, binding)| {
            let (state, error) = match binding.slot.snapshot() {
                Snapshot::Connecting => (BindingState::Connecting, None),
                Snapshot::Ready(_) => (BindingState::Ready, None),
                Snapshot::Failed(error) => (BindingState::Failed, Some(error)),
                Snapshot::Closed => (
                    BindingState::Failed,
                    Some("the connection is closed".to_string()),
                ),
            };
            BindingStatus {
                source: source.to_string(),
                key: key.clone(),
                revision: binding.revision,
                state,
                error,
            }
        })
        .collect()
}

fn closeable_slots(topic: &Topic) -> Vec<Arc<Slot>> {
    let mut slots: Vec<Arc<Slot>> = Vec::new();
    for bindings in topic.sources.values() {
        for binding in bindings.values() {
            if binding.slot.refs.load(Ordering::SeqCst) == 0
                && !slots.iter().any(|slot| Arc::ptr_eq(slot, &binding.slot))
            {
                slots.push(Arc::clone(&binding.slot));
            }
        }
    }
    slots
}

async fn open_slot(
    slot: &Slot,
    connector: &Connector,
) -> Result<(Connection, Option<JoinHandle<()>>)> {
    match &slot.definition {
        Definition::External(definition) => {
            Ok((connector.connect(&definition.config).await?, None))
        }
        Definition::Builtin(kind, roots) => crate::builtin::connect(*kind, roots, connector).await,
    }
}

async fn close_slot(slot: &Arc<Slot>, grace: Duration) {
    let _guard = slot.connect.lock().await;
    loop {
        if slot.refs.load(Ordering::SeqCst) > 0 {
            return;
        }
        if slot.in_flight.load(Ordering::SeqCst) == 0 {
            break;
        }
        let idle = slot.idle.notified();
        if tokio::time::timeout(grace, idle).await.is_err() {
            break;
        }
    }
    let Some((connection, server)) = slot.take_connection() else {
        return;
    };
    if let Err(error) = connection.close().await {
        log::warn!("[mcp] closing a connection failed: {error}");
    }
    if let Some(server) = server {
        server.abort();
    }
}

fn slot_id(slot: &Arc<Slot>) -> usize {
    Arc::as_ptr(slot) as usize
}

fn event_cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Internal,
        Scope::Global,
        Source::core("mcp"),
        EVENT_BUDGET,
    )
}

fn notify_stopped(bus: &Dispatcher, topic_id: i64, source: &str, key: &str) {
    bus.notify(
        &CORE_MCP_STOPPED,
        &event_cx(),
        McpStopped {
            topic_id,
            source: source.to_string(),
            key: key.to_string(),
        },
    );
}

fn binding_key_of(
    inner: &Mutex<Inner>,
    topic_id: i64,
    source: &str,
    slot: &Arc<Slot>,
) -> Option<String> {
    let inner = inner
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    inner
        .topics
        .get(&topic_id)
        .and_then(|topic| topic.sources.get(source))
        .and_then(|bindings| {
            bindings
                .iter()
                .find_map(|(key, binding)| Arc::ptr_eq(&binding.slot, slot).then(|| key.clone()))
        })
}

fn binding_keys_of(inner: &Mutex<Inner>, topic_id: i64, slot: &Arc<Slot>) -> Vec<(String, String)> {
    let inner = inner
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(topic) = inner.topics.get(&topic_id) else {
        return Vec::new();
    };
    let mut keys = Vec::new();
    for (source, bindings) in &topic.sources {
        for (key, binding) in bindings {
            if Arc::ptr_eq(&binding.slot, slot) {
                keys.push((source.clone(), key.clone()));
            }
        }
    }
    keys
}
