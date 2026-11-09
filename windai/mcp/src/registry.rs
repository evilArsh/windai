use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::slice::Iter;
use std::sync::Arc;

use strum::AsRefStr;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use wind_event::Dispatcher;

use crate::connector::{Connection, ToolDescriptor};
use crate::error::{McpError, Result};
use crate::events::StopReason;
use crate::roots::{Roots, ScopeId, ScopeRoots};
use crate::spec::{Fingerprint, ServiceSpec};

mod command;
mod run;

pub use run::{spawn, spawn_with};

use command::{Command, Effect, Event, ReportPhase};

pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

pub trait Launcher: Send + Sync {
    fn start<'a>(
        &'a self,
        request: &'a LaunchRequest,
        roots: &'a Roots,
        bus: &'a Dispatcher,
        generation: u64,
    ) -> BoxFuture<'a, Result<Started>>;

    fn list_tools<'a>(
        &'a self,
        name: &'a str,
        connection: &'a Arc<Connection>,
    ) -> BoxFuture<'a, Result<Vec<ToolDescriptor>>>;
}

pub struct Started {
    pub connection: Arc<Connection>,
    pub tools: Vec<ToolDescriptor>,
    pub server_task: Option<JoinHandle<()>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum BuiltinKind {
    Fs,
    Skills,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaunchRequest {
    External(ServiceSpec),
    Builtin { scope: ScopeId, kind: BuiltinKind },
}

impl LaunchRequest {
    pub fn name(&self) -> String {
        match self {
            Self::External(spec) => spec.name.clone(),
            Self::Builtin { scope, kind } => format!("{}:{scope}", kind.as_ref()),
        }
    }

    fn fingerprint(&self) -> Option<Fingerprint> {
        match self {
            Self::External(spec) => Some(spec.fingerprint()),
            Self::Builtin { .. } => None,
        }
    }
}

#[derive(Clone)]
pub struct ToolCall {
    pub server: String,
    pub tool: String,
    connection: Arc<Connection>,
}

impl fmt::Debug for ToolCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolCall")
            .field("server", &self.server)
            .field("tool", &self.tool)
            .finish_non_exhaustive()
    }
}

impl PartialEq for ToolCall {
    fn eq(&self, other: &Self) -> bool {
        self.server == other.server
            && self.tool == other.tool
            && Arc::ptr_eq(&self.connection, &other.connection)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoundTool {
    pub server: String,
    pub tool: ToolDescriptor,
    pub call: ToolCall,
}

/// 只含 Running 条目的工具缓存
#[derive(Clone, Default)]
pub struct Catalog(Arc<[BoundTool]>);

impl Catalog {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> Iter<'_, BoundTool> {
        self.0.iter()
    }
}

#[derive(Clone)]
pub struct RegistryHandle {
    commands: mpsc::UnboundedSender<Command>,
}

impl RegistryHandle {
    pub fn acquire(&self, ref_id: String, spec: ServiceSpec) {
        self.send(Command::Acquire {
            ref_id,
            request: LaunchRequest::External(spec),
        });
    }

    pub fn acquire_builtin(&self, ref_id: String, scope: ScopeId, kind: BuiltinKind) {
        self.send(Command::Acquire {
            ref_id,
            request: LaunchRequest::Builtin { scope, kind },
        });
    }

    pub fn set_scope_roots(&self, scope: ScopeId, roots: ScopeRoots) {
        self.send(Command::SetRoots { scope, roots });
    }

    pub fn release(&self, ref_id: String) {
        self.send(Command::Release { ref_id });
    }

    pub fn refresh(&self, name: &str) {
        self.send(Command::Refresh {
            name: name.to_owned(),
        });
    }

    fn send(&self, command: Command) {
        let _ = self.commands.send(command);
    }
}

enum State {
    Starting(u64),
    Running(u64),
    Stopping(u64),
    Failed {
        #[allow(dead_code)]
        error: String,
        generation: u64,
    },
}

impl State {
    fn generation(&self) -> u64 {
        match self {
            State::Starting(generation)
            | State::Running(generation)
            | State::Stopping(generation) => *generation,
            State::Failed { generation, .. } => *generation,
        }
    }
}

struct Entry {
    request: LaunchRequest,
    fingerprint: Option<Fingerprint>,
    refs: BTreeSet<String>,
    state: State,
    tools: Vec<ToolDescriptor>,
    connection: Option<Arc<Connection>>,
    server_task: Option<JoinHandle<()>>,
    /// 停止期间收到、待停止完成后执行的启动请求
    want: Option<LaunchRequest>,
    /// 启动尚未回报就被释放
    discard_start: bool,
}

struct StopRecord {
    refs: Vec<String>,
    reason: StopReason,
}

struct Registry {
    entries: BTreeMap<String, Entry>,
    roots: Roots,
    catalog: Catalog,
    dirty: bool,
    generation: u64,
    outstanding: usize,
    pending_shutdowns: Vec<oneshot::Sender<()>>,
    events: Vec<Event>,
    stops: BTreeMap<(String, u64), StopRecord>,
}

impl Registry {
    fn new(roots: Roots) -> Self {
        Self {
            entries: BTreeMap::new(),
            roots,
            catalog: Catalog::default(),
            dirty: false,
            generation: 0,
            outstanding: 0,
            pending_shutdowns: Vec::new(),
            events: Vec::new(),
            stops: BTreeMap::new(),
        }
    }

    fn handle(&mut self, command: Command) -> (Vec<Effect>, Vec<Event>) {
        let effects = match command {
            Command::Acquire { ref_id, request } => {
                if self.pending_shutdowns.is_empty() {
                    self.acquire(ref_id, request)
                } else {
                    Vec::new()
                }
            }
            Command::Release { ref_id } => {
                if self.pending_shutdowns.is_empty() {
                    self.release(&ref_id)
                } else {
                    Vec::new()
                }
            }
            Command::SetRoots { scope, roots } => {
                self.roots.set_scope(&scope, roots);
                Vec::new()
            }
            Command::ListTools { reply } => {
                self.flush_catalog();
                let _ = reply.send(self.catalog.clone());
                Vec::new()
            }
            Command::EntryGeneration { name, reply } => {
                let generation = self
                    .entries
                    .get(&name)
                    .map(|entry| entry.state.generation());
                let _ = reply.send(generation);
                Vec::new()
            }
            Command::Refresh { name } => {
                if self.pending_shutdowns.is_empty() {
                    self.refresh(&name)
                } else {
                    Vec::new()
                }
            }
            Command::Report {
                name,
                generation,
                phase,
            } => match phase {
                ReportPhase::Launch(result) => self.report_launch(name, generation, result),
                ReportPhase::Stop(result) => self.report_stop(&name, generation, result),
            },
            Command::RefreshDone {
                name,
                generation,
                result,
            } => {
                self.report_refresh(&name, generation, result);
                Vec::new()
            }
            Command::EffectDone => {
                self.outstanding = self.outstanding.saturating_sub(1);
                Vec::new()
            }
            Command::Shutdown { reply } => self.begin_shutdown(reply),
        };
        self.outstanding += effects
            .iter()
            .filter(|effect| {
                matches!(
                    effect,
                    Effect::Start { .. } | Effect::Stop { .. } | Effect::Discard(_)
                )
            })
            .count();
        self.flush_catalog();
        (effects, std::mem::take(&mut self.events))
    }

    fn acquire(&mut self, ref_id: String, request: LaunchRequest) -> Vec<Effect> {
        let name = request.name();
        let fingerprint = request.fingerprint();
        if !self.entries.contains_key(&name) {
            self.generation += 1;
            let generation = self.generation;
            let mut entry_refs = BTreeSet::new();
            entry_refs.insert(ref_id);
            let refs = refs_snapshot(&entry_refs);
            self.entries.insert(
                name.clone(),
                Entry {
                    request: request.clone(),
                    fingerprint,
                    refs: entry_refs,
                    state: State::Starting(generation),
                    tools: Vec::new(),
                    connection: None,
                    server_task: None,
                    want: None,
                    discard_start: false,
                },
            );
            self.events.push(Event::Starting {
                name: name.clone(),
                refs: refs.clone(),
            });
            return vec![Effect::Start {
                name,
                generation,
                request,
                refs,
                previous: None,
            }];
        }

        let Some(entry) = self.entries.get_mut(&name) else {
            log::error!("the mcp service {name} disappeared during acquire");
            return Vec::new();
        };
        entry.refs.insert(ref_id);
        if entry.fingerprint != fingerprint {
            self.generation += 1;
            let generation = self.generation;
            let previous = match &entry.request {
                LaunchRequest::External(spec) => Some(Box::new(spec.clone())),
                LaunchRequest::Builtin { .. } => None,
            };
            let mut effects = Vec::new();
            if matches!(&entry.state, State::Running(_)) {
                match entry.connection.take() {
                    Some(connection) => {
                        self.stops.insert(
                            (name.clone(), generation),
                            StopRecord {
                                refs: refs_snapshot(&entry.refs),
                                reason: StopReason::Replaced,
                            },
                        );
                        effects.push(Effect::Stop {
                            name: name.clone(),
                            generation,
                            connection,
                            server_task: entry.server_task.take(),
                        });
                    }
                    None => {
                        log::error!("the running mcp service {name} has no connection");
                        if let Some(task) = entry.server_task.take() {
                            task.abort();
                        }
                    }
                }
            }
            entry.request = request.clone();
            entry.fingerprint = fingerprint;
            entry.tools.clear();
            entry.connection = None;
            entry.server_task = None;
            entry.want = None;
            entry.state = State::Starting(generation);
            entry.discard_start = false;
            self.dirty = true;
            let refs = refs_snapshot(&entry.refs);
            self.events.push(Event::Starting {
                name: name.clone(),
                refs: refs.clone(),
            });
            effects.push(Effect::Start {
                name,
                generation,
                request,
                refs,
                previous,
            });
            return effects;
        }

        if matches!(&entry.state, State::Stopping(_)) {
            entry.want = Some(request);
        } else if matches!(&entry.state, State::Failed { .. }) {
            self.generation += 1;
            let generation = self.generation;
            entry.request = request.clone();
            entry.tools.clear();
            entry.state = State::Starting(generation);
            entry.discard_start = false;
            self.dirty = true;
            let refs = refs_snapshot(&entry.refs);
            self.events.push(Event::Starting {
                name: name.clone(),
                refs: refs.clone(),
            });
            return vec![Effect::Start {
                name,
                generation,
                request,
                refs,
                previous: None,
            }];
        }
        Vec::new()
    }

    fn release(&mut self, ref_id: &str) -> Vec<Effect> {
        let names: Vec<String> = self.entries.keys().cloned().collect();
        let mut effects = Vec::new();
        for name in names {
            let remove = {
                let Some(entry) = self.entries.get_mut(&name) else {
                    log::error!("the mcp service {name} disappeared during release");
                    continue;
                };
                let refs = refs_snapshot(&entry.refs);
                entry.refs.remove(ref_id);
                if !entry.refs.is_empty() {
                    continue;
                }
                match &entry.state {
                    State::Running(_) => match entry.connection.take() {
                        Some(connection) => {
                            self.generation += 1;
                            let generation = self.generation;
                            let server_task = entry.server_task.take();
                            entry.tools.clear();
                            entry.state = State::Stopping(generation);
                            self.dirty = true;
                            self.stops.insert(
                                (name.clone(), generation),
                                StopRecord {
                                    refs,
                                    reason: StopReason::Released,
                                },
                            );
                            effects.push(Effect::Stop {
                                name: name.clone(),
                                generation,
                                connection,
                                server_task,
                            });
                            false
                        }
                        None => {
                            log::error!("the running mcp service {name} has no connection");
                            if let Some(task) = entry.server_task.take() {
                                task.abort();
                            }
                            true
                        }
                    },
                    State::Starting(_) => {
                        self.generation += 1;
                        let generation = self.generation;
                        entry.state = State::Stopping(generation);
                        entry.discard_start = true;
                        false
                    }
                    State::Stopping(_) => {
                        entry.want = None;
                        false
                    }
                    State::Failed { .. } => true,
                }
            };
            if remove {
                self.drop_entry(&name);
            }
        }
        effects
    }

    fn refresh(&mut self, name: &str) -> Vec<Effect> {
        let Some(entry) = self.entries.get(name) else {
            return Vec::new();
        };
        let State::Running(generation) = entry.state else {
            return Vec::new();
        };
        let Some(connection) = &entry.connection else {
            return Vec::new();
        };
        vec![Effect::Refresh {
            name: name.to_owned(),
            generation,
            connection: Arc::clone(connection),
        }]
    }

    fn report_launch(
        &mut self,
        name: String,
        generation: u64,
        result: Result<Started>,
    ) -> Vec<Effect> {
        let started = match result {
            Ok(started) => started,
            Err(error) => return self.launch_failed(&name, generation, error),
        };
        let mut effects = Vec::new();
        let Some(entry) = self.entries.get_mut(&name) else {
            effects.push(Effect::Discard(started));
            return effects;
        };
        let current = entry.state.generation();
        let starting = matches!(&entry.state, State::Starting(_));
        let stopping = matches!(&entry.state, State::Stopping(_));
        let mut remove = false;
        if starting && current == generation {
            let refs = refs_snapshot(&entry.refs);
            let tools: Vec<String> = started.tools.iter().map(|tool| tool.name.clone()).collect();
            entry.state = State::Running(generation);
            entry.tools = started.tools;
            entry.connection = Some(started.connection);
            entry.server_task = started.server_task;
            entry.discard_start = false;
            self.dirty = true;
            self.events.push(Event::Started {
                name: name.clone(),
                refs,
                tools,
            });
        } else if stopping && generation < current {
            entry.discard_start = false;
            effects.push(Effect::Discard(started));
            if let Some(want) = entry.want.take() {
                self.generation += 1;
                let next = self.generation;
                entry.state = State::Starting(next);
                let refs = refs_snapshot(&entry.refs);
                self.events.push(Event::Starting {
                    name: name.clone(),
                    refs: refs.clone(),
                });
                effects.push(Effect::Start {
                    name: name.clone(),
                    generation: next,
                    request: want,
                    refs,
                    previous: None,
                });
            } else if entry.refs.is_empty() || !self.pending_shutdowns.is_empty() {
                remove = true;
            }
        } else {
            effects.push(Effect::Discard(started));
        }
        if remove {
            self.drop_entry(&name);
        }
        effects
    }

    fn launch_failed(&mut self, name: &str, generation: u64, error: McpError) -> Vec<Effect> {
        let message = error.to_string();
        let Some(entry) = self.entries.get_mut(name) else {
            return Vec::new();
        };
        let current = entry.state.generation();
        let starting = matches!(&entry.state, State::Starting(_));
        let stopping = matches!(&entry.state, State::Stopping(_));
        let mut remove = false;
        let mut restart = None;
        if starting && current == generation {
            let refs = refs_snapshot(&entry.refs);
            entry.state = State::Failed {
                error: message.clone(),
                generation,
            };
            entry.tools.clear();
            self.events.push(Event::Failed {
                name: name.to_owned(),
                refs,
                error: message,
            });
            if entry.refs.is_empty() {
                remove = true;
            } else if let Some(want) = entry.want.take() {
                self.generation += 1;
                let next = self.generation;
                entry.state = State::Starting(next);
                restart = Some((next, want));
            }
            self.dirty = true;
        } else if stopping && entry.discard_start && generation < current {
            entry.discard_start = false;
            if let Some(want) = entry.want.take() {
                self.generation += 1;
                let next = self.generation;
                entry.state = State::Starting(next);
                restart = Some((next, want));
            } else if entry.refs.is_empty() || !self.pending_shutdowns.is_empty() {
                remove = true;
            }
        }
        let mut effects = Vec::new();
        if let Some((generation, request)) = restart {
            let refs = refs_snapshot(&entry.refs);
            self.events.push(Event::Starting {
                name: name.to_owned(),
                refs: refs.clone(),
            });
            effects.push(Effect::Start {
                name: name.to_owned(),
                generation,
                request,
                refs,
                previous: None,
            });
        }
        if remove {
            self.drop_entry(name);
        }
        effects
    }

    fn report_stop(&mut self, name: &str, generation: u64, result: Result<()>) -> Vec<Effect> {
        if let Err(error) = &result {
            log::warn!("cannot stop the mcp service {name}: {error}");
        }
        if let Some(record) = self.stops.remove(&(name.to_owned(), generation)) {
            self.events.push(Event::Stopped {
                name: name.to_owned(),
                refs: record.refs,
                reason: record.reason,
            });
        }
        let mut effects = Vec::new();
        let mut remove = false;
        if let Some(entry) = self.entries.get_mut(name)
            && matches!(&entry.state, State::Stopping(current) if *current == generation)
        {
            if let Some(want) = entry.want.take() {
                self.generation += 1;
                let next = self.generation;
                entry.state = State::Starting(next);
                let refs = refs_snapshot(&entry.refs);
                self.events.push(Event::Starting {
                    name: name.to_owned(),
                    refs: refs.clone(),
                });
                effects.push(Effect::Start {
                    name: name.to_owned(),
                    generation: next,
                    request: want,
                    refs,
                    previous: None,
                });
            } else {
                remove = true;
            }
            self.dirty = true;
        }
        if remove {
            self.drop_entry(name);
        }
        effects
    }

    fn report_refresh(&mut self, name: &str, generation: u64, result: Result<Vec<ToolDescriptor>>) {
        match result {
            Ok(tools) => {
                if let Some(entry) = self.entries.get_mut(name)
                    && matches!(&entry.state, State::Running(current) if *current == generation)
                {
                    entry.tools = tools;
                    self.dirty = true;
                }
            }
            Err(error) => log::warn!("cannot refresh the tool list of {name}: {error}"),
        }
    }

    fn begin_shutdown(&mut self, reply: oneshot::Sender<()>) -> Vec<Effect> {
        self.pending_shutdowns.push(reply);
        let names: Vec<String> = self.entries.keys().cloned().collect();
        let mut effects = Vec::new();
        for name in names {
            let mut remove = false;
            {
                let Some(entry) = self.entries.get_mut(&name) else {
                    log::error!("the mcp service {name} disappeared during shutdown");
                    continue;
                };
                entry.want = None;
                if matches!(&entry.state, State::Running(_)) {
                    match entry.connection.take() {
                        Some(connection) => {
                            self.generation += 1;
                            let generation = self.generation;
                            let server_task = entry.server_task.take();
                            entry.tools.clear();
                            entry.state = State::Stopping(generation);
                            self.dirty = true;
                            self.stops.insert(
                                (name.clone(), generation),
                                StopRecord {
                                    refs: refs_snapshot(&entry.refs),
                                    reason: StopReason::Shutdown,
                                },
                            );
                            effects.push(Effect::Stop {
                                name: name.clone(),
                                generation,
                                connection,
                                server_task,
                            });
                        }
                        None => {
                            log::error!("the running mcp service {name} has no connection");
                            if let Some(task) = entry.server_task.take() {
                                task.abort();
                            }
                            remove = true;
                        }
                    }
                } else if matches!(&entry.state, State::Starting(_)) {
                    self.generation += 1;
                    let generation = self.generation;
                    entry.state = State::Stopping(generation);
                    entry.discard_start = true;
                } else if matches!(&entry.state, State::Failed { .. }) {
                    remove = true;
                }
            }
            if remove {
                self.drop_entry(&name);
            }
        }
        effects
    }

    fn take_finished_shutdown(&mut self) -> Vec<oneshot::Sender<()>> {
        if self.entries.is_empty() && self.outstanding == 0 && !self.pending_shutdowns.is_empty() {
            std::mem::take(&mut self.pending_shutdowns)
        } else {
            Vec::new()
        }
    }

    fn cleanup(&mut self) {
        for entry in self.entries.values_mut() {
            if let Some(task) = entry.server_task.take() {
                task.abort();
            }
        }
        self.entries.clear();
        self.stops.clear();
        self.events.clear();
    }

    fn drop_entry(&mut self, name: &str) {
        let Some(entry) = self.entries.remove(name) else {
            return;
        };
        if let LaunchRequest::Builtin { scope, .. } = &entry.request {
            let shared = self.entries.values().any(|other| {
                matches!(
                    &other.request,
                    LaunchRequest::Builtin { scope: other_scope, .. } if other_scope == scope
                )
            });
            if !shared {
                self.roots.drop_scope(scope);
            }
        }
        self.dirty = true;
    }

    fn flush_catalog(&mut self) {
        if !self.dirty {
            return;
        }
        let mut tools = Vec::new();
        for (name, entry) in &self.entries {
            let State::Running(_) = entry.state else {
                continue;
            };
            let Some(connection) = &entry.connection else {
                continue;
            };
            for tool in &entry.tools {
                tools.push(BoundTool {
                    server: name.clone(),
                    tool: tool.clone(),
                    call: ToolCall {
                        server: name.clone(),
                        tool: tool.name.clone(),
                        connection: Arc::clone(connection),
                    },
                });
            }
        }
        self.catalog = Catalog(Arc::from(tools));
        self.dirty = false;
    }
}

fn refs_snapshot(refs: &BTreeSet<String>) -> Vec<String> {
    refs.iter().cloned().collect()
}
