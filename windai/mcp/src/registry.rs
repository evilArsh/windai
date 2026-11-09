use super::builtin;
use super::connector::{CallOutcome, Connection, LogSink, ToolDescriptor};
use super::error::{McpError, Result};
use super::events::{
    self, CORE_MCP_FAILED, CORE_MCP_LAUNCH, CORE_MCP_LOG, CORE_MCP_REPLACED, CORE_MCP_STARTED,
    CORE_MCP_STARTING, CORE_MCP_STOPPED, McpFailed, McpLaunch, McpLog, McpReplaced, McpStarted,
    McpStarting, McpStopped, StopReason,
};
use super::options::McpOptions;
use super::runner::{self, LaunchPlan};
use rmcp::ServerHandler;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{self, WeakUnboundedSender};
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use wind_event::Dispatcher;
use wind_wire::mcp::{McpConfig, McpDefinition};

/// stderr 日志事件单行保留的字节数
const LOG_LINE_LIMIT: usize = 4096;

#[derive(Clone)]
pub struct ToolCall {
    pub server: String,
    pub tool: String,
    connection: Arc<Connection>,
}

impl ToolCall {
    pub async fn execute(
        self,
        arguments: Value,
        cancel: CancellationToken,
        deadline: Instant,
    ) -> CallOutcome {
        self.connection
            .call(&self.tool, arguments, cancel, deadline)
            .await
    }
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

impl Deref for Catalog {
    type Target = [BoundTool];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
pub struct RegistryHandle {
    commands: mpsc::UnboundedSender<Command>,
    options: McpOptions,
}

impl RegistryHandle {
    pub(crate) fn spawn(options: McpOptions, bus: Arc<Dispatcher>) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let actor = run(options.clone(), bus, sender.clone(), receiver);
        tokio::spawn(actor);
        Self {
            commands: sender,
            options,
        }
    }

    /// 按引用请求服务；同名同定义只启动一次
    pub fn acquire(&self, ref_id: String, definition: McpDefinition) {
        let _ = self.commands.send(Command::Acquire { ref_id, definition });
    }

    /// 按引用接入内建服务，实例名取自 `get_info` 的服务名
    pub async fn acquire_builtin<S>(&self, ref_id: String, server: S) -> Result<()>
    where
        S: ServerHandler,
    {
        let name = server.get_info().server_info.name;
        let (connection, server_task) = builtin::connect(server, &self.options).await?;
        let tools = match connection.list_tools().await {
            Ok(tools) => tools,
            Err(error) => {
                server_task.abort();
                connection.close().await;
                return Err(error);
            }
        };
        let command = Command::AdoptBuiltin(Adopted {
            ref_id,
            name,
            connection,
            tools,
            server_task,
        });
        if let Err(error) = self.commands.send(command) {
            if let Command::AdoptBuiltin(adopted) = error.0 {
                adopted.server_task.abort();
                adopted.connection.close().await;
            }
            return Err(McpError::ShuttingDown);
        }
        Ok(())
    }

    pub fn release(&self, ref_id: String) {
        let _ = self.commands.send(Command::Release { ref_id });
    }

    pub async fn list_tools(&self) -> Catalog {
        let (reply, response) = oneshot::channel();
        if self.commands.send(Command::ListTools { reply }).is_err() {
            return Catalog::default();
        }
        response.await.unwrap_or_default()
    }

    pub async fn shutdown(&self) {
        let (reply, response) = oneshot::channel();
        if self.commands.send(Command::Shutdown { reply }).is_err() {
            return;
        }
        let _ = response.await;
    }
}

struct Started {
    connection: Arc<Connection>,
    tools: Vec<ToolDescriptor>,
}

enum Definition {
    External(McpDefinition),
    Builtin,
}

#[derive(Clone, Copy)]
enum State {
    Starting(u64),
    Running,
    Stopping(u64),
    Failed,
}

struct PendingStart {
    definition: McpDefinition,
    previous: Option<McpDefinition>,
}

struct Adopted {
    ref_id: String,
    name: String,
    connection: Arc<Connection>,
    tools: Vec<ToolDescriptor>,
    server_task: JoinHandle<()>,
}

struct Entry {
    definition: Definition,
    refs: BTreeSet<String>,
    state: State,
    tools: Vec<ToolDescriptor>,
    connection: Option<Arc<Connection>>,
    /// 内建服务的服务端任务
    server_task: Option<JoinHandle<()>>,
    /// 停止完成后要执行的启动
    want: Option<PendingStart>,
}

impl Entry {
    fn wanted_definition(&self) -> Option<&McpDefinition> {
        match &self.want {
            Some(want) => Some(&want.definition),
            None => match &self.definition {
                Definition::External(definition) => Some(definition),
                Definition::Builtin => None,
            },
        }
    }
}

enum Command {
    Acquire {
        ref_id: String,
        definition: McpDefinition,
    },
    AdoptBuiltin(Adopted),
    Release {
        ref_id: String,
    },
    ListTools {
        reply: oneshot::Sender<Catalog>,
    },
    Report {
        name: String,
        generation: u64,
        result: Result<Started>,
    },
    StopDone {
        name: String,
        generation: u64,
        refs: Vec<String>,
        reason: StopReason,
    },
    EffectDone,
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

enum Effect {
    Start {
        name: String,
        generation: u64,
        definition: Box<McpDefinition>,
        refs: Vec<String>,
        previous: Option<Box<McpDefinition>>,
    },
    Stop {
        name: String,
        generation: u64,
        connection: Arc<Connection>,
        server_task: Option<JoinHandle<()>>,
        refs: Vec<String>,
        reason: StopReason,
    },
    Discard(Started),
    DiscardBuiltin {
        connection: Arc<Connection>,
        server_task: JoinHandle<()>,
    },
}

struct Registry {
    entries: BTreeMap<String, Entry>,
    locks: Arc<StartupLocks>,
    generation: u64,
    catalog: Catalog,
    dirty: bool,
    outstanding: usize,
    closed: bool,
    pending_shutdowns: Vec<oneshot::Sender<()>>,
}

impl Registry {
    fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            locks: Arc::new(StartupLocks::default()),
            generation: 0,
            catalog: Catalog::default(),
            dirty: false,
            outstanding: 0,
            closed: false,
            pending_shutdowns: Vec::new(),
        }
    }

    fn handle(&mut self, command: Command, bus: &Dispatcher) -> Vec<Effect> {
        match command {
            Command::ListTools { reply } => {
                self.flush_catalog();
                let _ = reply.send(self.catalog.clone());
                Vec::new()
            }
            Command::EffectDone => {
                self.outstanding = self.outstanding.saturating_sub(1);
                Vec::new()
            }
            command => {
                self.dirty = true;
                let mut effects = Vec::new();
                match command {
                    Command::Acquire { ref_id, definition } => {
                        self.acquire(ref_id, definition, bus, &mut effects);
                    }
                    Command::AdoptBuiltin(adopted) => {
                        self.adopt_builtin(adopted, bus, &mut effects);
                    }
                    Command::Release { ref_id } => {
                        self.release(&ref_id, &mut effects);
                    }
                    Command::Report {
                        name,
                        generation,
                        result,
                    } => {
                        if let Some(effect) = self.report(name, generation, result, bus) {
                            effects.push(effect);
                        }
                    }
                    Command::StopDone {
                        name,
                        generation,
                        refs,
                        reason,
                    } => {
                        self.stop_done(name, generation, refs, reason, bus, &mut effects);
                    }
                    Command::Shutdown { reply } => {
                        self.begin_shutdown(reply, &mut effects);
                    }
                    Command::ListTools { .. } | Command::EffectDone => {}
                }
                effects
            }
        }
    }

    fn acquire(
        &mut self,
        ref_id: String,
        definition: McpDefinition,
        bus: &Dispatcher,
        effects: &mut Vec<Effect>,
    ) {
        if self.closed {
            return;
        }
        let name = definition.name.clone();
        let generation = self.next_generation();
        let Some(entry) = self.entries.get_mut(&name) else {
            let refs = BTreeSet::from([ref_id]);
            emit_starting(bus, &name, &refs);
            effects.push(Effect::Start {
                name: name.clone(),
                generation,
                definition: Box::new(definition.clone()),
                refs: refs.iter().cloned().collect(),
                previous: None,
            });
            self.entries.insert(
                name,
                Entry {
                    definition: Definition::External(definition),
                    refs,
                    state: State::Starting(generation),
                    tools: Vec::new(),
                    connection: None,
                    server_task: None,
                    want: None,
                },
            );
            return;
        };
        entry.refs.insert(ref_id);
        let same = entry
            .wanted_definition()
            .is_some_and(|current| *current == definition);
        match entry.state {
            State::Failed => {
                entry.definition = Definition::External(definition.clone());
                entry.state = State::Starting(generation);
                entry.want = None;
                emit_starting(bus, &name, &entry.refs);
                effects.push(Effect::Start {
                    name,
                    generation,
                    definition: Box::new(definition),
                    refs: refs_of(&entry.refs),
                    previous: None,
                });
            }
            State::Running if same => {}
            State::Starting(_) if same => {}
            State::Stopping(_) if same => {}
            State::Running => {
                let previous = external_of(&entry.definition);
                let connection = entry.connection.take();
                let server_task = entry.server_task.take();
                let refs = refs_of(&entry.refs);
                entry.definition = Definition::External(definition.clone());
                entry.state = State::Stopping(generation);
                entry.want = Some(PendingStart {
                    definition: definition.clone(),
                    previous: previous.clone(),
                });
                if let Some(connection) = connection {
                    effects.push(Effect::Stop {
                        name,
                        generation,
                        connection,
                        server_task,
                        refs,
                        reason: StopReason::Replaced,
                    });
                } else {
                    entry.state = State::Starting(generation);
                    entry.want = None;
                    emit_starting(bus, &name, &entry.refs);
                    effects.push(Effect::Start {
                        name,
                        generation,
                        definition: Box::new(definition),
                        refs,
                        previous: previous.map(Box::new),
                    });
                }
            }
            State::Starting(_) => {
                entry.definition = Definition::External(definition.clone());
                entry.state = State::Starting(generation);
                emit_starting(bus, &name, &entry.refs);
                effects.push(Effect::Start {
                    name,
                    generation,
                    definition: Box::new(definition),
                    refs: refs_of(&entry.refs),
                    previous: None,
                });
            }
            State::Stopping(_) => {
                entry.definition = Definition::External(definition.clone());
                match &mut entry.want {
                    Some(want) => want.definition = definition,
                    None => {
                        entry.want = Some(PendingStart {
                            definition,
                            previous: None,
                        });
                    }
                }
            }
        }
    }

    fn adopt_builtin(&mut self, adopted: Adopted, bus: &Dispatcher, effects: &mut Vec<Effect>) {
        let Adopted {
            ref_id,
            name,
            connection,
            tools,
            server_task,
        } = adopted;
        if self.closed {
            effects.push(Effect::DiscardBuiltin {
                connection,
                server_task,
            });
            return;
        }
        if let Some(entry) = self.entries.get_mut(&name)
            && matches!(entry.state, State::Running)
            && matches!(entry.definition, Definition::Builtin)
        {
            entry.refs.insert(ref_id);
            effects.push(Effect::DiscardBuiltin {
                connection,
                server_task,
            });
            return;
        }
        let generation = self.next_generation();
        if let Some(mut old) = self.entries.remove(&name) {
            if let Some(connection) = old.connection.take() {
                let refs = refs_of(&old.refs);
                effects.push(Effect::Stop {
                    name: name.clone(),
                    generation,
                    connection,
                    server_task: old.server_task.take(),
                    refs,
                    reason: StopReason::Replaced,
                });
            } else if let Some(server_task) = old.server_task.take() {
                server_task.abort();
            }
        }
        let refs = BTreeSet::from([ref_id]);
        // emit_starting(bus, &name, &refs);
        emit_started(bus, &name, &refs, &tools);
        self.entries.insert(
            name,
            Entry {
                definition: Definition::Builtin,
                refs,
                state: State::Running,
                tools,
                connection: Some(connection),
                server_task: Some(server_task),
                want: None,
            },
        );
    }

    fn release(&mut self, ref_id: &str, effects: &mut Vec<Effect>) {
        if self.closed {
            return;
        }
        let names: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.refs.contains(ref_id))
            .map(|(name, _)| name.clone())
            .collect();
        for name in names {
            let generation = self.next_generation();
            let mut remove = false;
            {
                let Some(entry) = self.entries.get_mut(&name) else {
                    continue;
                };
                let refs = refs_of(&entry.refs);
                entry.refs.remove(ref_id);
                if !entry.refs.is_empty() {
                    continue;
                }
                match entry.state {
                    State::Running => {
                        let connection = entry.connection.take();
                        let server_task = entry.server_task.take();
                        entry.state = State::Stopping(generation);
                        entry.want = None;
                        if let Some(connection) = connection {
                            effects.push(Effect::Stop {
                                name: name.clone(),
                                generation,
                                connection,
                                server_task,
                                refs,
                                reason: StopReason::Released,
                            });
                        } else {
                            remove = true;
                        }
                    }
                    State::Starting(_) => {}
                    State::Stopping(_) => {
                        entry.want = None;
                    }
                    State::Failed => {
                        remove = true;
                    }
                }
            }
            if remove {
                self.entries.remove(&name);
            }
        }
    }

    fn report(
        &mut self,
        name: String,
        generation: u64,
        result: Result<Started>,
        bus: &Dispatcher,
    ) -> Option<Effect> {
        let Some(entry) = self.entries.get_mut(&name) else {
            return result.ok().map(Effect::Discard);
        };
        if !matches!(entry.state, State::Starting(current) if current == generation) {
            return result.ok().map(Effect::Discard);
        }
        match result {
            Ok(started) => {
                if entry.refs.is_empty() || self.closed {
                    self.entries.remove(&name);
                    return Some(Effect::Discard(started));
                }
                entry.state = State::Running;
                entry.tools = started.tools.clone();
                entry.connection = Some(started.connection);
                entry.server_task = None;
                emit_started(bus, &name, &entry.refs, &entry.tools);
                None
            }
            Err(error) => {
                entry.state = State::Failed;
                entry.connection = None;
                entry.server_task = None;
                emit_failed(bus, &name, &entry.refs, &error.to_string());
                if entry.refs.is_empty() {
                    self.entries.remove(&name);
                }
                None
            }
        }
    }

    fn stop_done(
        &mut self,
        name: String,
        generation: u64,
        refs: Vec<String>,
        reason: StopReason,
        bus: &Dispatcher,
        effects: &mut Vec<Effect>,
    ) {
        emit_stopped(bus, &name, &refs, reason);
        let Some(entry) = self.entries.remove(&name) else {
            return;
        };
        if !matches!(entry.state, State::Stopping(current) if current == generation) {
            self.entries.insert(name, entry);
            return;
        }
        let Some(want) = entry.want else {
            return;
        };
        let generation = self.next_generation();
        let refs = entry.refs;
        emit_starting(bus, &name, &refs);
        effects.push(Effect::Start {
            name: name.clone(),
            generation,
            definition: Box::new(want.definition.clone()),
            refs: refs_of(&refs),
            previous: want.previous.map(Box::new),
        });
        self.entries.insert(
            name,
            Entry {
                definition: Definition::External(want.definition),
                refs,
                state: State::Starting(generation),
                tools: Vec::new(),
                connection: None,
                server_task: None,
                want: None,
            },
        );
    }

    fn begin_shutdown(&mut self, reply: oneshot::Sender<()>, effects: &mut Vec<Effect>) {
        self.pending_shutdowns.push(reply);
        self.closed = true;
        let names: Vec<String> = self.entries.keys().cloned().collect();
        for name in names {
            let generation = self.next_generation();
            let Some(mut entry) = self.entries.remove(&name) else {
                continue;
            };
            if let Some(connection) = entry.connection.take() {
                effects.push(Effect::Stop {
                    name,
                    generation,
                    connection,
                    server_task: entry.server_task.take(),
                    refs: refs_of(&entry.refs),
                    reason: StopReason::Shutdown,
                });
            } else if let Some(server_task) = entry.server_task.take() {
                server_task.abort();
            }
        }
    }

    fn finish_shutdown(&mut self) {
        if self.entries.is_empty() && self.outstanding == 0 && !self.pending_shutdowns.is_empty() {
            for reply in self.pending_shutdowns.drain(..) {
                let _ = reply.send(());
            }
        }
    }

    fn flush_catalog(&mut self) {
        if !self.dirty {
            return;
        }
        let mut tools = Vec::new();
        for (name, entry) in &self.entries {
            let (State::Running, Some(connection)) = (&entry.state, &entry.connection) else {
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

    fn next_generation(&mut self) -> u64 {
        self.generation += 1;
        self.generation
    }

    async fn cleanup(&mut self) {
        for (_, entry) in std::mem::take(&mut self.entries) {
            if let Some(server_task) = entry.server_task {
                server_task.abort();
            }
            if let Some(connection) = entry.connection {
                connection.close().await;
            }
        }
    }
}

fn external_of(definition: &Definition) -> Option<McpDefinition> {
    match definition {
        Definition::External(definition) => Some(definition.clone()),
        Definition::Builtin => None,
    }
}

fn refs_of(refs: &BTreeSet<String>) -> Vec<String> {
    refs.iter().cloned().collect()
}

fn emit_starting(bus: &Dispatcher, name: &str, refs: &BTreeSet<String>) {
    bus.notify(
        &CORE_MCP_STARTING,
        &events::cx(),
        McpStarting {
            name: name.to_owned(),
            refs: refs_of(refs),
        },
    );
}

fn emit_started(bus: &Dispatcher, name: &str, refs: &BTreeSet<String>, tools: &[ToolDescriptor]) {
    bus.notify(
        &CORE_MCP_STARTED,
        &events::cx(),
        McpStarted {
            name: name.to_owned(),
            refs: refs_of(refs),
            tools: tools.iter().map(|tool| tool.name.clone()).collect(),
        },
    );
}

fn emit_failed(bus: &Dispatcher, name: &str, refs: &BTreeSet<String>, error: &str) {
    bus.notify(
        &CORE_MCP_FAILED,
        &events::cx(),
        McpFailed {
            name: name.to_owned(),
            refs: refs_of(refs),
            error: error.to_owned(),
        },
    );
}

fn emit_stopped(bus: &Dispatcher, name: &str, refs: &[String], reason: StopReason) {
    bus.notify(
        &CORE_MCP_STOPPED,
        &events::cx(),
        McpStopped {
            name: name.to_owned(),
            refs: refs.to_vec(),
            reason: reason.as_ref().to_owned(),
        },
    );
}

async fn run(
    options: McpOptions,
    bus: Arc<Dispatcher>,
    sender: mpsc::UnboundedSender<Command>,
    mut receiver: mpsc::UnboundedReceiver<Command>,
) {
    let weak = sender.downgrade();
    drop(sender);
    let mut registry = Registry::new();
    while let Some(command) = receiver.recv().await {
        let effects = registry.handle(command, &bus);
        registry.outstanding += effects.len();
        for effect in effects {
            dispatch(
                effect,
                weak.clone(),
                Arc::clone(&bus),
                options.clone(),
                Arc::clone(&registry.locks),
            );
        }
        registry.finish_shutdown();
    }
    registry.cleanup().await;
}

fn dispatch(
    effect: Effect,
    weak: WeakUnboundedSender<Command>,
    bus: Arc<Dispatcher>,
    options: McpOptions,
    locks: Arc<StartupLocks>,
) {
    tokio::spawn(async move {
        let done = weak.clone();
        let task = tokio::spawn(run_effect(effect, bus, options, locks, weak));
        let _ = task.await;
        if let Some(sender) = done.upgrade() {
            let _ = sender.send(Command::EffectDone);
        }
    });
}

async fn run_effect(
    effect: Effect,
    bus: Arc<Dispatcher>,
    options: McpOptions,
    locks: Arc<StartupLocks>,
    weak: WeakUnboundedSender<Command>,
) {
    match effect {
        Effect::Start {
            name,
            generation,
            definition,
            refs,
            previous,
        } => {
            let previous = previous.map(|previous| *previous);
            let definition = rewrite(&bus, *definition, &name, &refs, previous).await;
            let result = start(&bus, &options, &locks, &definition).await;
            let command = Command::Report {
                name,
                generation,
                result,
            };
            if let Some(sender) = weak.upgrade() {
                let _ = sender.send(command);
            } else if let Command::Report {
                result: Ok(started),
                ..
            } = command
            {
                started.connection.close().await;
            }
        }
        Effect::Stop {
            name,
            generation,
            connection,
            server_task,
            refs,
            reason,
        } => {
            if let Some(server_task) = server_task {
                server_task.abort();
                let _ = server_task.await;
            }
            connection.close().await;
            if let Some(sender) = weak.upgrade() {
                let _ = sender.send(Command::StopDone {
                    name,
                    generation,
                    refs,
                    reason,
                });
            }
        }
        Effect::Discard(started) => {
            started.connection.close().await;
        }
        Effect::DiscardBuiltin {
            connection,
            server_task,
        } => {
            server_task.abort();
            let _ = server_task.await;
            connection.close().await;
        }
    }
}

/// 发出可改写的启动事件并采用改写结果
async fn rewrite(
    bus: &Dispatcher,
    definition: McpDefinition,
    name: &str,
    refs: &[String],
    previous: Option<McpDefinition>,
) -> McpDefinition {
    let mut definition = definition;
    if let Some(old) = previous {
        let payload = McpReplaced {
            name: name.to_owned(),
            refs: refs.to_vec(),
            old,
            new: definition.clone(),
        };
        let payload = bus
            .decide(&CORE_MCP_REPLACED, &events::intercept_cx(), payload)
            .await;
        definition = payload.new;
    }
    let payload = McpLaunch {
        name: name.to_owned(),
        refs: refs.to_vec(),
        definition,
    };
    let payload = bus
        .decide(&CORE_MCP_LAUNCH, &events::intercept_cx(), payload)
        .await;
    let mut definition = payload.definition;
    definition.name = name.to_owned();
    definition
}

async fn start(
    bus: &Arc<Dispatcher>,
    options: &McpOptions,
    locks: &StartupLocks,
    definition: &McpDefinition,
) -> Result<Started> {
    let connection = match &definition.config {
        McpConfig::Stdio {
            command, args, cwd, ..
        } => {
            let plan = runner::plan(command, args, cwd, &options.mods_root)?;
            let _guard = locks.acquire(&plan.startup_key).await;
            Connection::open(
                definition,
                &plan,
                options,
                Some(log_sink(bus, &definition.name)),
            )
            .await?
        }
        McpConfig::StreamableHttp { .. } => {
            let plan = LaunchPlan {
                program: PathBuf::new(),
                args: Vec::new(),
                cwd: PathBuf::new(),
                startup_key: definition.name.clone(),
            };
            Connection::open(definition, &plan, options, None).await?
        }
    };
    match connection.list_tools().await {
        Ok(tools) => Ok(Started { connection, tools }),
        Err(error) => {
            connection.close().await;
            Err(error)
        }
    }
}

fn log_sink(bus: &Arc<Dispatcher>, name: &str) -> LogSink {
    let bus = Arc::clone(bus);
    let name = name.to_owned();
    Arc::new(move |line| {
        bus.notify(
            &CORE_MCP_LOG,
            &events::cx(),
            McpLog {
                name: name.clone(),
                stream: "stderr".to_owned(),
                line: truncate_line(line),
            },
        );
    })
}

fn truncate_line(line: &str) -> String {
    if line.len() <= LOG_LINE_LIMIT {
        return line.to_owned();
    }
    let mut end = LOG_LINE_LIMIT;
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    line[..end].to_owned()
}

#[derive(Default)]
struct StartupLocks(Mutex<HashMap<String, Arc<AsyncMutex<()>>>>);

impl StartupLocks {
    async fn acquire(&self, key: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = match self.0.lock() {
                Ok(locks) => locks,
                Err(poisoned) => poisoned.into_inner(),
            };
            Arc::clone(
                locks
                    .entry(key.to_owned())
                    .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
            )
        };
        lock.lock_owned().await
    }
}
