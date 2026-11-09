use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::mpsc::{self, WeakUnboundedSender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wind_event::Dispatcher;

use super::command::{Command, Effect, Event, ReportPhase};
use super::{
    BoxFuture, BuiltinKind, Catalog, LaunchRequest, Launcher, Registry, RegistryHandle, Started,
    ToolCall,
};
use crate::builtin::connect_builtin;
use crate::connector::{CallOutcome, Connection, ToolDescriptor};
use crate::error::{McpError, Result};
use crate::events::{
    self, CORE_MCP_FAILED, CORE_MCP_LAUNCH, CORE_MCP_LOG, CORE_MCP_REPLACED, CORE_MCP_STARTED,
    CORE_MCP_STARTING, CORE_MCP_STOPPED, McpFailed, McpLaunch, McpLog, McpReplaced, McpStarted,
    McpStarting, McpStopped,
};
use crate::options::McpOptions;
use crate::roots::Roots;
use crate::runner::{LaunchPlan, plan};
use crate::spec::{ServiceSpec, Transport};
use crate::startup::StartupLocks;

/// stderr 单行保留的字节数
const LOG_LINE_BYTES: usize = 4096;

pub fn spawn(options: McpOptions, roots: Roots, bus: Arc<Dispatcher>) -> RegistryHandle {
    let launcher = Arc::new(ProductionLauncher {
        options: options.clone(),
        locks: StartupLocks::new(),
        bus: Arc::clone(&bus),
    });
    spawn_inner(launcher, roots, bus, options)
}

pub fn spawn_with(
    launcher: Arc<dyn Launcher>,
    roots: Roots,
    bus: Arc<Dispatcher>,
) -> RegistryHandle {
    spawn_inner(launcher, roots, bus, McpOptions::default())
}

fn spawn_inner(
    launcher: Arc<dyn Launcher>,
    roots: Roots,
    bus: Arc<Dispatcher>,
    timers: McpOptions,
) -> RegistryHandle {
    let (sender, receiver) = mpsc::unbounded_channel();
    let handle = RegistryHandle {
        commands: sender.clone(),
    };
    let weak = sender.downgrade();
    drop(sender);
    tokio::spawn(run(receiver, launcher, roots, bus, weak, timers));
    handle
}

async fn run(
    mut receiver: mpsc::UnboundedReceiver<Command>,
    launcher: Arc<dyn Launcher>,
    roots: Roots,
    bus: Arc<Dispatcher>,
    weak: WeakUnboundedSender<Command>,
    timers: McpOptions,
) {
    let stop_budget = timers.shutdown_grace + timers.connect_timeout;
    let mut registry = Registry::new(roots.clone());
    while let Some(command) = receiver.recv().await {
        let Some(sender) = weak.upgrade() else {
            break;
        };
        let (effects, events) = registry.handle(command);
        for event in events {
            notify(event, &bus);
        }
        for effect in effects {
            dispatch(effect, &launcher, &roots, &bus, &sender, stop_budget);
        }
        let finished = registry.take_finished_shutdown();
        if !finished.is_empty() {
            for reply in finished {
                let _ = reply.send(());
            }
            break;
        }
    }
    registry.cleanup();
}

fn notify(event: Event, bus: &Dispatcher) {
    match event {
        Event::Starting { name, refs } => {
            bus.notify(
                &CORE_MCP_STARTING,
                &events::cx(),
                McpStarting { name, refs },
            );
        }
        Event::Started { name, refs, tools } => {
            bus.notify(
                &CORE_MCP_STARTED,
                &events::cx(),
                McpStarted { name, refs, tools },
            );
        }
        Event::Failed { name, refs, error } => {
            bus.notify(
                &CORE_MCP_FAILED,
                &events::cx(),
                McpFailed { name, refs, error },
            );
        }
        Event::Stopped { name, refs, reason } => {
            let reason = reason.as_ref().to_owned();
            bus.notify(
                &CORE_MCP_STOPPED,
                &events::cx(),
                McpStopped { name, refs, reason },
            );
        }
    }
}

async fn rewrite_request(
    name: String,
    request: LaunchRequest,
    previous: Option<Box<ServiceSpec>>,
    refs: Vec<String>,
    bus: &Dispatcher,
) -> LaunchRequest {
    let mut spec = match request {
        LaunchRequest::External(spec) => spec,
        builtin => return builtin,
    };
    if let Some(old) = previous {
        let payload = McpReplaced {
            name: name.clone(),
            refs: refs.clone(),
            old: *old,
            new: spec,
        };
        let mut rewritten = bus
            .decide(&CORE_MCP_REPLACED, &events::intercept_cx(), payload)
            .await;
        rewritten.new.name = name.clone();
        spec = rewritten.new;
    }
    let payload = McpLaunch {
        name: name.clone(),
        refs,
        spec,
    };
    let mut rewritten = bus
        .decide(&CORE_MCP_LAUNCH, &events::intercept_cx(), payload)
        .await;
    rewritten.spec.name = name;
    LaunchRequest::External(rewritten.spec)
}

fn dispatch(
    effect: Effect,
    launcher: &Arc<dyn Launcher>,
    roots: &Roots,
    bus: &Arc<Dispatcher>,
    sender: &mpsc::UnboundedSender<Command>,
    stop_budget: Duration,
) {
    match effect {
        Effect::Start {
            name,
            generation,
            request,
            refs,
            previous,
        } => {
            let launcher = Arc::clone(launcher);
            let roots = roots.clone();
            let bus = Arc::clone(bus);
            let sender = sender.clone();
            let task_name = name.clone();
            tokio::spawn(async move {
                // 启动各阶段各自有界，此处不做总体超时；内层 spawn 只用于承接 panic
                let task = tokio::spawn(async move {
                    let request = rewrite_request(task_name, request, previous, refs, &bus).await;
                    launcher.start(&request, &roots, &bus, generation).await
                });
                let result = match task.await {
                    Ok(result) => result,
                    Err(error) => Err(McpError::Service(format!(
                        "the launcher task failed: {error}"
                    ))),
                };
                if let Err(error) = &result {
                    log::warn!("cannot start the mcp service {name}: {error}");
                }
                let _ = sender.send(Command::Report {
                    name,
                    generation,
                    phase: ReportPhase::Launch(result),
                });
                let _ = sender.send(Command::EffectDone);
            });
        }
        Effect::Stop {
            name,
            generation,
            connection,
            server_task,
        } => {
            let sender = sender.clone();
            tokio::spawn(async move {
                let mut task = tokio::spawn(async move {
                    connection.close().await;
                });
                let closed = tokio::time::timeout(stop_budget, &mut task).await;
                if closed.is_err() {
                    task.abort();
                    log::warn!("the mcp service {name} did not close within the timeout");
                }
                if let Some(task) = server_task {
                    task.abort();
                }
                let result = match closed {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => {
                        Err(McpError::Service(format!("the close task failed: {error}")))
                    }
                    Err(_) => Err(McpError::Timeout),
                };
                let _ = sender.send(Command::Report {
                    name,
                    generation,
                    phase: ReportPhase::Stop(result),
                });
                let _ = sender.send(Command::EffectDone);
            });
        }
        Effect::Discard(started) => {
            let sender = sender.clone();
            tokio::spawn(async move {
                let closed = tokio::time::timeout(stop_budget, started.connection.close()).await;
                if closed.is_err() {
                    log::warn!("an obsolete mcp connection did not close within the timeout");
                }
                if let Some(task) = started.server_task {
                    task.abort();
                }
                let _ = sender.send(Command::EffectDone);
            });
        }
        Effect::Refresh {
            name,
            generation,
            connection,
        } => {
            let launcher = Arc::clone(launcher);
            let sender = sender.clone();
            tokio::spawn(async move {
                let result = launcher.list_tools(&name, &connection).await;
                let _ = sender.send(Command::RefreshDone {
                    name,
                    generation,
                    result,
                });
            });
        }
    }
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

impl RegistryHandle {
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

    pub async fn entry_generation(&self, name: &str) -> Option<u64> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::EntryGeneration {
                name: name.to_owned(),
                reply,
            })
            .ok()?;
        response.await.ok().flatten()
    }
}

struct ProductionLauncher {
    options: McpOptions,
    locks: StartupLocks,
    bus: Arc<Dispatcher>,
}

impl Launcher for ProductionLauncher {
    fn start<'a>(
        &'a self,
        request: &'a LaunchRequest,
        roots: &'a Roots,
        _bus: &'a Dispatcher,
        _generation: u64,
    ) -> BoxFuture<'a, Result<Started>> {
        Box::pin(async move {
            let spec = match request {
                LaunchRequest::Builtin { scope, kind } => {
                    if matches!(kind, BuiltinKind::Fs) {
                        let usable = roots.scope(scope).is_some_and(|scope_roots| {
                            scope_roots
                                .workspace
                                .iter()
                                .any(|path| !path.as_os_str().is_empty())
                        });
                        if !usable {
                            return Err(McpError::InvalidSpec(format!(
                                "the scope {scope} has no usable workspace roots"
                            )));
                        }
                    }
                    let (connection, server_task) =
                        connect_builtin(*kind, scope.clone(), roots.clone(), &self.options).await?;
                    return finish_start(connection, Some(server_task)).await;
                }
                LaunchRequest::External(spec) => spec,
            };
            match &spec.transport {
                Transport::Stdio(stdio) => {
                    let plan = plan(stdio, roots)?;
                    roots.check_cwd(&stdio.cwd)?;
                    if let Some(entry) = &plan.entry {
                        roots.check_cwd(entry)?;
                    }
                    let _guard = self.locks.lock(&plan.startup_key).await;
                    self.open(spec, &plan).await
                }
                Transport::Streamable { .. } => {
                    let plan = LaunchPlan {
                        program: PathBuf::new(),
                        args: Vec::new(),
                        startup_key: spec.name.clone(),
                        entry: None,
                    };
                    self.open(spec, &plan).await
                }
            }
        })
    }

    fn list_tools<'a>(
        &'a self,
        _name: &'a str,
        connection: &'a Arc<Connection>,
    ) -> BoxFuture<'a, Result<Vec<ToolDescriptor>>> {
        Box::pin(async move { connection.list_tools().await })
    }
}

impl ProductionLauncher {
    async fn open(&self, spec: &ServiceSpec, plan: &LaunchPlan) -> Result<Started> {
        let mut options = self.options.clone();
        let original = self.options.log.clone();
        let name = spec.name.clone();
        let bus = Arc::clone(&self.bus);
        options.log = Some(Arc::new(move |line: &str| {
            if let Some(original) = &original {
                original(line);
            }
            let payload = McpLog {
                name: name.clone(),
                stream: "stderr".to_owned(),
                line: truncate_log_line(line),
            };
            bus.notify(&CORE_MCP_LOG, &events::cx(), payload);
        }));
        let connection = Connection::open(spec, plan, &options).await?;
        finish_start(connection, None).await
    }
}

async fn finish_start(
    connection: Arc<Connection>,
    server_task: Option<JoinHandle<()>>,
) -> Result<Started> {
    let tools = match connection.list_tools().await {
        Ok(tools) => tools,
        Err(error) => {
            // 清理独立成 task，启动 task 被 abort 时不影响回收
            tokio::spawn(async move {
                connection.close().await;
                if let Some(task) = server_task {
                    task.abort();
                }
            });
            return Err(error);
        }
    };
    Ok(Started {
        connection,
        tools,
        server_task,
    })
}

fn truncate_log_line(line: &str) -> String {
    if line.len() <= LOG_LINE_BYTES {
        return line.to_owned();
    }
    let mut end = LOG_LINE_BYTES;
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    line[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::{LOG_LINE_BYTES, truncate_log_line};

    #[test]
    fn a_short_log_line_is_kept() {
        assert_eq!(truncate_log_line("hello"), "hello");
        assert_eq!(
            truncate_log_line(&"a".repeat(LOG_LINE_BYTES)),
            "a".repeat(LOG_LINE_BYTES)
        );
    }

    #[test]
    fn a_long_log_line_is_truncated_at_a_char_boundary() {
        let ascii = "a".repeat(LOG_LINE_BYTES + 10);
        assert_eq!(truncate_log_line(&ascii), "a".repeat(LOG_LINE_BYTES));

        let wide = "€".repeat(LOG_LINE_BYTES);
        let truncated = truncate_log_line(&wide);
        assert_eq!(truncated.len(), LOG_LINE_BYTES - 1);
        assert_eq!(truncated, "€".repeat((LOG_LINE_BYTES - 1) / 3));
    }
}
