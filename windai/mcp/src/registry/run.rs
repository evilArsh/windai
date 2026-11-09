use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{self, WeakUnboundedSender};
use tokio::sync::oneshot;
use wind_event::Dispatcher;

use super::command::{Command, Effect, ReportPhase};
use super::{BoxFuture, Catalog, LaunchRequest, Launcher, Registry, RegistryHandle, Started};
use crate::connector::{Connection, ToolDescriptor};
use crate::error::{McpError, Result};
use crate::options::McpOptions;
use crate::roots::Roots;
use crate::runner::{LaunchPlan, plan};
use crate::spec::{ServiceSpec, Transport};
use crate::startup::StartupLocks;

pub fn spawn(options: McpOptions, roots: Roots, bus: Arc<Dispatcher>) -> RegistryHandle {
    let launcher = Arc::new(ProductionLauncher {
        options: options.clone(),
        locks: StartupLocks::new(),
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
    let start_budget = timers.connect_timeout + timers.shutdown_grace;
    let stop_budget = timers.shutdown_grace + timers.connect_timeout;
    let mut registry = Registry::new(roots.clone());
    while let Some(command) = receiver.recv().await {
        let Some(sender) = weak.upgrade() else {
            break;
        };
        for effect in registry.handle(command) {
            dispatch(
                effect,
                &launcher,
                &roots,
                &bus,
                &sender,
                start_budget,
                stop_budget,
            );
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

fn dispatch(
    effect: Effect,
    launcher: &Arc<dyn Launcher>,
    roots: &Roots,
    bus: &Arc<Dispatcher>,
    sender: &mpsc::UnboundedSender<Command>,
    start_budget: Duration,
    stop_budget: Duration,
) {
    match effect {
        Effect::Start {
            name,
            generation,
            request,
        } => {
            let launcher = Arc::clone(launcher);
            let roots = roots.clone();
            let bus = Arc::clone(bus);
            let sender = sender.clone();
            tokio::spawn(async move {
                let mut task = tokio::spawn(async move {
                    launcher.start(&request, &roots, &bus, generation).await
                });
                let result = match tokio::time::timeout(start_budget, &mut task).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(error)) => Err(McpError::Service(format!(
                        "the launcher task failed: {error}"
                    ))),
                    Err(_) => {
                        task.abort();
                        Err(McpError::Timeout)
                    }
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
            let LaunchRequest::External(spec) = request else {
                return Err(McpError::InvalidSpec(
                    "builtin services are not connected yet".to_owned(),
                ));
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
        let connection = Connection::open(spec, plan, &self.options).await?;
        let tools = match connection.list_tools().await {
            Ok(tools) => tools,
            Err(error) => {
                connection.close().await;
                return Err(error);
            }
        };
        Ok(Started {
            connection,
            tools,
            server_task: None,
        })
    }
}
