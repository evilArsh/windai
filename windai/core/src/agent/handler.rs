use std::sync::Arc;
use std::time::Duration;

use super::events::CORE_AGENT_CREATE;
use super::jobs::{JobRequest, Jobs};
use super::tree::event::{Effect, Event};
use super::tree::view::TreeEvent;
use super::tree::{Domain, NodeId, Phase, Tree};
use crate::agent::events::{
    AgentPersistFailed, AgentSealed, AgentSpawn, AgentStatus, CORE_AGENT_PERSIST_FAILED,
    CORE_AGENT_SEALED, CORE_AGENT_SPAWN, CORE_AGENT_STATUS,
};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use wind_event::{DispatchCx, Dispatcher, Origin, Scope, Source};
use wind_storage::Storage;

const OBSERVE_BUDGET: Duration = Duration::from_millis(50);

/// 树节点快照
#[derive(Debug, Clone)]
pub struct NodeSnapshot<D: Domain> {
    pub id: NodeId,
    pub parent: Option<NodeId>,
    pub children: Vec<NodeId>,
    pub phase: Phase<D>,
    pub detached: bool,
    pub pending: Vec<D::Spec>,
}

enum Command<D: Domain> {
    Event(Event<D>),
    Spawn {
        id: NodeId,
        parent: Option<NodeId>,
        spec: D::Spec,
        detached: bool,
        // reply: oneshot::Sender<bool>,
    },
    Submit {
        node: NodeId,
        spec: D::Spec,
        // reply: oneshot::Sender<SubmitOutcome>,
    },
    Recover(Vec<NodeSnapshot<D>>, oneshot::Sender<()>),
    InFlight(oneshot::Sender<Vec<NodeId>>),
    Undelivered(oneshot::Sender<Vec<NodeId>>),
    Snapshot(oneshot::Sender<Vec<NodeSnapshot<D>>>),
    Shutdown,
}

enum Wake<D: Domain> {
    Event(Event<D>),
    Command(Command<D>),
    Exit,
}

/// Agent 对外 hub 句柄
#[derive(Clone)]
pub struct AgentHandle<D: Domain> {
    tx: mpsc::Sender<Command<D>>,
    bus: broadcast::Sender<TreeEvent<D>>,
    cancel: CancellationToken,
    dispatcher: Arc<Dispatcher>,
}
impl<D: Domain> AgentHandle<D> {
    // pub async fn submit(&self, node: NodeId, spec: AgentSpec) -> Result<SubmitOutcome> {}

    // pub async fn terminate(
    //     &self,
    //     cx: &DispatchCx,
    //     instance_id: NodeId,
    //     reason: &str,
    // ) -> Result<()> {}

    // pub async fn cancel(
    //     &self,
    //     cx: &DispatchCx,
    //     instance_id: NodeId,
    //     origin: CancelOrigin,
    // ) -> Result<()> {}
}

struct Agent<D: Domain> {
    topic_id: i64,
    storage: Storage,
    dispatcher: Arc<Dispatcher>,
    tree: Tree<D>,
    jobs: Jobs<D>,
    inbox_rx: mpsc::Receiver<Command<D>>,
    reports_rx: mpsc::Receiver<Event<D>>,
    cancel: CancellationToken,
}
impl<D: Domain> Agent<D> {
    async fn run(mut self) {
        loop {
            let wake = {
                tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => Wake::Exit,
                    report = self.reports_rx.recv() => match report {
                        Some(event) => Wake::Event(event),
                        None => Wake::Exit,
                    },
                    command = self.inbox_rx.recv() => match command {
                        Some(command) => Wake::Command(command),
                        None => Wake::Exit,
                    },
                }
            };
            // match wake {
            //     Wake::Event(event) => {
            //         self.apply(event).await;
            //     }
            //     Wake::Command(Command::Event(event)) => {
            //         self.apply(event).await;
            //     }
            //     Wake::Exit => todo!(),
            // }
        }

        self.jobs.shutdown();
    }

    async fn apply(&mut self, event: Event<D>) -> bool {
        let mut created = false;
        for effect in self.tree.reduce(event) {
            if matches!(effect, Effect::Publish(TreeEvent::NodeCreated { .. })) {
                created = true;
            }
            self.execute(effect).await;
        }
        created
    }
    async fn execute(&mut self, effect: Effect<D>) {
        match effect {
            Effect::Publish(event) => {
                self.notify(&event);
                // let _ = self.bus.send(event);
            }
            Effect::Persist { node } => self.persist(node).await,
            Effect::Launch { node, job, input } => {
                self.jobs.launch(JobRequest { node, job, input });
            }
            Effect::Stop { node, job } => {
                self.jobs.stop(node, job);
            }
        }
    }

    async fn persist(&self, node: NodeId) {
        let Some(phase) = self.tree.phase(node).cloned() else {
            log::warn!("[agent] persist for unknown node ignored: {node}");
            return;
        };
        let (phase, phase_payload) = match phase.columns() {
            Ok(columns) => columns,
            Err(error) => {
                log::error!("[agent] cannot encode the phase of {node}: {error}");
                return;
            }
        };
        if let Err(error) = self
            .storage
            .update_instance_phase(node, &phase, phase_payload)
            .await
        {
            log::error!("[agent] persist failed for node {node}: {error}");
            self.dispatcher.notify(
                &CORE_AGENT_PERSIST_FAILED,
                &observe_cx(self.topic_id, node),
                AgentPersistFailed {
                    topic_id: self.topic_id,
                    instance_id: node,
                    phase,
                    error: error.to_string(),
                },
            );
        }
    }

    fn notify(&self, event: &TreeEvent<D>) {
        match event {
            TreeEvent::NodeCreated { node, parent } => self.dispatcher.notify(
                &CORE_AGENT_SPAWN,
                &observe_cx(self.topic_id, *node),
                AgentSpawn {
                    instance_id: *node,
                    parent_id: *parent,
                    detached: self.tree.is_detached(*node),
                },
            ),
            TreeEvent::PhaseChanged { node, to } => {
                let (phase, phase_payload) = match to.parts() {
                    Ok(parts) => parts,
                    Err(error) => {
                        log::error!("[agent] cannot encode the phase of {node}: {error}");
                        return;
                    }
                };
                self.dispatcher.notify(
                    &CORE_AGENT_STATUS,
                    &observe_cx(self.topic_id, *node),
                    AgentStatus {
                        instance_id: *node,
                        phase,
                        phase_payload,
                    },
                );
            }
            TreeEvent::Sealed { root } => self.dispatcher.notify(
                &CORE_AGENT_SEALED,
                &observe_cx(self.topic_id, *root),
                AgentSealed { instance_id: *root },
            ),
        }
    }
}

#[inline]
fn observe_cx(topic_id: i64, node_id: NodeId) -> DispatchCx {
    DispatchCx::root(
        Origin::Internal,
        Scope::Topic {
            topic_id,
            instance_id: node_id,
        },
        Source::core("agent"),
        OBSERVE_BUDGET,
    )
}
