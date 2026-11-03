use super::events::{
    AgentPersistFailed, AgentSealed, AgentSpawn, AgentStatus, CORE_AGENT_PERSIST_FAILED,
    CORE_AGENT_SEALED, CORE_AGENT_SPAWN, CORE_AGENT_STATUS,
};
use super::jobs::{JobRequest, Jobs};
use super::tree::event::{Effect, Event};
use super::tree::phase::CancelOrigin;
use super::tree::view::TreeEvent;
use super::tree::{Domain, NodeId, Phase, Tree};
use crate::agent::Runner;
use crate::error::{CoreError, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wind_event::{DispatchCx, Dispatcher, Origin, Scope, Source};
use wind_storage::Storage;

/// 事件循环最多积压的事件数
const EVENT_CAPACITY: usize = 1024;

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

/// Agent 对外 hub 句柄
#[derive(Clone)]
pub struct AgentHandle<D: Domain> {
    tx: mpsc::Sender<Event<D>>,
    cancel: CancellationToken,
}

impl<D: Domain> AgentHandle<D> {
    pub async fn spawn(
        &self,
        id: NodeId,
        parent: Option<NodeId>,
        spec: D::Spec,
        detached: bool,
    ) -> Result<()> {
        self.send(Event::Spawn {
            id,
            parent,
            spec,
            detached,
        })
        .await
    }

    pub async fn enqueue(&self, node: NodeId, spec: D::Spec) -> Result<()> {
        self.send(Event::Enqueue { node, spec }).await
    }

    pub async fn resume(&self, node: NodeId, expect: D::Suspend, input: D::Resume) -> Result<()> {
        self.send(Event::Resume {
            node,
            expect,
            input,
        })
        .await
    }

    pub async fn cancel(&self, root: NodeId, origin: CancelOrigin) -> Result<()> {
        self.send(Event::CancelRequest { root, origin }).await
    }

    pub async fn deliver(&self, node: NodeId) -> Result<()> {
        self.send(Event::Deliver { node }).await
    }

    pub fn shutdown(&self) {
        self.cancel.cancel();
    }

    pub fn is_stopped(&self) -> bool {
        self.cancel.is_cancelled()
    }

    pub async fn wait_stopped(&self) {
        self.cancel.cancelled().await;
    }

    async fn send(&self, event: Event<D>) -> Result<()> {
        self.tx.send(event).await.map_err(|_| CoreError::Closed)
    }
}

pub struct AgentOptions<D: Domain> {
    pub topic_id: i64,
    pub storage: Storage,
    pub dispatcher: Arc<Dispatcher>,
    pub runner: Arc<dyn Runner<D>>,
    pub restored: Vec<NodeSnapshot<D>>,
    pub cancel: CancellationToken,
}

pub struct Agent<D: Domain> {
    topic_id: i64,
    storage: Storage,
    dispatcher: Arc<Dispatcher>,
    tree: Tree<D>,
    jobs: Jobs<D>,
    event_rx: mpsc::Receiver<Event<D>>,
    cancel: CancellationToken,
}

impl<D: Domain> Agent<D> {
    pub fn new(options: AgentOptions<D>) -> (Self, AgentHandle<D>) {
        let (tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
        let handle = AgentHandle {
            tx: tx.clone(),
            cancel: options.cancel.clone(),
        };
        let jobs = Jobs::new(options.runner, tx);
        let mut tree = Tree::new();
        for node in options.restored {
            tree.restore_node(
                node.id,
                node.parent,
                node.children,
                node.phase,
                node.detached,
                node.pending,
            );
        }
        let agent = Self {
            topic_id: options.topic_id,
            storage: options.storage,
            dispatcher: options.dispatcher,
            tree,
            jobs,
            event_rx,
            cancel: options.cancel,
        };
        (agent, handle)
    }

    pub fn start(options: AgentOptions<D>) -> AgentHandle<D> {
        let (agent, handle) = Self::new(options);
        tokio::spawn(agent.run());
        handle
    }

    pub async fn run(mut self) {
        let restored = self.tree.all_node_ids();
        for node in restored {
            self.apply(Event::Recovered { node }).await;
        }
        loop {
            tokio::select! {
                biased;
                () = self.cancel.cancelled() => break,
                event = self.event_rx.recv() => match event {
                    Some(event) => self.apply(event).await,
                    None => break,
                },
            }
        }
        self.jobs.shutdown();
    }

    async fn apply(&mut self, event: Event<D>) {
        for effect in self.tree.reduce(event) {
            self.execute(effect).await;
        }
    }

    async fn execute(&mut self, effect: Effect<D>) {
        match effect {
            Effect::Publish(event) => self.notify(&event),
            Effect::Persist { node } => self.persist(node).await,
            Effect::Launch { node, job, input } => {
                self.jobs.launch(JobRequest { node, job, input });
            }
            Effect::Stop { node, job } => self.jobs.stop(node, job),
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
