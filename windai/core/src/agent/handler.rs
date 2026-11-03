use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::agent::events::directory;

use super::jobs::{JobRequest, Jobs};
use super::tree::event::{Effect, Event};
use super::tree::view::TreeEvent;
use super::tree::{Domain, NodeId, Phase, Tree};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use wind_event::{Directory, DispatchCx, Dispatcher, EventDef, Origin, Scope, Source};

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

struct Agent<D: Domain> {
    topic_id: i64,
    dispatcher: Arc<Dispatcher>,
    tree: Tree<D>,
    jobs: Jobs<D>,
    // persist: Arc<dyn Persist<D>>,
    bus: broadcast::Sender<TreeEvent<D>>,
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
            match wake {
                Wake::Event(event) => todo!(),
                Wake::Command(command) => todo!(),
                Wake::Exit => todo!(),
            }
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
                let _ = self.bus.send(event);
            }
            Effect::Persist { node } => {
                todo!()
                // if let Some(phase) = self.tree.phase(node).cloned()
                //     && let Err(err) = self.persist.save(node, &phase).await
                // {
                //     log::error!("[hub] persist failed for node {node}: {err}");
                // }
            }
            Effect::Launch { node, job, input } => {
                self.jobs.launch(JobRequest { node, job, input });
            }
            Effect::Stop { node, job } => {
                self.jobs.stop(node, job);
            }
        }
    }

    fn notify(&self, event: &TreeEvent<D>) {
        todo!()
        // for (name, payload) in transfer_event(event) {
        //     let Some(def) = observe_def(item.name) else {
        //         log::error!("Translated event {} has no definition", item.name);
        //         continue;
        //     };
        //     self.dispatcher.notify(
        //         &def,
        //         &observe_cx(self.topic_id, event.node_id()),
        //         item.payload,
        //     )
        // }
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

fn catalog() -> &'static Directory {
    static CATALOG: OnceLock<Directory> = OnceLock::new();
    CATALOG.get_or_init(directory)
}

fn observe_def(name: &str) -> Option<EventDef<Value>> {
    let meta = catalog().get(name)?;
    meta.is_observe()
        .then(|| EventDef::new(meta.name, meta.shape, meta.log, meta.schema))
}

fn transfer_event<D: Domain>(event: &TreeEvent<D>) -> (&str, Value) {
    todo!()
}
