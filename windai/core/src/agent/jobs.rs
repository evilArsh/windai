use super::handler::Command;
use super::tree::event::Event;
use super::tree::{Domain, JobId, NodeId};
use super::{Ctx, Input, Outcome, Runner};
use crate::error::panic_text;
use futures::FutureExt;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct JobRequest<D: Domain> {
    pub node: NodeId,
    pub job: JobId,
    pub input: Input<D>,
}

pub struct Jobs<D: Domain> {
    runner: Arc<dyn Runner<D>>,
    sink: mpsc::Sender<Command<D>>,
    running: Arc<Mutex<HashMap<(NodeId, JobId), CancellationToken>>>,
}

impl<D: Domain> Jobs<D> {
    pub fn new(runner: Arc<dyn Runner<D>>, sink: mpsc::Sender<Command<D>>) -> Self {
        Self {
            runner,
            sink,
            running: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn launch(&self, request: JobRequest<D>) {
        let JobRequest { node, job, input } = request;
        let cancel = CancellationToken::new();
        self.running
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert((node, job), cancel.clone());

        let runner = Arc::clone(&self.runner);
        let sink = self.sink.clone();
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            let ctx = Ctx::new(node, job, cancel.clone());
            let caught = tokio::select! {
                biased;
                () = cancel.cancelled() => None,
                result = AssertUnwindSafe(runner.run(ctx, input)).catch_unwind() => Some(result),
            };
            running
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&(node, job));

            let Some(caught) = caught else {
                return;
            };
            let event = match caught {
                Ok(outcome) => report(node, job, outcome),
                Err(payload) => {
                    log::error!(
                        "[jobs] runner panicked on node {node} job {job}: {}",
                        panic_text(payload.as_ref())
                    );
                    Event::Aborted { node, job }
                }
            };
            if sink
                .send(Command {
                    event,
                    applied: None,
                })
                .await
                .is_err()
            {
                log::debug!("[jobs] report dropped because the agent is gone");
            }
        });
    }

    pub fn stop(&self, node: NodeId, job: JobId) {
        let token = self
            .running
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(node, job))
            .cloned();
        if let Some(token) = token {
            token.cancel();
        }
    }

    pub fn shutdown(&self) {
        let mut running = self
            .running
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for token in running.values() {
            token.cancel();
        }
        running.clear();
    }
}

fn report<D: Domain>(node: NodeId, job: JobId, outcome: Outcome<D>) -> Event<D> {
    match outcome {
        Outcome::Completed(output) => Event::Completed { node, job, output },
        Outcome::Failed(error) => Event::Failed { node, job, error },
        Outcome::Suspend(payload) => Event::Suspended { node, job, payload },
        Outcome::Spawn { spawn } => Event::Spawned { node, job, spawn },
    }
}
