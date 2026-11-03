use super::tree::{Domain, JobId, NodeId};
use super::{Input, Runner};
use std::sync::Arc;

pub struct JobRequest<D: Domain> {
    pub node: NodeId,
    pub job: JobId,
    pub input: Input<D>,
}

pub struct Jobs<D: Domain> {
    runner: Arc<dyn Runner<D>>,
    // sink: JobSink<D>,
    // grace: Duration,
    // running: Arc<Mutex<HashMap<(NodeId, JobId), CancellationToken>>>,
}
impl<D: Domain> Jobs<D> {
    pub fn shutdown(&self) {
        todo!()
    }

    pub fn launch(&self, request: JobRequest<D>) {
        todo!()
    }

    pub fn stop(&self, node: NodeId, job: JobId) {
        todo!()
    }
}
