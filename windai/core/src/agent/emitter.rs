use super::tree::{JobId, NodeId};
use tokio_util::sync::CancellationToken;

pub struct BlockEmitter {
    node: NodeId,
    job: JobId,
    cancel: CancellationToken,
    sink: Box<dyn Fn(String) + Send + Sync>,
}

impl BlockEmitter {
    pub fn new(
        node: NodeId,
        job: JobId,
        cancel: CancellationToken,
        sink: Box<dyn Fn(String) + Send + Sync>,
    ) -> Self {
        Self {
            node,
            job,
            cancel,
            sink,
        }
    }

    pub fn node(&self) -> NodeId {
        self.node
    }

    pub fn job(&self) -> JobId {
        self.job
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    pub fn publish(&self, fragment: &str) {
        if self.cancel.is_cancelled() {
            return;
        }
        (self.sink)(fragment.to_string());
    }
}
