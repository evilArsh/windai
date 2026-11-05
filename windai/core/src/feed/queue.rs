use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_util::sync::CancellationToken;
use wind_wire::event::FeedEvent;

pub(crate) struct Subscriber {
    tx: mpsc::Sender<Arc<FeedEvent>>,
    cancel: CancellationToken,
    capacity: usize,
}

impl Subscriber {
    pub(crate) fn new(
        capacity: usize,
        cancel: CancellationToken,
    ) -> (Self, mpsc::Receiver<Arc<FeedEvent>>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                tx,
                cancel,
                capacity,
            },
            rx,
        )
    }

    /// 返回 false 表示该连接已取消
    pub(crate) fn deliver(&self, event: &Arc<FeedEvent>) -> bool {
        match self.tx.try_send(Arc::clone(event)) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                self.cancel.cancel();
                false
            }
        }
    }

    pub(crate) fn backlog(&self) -> usize {
        self.capacity - self.tx.capacity()
    }
}
