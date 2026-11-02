use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::frame::Frame;

/// 一个作用域下的订阅者发送端
#[derive(Default)]
pub(crate) struct Subscribers {
    senders: Vec<mpsc::Sender<Frame>>,
}

impl Subscribers {
    pub(crate) fn register(&mut self, sender: mpsc::Sender<Frame>) {
        self.senders.push(sender);
    }

    /// 落库帧队列满时断开订阅，实时增量队列满时丢弃
    pub(crate) fn publish(&mut self, frame: &Frame, durable: bool) {
        self.senders
            .retain(|sender| match sender.try_send(frame.clone()) {
                Ok(()) => true,
                Err(TrySendError::Full(_)) => !durable,
                Err(TrySendError::Closed(_)) => false,
            });
    }
}
