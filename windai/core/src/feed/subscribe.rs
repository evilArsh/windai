//! 订阅握手

use super::frame::Frame;
use super::hub::{Delivery, SubscriberPoll};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use wind_event::envelope::{EventSeq, Scope};

#[derive(Debug, Clone)]
pub struct Subscribe {
    /// 只收该作用域的事件
    pub scope: Scope,
    /// 省略时不回补
    pub since_seq: Option<EventSeq>,
    /// 省略时追上进度即转增量
    pub until_seq: Option<EventSeq>,
    pub blocks: bool,
}

pub struct Opening {
    pub id: u64,
    pub subscriber: Subscriber,
}

pub struct Subscriber {
    id: u64,
    shared: Arc<Mutex<Delivery>>,
    wake: Arc<Notify>,
    first: VecDeque<Frame>,
}

impl Subscriber {
    pub(crate) fn new(
        id: u64,
        shared: Arc<Mutex<Delivery>>,
        wake: Arc<Notify>,
        first: VecDeque<Frame>,
    ) -> Self {
        Self {
            id,
            shared,
            wake,
            first,
        }
    }

    pub async fn next(&mut self) -> Option<Frame> {
        if let Some(frame) = self.first.pop_front() {
            return Some(frame);
        }
        loop {
            let wake = Arc::clone(&self.wake);
            let notified = wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let polled = {
                let mut delivery = self
                    .shared
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                delivery.poll_subscriber(self.id)
            };
            match polled {
                SubscriberPoll::Frame(frame) => return Some(frame),
                SubscriberPoll::Disconnected => return None,
                SubscriberPoll::Empty => {}
            }
            notified.await;
        }
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        if let Ok(mut delivery) = self.shared.lock() {
            delivery.unregister(self.id);
        }
    }
}
