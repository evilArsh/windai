//! 事件帧与事件日志的送达

use super::frame::{Baseline, Frame};
use super::log::{BACKFILL_LIMIT, LogRecord};
use super::queue::{Channel, FrameQueue, Overflow, channel_of};
use super::subscribe::{Opening, Subscribe, Subscriber};
use crate::error::Result;
use crate::storage::Storage;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::mem::take;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use wind_event::def::{EventMeta, Shape, visible_to_frontend};
use wind_event::envelope::{EventSeq, Scope};
use wind_event::observer::BuiltinObserver;
use wind_stream::accumulator::{BlockAccumulator, BlockChange};
use wind_stream::block::Block;
use wind_stream::id::{BlockId, MessageId};
use wind_stream::payload::BlockCommit;
use wind_wire::Message;

/// 单个订阅方允许积压的帧数上限
const SUBSCRIBER_CAPACITY: usize = 256;

/// 总线上的内置观察者
///
/// 维护所有要送往前端的事件帧和事件日志，
/// 同时保存流式块的中间状态
pub struct FeedHub {
    shared: Arc<Mutex<Delivery>>,
    wake: Arc<Notify>,
    baselines: RwLock<Option<Arc<dyn BaselineProvider>>>,
    storage: Storage,
    log_writes: AsyncMutex<()>,
}

pub trait BaselineProvider: Send + Sync {
    fn stateful(&self, scope: &Scope) -> Vec<(String, serde_json::Value)>;
}

pub(crate) struct Delivery {
    blocks: BlockAccumulator,
    inflight: HashMap<i64, BTreeSet<MessageId>>,
    pending: Vec<LogRecord>,
    subscribers: Vec<Registered>,
    high_water: u64,
    next_subscriber: u64,
}

struct Registered {
    id: u64,
    scope: Scope,
    queue: FrameQueue,
    disconnected: bool,
}

pub(crate) enum SubscriberPoll {
    Frame(Frame),
    Empty,
    Disconnected,
}

impl FeedHub {
    pub async fn open(
        storage: Storage,
        baselines: Option<Arc<dyn BaselineProvider>>,
    ) -> Result<Arc<Self>> {
        let seed = storage.block_id_seed().await?;
        Ok(Arc::new(Self {
            shared: Arc::new(Mutex::new(Delivery::new(BlockId(seed)))),
            wake: Arc::new(Notify::new()),
            baselines: RwLock::new(baselines),
            storage,
            log_writes: AsyncMutex::new(()),
        }))
    }

    pub fn set_baselines(&self, baselines: Arc<dyn BaselineProvider>) {
        *self
            .baselines
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(baselines);
    }

    pub fn publish(&self, name: &str, channel: Channel, log: bool, envelope: serde_json::Value) {
        self.deliver(name, channel, log, Shape::Observe, &envelope);
    }

    pub fn absorb(
        &self,
        topic_id: i64,
        of_message: MessageId,
        partial: &Message,
    ) -> Vec<BlockChange> {
        let mut delivery = self.lock();
        let changes = delivery.blocks.absorb(of_message, partial);
        delivery
            .inflight
            .entry(topic_id)
            .or_default()
            .insert(of_message);
        changes
    }

    pub fn candidate(&self, id: BlockId) -> Option<BlockCommit> {
        self.lock().blocks.candidate(id)
    }

    pub fn allocate_block_id(&self) -> BlockId {
        self.lock().blocks.allocate_id()
    }

    /// 定稿一份副本，块仍留在累加器里
    pub(crate) fn settled(
        &self,
        id: BlockId,
        content: String,
        data: Option<serde_json::Value>,
    ) -> Option<Block> {
        self.lock().blocks.settled(id, content, data)
    }

    /// 提交一个块，直接整体替换内容与载荷，标为已提交并移出累加器
    pub fn commit(
        &self,
        id: BlockId,
        content: String,
        data: Option<serde_json::Value>,
    ) -> Option<Block> {
        self.lock().blocks.commit(id, content, data)
    }

    pub fn discard(&self, id: BlockId) -> Option<Block> {
        self.lock().blocks.discard(id)
    }

    pub fn snapshot_blocks(&self, scope: &Scope) -> Vec<Block> {
        self.lock().snapshot_blocks(scope)
    }

    pub async fn subscribe(&self, request: Subscribe) -> Result<Opening> {
        let (id, boundary, baselines) = {
            let mut delivery = self.lock();

            let mut baselines = Vec::new();
            if request.blocks {
                baselines.push(Frame::Snapshot {
                    baseline: Baseline::Blocks {
                        blocks: delivery.snapshot_blocks(&request.scope),
                    },
                });
            }
            if let Some(provider) = self
                .baselines
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
            {
                for (key, value) in provider.stateful(&request.scope) {
                    baselines.push(Frame::Snapshot {
                        baseline: Baseline::Stateful { key, value },
                    });
                }
            }

            let id = delivery.register(&request);
            (id, delivery.high_water, baselines)
        };

        let history = if request.since_seq.is_some() {
            match self.history(&request, boundary).await {
                Ok(history) => history,
                Err(error) => {
                    self.lock().unregister(id);
                    return Err(error);
                }
            }
        } else {
            Vec::new()
        };

        let mut first: VecDeque<Frame> = history
            .into_iter()
            .map(|record| Frame::Event {
                envelope: record.envelope(),
            })
            .collect();
        first.extend(baselines);

        Ok(Opening {
            id,
            subscriber: Subscriber::new(
                id,
                Arc::clone(&self.shared),
                Arc::clone(&self.wake),
                first,
            ),
        })
    }

    pub fn high_water(&self) -> EventSeq {
        EventSeq(self.lock().high_water)
    }

    pub fn seed_high_water(&self, seq: EventSeq) {
        let mut delivery = self.lock();
        delivery.high_water = delivery.high_water.max(seq.0);
    }

    pub fn unsubscribe(&self, id: u64) {
        self.lock().unregister(id);
    }

    pub fn take_pending(&self) -> Vec<LogRecord> {
        take(&mut self.lock().pending)
    }

    pub async fn flush(&self) -> Result<()> {
        let _writing = self.log_writes.lock().await;
        let pending = self.take_pending();
        if pending.is_empty() {
            return Ok(());
        }
        let count = pending.len();
        if let Err(error) = self.storage.event_log_append(&pending).await {
            log::error!("[feed] appending {count} log records failed: {error}");
            self.lock().restore_pending(pending);
            return Err(error);
        }
        Ok(())
    }

    fn deliver(
        &self,
        name: &str,
        channel: Channel,
        log: bool,
        shape: Shape,
        event: &serde_json::Value,
    ) {
        let mut delivery = self.lock();
        let seq = event["seq"].as_u64().unwrap_or(delivery.high_water);
        delivery.high_water = delivery.high_water.max(seq);
        if log && let Some(record) = LogRecord::from_envelope(name, shape, event) {
            delivery.pending.push(record);
        }
        if !visible_to_frontend(shape) {
            return;
        }
        let Ok(scope) = serde_json::from_value::<Scope>(event["scope"].clone()) else {
            log::warn!("[feed] event {name} carries no usable scope, its frame is dropped");
            return;
        };
        let frame = Frame::Event {
            envelope: event.clone(),
        };
        delivery.publish(&scope, &frame, channel, &self.wake);
    }

    async fn history(&self, request: &Subscribe, boundary: u64) -> Result<Vec<LogRecord>> {
        self.flush().await?;
        let since = request.since_seq.map(|seq| seq.0).unwrap_or(0);
        let until = request
            .until_seq
            .map(|seq| seq.0.min(boundary))
            .unwrap_or(boundary);
        let records = self
            .storage
            .event_log_range(request.scope.topic_id(), since, BACKFILL_LIMIT, until)
            .await?;
        Ok(records
            .into_iter()
            .filter(LogRecord::visible_to_frontend)
            .collect())
    }

    fn lock(&self) -> MutexGuard<'_, Delivery> {
        self.shared
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[async_trait::async_trait]
impl BuiltinObserver for FeedHub {
    async fn on_event(&self, meta: &EventMeta, event: &serde_json::Value) {
        self.deliver(
            meta.name,
            channel_of(meta.name),
            meta.log,
            meta.shape,
            event,
        );
    }
}

impl Delivery {
    fn new(seed: BlockId) -> Self {
        Self {
            blocks: BlockAccumulator::after(seed),
            inflight: HashMap::new(),
            pending: Vec::new(),
            subscribers: Vec::new(),
            high_water: 0,
            next_subscriber: 0,
        }
    }

    fn snapshot_blocks(&mut self, scope: &Scope) -> Vec<Block> {
        let Some(topic_id) = scope.topic_id() else {
            return Vec::new();
        };
        let snapshot = self.blocks.snapshot();
        let live: BTreeSet<MessageId> = snapshot.iter().map(|block| block.of_message).collect();
        self.inflight.retain(|_, messages| {
            messages.retain(|message| live.contains(message));
            !messages.is_empty()
        });
        let Some(messages) = self.inflight.get(&topic_id) else {
            return Vec::new();
        };
        snapshot
            .into_iter()
            .filter(|block| messages.contains(&block.of_message))
            .collect()
    }

    fn restore_pending(&mut self, mut records: Vec<LogRecord>) {
        records.append(&mut self.pending);
        self.pending = records;
    }

    pub(crate) fn unregister(&mut self, id: u64) {
        self.subscribers.retain(|registered| registered.id != id);
    }

    fn register(&mut self, request: &Subscribe) -> u64 {
        self.subscribers
            .retain(|registered| !registered.disconnected);
        self.next_subscriber += 1;
        let id = self.next_subscriber;
        self.subscribers.push(Registered {
            id,
            scope: request.scope.clone(),
            queue: FrameQueue::new(SUBSCRIBER_CAPACITY),
            disconnected: false,
        });
        id
    }

    fn publish(&mut self, scope: &Scope, frame: &Frame, channel: Channel, wake: &Notify) {
        let mut delivered = false;
        for registered in &mut self.subscribers {
            if registered.disconnected || &registered.scope != scope {
                continue;
            }
            if registered.queue.push(frame.clone(), channel) == Some(Overflow::Disconnect) {
                registered.disconnected = true;
            }
            delivered = true;
        }
        if delivered {
            wake.notify_waiters();
        }
    }

    pub(crate) fn poll_subscriber(&mut self, id: u64) -> SubscriberPoll {
        let Some(registered) = self
            .subscribers
            .iter_mut()
            .find(|registered| registered.id == id)
        else {
            return SubscriberPoll::Disconnected;
        };
        if registered.disconnected {
            return SubscriberPoll::Disconnected;
        }
        match registered.queue.pop() {
            Some(frame) => SubscriberPoll::Frame(frame),
            None => SubscriberPoll::Empty,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一个只要实时帧的订阅请求
    fn request() -> Subscribe {
        Subscribe {
            scope: Scope::Topic { topic_id: 1 },
            since_seq: None,
            until_seq: None,
            blocks: false,
        }
    }

    /// 一帧只带序号的事件
    fn event(seq: u64) -> Frame {
        Frame::Event {
            envelope: serde_json::json!({ "seq": seq }),
        }
    }

    #[test]
    fn dropping_a_subscriber_stops_frames_piling_up_in_its_queue() {
        let shared = Arc::new(Mutex::new(Delivery::new(BlockId(0))));
        let wake = Arc::new(Notify::new());
        let scope = Scope::Topic { topic_id: 1 };
        let id = shared.lock().expect("lock the state").register(&request());
        let subscriber =
            Subscriber::new(id, Arc::clone(&shared), Arc::clone(&wake), VecDeque::new());

        {
            let mut delivery = shared.lock().expect("lock the state");
            delivery.publish(&scope, &event(1), Channel::Journal, &wake);
            let registered = delivery
                .subscribers
                .iter()
                .find(|registered| registered.id == id)
                .expect("find the registered subscriber");
            assert_eq!(
                registered.queue.len(),
                1,
                "publishing while registered grows the queue"
            );
        }

        drop(subscriber);

        let mut delivery = shared.lock().expect("lock the state");
        assert!(
            delivery.subscribers.is_empty(),
            "dropping a subscriber must unsubscribe it, and its queue goes with it"
        );
        delivery.publish(&scope, &event(2), Channel::Journal, &wake);
        assert!(
            delivery.subscribers.is_empty(),
            "publishing after unsubscribing leaves no queue to grow"
        );
    }
}
