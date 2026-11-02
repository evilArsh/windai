use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::sync::mpsc;
use wind_event::{Committed, Envelope, Shape};
use wind_storage::LogRecord;
use wind_storage::Storage;
use wind_storage::error::Result as StorageResult;

use crate::frame::Frame;
use crate::queue::Subscribers;

const SUBSCRIBER_CAPACITY: usize = 512;
const DEFAULT_HISTORY_LIMIT: i64 = 4096;

/// 把日志记录还原成回放载荷
#[async_trait::async_trait]
pub trait Resolver: Send + Sync {
    /// 返回 `None` 时沿用日志里保存的载荷
    async fn resolve(&self, record: &LogRecord) -> Option<Value>;
}

/// 为首次订阅提供状态快照
#[async_trait::async_trait]
pub trait SnapshotProvider: Send + Sync {
    /// `boundary` 是快照之后实时帧的起点
    async fn snapshot(&self, topic_id: Option<i64>, boundary: Option<i64>) -> Option<Value>;
}

pub struct Hub {
    storage: Storage,
    subscribers: Mutex<HashMap<Option<i64>, Subscribers>>,
    resolver: Option<Arc<dyn Resolver>>,
    snapshot: Option<Arc<dyn SnapshotProvider>>,
    history_limit: i64,
}

impl Hub {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage,
            subscribers: Mutex::new(HashMap::new()),
            resolver: None,
            snapshot: None,
            history_limit: DEFAULT_HISTORY_LIMIT,
        }
    }

    pub fn with_resolver(mut self, resolver: Arc<dyn Resolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    pub fn with_snapshot(mut self, snapshot: Arc<dyn SnapshotProvider>) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    pub fn with_history_limit(mut self, limit: i64) -> Self {
        self.history_limit = limit.max(1);
        self
    }

    /// 广播一条已落库事件
    pub fn publish_committed(&self, committed: &Committed) {
        if !committed.envelope.shape.visible_to_frontend() {
            return;
        }
        let frame = Frame::Event {
            log_id: Some(committed.log_id),
            envelope: committed.envelope.clone(),
        };
        self.deliver(committed.envelope.scope.topic_id(), frame, true);
    }

    /// 广播一条不落库的实时增量
    pub fn publish_unlogged(&self, envelope: Envelope<Value>) {
        if !envelope.shape.visible_to_frontend() {
            return;
        }
        let topic_id = envelope.scope.topic_id();
        let frame = Frame::Event {
            log_id: None,
            envelope,
        };
        self.deliver(topic_id, frame, false);
    }

    /// 建立订阅，返回历史帧与后续实时帧
    ///
    /// 先注册再读 boundary，避免注册窗口内提交的事件既不在历史也不在实时队列
    pub async fn subscribe(
        &self,
        topic_id: Option<i64>,
        since: Option<i64>,
        limit: i64,
    ) -> StorageResult<(Vec<Frame>, mpsc::Receiver<Frame>)> {
        let (sender, receiver) = mpsc::channel(SUBSCRIBER_CAPACITY);
        {
            let mut subscribers = self
                .subscribers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            subscribers.entry(topic_id).or_default().register(sender);
        }

        let boundary = self.storage.event_log_boundary(topic_id).await?;
        let limit = if limit <= 0 {
            self.history_limit
        } else {
            limit
        };
        let records = self
            .storage
            .event_log_range(topic_id, since, boundary, Some(Shape::Observe), limit)
            .await?;

        let mut history = Vec::with_capacity(records.len() + 1);
        if since.is_none()
            && let Some(snapshot) = &self.snapshot
            && let Some(data) = snapshot.snapshot(topic_id, boundary).await
        {
            history.push(Frame::Snapshot { data });
        }
        for record in records {
            let payload = match &self.resolver {
                Some(resolver) => resolver.resolve(&record).await,
                None => None,
            };
            history.push(Frame::from_record(record, payload));
        }
        Ok((history, receiver))
    }

    fn deliver(&self, topic_id: Option<i64>, frame: Frame, durable: bool) {
        let mut subscribers = self
            .subscribers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(subscribers) = subscribers.get_mut(&topic_id) {
            subscribers.publish(&frame, durable);
        }
    }
}
