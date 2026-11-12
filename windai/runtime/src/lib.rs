mod background;
mod directory;
mod error;
mod input;
mod setup;
mod shutdown;
mod topic;

pub use error::RuntimeError;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use tokio::task::JoinHandle;
use topic::TopicAgents;
use wind_core::feed::{FeedError, FeedHandle, FeedOptions, Opening, ShutdownReport};
use wind_core::resources::ResourceService;
use wind_event::{Dispatcher, Limits, Observer};
use wind_storage::Storage;
use wind_wire::AppDirs;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitAck {
    pub topic_id: i64,
    pub instance_id: i64,
    pub pending_id: i64,
}

pub struct RuntimeOptions {
    pub dirs: AppDirs,
    pub feed: FeedOptions,
    pub limits: Limits,
}

impl RuntimeOptions {
    pub fn new(dirs: AppDirs) -> Self {
        Self {
            dirs,
            feed: FeedOptions::default(),
            limits: Limits::default(),
        }
    }
}

#[derive(Clone)]
pub struct Runtime {
    inner: Arc<Inner>,
}

struct Inner {
    storage: Storage,
    dispatcher: Arc<Dispatcher>,
    services: Arc<ResourceService>,
    agents: Arc<TopicAgents>,
    feed: FeedHandle,
    _observers: Vec<Arc<dyn Observer>>,
    feed_task: Mutex<Option<JoinHandle<Result<ShutdownReport, FeedError>>>>,
    stopped: AtomicBool,
    operations: RwLock<()>,
    shutdown_task: Mutex<Option<JoinHandle<Result<(), RuntimeError>>>>,
}

impl Runtime {
    pub async fn open(options: RuntimeOptions) -> Result<Self, RuntimeError> {
        setup::open(options).await
    }

    pub fn storage(&self) -> &Storage {
        &self.inner.storage
    }

    pub async fn subscribe(&self) -> Result<Opening, RuntimeError> {
        let _operation = self.enter().await?;
        Ok(self.inner.feed.subscribe().await?)
    }

    async fn enter(&self) -> Result<RwLockReadGuard<'_, ()>, RuntimeError> {
        if self.inner.stopped.load(Ordering::SeqCst) {
            return Err(RuntimeError::Stopped);
        }
        let guard = self.inner.operations.read().await;
        if self.inner.stopped.load(Ordering::SeqCst) {
            return Err(RuntimeError::Stopped);
        }
        Ok(guard)
    }
}
