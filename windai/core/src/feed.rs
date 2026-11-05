mod buffer;
mod error;
mod hub;
mod queue;
mod stats;
mod writer;

pub use error::FeedError;
pub use hub::{FeedHandle, FeedRuntime, Opening, Subscription, open};
pub use stats::{FeedStats, ShutdownReport, WriterState};

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct FeedOptions {
    pub subscriber_capacity: usize,
    pub batch_size: usize,
    pub shutdown_timeout: Duration,
}

impl Default for FeedOptions {
    fn default() -> Self {
        Self {
            subscriber_capacity: 512,
            batch_size: 256,
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}
