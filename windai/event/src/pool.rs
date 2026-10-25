use super::context::DispatchCx;
use super::envelope::{EventSeq, Scope};
use super::events::DispatchFailure;
use super::host::{CallCx, CallMode, HostError, ModHost};
use super::subscription::Subscription;
use super::tracking::{DeliveryOutcome, DeliveryRecord, DeliveryTarget};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub struct Job {
    pub seq: EventSeq,
    pub event: &'static str,
    pub scope: Scope,
    /// 一个事件的载荷
    pub payload: serde_json::Value,
    pub sub: Subscription,
}

pub type DeliverySink = Arc<dyn Fn(&'static str, DeliveryRecord) + Send + Sync>;

/// 一次投递产生的、需要让观察者看到的异常
pub enum Notice<'a> {
    /// 订阅方超时
    Timeout {
        event_seq: EventSeq,
        target: &'a str,
        duration_ms: u64,
    },
    /// 派发失败
    Failed {
        event_seq: Option<EventSeq>,
        target: Option<&'a str>,
        failure: DispatchFailure,
        reason: &'a str,
    },
}

/// 把超时与失败派发给内置观察者
#[async_trait::async_trait]
pub trait DispatchNotifier: Send + Sync {
    async fn emit(&self, event: &'static str, scope: Scope, notice: Notice<'_>);
}

type BatchGroup = (Subscription, EventSeq, &'static str, Vec<serde_json::Value>);

pub struct ObservePool {
    tx: mpsc::Sender<Job>,
    dropped: Arc<AtomicU64>,
}

impl ObservePool {
    /// 队列上界
    pub const CAPACITY: usize = 1024;

    /// 合批窗口
    pub const DEFAULT_BATCH_WINDOW: Duration = Duration::from_millis(5);

    pub fn new(
        host: Arc<dyn ModHost>,
        sink: DeliverySink,
        notifier: Arc<dyn DispatchNotifier>,
    ) -> Self {
        Self::with_options(
            host,
            sink,
            notifier,
            Self::CAPACITY,
            Self::DEFAULT_BATCH_WINDOW,
        )
    }

    pub fn with_options(
        host: Arc<dyn ModHost>,
        sink: DeliverySink,
        notifier: Arc<dyn DispatchNotifier>,
        capacity: usize,
        batch_window: Duration,
    ) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        tokio::spawn(run(rx, host, sink, notifier, batch_window));
        Self { tx, dropped }
    }

    pub fn enqueue(&self, job: Job) {
        if self.tx.try_send(job).is_err() {
            let total = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            log::warn!("[event] observe queue full, dropped {total} jobs so far");
        }
    }

    pub fn dropped_total(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

async fn run(
    mut rx: mpsc::Receiver<Job>,
    host: Arc<dyn ModHost>,
    sink: DeliverySink,
    notifier: Arc<dyn DispatchNotifier>,
    batch_window: Duration,
) {
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        let deadline = tokio::time::Instant::now() + batch_window;
        while let Ok(Some(job)) = tokio::time::timeout_at(deadline, rx.recv()).await {
            batch.push(job);
        }
        deliver(batch, &host, &sink, &notifier).await;
    }
}

async fn deliver(
    batch: Vec<Job>,
    host: &Arc<dyn ModHost>,
    sink: &DeliverySink,
    notifier: &Arc<dyn DispatchNotifier>,
) {
    let mut grouped: HashMap<(String, &'static str, Scope), BatchGroup> = HashMap::new();

    for job in batch {
        let key = (job.sub.mod_name.clone(), job.event, job.scope.clone());
        let Job {
            seq,
            event,
            payload,
            sub,
            ..
        } = job;
        grouped
            .entry(key)
            .or_insert_with(|| (sub, seq, event, Vec::new()))
            .3
            .push(payload);
    }

    let mut groups: Vec<_> = grouped.into_iter().collect();
    groups.sort_by_key(|(_, (_, seq, _, _))| *seq);

    for ((_, _, scope), (sub, seq, event, mut values)) in groups {
        let payload = if values.len() == 1 {
            values.pop().expect("one payload")
        } else {
            serde_json::Value::Array(values)
        };
        call_one(host, sink, notifier, &sub, event, seq, scope, payload).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn call_one(
    host: &Arc<dyn ModHost>,
    sink: &DeliverySink,
    notifier: &Arc<dyn DispatchNotifier>,
    sub: &Subscription,
    event: &'static str,
    seq: EventSeq,
    scope: Scope,
    payload: serde_json::Value,
) {
    let call_cx = CallCx {
        deadline: Instant::now() + sub.timeout(),
        cancel: CancellationToken::new(),
        mode: CallMode::Observe,
        scope,
    };
    let call_scope = call_cx.scope.clone();

    let started = Instant::now();
    let result = tokio::time::timeout(sub.timeout(), host.call(sub, payload, call_cx)).await;
    let duration_ms = started.elapsed().as_millis() as u64;

    let (outcome, error) = match result {
        Ok(Ok(_)) => (DeliveryOutcome::Observed, None),
        Ok(Err(HostError::Handler(msg))) => {
            notifier
                .emit(
                    event,
                    call_scope.clone(),
                    Notice::Failed {
                        event_seq: Some(seq),
                        target: Some(&sub.mod_name),
                        failure: DispatchFailure::HandlerError,
                        reason: &msg,
                    },
                )
                .await;
            (DeliveryOutcome::Error, Some(msg))
        }
        Ok(Err(HostError::Internal(msg))) => {
            log::error!(
                "[event] host failure delivering {event} to {}: {msg}",
                sub.mod_name
            );
            notifier
                .emit(
                    event,
                    call_scope,
                    Notice::Failed {
                        event_seq: Some(seq),
                        target: Some(&sub.mod_name),
                        failure: DispatchFailure::HostError,
                        reason: &msg,
                    },
                )
                .await;
            (DeliveryOutcome::Error, Some(msg))
        }
        Err(_) => {
            notifier
                .emit(
                    event,
                    call_scope,
                    Notice::Timeout {
                        event_seq: seq,
                        target: &sub.mod_name,
                        duration_ms,
                    },
                )
                .await;
            (DeliveryOutcome::Timeout, None)
        }
    };

    sink(
        event,
        DeliveryRecord {
            event_seq: seq,
            target: DeliveryTarget::mod_(sub.mod_name.clone()),
            outcome,
            duration_ms,
            error,
        },
    );
}

pub fn job_from(
    cx: &DispatchCx,
    event: &'static str,
    seq: EventSeq,
    payload: serde_json::Value,
    sub: Subscription,
) -> Job {
    Job {
        seq,
        event,
        scope: cx.scope.clone(),
        payload,
        sub,
    }
}
