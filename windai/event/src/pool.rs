//! observe 事件的投递池
//!
//! mod 在清单里声明订阅某个事件；事件提交之后，这里把事件交给那个 mod 的处理函数。
//!
//! ```text
//! 清单 events.on: { "core/block.committed": { "handler": "onCommit", "timeoutMs": 50 } }
//!   │ 装载
//!   ▼
//! Subscription { mod_name, event, handler, timeout_ms }
//!   │ 事件提交后，Dispatcher 查索引得到订阅名单
//!   ▼
//! Job { event, scope, payload, sub } ──► 队列
//!                                          │ 5ms 一个窗口，按 (mod, event, scope) 聚合
//!                                          ▼
//!                        ModCaller::call(sub, payload, CallCx::observe)
//!                                          │
//!             ┌────────────────────────────┼────────────────────────────┐
//!             ▼                            ▼                            ▼
//!         正常返回                  超过 timeoutMs                 handler 报错
//!        （不能改写）             core/dispatch.timeout         core/dispatch.failed
//! ```
//!
//! 合批键是 `(mod, event, scope)`，窗口 5ms，队列上界 1024 条；组内一条时载荷是值本身，
//! 多条时是数组。
//!
//! 单次预算取 `docs/manifest.schema.json` 的 `events.on[].timeoutMs`，清单上限 60000ms。
//! 总预算的裁剪只发生在 intercept 链；observe 投递在提交之后，不再受它约束。
//!
//! 池只处理 observe；intercept 在派发链里同步调用，其 `phase` 不经过这里。
//! 队列满时丢弃并计数；失败只上报，不重试。
//!
//! 不在这里的事：谁订阅了（`subscription.rs` 的索引）、要不要投（`dispatch.rs` 的
//! `on_committed`）、intercept 的同步改写（`dispatch.rs` 的 `decide`）。

use crate::ModCaller;
use crate::def::Shape;
use crate::envelope::{Envelope, Origin, Scope, Source};
use crate::events::{
    CORE_DISPATCH_FAILED, CORE_DISPATCH_TIMEOUT, DeliveryTarget, DispatchFailed, DispatchFailure,
    DispatchTimeout,
};
use crate::mod_call::{CallCx, CallError, CallMode};
use crate::outlet::{EventOutlet, Pending};
use crate::subscription::Subscription;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// 一次 observe 投递
pub struct Job {
    pub event: &'static str,
    pub scope: Scope,
    pub payload: Option<Value>,
    pub sub: Subscription,
}

/// 合批窗口
const BATCH_WINDOW: Duration = Duration::from_millis(5);
/// 队列上界
const CAPACITY: usize = 1024;

/// observe 事件的投递池
///
/// 异步、按 `(mod, event, scope)` 合批、单次调用有超时。
pub struct ObservePool {
    tx: mpsc::Sender<Job>,
    dropped: Arc<AtomicU64>,
}

impl ObservePool {
    pub fn start(caller: Arc<dyn ModCaller>, outlet: Arc<dyn EventOutlet>) -> Self {
        let (tx, rx) = mpsc::channel(CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        tokio::spawn(run(rx, caller, outlet));
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
    caller: Arc<dyn ModCaller>,
    outlet: Arc<dyn EventOutlet>,
) {
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        let deadline = Instant::now() + BATCH_WINDOW;
        while let Ok(Some(job)) = tokio::time::timeout_at(deadline, rx.recv()).await {
            batch.push(job);
        }
        deliver(batch, &caller, &outlet).await;
    }
}

async fn deliver(batch: Vec<Job>, caller: &Arc<dyn ModCaller>, outlet: &Arc<dyn EventOutlet>) {
    let mut order: Vec<(String, &'static str, Scope)> = Vec::new();
    let mut groups: HashMap<(String, &'static str, Scope), (Subscription, Vec<Value>)> =
        HashMap::new();

    for job in batch {
        let key = (job.sub.mod_name.clone(), job.event, job.scope);
        if !groups.contains_key(&key) {
            order.push(key.clone());
        }
        groups
            .entry(key)
            .or_insert_with(|| (job.sub, Vec::new()))
            .1
            .push(job.payload.unwrap_or(Value::Null));
    }
    for key in order {
        let Some((sub, mut payloads)) = groups.remove(&key) else {
            continue;
        };
        let payload = match payloads.len() {
            0 => continue,
            1 => payloads.pop().unwrap(),
            _ => Value::Array(payloads),
        };
        call_one(caller, outlet, &sub, key.1, key.2, payload).await;
    }
}

async fn call_one(
    caller: &Arc<dyn ModCaller>,
    outlet: &Arc<dyn EventOutlet>,
    sub: &Subscription,
    event: &'static str,
    scope: Scope,
    payload: Value,
) {
    let call_cx = CallCx {
        deadline: Instant::now() + sub.timeout(),
        cancel: CancellationToken::new(),
        mode: CallMode::Observe,
        scope: scope.clone(),
    };
    let started = Instant::now();
    let result = tokio::time::timeout(sub.timeout(), caller.call(sub, payload, call_cx)).await;
    let duration_ms = started.elapsed().as_millis() as u64;

    match result {
        Ok(Ok(_)) => {}
        Ok(Err(CallError::Handler(message))) => {
            failed(
                outlet,
                event,
                &scope,
                Some(&sub.mod_name),
                DispatchFailure::HandlerError,
                message,
            );
        }
        Ok(Err(CallError::Internal(message))) => {
            failed(
                outlet,
                event,
                &scope,
                Some(&sub.mod_name),
                DispatchFailure::CallError,
                message,
            );
        }
        Err(_) => {
            log::warn!("[event] mod {} timed out on {event}", sub.mod_name);
            let value = serde_json::to_value(DispatchTimeout {
                event: event.to_string(),
                target: DeliveryTarget::mod_(sub.mod_name.clone()),
                duration_ms,
            })
            .ok();
            if let Some(value) = value {
                outlet.submit(Pending {
                    log: true,
                    envelope: Envelope::new(
                        CORE_DISPATCH_TIMEOUT.name,
                        scope,
                        Origin::Internal,
                        Source::core("event"),
                        Some(value),
                        Shape::Observe,
                    ),
                });
            }
        }
    }
}

fn failed(
    outlet: &Arc<dyn EventOutlet>,
    event: &'static str,
    scope: &Scope,
    target: Option<&str>,
    failure: DispatchFailure,
    reason: String,
) {
    log::error!("[event] {event} delivery failed ({failure:?}): {reason}");
    let value = serde_json::to_value(DispatchFailed {
        event: event.to_string(),
        failure,
        target: target.map(DeliveryTarget::mod_),
        reason,
    })
    .ok();
    if let Some(value) = value {
        outlet.submit(Pending {
            log: true,
            envelope: Envelope::new(
                CORE_DISPATCH_FAILED.name,
                scope.clone(),
                Origin::Internal,
                Source::core("event"),
                Some(value),
                Shape::Observe,
            ),
        });
    }
}
