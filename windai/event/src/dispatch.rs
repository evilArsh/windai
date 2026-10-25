use super::context::DispatchCx;
use super::def::{Directory, EventDef, Shape};
use super::envelope::{EventEnvelope, EventSeq, Origin, Scope, Source};
use super::events::{
    CORE_DISPATCH_FAILED, CORE_DISPATCH_TIMEOUT, DispatchFailed, DispatchFailure, DispatchTimeout,
};
use super::guard::RateLimiter;
use super::host::{CallCx, CallMode, HostError, ModEnablement, ModHost};
use super::observer::BuiltinObserver;
use super::pool::{DeliverySink, DispatchNotifier, Notice, ObservePool, job_from};
use super::subscription::{EnabledSet, Subscription, SubscriptionIndex};
use super::tracking::{DeliveryOutcome, DeliveryRecord, DeliveryTarget, should_record};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::time::Instant;

fn to_json<P: Serialize>(event: &'static str, payload: &P) -> Result<serde_json::Value, BusError> {
    serde_json::to_value(payload).map_err(|e| BusError::BadPayload {
        event,
        detail: e.to_string(),
    })
}

/// 编码一条派发通知。失败时记录并放弃这条通知，不影响调用方
fn encode_notice<T: Serialize>(name: &str, value: &T) -> Option<serde_json::Value> {
    match serde_json::to_value(value) {
        Ok(value) => Some(value),
        Err(error) => {
            log::warn!("[event] cannot encode {name}: {error}");
            None
        }
    }
}

/// 闭环防护与预算的阈值
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// 起因链深度上限
    pub max_depth: u32,
    /// 单次触发产生的事件数上限
    pub max_events_per_trigger: u32,
    /// 逐 mod 速率限制的时间窗
    pub window: Duration,
    /// 每个时间窗内允许产生的事件数
    pub per_window: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_depth: 8,
            max_events_per_trigger: 1000,
            window: Duration::from_secs(3),
            per_window: 100,
        }
    }
}

/// 派发失败，被拒绝不在此列
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("event {name} has the wrong shape for this call")]
    WrongShape { name: &'static str },
    #[error("mod {mod_name} rewrote {event} into a value the declared schema rejects")]
    BadRewrite {
        mod_name: String,
        event: &'static str,
    },
    #[error("payload for {event} could not be serialized: {detail}")]
    BadPayload { event: &'static str, detail: String },
    #[error("event chain exceeded max depth {max} at {name}")]
    TooDeep { max: u32, name: &'static str },
    #[error("single trigger exceeded {max} events")]
    TooManyEvents { max: u32 },
    #[error("event {name} is not in the directory")]
    NotInDirectory { name: &'static str },
}

fn shape_failure(error: &BusError) -> DispatchFailure {
    match error {
        BusError::WrongShape { .. } => DispatchFailure::WrongShape,
        _ => DispatchFailure::NotInDirectory,
    }
}

/// 全局事件总线
pub struct Dispatcher {
    directory: Directory,
    index: RwLock<SubscriptionIndex>,
    enablement: Arc<dyn ModEnablement>,
    host: Arc<dyn ModHost>,
    seq: Arc<AtomicU64>,
    observe: Arc<ObservePool>,
    observers: Arc<RwLock<Vec<Arc<dyn BuiltinObserver>>>>,
    notices: Arc<dyn DispatchNotifier>,
    rate: Mutex<RateLimiter>,
    /// 投递追踪的开关，默认关闭
    tracking: Arc<AtomicBool>,
    deliveries: Arc<Mutex<Vec<DeliveryRecord>>>,
    limits: Limits,
}

impl Dispatcher {
    pub fn new(
        directory: Directory,
        enablement: Arc<dyn ModEnablement>,
        host: Arc<dyn ModHost>,
        limits: Limits,
    ) -> Self {
        let tracking = Arc::new(AtomicBool::new(false));
        let deliveries: Arc<Mutex<Vec<DeliveryRecord>>> = Arc::new(Mutex::new(Vec::new()));

        let sink: DeliverySink = {
            let tracking = Arc::clone(&tracking);
            let deliveries = Arc::clone(&deliveries);
            Arc::new(move |event, record| {
                if tracking.load(Ordering::Relaxed)
                    && should_record(event)
                    && let Ok(mut v) = deliveries.lock()
                {
                    v.push(record);
                }
            })
        };

        let seq = Arc::new(AtomicU64::new(0));
        let observers: Arc<RwLock<Vec<Arc<dyn BuiltinObserver>>>> =
            Arc::new(RwLock::new(Vec::new()));
        let notices: Arc<dyn DispatchNotifier> = Arc::new(DispatcherNotices {
            seq: Arc::clone(&seq),
            observers: Arc::clone(&observers),
        });
        let observe = Arc::new(ObservePool::new(
            Arc::clone(&host),
            sink,
            Arc::clone(&notices),
        ));

        Self {
            directory,
            index: RwLock::new(SubscriptionIndex::new()),
            enablement,
            host,
            seq,
            observe,
            observers,
            notices,
            rate: Mutex::new(RateLimiter::new(limits)),
            tracking,
            deliveries,
            limits,
        }
    }

    pub fn rebuild_index(&self, subs: Vec<Subscription>) {
        let mut index = self
            .index
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        index.rebuild(subs);
    }

    pub fn directory(&self) -> &Directory {
        &self.directory
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    pub fn observe_pool(&self) -> Arc<ObservePool> {
        Arc::clone(&self.observe)
    }

    pub fn attach_observer(&self, observer: Arc<dyn BuiltinObserver>) {
        self.observers
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(observer);
    }

    pub fn set_tracking(&self, on: bool) {
        self.tracking.store(on, Ordering::Relaxed);
    }

    pub fn tracking_enabled(&self) -> bool {
        self.tracking.load(Ordering::Relaxed)
    }

    pub fn record_delivery(&self, record: DeliveryRecord) {
        if let Ok(mut v) = self.deliveries.lock() {
            v.push(record);
        }
    }

    pub fn delivery_records(&self) -> Vec<DeliveryRecord> {
        self.deliveries
            .lock()
            .map(|v| v.clone())
            .unwrap_or_default()
    }

    pub fn clear_deliveries(&self) {
        if let Ok(mut v) = self.deliveries.lock() {
            v.clear();
        }
    }

    fn next_seq(&self) -> EventSeq {
        EventSeq(self.seq.fetch_add(1, Ordering::Relaxed) + 1)
    }

    pub fn emit_seq(&self, cx: &DispatchCx) -> Result<EventSeq, BusError> {
        if !cx.count_emitted(self.limits.max_events_per_trigger) {
            log::warn!(
                "[event] single trigger exceeded {} events, dropped",
                self.limits.max_events_per_trigger
            );
            return Err(BusError::TooManyEvents {
                max: self.limits.max_events_per_trigger,
            });
        }
        Ok(self.next_seq())
    }

    pub fn current_seq(&self) -> EventSeq {
        EventSeq(self.seq.load(Ordering::Relaxed))
    }

    pub fn seed_seq(&self, seq: EventSeq) {
        self.seq.store(seq.0, Ordering::Relaxed);
    }

    fn require_shape(&self, def: &EventDef<impl Sized>, want: Shape) -> Result<(), BusError> {
        let meta = self
            .directory
            .get(def.name)
            .ok_or(BusError::NotInDirectory { name: def.name })?;
        if meta.shape != want {
            return Err(BusError::WrongShape { name: def.name });
        }
        Ok(())
    }

    fn enabled(&self, cx: &DispatchCx) -> EnabledSet {
        self.enablement.enabled(&cx.scope)
    }

    /// 订阅 `event` 的所有 mod
    fn chain_of(&self, event: &'static str, cx: &DispatchCx) -> Option<Vec<Subscription>> {
        let index = self
            .index
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !index.has_any(event) {
            return None;
        }
        let enabled = self.enabled(cx);
        Some(
            index
                .ordered_in(event, &enabled)
                .into_iter()
                .cloned()
                .collect(),
        )
    }

    async fn observe<P: Serialize>(
        &self,
        def: &EventDef<impl Sized>,
        cx: &DispatchCx,
        seq: EventSeq,
        payload: &P,
    ) -> Result<(), BusError> {
        let observers = {
            let list = self
                .observers
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if list.is_empty() {
                return Ok(());
            }
            list.clone()
        };
        let envelope = EventEnvelope {
            seq,
            time: EventEnvelope::<()>::now_ms(),
            name: def.name.to_string(),
            scope: cx.scope.clone(),
            origin: cx.origin,
            source: cx.source.clone(),
            cause: cx.cause,
            payload: to_json(def.name, payload)?,
        };
        let event = serde_json::to_value(&envelope).map_err(|error| BusError::BadPayload {
            event: def.name,
            detail: error.to_string(),
        })?;
        let meta = def.meta();
        for observer in observers {
            observer.on_event(&meta, &event).await;
        }
        Ok(())
    }

    pub async fn decide<P>(&self, def: &EventDef<P>, cx: &DispatchCx, payload: P) -> P
    where
        P: Serialize + DeserializeOwned + Send + Sync,
    {
        if let Err(error) = self.require_shape(def, Shape::Intercept) {
            self.failed(
                def.name,
                cx,
                shape_failure(&error),
                None,
                None,
                error.to_string(),
            )
            .await;
            return payload;
        }

        if !cx.depth_ok(self.limits.max_depth) {
            self.failed(
                def.name,
                cx,
                DispatchFailure::TooDeep,
                None,
                None,
                format!("event chain exceeded max depth {}", self.limits.max_depth),
            )
            .await;
            return payload;
        }

        let Some(event_seq) = self.emit_seq(cx).ok() else {
            self.failed(
                def.name,
                cx,
                DispatchFailure::TooManyEvents,
                None,
                None,
                format!(
                    "single trigger exceeded {} events",
                    self.limits.max_events_per_trigger
                ),
            )
            .await;
            return payload;
        };

        // 找出订阅该事件的所有 mod。
        // 没有 mod 订阅时，发送给所有内建观察者，然后将数据原路返回
        let Some(chain) = self.chain_of(def.name, cx) else {
            self.observe_safely(def, cx, event_seq, &payload, None)
                .await;
            return payload;
        };

        if cx.origin == Origin::Ui {
            self.observe_safely(def, cx, event_seq, &payload, None)
                .await;
            return payload;
        }

        // 经过每个 mod 改写后的累计最终结果
        let mut accumulated = payload;

        for sub in chain {
            let remaining = cx.remaining_budget();
            if remaining.is_zero() {
                log::debug!(
                    "[event] budget exhausted, skipping {} for {}",
                    sub.mod_name,
                    def.name
                );
                self.record(
                    &event_seq,
                    &sub,
                    DeliveryOutcome::Skipped,
                    0,
                    None,
                    def.name,
                );
                continue;
            }

            let budget = sub.timeout().min(remaining);
            let Some(as_value) = to_json(def.name, &accumulated).ok() else {
                self.failed(
                    def.name,
                    cx,
                    DispatchFailure::BadPayload,
                    Some(&sub.mod_name),
                    Some(event_seq),
                    "the payload could not be serialized".to_string(),
                )
                .await;
                break;
            };

            let started = Instant::now();
            let call_cx = CallCx {
                deadline: Instant::now() + budget,
                cancel: cx.cancel().clone(),
                mode: CallMode::Intercept,
                scope: cx.scope.clone(),
            };

            let step = tokio::select! {
                biased;
                _ = cx.cancel().cancelled() => Step::Cancelled,
                r = tokio::time::timeout(budget, self.host.call(&sub, as_value, call_cx)) => match r {
                    Ok(done) => Step::Done(done),
                    Err(_) => Step::TimedOut,
                },
            };

            let duration_ms = started.elapsed().as_millis() as u64;

            let rewrite = match step {
                Step::Cancelled => continue,
                Step::TimedOut => {
                    log::warn!("[event] mod {} timed out on {}", sub.mod_name, def.name);
                    self.record(
                        &event_seq,
                        &sub,
                        DeliveryOutcome::Timeout,
                        duration_ms,
                        None,
                        def.name,
                    );
                    self.notices
                        .emit(
                            def.name,
                            cx.scope.clone(),
                            Notice::Timeout {
                                event_seq,
                                target: &sub.mod_name,
                                duration_ms,
                            },
                        )
                        .await;
                    continue;
                }
                Step::Done(Err(HostError::Handler(msg))) => {
                    log::warn!(
                        "[event] {} failed handling {}: {msg}",
                        sub.mod_name,
                        def.name
                    );
                    self.record(
                        &event_seq,
                        &sub,
                        DeliveryOutcome::Error,
                        duration_ms,
                        Some(msg.clone()),
                        def.name,
                    );
                    self.failed(
                        def.name,
                        cx,
                        DispatchFailure::HandlerError,
                        Some(&sub.mod_name),
                        Some(event_seq),
                        msg,
                    )
                    .await;
                    continue;
                }
                Step::Done(Err(HostError::Internal(msg))) => {
                    log::error!(
                        "[event] host failure calling {} on {}: {msg}",
                        sub.mod_name,
                        def.name
                    );
                    self.record(
                        &event_seq,
                        &sub,
                        DeliveryOutcome::Error,
                        duration_ms,
                        Some(msg.clone()),
                        def.name,
                    );
                    self.failed(
                        def.name,
                        cx,
                        DispatchFailure::HostError,
                        Some(&sub.mod_name),
                        Some(event_seq),
                        msg,
                    )
                    .await;
                    continue;
                }
                Step::Done(Ok(rewrite)) => rewrite,
            };

            match rewrite {
                None => {
                    self.record(
                        &event_seq,
                        &sub,
                        DeliveryOutcome::Pass,
                        duration_ms,
                        None,
                        def.name,
                    );
                }
                Some(value) => match serde_json::from_value(value) {
                    Ok(next) => {
                        accumulated = next;
                        self.record(
                            &event_seq,
                            &sub,
                            DeliveryOutcome::Rewrite,
                            duration_ms,
                            None,
                            def.name,
                        );
                    }
                    Err(error) => {
                        self.record(
                            &event_seq,
                            &sub,
                            DeliveryOutcome::Error,
                            duration_ms,
                            Some(error.to_string()),
                            def.name,
                        );
                        self.failed(
                            def.name,
                            cx,
                            DispatchFailure::BadRewrite,
                            Some(&sub.mod_name),
                            Some(event_seq),
                            error.to_string(),
                        )
                        .await;
                    }
                },
            }
        }

        self.observe_safely(def, cx, event_seq, &accumulated, Some(event_seq))
            .await;
        accumulated
    }

    pub async fn notify<P>(&self, def: &EventDef<P>, cx: &DispatchCx, payload: P)
    where
        P: Serialize + Send + Sync,
    {
        if let Err(error) = self.require_shape(def, Shape::Observe) {
            self.failed(
                def.name,
                cx,
                shape_failure(&error),
                None,
                None,
                error.to_string(),
            )
            .await;
            return;
        }

        let Some(value) = to_json(def.name, &payload).ok() else {
            self.failed(
                def.name,
                cx,
                DispatchFailure::BadPayload,
                None,
                None,
                "the payload could not be serialized".to_string(),
            )
            .await;
            return;
        };

        let Some(seq) = self.emit_seq(cx).ok() else {
            self.failed(
                def.name,
                cx,
                DispatchFailure::TooManyEvents,
                None,
                None,
                format!(
                    "single trigger exceeded {} events",
                    self.limits.max_events_per_trigger
                ),
            )
            .await;
            return;
        };

        if self.observe(def, cx, seq, &value).await.is_err() {
            self.failed(
                def.name,
                cx,
                DispatchFailure::BadPayload,
                None,
                Some(seq),
                "the payload could not be serialized".to_string(),
            )
            .await;
        }

        let Some(subs) = self.chain_of(def.name, cx) else {
            return;
        };

        for sub in subs {
            self.observe
                .enqueue(job_from(cx, def.name, seq, value.clone(), sub));
        }
    }

    /// 每个载荷各自生成 seq 与 envelope；合批由投递窗口完成
    pub async fn notify_all<P, I>(&self, def: &EventDef<P>, cx: &DispatchCx, payloads: I)
    where
        P: Serialize + Send + Sync,
        I: IntoIterator<Item = P>,
    {
        for payload in payloads {
            self.notify(def, cx, payload).await;
        }
    }

    /// 报告一次派发失败；失败本身不能再失败
    async fn failed(
        &self,
        event: &'static str,
        cx: &DispatchCx,
        failure: DispatchFailure,
        target: Option<&str>,
        event_seq: Option<EventSeq>,
        reason: String,
    ) {
        log::error!("[event] {event} dispatch failed ({failure:?}): {reason}");
        self.notices
            .emit(
                event,
                cx.scope.clone(),
                Notice::Failed {
                    event_seq,
                    target,
                    failure,
                    reason: &reason,
                },
            )
            .await;
    }

    /// 派发最终载荷；观察失败只记录，不影响返回值
    async fn observe_safely<P: Serialize>(
        &self,
        def: &EventDef<impl Sized>,
        cx: &DispatchCx,
        seq: EventSeq,
        payload: &P,
        event_seq: Option<EventSeq>,
    ) {
        if self.observe(def, cx, seq, payload).await.is_err() {
            self.failed(
                def.name,
                cx,
                DispatchFailure::BadPayload,
                None,
                event_seq,
                "the payload could not be serialized".to_string(),
            )
            .await;
        }
    }

    pub fn seq_only(&self, cx: &DispatchCx) -> Result<EventSeq, BusError> {
        self.emit_seq(cx)
    }

    fn record(
        &self,
        event_seq: &EventSeq,
        sub: &Subscription,
        outcome: DeliveryOutcome,
        duration_ms: u64,
        error: Option<String>,
        event: &'static str,
    ) {
        if !self.tracking_enabled() || !should_record(event) {
            return;
        }
        self.record_delivery(DeliveryRecord {
            event_seq: *event_seq,
            target: DeliveryTarget::mod_(sub.mod_name.clone()),
            outcome,
            duration_ms,
            error,
        });
    }

    pub fn record_observer(
        &self,
        event: &'static str,
        event_seq: EventSeq,
        observer: &str,
        outcome: DeliveryOutcome,
        duration_ms: u64,
    ) {
        if !self.tracking_enabled() || !should_record(event) {
            return;
        }
        self.record_delivery(DeliveryRecord {
            event_seq,
            target: DeliveryTarget::observer(observer),
            outcome,
            duration_ms,
            error: None,
        });
    }

    pub fn evict_stale_rates(&self) {
        if let Ok(mut rate) = self.rate.lock() {
            rate.evict_stale();
        }
    }

    pub fn allow_mod_emit(&self, mod_name: &str) -> bool {
        match self.rate.lock() {
            Ok(mut rate) => rate.allow_emit(mod_name),
            Err(_) => false,
        }
    }
}

/// 一次订阅调用的结束方式
enum Step {
    Done(Result<Option<serde_json::Value>, HostError>),
    TimedOut,
    Cancelled,
}

/// 把超时与失败派发给内置观察者
struct DispatcherNotices {
    seq: Arc<AtomicU64>,
    observers: Arc<RwLock<Vec<Arc<dyn BuiltinObserver>>>>,
}

#[async_trait::async_trait]
impl DispatchNotifier for DispatcherNotices {
    async fn emit(&self, event: &'static str, scope: Scope, notice: Notice<'_>) {
        let observers = {
            let list = self
                .observers
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if list.is_empty() {
                return;
            }
            list.clone()
        };
        let seq = EventSeq(self.seq.fetch_add(1, Ordering::Relaxed) + 1);
        let (meta, cause, payload) = match notice {
            Notice::Timeout {
                event_seq,
                target,
                duration_ms,
            } => {
                let Some(payload) = encode_notice(
                    CORE_DISPATCH_TIMEOUT.name,
                    &DispatchTimeout {
                        event: event.to_string(),
                        event_seq: event_seq.0,
                        target: DeliveryTarget::mod_(target),
                        duration_ms,
                    },
                ) else {
                    return;
                };
                (CORE_DISPATCH_TIMEOUT.meta(), Some(event_seq), payload)
            }
            Notice::Failed {
                event_seq,
                target,
                failure,
                reason,
            } => {
                let Some(payload) = encode_notice(
                    CORE_DISPATCH_FAILED.name,
                    &DispatchFailed {
                        event: event.to_string(),
                        failure,
                        target: target.map(DeliveryTarget::mod_),
                        event_seq: event_seq.map(|seq| seq.0),
                        reason: reason.to_string(),
                    },
                ) else {
                    return;
                };
                (CORE_DISPATCH_FAILED.meta(), event_seq, payload)
            }
        };
        let envelope = EventEnvelope {
            seq,
            time: EventEnvelope::<()>::now_ms(),
            name: meta.name.to_string(),
            scope,
            origin: Origin::Internal,
            source: Source::core("event"),
            cause,
            payload,
        };
        let Some(event_value) = encode_notice(meta.name, &envelope) else {
            return;
        };
        for observer in observers {
            observer.on_event(&meta, &event_value).await;
        }
    }
}
