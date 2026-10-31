use crate::context::DispatchCx;
use crate::def::{Directory, EventDef, EventMeta, Shape};
use crate::envelope::{Envelope, Origin, Scope, Source};
use crate::events::{
    CORE_DISPATCH_FAILED, CORE_DISPATCH_TIMEOUT, DeliveryTarget, DispatchFailed, DispatchFailure,
    DispatchTimeout,
};
use crate::guard::{Guards, Limits, OverLimit};
use crate::mod_call::{CallCx, CallError, CallMode};
use crate::outlet::{Committed, EventOutlet, Pending};
use crate::pool::{Job, ObservePool};
use crate::subscription::{Subscription, SubscriptionIndex};
use crate::{ModCaller, ModRoster, Observer};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::sync::{Arc, RwLock};
use tokio::time::Instant;

fn to_json<P: Serialize>(event: &'static str, payload: &P) -> Result<Value, BusError> {
    serde_json::to_value(payload).map_err(|e| BusError::BadPayload {
        event,
        detail: e.to_string(),
    })
}

/// 派发失败，被拒绝不在此列
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("event {name} has the wrong shape for this call")]
    WrongShape { name: &'static str },
    #[error("payload for {event} could not be serialized: {detail}")]
    BadPayload { event: &'static str, detail: String },
    #[error("event {name} is not in the directory")]
    NotInDirectory { name: &'static str },
}

/// 事件派发总线
pub struct Dispatcher {
    directory: Directory,
    index: RwLock<SubscriptionIndex>,
    /// mod 清单索引
    mod_roster: Arc<dyn ModRoster>,
    mod_caller: Arc<dyn ModCaller>,
    /// mod observe 事件投递
    mod_ob: Arc<ObservePool>,
    ev_out: Arc<dyn EventOutlet>,
    guards: Arc<Guards>,
}

impl Dispatcher {
    pub fn new(
        directory: Directory,
        mod_roster: Arc<dyn ModRoster>,
        mod_caller: Arc<dyn ModCaller>,
        ev_out: Arc<dyn EventOutlet>,
        limits: Limits,
    ) -> Self {
        Self {
            directory,
            index: RwLock::new(SubscriptionIndex::new()),
            mod_roster,
            mod_ob: Arc::new(ObservePool::start(
                Arc::clone(&mod_caller),
                Arc::clone(&ev_out),
            )),
            mod_caller,
            ev_out,
            guards: Arc::new(Guards::new(limits)),
        }
    }

    pub fn guards(&self) -> Arc<Guards> {
        Arc::clone(&self.guards)
    }

    pub fn directory(&self) -> &Directory {
        &self.directory
    }

    pub fn limits(&self) -> Limits {
        self.guards.limits()
    }

    /// 重建订阅索引
    pub fn rebuild_index(&self, subs: Vec<Subscription>) {
        let mut index = self
            .index
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        index.rebuild(subs);
    }

    /// 只观测、不等待结果的事件
    pub fn notify<P>(&self, def: &EventDef<P>, cx: &DispatchCx, payload: P)
    where
        P: Serialize + Send + Sync,
    {
        if let Err(error) = self.require_shape(def, Shape::Observe) {
            self.failed(def.name, cx, failure_of(&error), None, error.to_string());
            return;
        }
        if let Err(over) = self.guards.admit(cx) {
            self.over_limit(def.name, cx, over);
            return;
        }
        self.emit(def, cx, &payload);
    }

    /// 允许 mod 改写载荷的事件；返回改写后的值
    ///
    /// mod 改写失败时，将原值返回
    pub async fn decide<P>(&self, def: &EventDef<P>, cx: &DispatchCx, payload: P) -> P
    where
        P: Serialize + DeserializeOwned + Send + Sync,
    {
        if let Err(error) = self.require_shape(def, Shape::Intercept) {
            self.failed(def.name, cx, failure_of(&error), None, error.to_string());
            return payload;
        }
        if let Err(over) = self.guards.admit(cx) {
            self.over_limit(def.name, cx, over);
            return payload;
        }

        if let Some(subs) = self.chain_of(def.name, cx)
            && !subs.is_empty()
        {
            let mut accumulated = payload;
            for sub in self.chain_of(def.name, cx).unwrap_or_default() {
                let budget = sub.timeout().min(cx.remaining_budget());
                if budget.is_zero() {
                    log::debug!(
                        "[event] budget exhausted, skipping {} for {}",
                        sub.mod_name,
                        def.name
                    );
                    continue;
                }

                let Ok(value) = to_json(def.name, &accumulated) else {
                    self.failed(
                        def.name,
                        cx,
                        DispatchFailure::BadPayload,
                        None,
                        "the payload could not be serialized".to_string(),
                    );
                    break;
                };

                let started = Instant::now();
                let call_cx = CallCx {
                    deadline: Instant::now() + budget,
                    cancel: cx.cancel().clone(),
                    mode: CallMode::Intercept,
                    scope: cx.scope.clone(),
                };

                let outcome = tokio::select! {
                    biased;
                    _ = cx.cancel().cancelled() => continue,
                    result = tokio::time::timeout(budget, self.mod_caller.call(&sub, value, call_cx)) => result,
                };
                let duration_ms = started.elapsed().as_millis() as u64;

                match outcome {
                    Err(_) => self.timeout(def.name, cx, &sub.mod_name, duration_ms),
                    Ok(Err(CallError::Handler(message))) => self.failed(
                        def.name,
                        cx,
                        DispatchFailure::HandlerError,
                        Some(&sub.mod_name),
                        message,
                    ),
                    Ok(Err(CallError::Internal(message))) => self.failed(
                        def.name,
                        cx,
                        DispatchFailure::CallError,
                        Some(&sub.mod_name),
                        message,
                    ),
                    Ok(Ok(None)) => {}
                    Ok(Ok(Some(value))) => match serde_json::from_value::<P>(value) {
                        Ok(next) => accumulated = next,
                        Err(error) => self.failed(
                            def.name,
                            cx,
                            DispatchFailure::BadRewrite,
                            Some(&sub.mod_name),
                            error.to_string(),
                        ),
                    },
                }
            }
            self.emit(def, cx, &accumulated);
            accumulated
        } else {
            payload
        }
    }

    /// 指定事件的订阅清单，顺序由索引给出
    fn chain_of(&self, event: &'static str, cx: &DispatchCx) -> Option<Vec<Subscription>> {
        self.chain_for(event, &cx.scope)
    }

    fn chain_for(&self, event: &'static str, scope: &Scope) -> Option<Vec<Subscription>> {
        let index = self
            .index
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !index.has_any(event) {
            return None;
        }
        let enabled = self.mod_roster.enabled(scope);
        Some(
            index
                .ordered_in(event, &enabled)
                .into_iter()
                .cloned()
                .collect(),
        )
    }

    fn emit<P: Serialize>(&self, def: &EventDef<P>, cx: &DispatchCx, payload: &P) {
        match to_json(def.name, payload) {
            Ok(value) => self.ev_out.submit(Pending {
                shape: def.shape,
                log: def.log,
                envelope: Envelope::of(def, cx, value),
            }),
            Err(error) => self.failed(
                def.name,
                cx,
                DispatchFailure::BadPayload,
                None,
                error.to_string(),
            ),
        }
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

    fn failed(
        &self,
        event: &'static str,
        cx: &DispatchCx,
        failure: DispatchFailure,
        target: Option<&str>,
        reason: String,
    ) {
        log::error!("[event] {event} dispatch failed ({failure:?}): {reason}");
        self.notice(
            CORE_DISPATCH_FAILED.name,
            &cx.scope,
            DispatchFailed {
                event: event.to_string(),
                failure,
                target: target.map(DeliveryTarget::mod_),
                reason,
            },
        );
    }

    fn over_limit(&self, event: &'static str, cx: &DispatchCx, over: OverLimit) {
        let failure = match over {
            OverLimit::TooDeep { .. } => DispatchFailure::TooDeep,
            OverLimit::TooManyEvents { .. } => DispatchFailure::TooManyEvents,
            OverLimit::TooFast { .. } => DispatchFailure::RateLimited,
        };
        self.failed(event, cx, failure, None, over.to_string());
    }

    fn timeout(&self, event: &'static str, cx: &DispatchCx, target: &str, duration_ms: u64) {
        log::warn!("[event] mod {target} timed out on {event}");
        self.notice(
            CORE_DISPATCH_TIMEOUT.name,
            &cx.scope,
            DispatchTimeout {
                event: event.to_string(),
                target: DeliveryTarget::mod_(target),
                duration_ms,
            },
        );
    }

    /// 派发层自己的事件：来源固定为 event 子系统
    fn notice<P: Serialize>(&self, name: &'static str, scope: &Scope, payload: P) {
        let Ok(value) = serde_json::to_value(payload) else {
            log::error!("[event] cannot encode {name}");
            return;
        };
        self.ev_out.submit(Pending {
            shape: Shape::Observe,
            log: true,
            envelope: Envelope::new(
                name,
                scope.clone(),
                Origin::Internal,
                Source::core("event"),
                value,
            ),
        });
    }
}

impl Observer for Dispatcher {
    fn on_committed(&self, meta: &EventMeta, committed: &Committed) {
        if !meta.is_observe() {
            return;
        }
        let Some(chain) = self.chain_for(meta.name, &committed.envelope.scope) else {
            return;
        };
        for sub in chain {
            self.mod_ob.enqueue(Job {
                event: meta.name,
                scope: committed.envelope.scope.clone(),
                payload: committed.envelope.payload.clone(),
                sub,
            });
        }
    }
}

fn failure_of(error: &BusError) -> DispatchFailure {
    match error {
        BusError::WrongShape { .. } => DispatchFailure::WrongShape,
        BusError::BadPayload { .. } => DispatchFailure::BadPayload,
        BusError::NotInDirectory { .. } => DispatchFailure::NotInDirectory,
    }
}
