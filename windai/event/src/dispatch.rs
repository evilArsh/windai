use crate::context::DispatchCx;
use crate::def::{Directory, EventDef, Shape};
use crate::envelope::Envelope;
use crate::events::DispatchFailure;
use crate::guard::{Limits, RateLimiter};
use crate::outlet::{EventOutlet, Pending};
use crate::subscription::{EnabledSet, Subscription, SubscriptionIndex};
use crate::{ModCaller, ModRoster};
// use crate::tracking::DeliveryRecord;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};

fn to_json<P: Serialize>(event: &'static str, payload: &P) -> Result<serde_json::Value, BusError> {
    serde_json::to_value(payload).map_err(|e| BusError::BadPayload {
        event,
        detail: e.to_string(),
    })
}

/// 派发失败，被拒绝不在此列
#[derive(Debug, thiserror::Error)]
enum BusError {
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

pub struct Dispatcher {
    directory: Directory,
    index: RwLock<SubscriptionIndex>,
    /// mod 清单索引
    mod_roster: Arc<dyn ModRoster>,
    mod_caller: Arc<dyn ModCaller>,
    ev_out: Arc<dyn EventOutlet>,
    rate: Mutex<RateLimiter>,
    limits: Limits,
}

impl Dispatcher {
    pub fn new(
        directory: Directory,
        mod_roster: Arc<dyn ModRoster>,
        mod_caller: Arc<dyn ModCaller>,
        limits: Limits,
    ) -> Self {
        todo!()
    }

    pub fn notify<P>(&self, def: &EventDef<P>, cx: &DispatchCx, payload: P)
    where
        P: Serialize + Send + Sync,
    {
        if self.require_shape(def, Shape::Observe).is_err() {
            /* failed */
            return;
        }
        let Some(value) = to_json(def.name, &payload).ok() else {
            /* failed */
            return;
        };
        if !self.reserve(cx) {
            /* failed(TooManyEvents) */
            return;
        }

        self.ev_out.submit(Pending {
            shape: Shape::Observe,
            log: def.log,
            envelope: Envelope::of(&def.meta(), cx, value),
        });
    }

    pub async fn decide<P>(&self, def: &EventDef<P>, cx: &DispatchCx, payload: P) -> P
    where
        P: Serialize + DeserializeOwned + Send + Sync,
    {
        // 形态、深度、预算
        let mut accumulated = payload;
        for sub in self.chain_of(def.name, cx).unwrap_or_default() {
            // host.call：超时、报错、改写
        }
        match to_json(def.name, &accumulated) {
            Ok(value) => self.ev_out.submit(Pending {
                shape: Shape::Intercept,
                log: def.log,
                envelope: Envelope::of(&def.meta(), cx, value),
            }),
            Err(error) => {
                self.failed(
                    def.name,
                    cx,
                    DispatchFailure::BadPayload,
                    error.to_string(),
                    None,
                )
                .await
            }
        }
        accumulated
    }

    /// 重建订阅索引
    pub fn rebuild_index(&self, subs: Vec<Subscription>) {
        let mut index = self
            .index
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        index.rebuild(subs);
    }

    fn enabled(&self, cx: &DispatchCx) -> EnabledSet {
        self.mod_roster.enabled(&cx.scope)
    }

    /// 指定事件的订阅清单
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

    async fn failed(
        &self,
        event: &'static str,
        cx: &DispatchCx,
        failure: DispatchFailure,
        reason: String,
        target: Option<&str>,
    ) {
        log::error!("[event] {event} dispatch failed ({failure:?}): {reason}");
        // self.notices
        //     .emit(
        //         event,
        //         cx.scope.clone(),
        //         Notice::Failed {
        //             event_seq,
        //             target,
        //             failure,
        //             reason: &reason,
        //         },
        //     )
        //     .await;
    }
    // directory() / limits()
    // set_tracking(bool) / tracking_enabled() / record_delivery / delivery_records / clear_deliveries
    // allow_mod_emit(&str) / evict_stale_rates()
}
