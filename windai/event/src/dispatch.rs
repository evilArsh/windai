use crate::context::DispatchCx;
use crate::def::{Directory, EventDef, Shape};
use crate::envelope::Envelope;
use crate::guard::{Limits, RateLimiter};
use crate::mod_call::{ModCaller, ModRoster};
use crate::outlet::{EventOutlet, Pending};
use crate::subscription::SubscriptionIndex;
use crate::tracking::DeliveryRecord;
use crate::{DispatchError, to_json};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};

pub struct Dispatcher {
    directory: Directory,
    index: RwLock<SubscriptionIndex>,
    mod_roster: Arc<dyn ModRoster>,
    mod_caller: Arc<dyn ModCaller>,
    ev_out: Arc<dyn EventOutlet>,
    rate: Mutex<RateLimiter>,
    tracking: Arc<AtomicBool>,
    deliveries: Arc<Mutex<Vec<DeliveryRecord>>>,
    limits: Limits,
}

impl Dispatcher {
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
            envelope: Envelope::of(def, cx, value),
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
                envelope: Envelope::of(def, cx, value),
            }),
            Err(error) => {
                self.failed(def.name, cx, DispatchFailure::BadPayload, error.to_string())
                    .await
            }
        }
        accumulated
    }

    fn require_shape(&self, def: &EventDef<impl Sized>, want: Shape) -> Result<(), DispatchError> {
        let meta = self
            .directory
            .get(def.name)
            .ok_or(DispatchError::NotInDirectory { name: def.name })?;
        if meta.shape != want {
            return Err(DispatchError::WrongShape { name: def.name });
        }
        Ok(())
    }

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
}
