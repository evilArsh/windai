//! mod 侧的两个装配部件

use super::manifest::parse;
use wind_core::error::CoreError;
use wind_core::storage::Storage;
use wind_event::dispatch::Dispatcher;
use wind_event::host::{CallCx, HostError, ModHost};
use wind_event::subscription::Subscription;

pub struct NoHost;

#[async_trait::async_trait]
impl ModHost for NoHost {
    async fn call(
        &self,
        _sub: &Subscription,
        _payload: serde_json::Value,
        _cx: CallCx,
    ) -> Result<Option<serde_json::Value>, HostError> {
        Ok(None)
    }
}

pub async fn rebuild_subscriptions(
    storage: &Storage,
    bus: &Dispatcher,
    default_timeout_ms: u64,
) -> Result<(), CoreError> {
    let mut subs: Vec<Subscription> = Vec::new();
    for (_, raw) in storage.mod_current_manifests().await? {
        match parse(&raw, bus.directory()) {
            Ok(manifest) => {
                subs.extend(manifest.subscriptions(bus.directory(), default_timeout_ms));
            }
            Err(error) => {
                log::warn!("[mod] an installed manifest is unreadable: {error}");
            }
        }
    }
    bus.rebuild_index(subs);
    Ok(())
}
