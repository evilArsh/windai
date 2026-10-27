use self::def::EventMeta;
use self::envelope::Scope;
use self::mod_call::{CallCx, CallError};
use self::subscription::{EnabledSet, Subscription};
use std::sync::Arc;

pub mod context;
pub mod def;
pub mod dispatch;
pub mod envelope;
pub mod events;
pub mod guard;
pub mod mod_call;
pub mod outlet;
pub mod subscription;

/// 事件观察者接口
#[async_trait::async_trait]
pub trait Observer: Send + Sync {
    async fn on_event(&self, meta: &EventMeta, event: &serde_json::Value);
}

/// 调用 mod js模块的接口
#[async_trait::async_trait]
pub trait ModCaller: Send + Sync {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        cx: CallCx,
    ) -> Result<Option<serde_json::Value>, CallError>;
}

/// mod 清单
pub trait ModRoster: Send + Sync {
    fn enabled(&self, scope: &Scope) -> EnabledSet;
}

impl<T: ModRoster + ?Sized> ModRoster for Arc<T> {
    fn enabled(&self, scope: &Scope) -> EnabledSet {
        (**self).enabled(scope)
    }
}
