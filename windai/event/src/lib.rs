pub use context::DispatchCx;
pub use def::{Directory, EventDef, EventMeta, Shape, schema};
pub use dispatch::Dispatcher;
pub use envelope::{Envelope, Origin, Scope, Source};
pub use events::{
    CORE_DISPATCH_FAILED, CORE_DISPATCH_TIMEOUT, DeliveryTarget, DispatchFailed, DispatchFailure,
    DispatchTimeout, directory,
};
pub use guard::Limits;
pub use mod_call::{CallCx, CallError, CallMode};
pub use outlet::{Committed, EventOutlet, Pending};
pub use pool::{Job, ObservePool};
pub use subscription::{EnabledSet, Subscription, SubscriptionIndex};

use std::sync::Arc;

pub mod context;
pub mod def;
pub mod dispatch;
pub mod envelope;
pub mod events;
pub mod guard;
pub mod mod_call;
pub mod outlet;
pub mod pool;
pub mod subscription;

/// 已提交事件的接收者；写入者按 pos 顺序调用，实现方不得阻塞
pub trait Observer: Send + Sync {
    fn on_committed(&self, meta: &EventMeta, committed: &Committed);
}

/// 调用 mod 处理函数的入口
#[async_trait::async_trait]
pub trait ModCaller: Send + Sync {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        cx: CallCx,
    ) -> Result<Option<serde_json::Value>, CallError>;
}

/// 某个作用域下启用的 mod 清单
pub trait ModRoster: Send + Sync {
    fn enabled(&self, scope: &Scope) -> EnabledSet;
}

#[async_trait::async_trait]
impl<T: ModCaller + ?Sized> ModCaller for Arc<T> {
    async fn call(
        &self,
        sub: &Subscription,
        payload: serde_json::Value,
        cx: CallCx,
    ) -> Result<Option<serde_json::Value>, CallError> {
        (**self).call(sub, payload, cx).await
    }
}

impl<T: ModRoster + ?Sized> ModRoster for Arc<T> {
    fn enabled(&self, scope: &Scope) -> EnabledSet {
        (**self).enabled(scope)
    }
}
