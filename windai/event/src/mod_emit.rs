//! ```text
//!   open()  ──────────────────────────────► Arc<FeedHandle>   (EventOutlet)
//!   Dispatcher::new(..)  ── guards() ─────► Arc<Guards>
//!                                             │
//!   宿主装载清单（每个 mod）                   │
//!     emit 段的 payload schema ──编译──► Validator
//!     ModEmitter::new(ModIdentity{name, version},
//!                     channels, ev_out, guards)
//!                                             │
//!   ObservePool ─► ModCaller::call(sub, payload, cx: CallCx)
//!                                             │
//!                           QuickJS 里执行 handler
//!                                             │
//!                           host.emit(channel, payload)
//!                                             │
//!                           ModEmitter::emit(cx, channel, payload)
//!                                             │
//!                           EventOutlet::submit(Pending)
//! ```

use crate::context::DispatchCx;
use crate::def::Shape;
use crate::envelope::Envelope;
use crate::guard::{Guards, OverLimit};
use crate::outlet::{EventOutlet, Pending, Retention, SubmitError};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use wind_wire::event::{Source, Target};

/// 宿主为 mod 固定的身份
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModIdentity {
    pub name: String,
    pub version: u32,
}

/// 载荷检查函数
pub type PayloadValidator = Arc<dyn Fn(&Value) -> Result<(), String> + Send + Sync>;

/// 一个允许 mod 发出的频道
pub struct EmitChannel {
    name: String,
    event: String,
    retention: Retention,
    validate: PayloadValidator,
}

impl EmitChannel {
    pub fn new(
        name: impl Into<String>,
        event: impl Into<String>,
        retention: Retention,
        validate: PayloadValidator,
    ) -> Self {
        Self {
            name: name.into(),
            event: event.into(),
            retention,
            validate,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EmitError {
    #[error("the mod has not declared this channel")]
    UnknownChannel,
    #[error("the payload does not match the channel schema: {0}")]
    InvalidPayload(String),
    #[error("the event exceeded a dispatch limit: {0}")]
    OverLimit(OverLimit),
    #[error("the feed outlet is closed")]
    Closed,
}

/// 可信的 mod 事件入口
///
/// 名称和保留策略由宿主固定，脚本只能提供声明过的频道与载荷
pub struct ModEmitter {
    identity: ModIdentity,
    channels: HashMap<String, Arc<EmitChannel>>,
    outlet: Arc<dyn EventOutlet>,
    guards: Arc<Guards>,
}

impl ModEmitter {
    pub fn new(
        identity: ModIdentity,
        channels: Vec<EmitChannel>,
        outlet: Arc<dyn EventOutlet>,
        guards: Arc<Guards>,
    ) -> Self {
        Self {
            identity,
            channels: channels
                .into_iter()
                .map(|channel| (channel.name.clone(), Arc::new(channel)))
                .collect(),
            outlet,
            guards,
        }
    }

    pub fn emit(&self, cx: &DispatchCx, channel: &str, payload: Value) -> Result<(), EmitError> {
        let Some(channel) = self.channels.get(channel) else {
            return Err(EmitError::UnknownChannel);
        };
        (channel.validate)(&payload).map_err(EmitError::InvalidPayload)?;
        self.guards.admit(cx).map_err(EmitError::OverLimit)?;
        self.guards
            .admit_mod_emit(&self.identity.name)
            .map_err(EmitError::OverLimit)?;

        let envelope = Envelope::new(
            channel.event.clone(),
            cx.scope.clone(),
            cx.origin,
            Source::module(self.identity.name.clone(), self.identity.version),
            Some(payload),
            Shape::Observe,
        );
        self.outlet
            .submit(Pending {
                envelope,
                retention: channel.retention.clone(),
                target: Target::module(self.identity.name.clone(), channel.name.clone()),
            })
            .map_err(|SubmitError::Closed| EmitError::Closed)
    }
}
