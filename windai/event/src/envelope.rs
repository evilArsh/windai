pub use wind_wire::event::{Envelope, Origin, Scope, Source};

use crate::context::DispatchCx;
use crate::def::EventDef;
use serde_json::Value;

/// 沿调用方的上下文组信封
pub fn envelope_of<P>(def: &EventDef<P>, cx: &DispatchCx, payload: Value) -> Envelope<Value> {
    Envelope::new(
        def.name,
        cx.scope.clone(),
        cx.origin,
        cx.source.clone(),
        Some(payload),
        def.shape,
    )
}
