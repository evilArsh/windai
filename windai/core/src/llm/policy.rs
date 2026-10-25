//! 工具调用的放行

use super::events::{CORE_LLM_TOOLCALL, ToolCalls};
use crate::models::AllowPolicy;
use std::collections::BTreeSet;
use std::sync::Arc;
use wind_event::context::DispatchCx;
use wind_event::dispatch::Dispatcher;
use wind_wire::tool::FunctionCall;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Whitelisted {
    Hit,
    Miss,
}

impl AllowPolicy {
    pub fn admits(&self, tool: &str) -> Whitelisted {
        let hit = self.allow.iter().any(|entry| entry == "*" || entry == tool);
        if hit {
            Whitelisted::Hit
        } else {
            Whitelisted::Miss
        }
    }
}

#[derive(Debug, Clone)]
pub struct CallPlan {
    pub admitted: Vec<FunctionCall>,
    pub pending: Vec<FunctionCall>,
}

pub async fn decide_calls(
    bus: &Arc<Dispatcher>,
    cx: &DispatchCx,
    policy: &AllowPolicy,
    calls: Vec<FunctionCall>,
    instance_id: i64,
) -> CallPlan {
    let mut admitted = Vec::new();
    let mut to_ask = Vec::new();
    for call in calls {
        match policy.admits(&call.name) {
            Whitelisted::Hit => admitted.push(call),
            Whitelisted::Miss => to_ask.push(call),
        }
    }
    if to_ask.is_empty() {
        return CallPlan {
            admitted,
            pending: Vec::new(),
        };
    }

    let payload = ToolCalls {
        instance_id,
        calls: to_ask.clone(),
        suspended: Vec::new(),
    };
    let decided = bus.decide(&CORE_LLM_TOOLCALL, cx, payload).await;
    let suspended: BTreeSet<&str> = decided.suspended.iter().map(String::as_str).collect();
    let mut pending = Vec::new();
    for call in decided.calls {
        if suspended.contains(call.id.as_str()) {
            pending.push(call);
        } else {
            admitted.push(call);
        }
    }
    CallPlan { admitted, pending }
}
