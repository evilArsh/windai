// use super::events::{CORE_LLM_TOOLCALL, ToolCalls};
// use std::collections::BTreeSet;
// use wind_wire::tool::FunctionCall;
// use wind_wire::{ToolPolicy, Whitelist};

// #[derive(Debug, Clone)]
// pub struct CallPlan {
//     pub admitted: Vec<FunctionCall>,
//     pub pending: Vec<FunctionCall>,
// }

// pub fn decide_calls(policy: &ToolPolicy, calls: Vec<FunctionCall>) -> CallPlan {
//     let mut admitted = Vec::new();
//     let mut to_ask = Vec::new();
//     for call in calls {
//         match policy.admits(&call.name) {
//             Whitelist::Hit => admitted.push(call),
//             Whitelist::Miss => to_ask.push(call),
//         };
//     }
//     return CallPlan {
//         admitted,
//         pending: to_ask,
//     };
// if to_ask.is_empty() {
//     return CallPlan {
//         admitted,
//         pending: Vec::new(),
//     };
// }

// let payload = ToolCalls {
//     instance_id,
//     calls: to_ask.clone(),
//     suspended: Vec::new(),
// };
// let decided = bus.decide(&CORE_LLM_TOOLCALL, cx, payload).await;
// let suspended: BTreeSet<&str> = decided.suspended.iter().map(String::as_str).collect();
// let mut pending = Vec::new();
// for call in decided.calls {
//     if suspended.contains(call.id.as_str()) {
//         pending.push(call);
//     } else {
//         admitted.push(call);
//     }
// }
// CallPlan { admitted, pending }
// }
