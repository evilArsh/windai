use chrono::{DateTime, Local};
use serde::de::DeserializeOwned;
use serde_json::Value;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};
use wind_core::agent::events::{AgentSealed, AgentSpawn, AgentStatus};
use wind_core::llm::events::{
    BlockCommitted, BlockDelta, InputAccepted, InterruptedReason, MessageInterrupted, MessageSeal,
};
use wind_wire::Body;

pub fn format_time(ms: i64) -> String {
    DateTime::from_timestamp_millis(ms)
        .map(|time| time.with_timezone(&Local).format("%H:%M:%S").to_string())
        .unwrap_or_else(|| "--:--:--".to_owned())
}

pub fn concise(text: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let cleaned: String = text
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect();
    let trimmed = cleaned.trim();
    if trimmed.width() <= width {
        return trimmed.to_owned();
    }
    let mut out = String::new();
    let mut used = 0;
    for ch in trimmed.chars() {
        let ch_width = ch.width().unwrap_or(0);
        if used + ch_width > width - 1 {
            break;
        }
        used += ch_width;
        out.push(ch);
    }
    out.push('…');
    out
}

pub fn summarize(name: &str, payload: &Value) -> String {
    match name {
        "core/block.delta" => match decode::<BlockDelta>(payload) {
            Some(event) => format!(
                "msg {} seq {} rev {} {} {}B",
                event.message_id,
                event.delta.seq,
                event.delta.revision,
                event.delta.tag(),
                body_bytes(&event.delta.body)
            ),
            None => dash(),
        },
        "core/block.committed" => match decode::<BlockCommitted>(payload) {
            Some(event) => {
                let state = if event.block.is_some() {
                    "full"
                } else {
                    "partial"
                };
                format!(
                    "msg {} seq {} block {} {}",
                    event.message_id, event.seq, event.block_id, state
                )
            }
            None => dash(),
        },
        "core/message.seal" => match decode::<MessageSeal>(payload) {
            Some(event) => format!(
                "msg {} {} in {} out {}",
                event.message_id,
                event.finish_reason,
                event.usage.input_tokens,
                event.usage.output_tokens
            ),
            None => dash(),
        },
        "core/message.interrupted" => match decode::<MessageInterrupted>(payload) {
            Some(event) => format!(
                "msg {} {}",
                event.message_id,
                interrupted_reason(event.reason)
            ),
            None => dash(),
        },
        "core/agent.spawn" => match decode::<AgentSpawn>(payload) {
            Some(event) => {
                let parent = event
                    .parent_id
                    .map_or_else(|| "-".to_owned(), |parent| parent.to_string());
                format!("agent {} parent {parent}", event.instance_id)
            }
            None => dash(),
        },
        "core/agent.status" => match decode::<AgentStatus>(payload) {
            Some(event) => format!("agent {} {}", event.instance_id, event.phase),
            None => dash(),
        },
        "core/agent.sealed" => match decode::<AgentSealed>(payload) {
            Some(event) => format!("agent {} sealed", event.instance_id),
            None => dash(),
        },
        "core/input.accepted" => match decode::<InputAccepted>(payload) {
            Some(event) => format!(
                "topic {} agent {} pending {}",
                event.topic_id, event.instance_id, event.pending_id
            ),
            None => dash(),
        },
        "core/mod.installed" | "core/mod.updated" | "core/mod.deleted" => mod_summary(payload),
        _ => dash(),
    }
}

fn decode<T: DeserializeOwned>(payload: &Value) -> Option<T> {
    T::deserialize(payload).ok()
}

fn body_bytes(body: &Body) -> usize {
    match body {
        Body::Reasoning { content, .. }
        | Body::Text { content }
        | Body::Image { content }
        | Body::File { content }
        | Body::Audio { content, .. } => content.len(),
        Body::ToolCall { call_arguments, .. } => call_arguments.len(),
        Body::ToolResult { call_res, .. } => {
            serde_json::to_vec(call_res).map_or(0, |bytes| bytes.len())
        }
    }
}

fn interrupted_reason(reason: InterruptedReason) -> &'static str {
    match reason {
        InterruptedReason::Failed => "failed",
        InterruptedReason::Cancelled => "cancelled",
        InterruptedReason::Aborted => "aborted",
    }
}

fn mod_summary(payload: &Value) -> String {
    let name = payload.get("name").and_then(Value::as_str);
    let version = payload.get("version").and_then(Value::as_u64);
    match (name, version) {
        (Some(name), Some(version)) => format!("{name} v{version}"),
        _ => dash(),
    }
}

fn dash() -> String {
    "-".to_owned()
}
