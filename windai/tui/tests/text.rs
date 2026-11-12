use serde_json::json;
use wind_tui::text::{concise, format_time, summarize};

#[test]
fn concise_replaces_control_characters_with_spaces() {
    assert_eq!(concise("alpha\nbeta\ttail", 40), "alpha beta tail");
    assert_eq!(concise("alpha\r\nbeta", 40), "alpha  beta");
}

#[test]
fn concise_trims_surrounding_whitespace() {
    assert_eq!(concise("  padded  ", 40), "padded");
    assert_eq!(concise("\n\tpadded\r\n", 40), "padded");
}

#[test]
fn concise_truncates_wide_text_with_ellipsis() {
    assert_eq!(concise("abcdefghij", 5), "abcd…");
    assert_eq!(concise("abcdefghij", 1), "…");
}

#[test]
fn concise_keeps_text_that_fits() {
    assert_eq!(concise("abcde", 5), "abcde");
    assert_eq!(concise("", 5), "");
}

#[test]
fn concise_returns_empty_for_zero_width() {
    assert_eq!(concise("abc", 0), "");
    assert_eq!(concise("", 0), "");
}

#[test]
fn concise_counts_cjk_by_display_width() {
    assert_eq!(concise("你好世界", 5), "你好…");
    assert_eq!(concise("你好世界", 4), "你…");
}

#[test]
fn format_time_renders_hour_minute_second() {
    let text = format_time(1_700_000_000_000);
    assert_eq!(text.len(), 8, "unexpected time length: {text}");
    for (index, byte) in text.bytes().enumerate() {
        match index {
            2 | 5 => assert_eq!(byte, b':', "missing separator in {text}"),
            _ => assert!(byte.is_ascii_digit(), "unexpected digit in {text}"),
        }
    }
}

#[test]
fn summarize_block_delta_picks_fields() {
    let payload = json!({
        "message_id": 92,
        "role": "assistant",
        "seq": 1,
        "revision": 7,
        "kind": "text",
        "content": "hello"
    });
    assert_eq!(
        summarize("core/block.delta", &payload),
        "msg 92 seq 1 rev 7 text 5B"
    );
}

#[test]
fn summarize_block_delta_counts_tool_arguments_and_results() {
    let call = json!({
        "message_id": 4,
        "role": "assistant",
        "seq": 2,
        "revision": 3,
        "kind": "tool_call",
        "call_id": "call-1",
        "call_name": "read",
        "call_arguments": "{\"path\":\"a\"}"
    });
    assert_eq!(
        summarize("core/block.delta", &call),
        "msg 4 seq 2 rev 3 tool_call 12B"
    );
    let result = json!({
        "message_id": 4,
        "role": "tool",
        "seq": 3,
        "revision": 1,
        "kind": "tool_result",
        "call_id": "call-1",
        "call_res": {"ok": true}
    });
    assert_eq!(
        summarize("core/block.delta", &result),
        "msg 4 seq 3 rev 1 tool_result 11B"
    );
}

#[test]
fn summarize_block_committed_marks_full_and_partial() {
    let full = json!({
        "message_id": 5,
        "seq": 2,
        "block_id": 17,
        "block": {
            "role": "assistant",
            "seq": 2,
            "revision": 1,
            "kind": "text",
            "content": "x"
        }
    });
    assert_eq!(
        summarize("core/block.committed", &full),
        "msg 5 seq 2 block 17 full"
    );
    let partial = json!({
        "message_id": 5,
        "seq": 2,
        "block_id": 17,
        "block": null
    });
    assert_eq!(
        summarize("core/block.committed", &partial),
        "msg 5 seq 2 block 17 partial"
    );
}

#[test]
fn summarize_message_seal_picks_usage() {
    let payload = json!({
        "message_id": 7,
        "finish_reason": "stop",
        "usage": {"input_tokens": 11, "output_tokens": 3}
    });
    assert_eq!(
        summarize("core/message.seal", &payload),
        "msg 7 stop in 11 out 3"
    );
}

#[test]
fn summarize_message_interrupted_picks_reason() {
    let payload = json!({"message_id": 8, "reason": "cancelled"});
    assert_eq!(
        summarize("core/message.interrupted", &payload),
        "msg 8 cancelled"
    );
}

#[test]
fn summarize_agent_events_pick_fields() {
    let spawn = json!({"instance_id": 3, "parent_id": 1, "detached": false});
    assert_eq!(summarize("core/agent.spawn", &spawn), "agent 3 parent 1");
    let root = json!({"instance_id": 4, "detached": true});
    assert_eq!(summarize("core/agent.spawn", &root), "agent 4 parent -");
    let status = json!({"instance_id": 3, "phase": "running"});
    assert_eq!(summarize("core/agent.status", &status), "agent 3 running");
    let sealed = json!({"instance_id": 3});
    assert_eq!(summarize("core/agent.sealed", &sealed), "agent 3 sealed");
}

#[test]
fn summarize_input_accepted_picks_fields() {
    let payload = json!({"topic_id": 1, "instance_id": 2, "pending_id": 3});
    assert_eq!(
        summarize("core/input.accepted", &payload),
        "topic 1 agent 2 pending 3"
    );
}

#[test]
fn summarize_mod_events_use_name_and_version() {
    let installed = json!({"name": "weather", "version": 2, "manifest": {}});
    assert_eq!(summarize("core/mod.installed", &installed), "weather v2");
    let deleted = json!({"name": "weather", "version": 2});
    assert_eq!(summarize("core/mod.deleted", &deleted), "weather v2");
    let updated = json!({"name": "weather", "from_version": 1, "to_version": 2});
    assert_eq!(summarize("core/mod.updated", &updated), "weather v1 -> v2");
}

#[test]
fn summarize_returns_dash_for_unknown_events_and_bad_payloads() {
    assert_eq!(summarize("core/unknown", &json!({"value": 1})), "-");
    assert_eq!(summarize("core/block.delta", &json!(null)), "-");
    assert_eq!(
        summarize("core/block.delta", &json!({"message_id": 1})),
        "-"
    );
    assert_eq!(
        summarize("core/message.seal", &json!({"message_id": 1})),
        "-"
    );
    assert_eq!(
        summarize("core/mod.installed", &json!({"name": "weather"})),
        "-"
    );
    assert_eq!(summarize("core/mod.installed", &json!(null)), "-");
    assert_eq!(
        summarize(
            "core/mod.updated",
            &json!({"name": "weather", "from_version": 1})
        ),
        "-"
    );
}
