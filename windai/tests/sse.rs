// use bytes::Bytes;
// use windai::adaptor::sse::SSEBlock;

// fn b(s: &str) -> Bytes {
//     Bytes::from(s.to_string())
// }

// // ── basic single event ──────────────────────────────────────────

// #[test]
// fn single_data_only() {
//     let events = SSEBlock::parse_all(b("data: hello\n\n"));
//     assert_eq!(events.len(), 1);
//     assert_eq!(events[0].data.as_deref(), Some("hello"));
//     assert!(events[0].event.is_none());
//     assert!(events[0].id.is_none());
//     assert!(events[0].retry.is_none());
//     assert!(events[0].comment.is_none());
// }

// #[test]
// fn all_fields_present() {
//     let raw = "event: update\nid: 42\nretry: 5000\ndata: payload\n\n";
//     let events = SSEBlock::parse_all(b(raw));
//     assert_eq!(events.len(), 1);
//     let e = &events[0];
//     assert_eq!(e.event.as_deref(), Some("update"));
//     assert_eq!(e.id.as_deref(), Some("42"));
//     assert_eq!(e.retry, Some(5000));
//     assert_eq!(e.data.as_deref(), Some("payload"));
// }

// #[test]
// fn parse_returns_first_only() {
//     let raw = "data: first\n\ndata: second\n\n";
//     let result = SSEBlock::parse(b(raw));
//     assert!(result.is_some());
//     assert_eq!(result.unwrap().data.as_deref(), Some("first"));
// }

// #[test]
// fn parse_empty_input() {
//     assert!(SSEBlock::parse(b("")).is_none());
//     assert!(SSEBlock::parse(b("\n\n\n")).is_none());
// }

// #[test]
// fn parse_all_empty() {
//     let events = SSEBlock::parse_all(b(""));
//     assert!(events.is_empty());
// }

// // ── multiple events ─────────────────────────────────────────────

// #[test]
// fn multiple_events() {
//     let raw = "data: first\n\nevent: ping\n\ndata: second\nid: 3\n\n";
//     let events = SSEBlock::parse_all(b(raw));
//     assert_eq!(events.len(), 3);

//     assert_eq!(events[0].data.as_deref(), Some("first"));
//     assert!(events[0].event.is_none());

//     assert_eq!(events[1].event.as_deref(), Some("ping"));
//     assert!(events[1].data.is_none());

//     assert_eq!(events[2].data.as_deref(), Some("second"));
//     assert_eq!(events[2].id.as_deref(), Some("3"));
// }

// #[test]
// fn no_trailing_blank_line() {
//     // input without trailing blank line should still flush last event
//     let events = SSEBlock::parse_all(b("data: trailing"));
//     assert_eq!(events.len(), 1);
//     assert_eq!(events[0].data.as_deref(), Some("trailing"));
// }

// // ── multi-line data ─────────────────────────────────────────────

// #[test]
// fn multi_line_data() {
//     let raw = "data: line1\ndata: line2\ndata: line3\n\n";
//     let events = SSEBlock::parse_all(b(raw));
//     assert_eq!(events.len(), 1);
//     assert_eq!(events[0].data.as_deref(), Some("line1\nline2\nline3"));
// }

// #[test]
// fn multi_line_data_single_value() {
//     // single data: line should NOT contain a trailing newline
//     let events = SSEBlock::parse_all(b("data: only\n\n"));
//     assert_eq!(events[0].data.as_deref(), Some("only"));
// }

// // ── comment lines ───────────────────────────────────────────────

// #[test]
// fn comment_only_event_ignored() {
//     // a block with only comments should NOT be emitted
//     let events = SSEBlock::parse_all(b(": just a comment\n\n"));
//     assert!(events.is_empty());
// }

// #[test]
// fn comment_before_data() {
//     let raw = ": comment\ndata: real\n\n";
//     let events = SSEBlock::parse_all(b(raw));
//     assert_eq!(events.len(), 1);
//     assert_eq!(events[0].data.as_deref(), Some("real"));
//     assert_eq!(events[0].comment.as_deref(), Some(": comment"));
// }

// #[test]
// fn multiple_comments() {
//     let raw = ": first\n: second\ndata: ok\n\n";
//     let events = SSEBlock::parse_all(b(raw));
//     assert_eq!(events.len(), 1);
//     assert_eq!(events[0].comment.as_deref(), Some(": first\n: second"));
// }

// // ── edge cases ──────────────────────────────────────────────────

// #[test]
// fn value_containing_colon() {
//     // field value that itself contains a colon
//     let events = SSEBlock::parse_all(b("data: http://example.com:8080\n\n"));
//     assert_eq!(events[0].data.as_deref(), Some("http://example.com:8080"));
// }

// #[test]
// fn empty_data_value() {
//     // "data: " with empty value should still produce an empty string
//     let events = SSEBlock::parse_all(b("data: \n\n"));
//     assert_eq!(events[0].data.as_deref(), Some(""));
// }

// #[test]
// fn unknown_field_ignored() {
//     let events = SSEBlock::parse_all(b("foo: bar\ndata: real\n\n"));
//     assert_eq!(events.len(), 1);
//     assert_eq!(events[0].data.as_deref(), Some("real"));
// }

// #[test]
// fn line_without_colon_ignored() {
//     let events = SSEBlock::parse_all(b("no colon here\ndata: ok\n\n"));
//     assert_eq!(events.len(), 1);
//     assert_eq!(events[0].data.as_deref(), Some("ok"));
// }

// #[test]
// fn retry_invalid_values() {
//     let raw = "retry: abc\ndata: x\n\n";
//     let events = SSEBlock::parse_all(b(raw));
//     assert_eq!(events[0].retry, None);
//     assert_eq!(events[0].data.as_deref(), Some("x"));
// }

// #[test]
// fn retry_valid() {
//     // retry needs to accompany data to be collected as an event
//     let events = SSEBlock::parse_all(b("retry: 3000\ndata: ok\n\n"));
//     assert_eq!(events[0].retry, Some(3000));
// }

// #[test]
// fn only_retry_event() {
//     // retry alone is not collected because has_data only checks data field
//     let events = SSEBlock::parse_all(b("retry: 1000\n\n"));
//     assert!(events.is_empty());
// }

// #[test]
// fn crlf_line_endings() {
//     let events = SSEBlock::parse_all(b("data: hello\r\n\r\ndata: world\r\n\r\n"));
//     assert_eq!(events.len(), 2);
//     assert_eq!(events[0].data.as_deref(), Some("hello"));
//     assert_eq!(events[1].data.as_deref(), Some("world"));
// }

// #[test]
// fn consecutive_blank_lines_no_extra_events() {
//     let raw = "data: one\n\n\n\ndata: two\n\n";
//     let events = SSEBlock::parse_all(b(raw));
//     assert_eq!(events.len(), 2);
//     assert_eq!(events[0].data.as_deref(), Some("one"));
//     assert_eq!(events[1].data.as_deref(), Some("two"));
// }

// #[test]
// fn id_and_event_without_data() {
//     // events that only have id/event should still be collected
//     let events = SSEBlock::parse_all(b("id: 99\nevent: ping\n\n"));
//     assert_eq!(events.len(), 1);
//     assert_eq!(events[0].id.as_deref(), Some("99"));
//     assert_eq!(events[0].event.as_deref(), Some("ping"));
//     assert!(events[0].data.is_none());
// }

// #[test]
// fn openai_chat_sse() {
//     let raw = b"data: {\"id\":\"a208d9cd-2681-407a-8e60-29aa45179b71\",\"object\":\"chat.completion.chunk\",\"created\":1775619894,\"model\":\"deepseek-chat\",\"system_fingerprint\":\"fp_eaab8d114b_prod0820_fp8_kvcache_new_kvcache\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"logprobs\":null,\"finish_reason\":null}]}\n\ndata: {\"id\":\"a208d9cd-2681-407a-8e60-29aa45179b71\",\"object\":\"chat.completion.chunk\",\"created\":1775619894,\"model\":\"deepseek-chat\",\"system_fingerprint\":\"fp_eaab8d114b_prod0820_fp8_kvcache_new_kvcache\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\xe6\x88\x91\xe6\x98\xaf\"},\"logprobs\":null,\"finish_reason\":null}]}\n\n";
//     let data = bytes::Bytes::from(raw.as_slice());
//     let block = SSEBlock::parse_all(data);
//     assert_eq!(block.len(), 2);
//     block.iter().for_each(|b|{
//         println!("====================");
//         dbg!(&b.data);
//     });
// }
