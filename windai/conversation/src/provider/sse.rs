//! HTTP SSE（Server-Sent Events）数据解析器
//!
//! 参考：https://developer.mozilla.org/zh-CN/docs/Web/API/Server-sent_events/Using_server-sent_events
//! 参考：https://html.spec.whatwg.org/multipage/server-sent-events.html#dispatchMessage

use std::fmt;

/// 解析后的单个 SSE 事件
///
/// 五个字段对应 SSE 协议的五个字段前缀，并非所有字段都会同时出现。
#[derive(Debug, Clone, Default)]
pub struct SseBlock {
    /// data: 消息数据，多行连续data: 会以\n拼接
    pub data: Option<String>,
    /// event: 事件类型
    pub event: Option<String>,
    /// id: 事件唯一标识
    pub id: Option<String>,
    /// retry: 重连间隔（毫秒）
    pub retry: Option<u64>,
    /// comment: 以 : 开头的注释行
    pub comment: Option<String>,
}

impl fmt::Display for SseBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref event) = self.event {
            write!(f, "SseBlock(event={}", event)?;
            if let Some(ref id) = self.id {
                write!(f, ", id={}", id)?;
            }
            writeln!(f, ")")?;
        } else {
            writeln!(f, "SseBlock {{")?;
        }
        if let Some(ref data) = self.data {
            writeln!(f, "  data: {}", data)?;
        }
        if let Some(ref retry) = self.retry {
            writeln!(f, "  retry: {}ms", retry)?;
        }
        if let Some(ref comment) = self.comment {
            writeln!(f, "  comment: {}", comment)?;
        }
        write!(f, "}}")
    }
}
fn debug_sse(s: &SseBlock) {
    log::debug!("[SseBlock]\n{}", &s);
}
impl SseBlock {
    /// 解析原始 SSE bytes，返回所有解析的结果
    /// - 输入可能包含多个拼接的事件，以空行分隔。
    pub fn parse(input: &[u8]) -> Vec<Self> {
        let text = String::from_utf8_lossy(&input);
        let mut events = Vec::new();
        let mut current = SseBlock::default();
        for line in text.lines() {
            if line.is_empty() {
                // 空行表示当前事件结束
                if current.has_data() || current.has_event() {
                    debug_sse(&current);
                    events.push(std::mem::take(&mut current));
                }
                continue;
            }
            // 以 : 开头的行 为注释
            if line.starts_with(':') {
                current.append_comment(line);
                continue;
            }
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            let value = value.trim();
            match key.trim() {
                "data" => current.append_data(value),
                "event" => current.event = Some(value.to_string()),
                "id" => current.id = Some(value.to_string()),
                "retry" => {
                    if let Ok(ms) = value.parse::<u64>() {
                        current.retry = Some(ms);
                    }
                }
                _ => {}
            }
        }
        if current.has_data() || current.has_event() {
            debug_sse(&current);
            events.push(current);
        }
        events
    }

    fn has_data(&self) -> bool {
        self.data.is_some()
    }
    fn has_event(&self) -> bool {
        self.event.is_some()
    }
    /// 追加数据,如果已经有数据，会先追加一个\n，最终的字符串末尾不会添加\n
    pub fn append_data(&mut self, value: &str) {
        if value == "[DONE]" {
            return;
        }

        let buf = self.data.get_or_insert_with(String::new);
        if !buf.is_empty() {
            buf.push('\n');
        }
        buf.push_str(value);
    }

    pub fn append_comment(&mut self, line: &str) {
        let buf = self.comment.get_or_insert_with(String::new);
        if !buf.is_empty() {
            buf.push('\n');
        }
        buf.push_str(line);
    }
}

#[cfg(test)]
mod tests {
    use crate::message::Message;
    use crate::model::AdaptorType;
    use crate::provider::adaptor;

    use super::*;
    use bytes::Bytes;
    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    // ── basic single event ──────────────────────────────────────────

    #[test]
    fn single_data_only() {
        let events = SseBlock::parse(&b("data: hello\n\n"));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.as_deref(), Some("hello"));
        assert!(events[0].event.is_none());
        assert!(events[0].id.is_none());
        assert!(events[0].retry.is_none());
        assert!(events[0].comment.is_none());
    }

    #[test]
    fn all_fields_present() {
        let raw = "event: update\nid: 42\nretry: 5000\ndata: payload\n\n";
        let events = SseBlock::parse(&b(raw));
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.event.as_deref(), Some("update"));
        assert_eq!(e.id.as_deref(), Some("42"));
        assert_eq!(e.retry, Some(5000));
        assert_eq!(e.data.as_deref(), Some("payload"));
    }

    #[test]
    fn parse_all_empty() {
        let events = SseBlock::parse(&b(""));
        assert!(events.is_empty());
    }

    // ── multiple events ─────────────────────────────────────────────

    #[test]
    fn multiple_events() {
        let raw = "data: first\n\nevent: ping\n\ndata: second\nid: 3\n\n";
        let events = SseBlock::parse(&b(raw));
        assert_eq!(events.len(), 3);

        assert_eq!(events[0].data.as_deref(), Some("first"));
        assert!(events[0].event.is_none());

        assert_eq!(events[1].event.as_deref(), Some("ping"));
        assert!(events[1].data.is_none());

        assert_eq!(events[2].data.as_deref(), Some("second"));
        assert_eq!(events[2].id.as_deref(), Some("3"));
    }

    #[test]
    fn no_trailing_blank_line() {
        // input without trailing blank line should still flush last event
        let events = SseBlock::parse(&b("data: trailing"));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.as_deref(), Some("trailing"));
    }

    // ── multi-line data ─────────────────────────────────────────────

    #[test]
    fn multi_line_data() {
        let raw = "data: line1\ndata: line2\ndata: line3\n\n";
        let events = SseBlock::parse(&b(raw));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.as_deref(), Some("line1\nline2\nline3"));
    }

    #[test]
    fn multi_line_data_single_value() {
        // single data: line should NOT contain a trailing newline
        let events = SseBlock::parse(&b("data: only\n\n"));
        assert_eq!(events[0].data.as_deref(), Some("only"));
    }

    // ── comment lines ───────────────────────────────────────────────

    #[test]
    fn comment_only_event_ignored() {
        // a block with only comments should NOT be emitted
        let events = SseBlock::parse(&b(": just a comment\n\n"));
        assert!(events.is_empty());
    }

    #[test]
    fn comment_before_data() {
        let raw = ": comment\ndata: real\n\n";
        let events = SseBlock::parse(&b(raw));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.as_deref(), Some("real"));
        assert_eq!(events[0].comment.as_deref(), Some(": comment"));
    }

    #[test]
    fn multiple_comments() {
        let raw = ": first\n: second\ndata: ok\n\n";
        let events = SseBlock::parse(&b(raw));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].comment.as_deref(), Some(": first\n: second"));
    }

    // ── edge cases ──────────────────────────────────────────────────

    #[test]
    fn value_containing_colon() {
        // field value that itself contains a colon
        let events = SseBlock::parse(&b("data: http://example.com:8080\n\n"));
        assert_eq!(events[0].data.as_deref(), Some("http://example.com:8080"));
    }

    #[test]
    fn empty_data_value() {
        // "data: " with empty value should still produce an empty string
        let events = SseBlock::parse(&b("data: \n\n"));
        assert_eq!(events[0].data.as_deref(), Some(""));
    }

    #[test]
    fn unknown_field_ignored() {
        let events = SseBlock::parse(&b("foo: bar\ndata: real\n\n"));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.as_deref(), Some("real"));
    }

    #[test]
    fn line_without_colon_ignored() {
        let events = SseBlock::parse(&b("no colon here\ndata: ok\n\n"));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.as_deref(), Some("ok"));
    }

    #[test]
    fn retry_invalid_values() {
        let raw = "retry: abc\ndata: x\n\n";
        let events = SseBlock::parse(&b(raw));
        assert_eq!(events[0].retry, None);
        assert_eq!(events[0].data.as_deref(), Some("x"));
    }

    #[test]
    fn retry_valid() {
        // retry needs to accompany data to be collected as an event
        let events = SseBlock::parse(&b("retry: 3000\ndata: ok\n\n"));
        assert_eq!(events[0].retry, Some(3000));
    }

    #[test]
    fn only_retry_event() {
        // retry alone is not collected because has_data only checks data field
        let events = SseBlock::parse(&b("retry: 1000\n\n"));
        assert!(events.is_empty());
    }

    #[test]
    fn crlf_line_endings() {
        let events = SseBlock::parse(&b("data: hello\r\n\r\ndata: world\r\n\r\n"));
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data.as_deref(), Some("hello"));
        assert_eq!(events[1].data.as_deref(), Some("world"));
    }

    #[test]
    fn consecutive_blank_lines_no_extra_events() {
        let raw = "data: one\n\n\n\ndata: two\n\n";
        let events = SseBlock::parse(&b(raw));
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data.as_deref(), Some("one"));
        assert_eq!(events[1].data.as_deref(), Some("two"));
    }

    #[test]
    fn id_and_event_without_data() {
        // events that only have id/event should still be collected
        let events = SseBlock::parse(&b("id: 99\nevent: ping\n\n"));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id.as_deref(), Some("99"));
        assert_eq!(events[0].event.as_deref(), Some("ping"));
        assert!(events[0].data.is_none());
    }

    #[test]
    fn openai_chat_sse() {
        let raw = b"data: {\"id\":\"a208d9cd-2681-407a-8e60-29aa45179b71\",\"object\":\"chat.completion.chunk\",\"created\":1775619894,\"model\":\"deepseek-chat\",\"system_fingerprint\":\"fp_eaab8d114b_prod0820_fp8_kvcache_new_kvcache\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"logprobs\":null,\"finish_reason\":null}]}\n\ndata: {\"id\":\"a208d9cd-2681-407a-8e60-29aa45179b71\",\"object\":\"chat.completion.chunk\",\"created\":1775619894,\"model\":\"deepseek-chat\",\"system_fingerprint\":\"fp_eaab8d114b_prod0820_fp8_kvcache_new_kvcache\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\xe6\x88\x91\xe6\x98\xaf\"},\"logprobs\":null,\"finish_reason\":null}]}\n\n";
        let block = SseBlock::parse(raw);
        assert_eq!(block.len(), 2);
        block.iter().for_each(|b| {
            println!("====================");
            dbg!(&b.data);
        });
    }

    #[test]
    fn function_call_sse() {
        let _ = env_logger::builder().is_test(true).try_init();

        let raw = r#"data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}

    data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_ZogyTlZEZW1UhS1KNyH7b1O2","type":"function","function":{"name":"get_local_weather","arguments":""}}]},"finish_reason":null}]}

    data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"","arguments":"{\"area\":\"Shanghai\"}"}}]},"finish_reason":null}]}

    data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"call_MEdPCSVxuSuiCiUW04tsaxuH","type":"function","function":{"name":"get_local_date","arguments":""}}]},"finish_reason":null}]}

    data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"name":"","arguments":"{\"area\":\"Beijing\"}"}}]},"finish_reason":null}]}

    data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[{"index":0,"delta":{"content":""},"finish_reason":"tool_calls"}]}

    data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[],"usage":{"prompt_tokens":146,"completion_tokens":81,"total_tokens":227}}

    data: [DONE]"#;
        let data = bytes::Bytes::from(raw);
        let chat_adaptor = adaptor::get_chat_adaptor(AdaptorType::OpenAICompletion);
        let mut msg = Message::default();
        let chunks = chat_adaptor.parse_stream_chunk(&data).unwrap();
        for chunk in chunks {
            msg.append_chunk(chunk);
        }
        println!("{}", msg);
        assert!(msg.tool_calls.is_some());
        assert!(msg.tool_calls.unwrap().len() == 2);
    }

    #[test]
    fn function_call_response_sse() {
        let _ = env_logger::builder().is_test(true).try_init();
        let raw = r#"event: response.output_item.added
    data: {"type":"response.output_item.added","item":{"id":"fc_03c724975b1dc3ab0169f9f80264288191a6f887f3d05b81ef","type":"function_call","status":"in_progress","arguments":"","call_id":"call_pYUWuWYa6bvE25kogvRkLNrf","name":"get_local_weather"},"output_index":0,"sequence_number":2}

    event: response.function_call_arguments.delta
    data: {"type":"response.function_call_arguments.delta","delta":"{\"area\":\"Shanghai\"}","item_id":"fc_03c724975b1dc3ab0169f9f80264288191a6f887f3d05b81ef","obfuscation":"6Vq5uirp4nzIG","output_index":0,"sequence_number":3}

    event: response.function_call_arguments.done
    data: {"type":"response.function_call_arguments.done","arguments":"{\"area\":\"Shanghai\"}","item_id":"fc_03c724975b1dc3ab0169f9f80264288191a6f887f3d05b81ef","output_index":0,"sequence_number":4}

    event: response.output_item.done
    data: {"type":"response.output_item.done","item":{"id":"fc_03c724975b1dc3ab0169f9f80264288191a6f887f3d05b81ef","type":"function_call","status":"completed","arguments":"{\"area\":\"Shanghai\"}","call_id":"call_pYUWuWYa6bvE25kogvRkLNrf","name":"get_local_weather"},"output_index":0,"sequence_number":5}

    event: response.output_item.added
    data: {"type":"response.output_item.added","item":{"id":"fc_03c724975b1dc3ab0169f9f802643c819197aab6a2258c1fd0","type":"function_call","status":"in_progress","arguments":"","call_id":"call_dCn8RbrFpaX0ng3zhIt9IL66","name":"get_local_date"},"output_index":1,"sequence_number":6}

    event: response.function_call_arguments.delta
    data: {"type":"response.function_call_arguments.delta","delta":"{\"area\":\"Beijing\"}","item_id":"fc_03c724975b1dc3ab0169f9f802643c819197aab6a2258c1fd0","obfuscation":"o3W3uJnrGpccwW","output_index":1,"sequence_number":7}

    event: response.function_call_arguments.done
    data: {"type":"response.function_call_arguments.done","arguments":"{\"area\":\"Beijing\"}","item_id":"fc_03c724975b1dc3ab0169f9f802643c819197aab6a2258c1fd0","output_index":1,"sequence_number":8}

    event: response.output_item.done
    data: {"type":"response.output_item.done","item":{"id":"fc_03c724975b1dc3ab0169f9f802643c819197aab6a2258c1fd0","type":"function_call","status":"completed","arguments":"{\"area\":\"Beijing\"}","call_id":"call_dCn8RbrFpaX0ng3zhIt9IL66","name":"get_local_date"},"output_index":1,"sequence_number":9}

    event: response.completed
    data: {"type":"response.completed","response":{"id":"resp_03c724975b1dc3ab0169f9f800e9ac819196212334c1889489","object":"response","created_at":1777989632,"status":"completed","background":false,"completed_at":1777989634,"error":null,"frequency_penalty":0.0,"incomplete_details":null,"instructions":"You are a helpful coding assistant.","max_output_tokens":null,"max_tool_calls":null,"model":"gpt-5.4","moderation":null,"output":[],"parallel_tool_calls":true,"presence_penalty":0.0,"previous_response_id":null,"prompt_cache_key":"5f05c2a2-92b7-4aaf-b5c6-e9e9bf5d721e","prompt_cache_retention":"24h","reasoning":{"effort":"none","summary":null},"safety_identifier":"user-xF6Vy8Q10Oaiori4gfaCK4fK","service_tier":"default","store":false,"temperature":1.0,"text":{"format":{"type":"text"},"verbosity":"medium"},"tool_choice":"auto","tool_usage":{"image_gen":{"input_tokens":0,"input_tokens_details":{"image_tokens":0,"text_tokens":0},"output_tokens":0,"output_tokens_details":{"image_tokens":0,"text_tokens":0},"total_tokens":0},"web_search":{"num_requests":0}},"tools":[{"type":"function","description":"根据输入的地区查询当地的天气情况","name":"get_local_weather","parameters":{"properties":{"area":{"description":"要查询的指定地区的拼音简写. 比如: 北京 -> Beijing","type":"string"}},"required":["area"],"type":"object","additionalProperties":false},"strict":true},{"type":"function","description":"根据输入的地区查询该地区当前的时间","name":"get_local_date","parameters":{"properties":{"area":{"description":"要查询的指定地区的拼音简写. 比如: 北京 -> Beijing","type":"string"}},"required":["area"],"type":"object","additionalProperties":false},"strict":true}],"top_logprobs":0,"top_p":0.98,"truncation":"disabled","usage":{"input_tokens":146,"input_tokens_details":{"cached_tokens":0},"output_tokens":51,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":197},"user":null,"metadata":{}},"sequence_number":10}"#;

        let data = bytes::Bytes::from(raw);
        let chat_adaptor = adaptor::get_chat_adaptor(AdaptorType::OpenAIResponse);
        let mut msg = Message::default();
        let chunks = chat_adaptor.parse_stream_chunk(&data).unwrap();
        for chunk in chunks {
            msg.append_chunk(chunk);
        }
        println!("{}", msg);
        assert!(msg.tool_calls.is_some());
        assert!(msg.tool_calls.unwrap().len() == 2);
    }
}
