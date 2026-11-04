//! 适配器的种类解析与请求构造

use wind_ai::adapter::adapter_of;
use wind_ai::model::ChatModel;
use wind_ai::schema::openai_completion::ChatCompletion;
use wind_ai::schema::openai_responses::Response;
use wind_wire::AdapterType;
use wind_wire::message::{Content, Message, Role};
use wind_wire::tool::{FunctionCall, FunctionTool, Tools};

#[test]
fn an_adapter_type_parses_from_its_serialized_literal() {
    assert_eq!(
        AdapterType::parse("openai_completion").expect("parse"),
        AdapterType::OpenAICompletion
    );
    assert_eq!(
        AdapterType::parse("openai_response").expect("parse"),
        AdapterType::OpenAIResponse
    );
}

#[test]
fn an_unknown_adapter_name_is_refused_rather_than_defaulted() {
    let error = AdapterType::parse("openai-chat").expect_err("reject the unknown name");
    assert!(error.to_string().contains("openai-chat"), "{error}");
}

#[test]
fn each_kind_resolves_to_its_own_path() {
    assert_eq!(
        adapter_of(AdapterType::OpenAICompletion).path(),
        "/chat/completions"
    );
    assert_eq!(adapter_of(AdapterType::OpenAIResponse).path(), "/responses");
}

#[test]
fn the_provider_schemas_round_trip_representative_bodies() {
    // `openai_completion` 与 `openai_responses` 是纯数据描述，验收是一段真实响应体能解析
    let completion: ChatCompletion = serde_json::from_str(
        r#"{"id":"x","object":"chat.completion","created":1,"model":"m",
            "choices":[{"index":0,"message":{"role":"assistant","content":"hi"},
                        "finish_reason":"stop"}]}"#,
    )
    .expect("parse the chat completions body");
    assert_eq!(completion.choices.len(), 1);

    let response: Response = serde_json::from_str(
        r#"{"id":"x","object":"response","created_at":1,"model":"m","output":[],
            "parallel_tool_calls":false,"temperature":0.0,"top_p":0.0,
            "status":"completed"}"#,
    )
    .expect("parse the responses body");
    assert_eq!(response.output.len(), 0);
}

/// 构造一个走 chat completions 的模型描述
fn model() -> ChatModel {
    ChatModel {
        name: "deepseek-chat".to_string(),
        adapter: AdapterType::OpenAICompletion,
        endpoint: None,
        config: Some(serde_json::json!({ "stream": true })),
    }
}

/// 构造一个带请求配置的模型描述
fn model_with_config() -> ChatModel {
    ChatModel {
        config: Some(serde_json::json!({
            "stream": true,
            "reasoning": "high",
            "n": 3,
            "temperature": 0.3,
            "thinking": { "type": "enabled" }
        })),
        ..model()
    }
}

#[test]
fn the_request_carries_the_model_and_its_config() {
    let adapter = adapter_of(AdapterType::OpenAICompletion);
    let body = adapter
        .build_request(
            &model(),
            &[Message {
                role: Role::User,
                content: vec![Content::new_text("hello".into())],
                ..Message::default()
            }],
            None,
        )
        .expect("construct");
    assert_eq!(body["model"], serde_json::json!("deepseek-chat"));
    assert_eq!(body["stream"], serde_json::json!(true));
    assert_eq!(body["messages"][0]["role"], serde_json::json!("user"));
    assert_eq!(body["messages"][0]["content"], serde_json::json!("hello"));
}

#[test]
fn a_tool_call_becomes_a_tool_calls_array() {
    let adapter = adapter_of(AdapterType::OpenAICompletion);
    let body = adapter
        .build_request(
            &model(),
            &[Message {
                role: Role::Assistant,
                tool_calls: Some(vec![FunctionCall {
                    item_id: None,
                    call_id: "call_1".into(),
                    name: "get_weather".into(),
                    arguments: "{\"city\":\"SF\"}".into(),
                }]),
                ..Message::default()
            }],
            None,
        )
        .expect("construct");
    assert_eq!(
        body["messages"][0]["tool_calls"][0]["id"],
        serde_json::json!("call_1")
    );
    assert_eq!(
        body["messages"][0]["tool_calls"][0]["function"]["name"],
        serde_json::json!("get_weather")
    );
    assert_eq!(
        body["messages"][0]["tool_calls"][0]["function"]["arguments"],
        serde_json::json!("{\"city\":\"SF\"}"),
        "the raw arguments are passed through; the adapter does not parse them"
    );
}

#[test]
fn a_tool_result_becomes_a_tool_role_message() {
    let adapter = adapter_of(AdapterType::OpenAICompletion);
    let body = adapter
        .build_request(
            &model(),
            &[Message {
                role: Role::Tool,
                content: vec![Content::new_function_call(
                    "call_1".into(),
                    serde_json::json!("22 degrees"),
                )],
                ..Message::default()
            }],
            None,
        )
        .expect("construct");
    assert_eq!(body["messages"][0]["role"], serde_json::json!("tool"));
    assert_eq!(
        body["messages"][0]["tool_call_id"],
        serde_json::json!("call_1")
    );
}

#[test]
fn the_declared_tools_reach_the_request() {
    let adapter = adapter_of(AdapterType::OpenAICompletion);
    let tools = vec![Tools::Function(FunctionTool {
        name: "get_weather".into(),
        description: Some("look up the weather".into()),
        parameters: Some(serde_json::json!({ "type": "object" })),
        strict: Some(true),
    })];
    let body = adapter
        .build_request(&model(), &[], Some(&tools))
        .expect("construct");
    assert_eq!(body["tools"][0]["type"], serde_json::json!("function"));
    assert_eq!(
        body["tools"][0]["function"]["name"],
        serde_json::json!("get_weather")
    );
}

#[test]
fn a_declared_model_without_tools_omits_the_field() {
    let adapter = adapter_of(AdapterType::OpenAICompletion);
    let body = adapter
        .build_request(&model(), &[], None)
        .expect("construct");
    assert!(
        body.get("tools").is_none(),
        "a tool-less model must not send an empty array: some providers treat it as \"tool calls forbidden\""
    );
}

#[test]
fn the_declared_config_is_merged_into_the_request() {
    let adapter = adapter_of(AdapterType::OpenAICompletion);
    let body = adapter
        .build_request(&model_with_config(), &[], None)
        .expect("construct");
    assert_eq!(
        body["temperature"],
        serde_json::json!(0.3),
        "known fields are overridden by the config"
    );
    assert_eq!(
        body["n"],
        serde_json::json!(3),
        "explicitly set fields are overridden all the same"
    );
    assert_eq!(
        body["thinking"],
        serde_json::json!({ "type": "enabled" }),
        "unknown fields are merged in too: this is the opening for intermediary vendors' extensions"
    );
    assert_eq!(body["reasoning_effort"], serde_json::json!("high"));
    assert!(
        body.get("reasoning").is_none(),
        "stream and reasoning are handled explicitly and are not merged again"
    );
}

#[test]
fn a_whole_response_becomes_one_message_with_its_usage() {
    let adapter = adapter_of(AdapterType::OpenAICompletion);
    let message = adapter
        .parse_response(
            r#"{"id":"x","object":"chat.completion","created":7,"model":"m",
                 "choices":[{"index":0,
                             "message":{"role":"assistant","content":"hello"},
                             "finish_reason":"stop"}],
                 "usage":{"prompt_tokens":120,"completion_tokens":45,"total_tokens":165}}"#
                .as_bytes(),
        )
        .expect("parse");
    assert_eq!(message.role, Role::Assistant);
    assert_eq!(message.content.len(), 1);
    assert_eq!(message.input_tokens, 120);
    assert_eq!(message.output_tokens, 45);
    assert_eq!(message.created_at, 7);
}

#[test]
fn a_stream_chunk_becomes_an_incremental_message() {
    let adapter = adapter_of(AdapterType::OpenAICompletion);
    let event = wind_ai::eventsource::Event {
        event: String::new(),
        data: r#"{"id":"x","object":"chat.completion.chunk","created":7,"model":"m",
                  "choices":[{"index":0,"delta":{"content":"world"},"finish_reason":null}]}"#
            .to_string(),
        id: String::new(),
        retry: None,
    };
    let chunk = adapter
        .parse_stream_chunk(&event)
        .expect("parse")
        .expect("unwrap the chunk");
    assert_eq!(chunk.content.len(), 1);
}

#[test]
fn a_streamed_tool_call_splits_into_two_chunks_that_the_block_layer_joins() {
    // 适配器只解出每一片，不负责拼接
    let adapter = adapter_of(AdapterType::OpenAICompletion);
    let first = wind_ai::eventsource::Event {
        data: r#"{"id":"x","object":"chat.completion.chunk","created":7,"model":"m",
                  "choices":[{"index":0,"delta":{"tool_calls":[
                      {"index":0,"id":"call_1","type":"function",
                       "function":{"name":"get_weather","arguments":""}}]},
                  "finish_reason":null}]}"#
            .to_string(),
        ..Default::default()
    };
    let second = wind_ai::eventsource::Event {
        data: r#"{"id":"x","object":"chat.completion.chunk","created":7,"model":"m",
                  "choices":[{"index":0,"delta":{"tool_calls":[
                      {"index":0,"function":{"name":"","arguments":"{\"city\":\"SF\"}"}}]},
                  "finish_reason":null}]}"#
            .to_string(),
        ..Default::default()
    };
    let a = adapter
        .parse_stream_chunk(&first)
        .expect("parse")
        .expect("unwrap the chunk");
    let b = adapter
        .parse_stream_chunk(&second)
        .expect("parse")
        .expect("unwrap the chunk");
    // 两片各自带上自己的一段参数，都没有丢失
    assert!(a.tool_calls.is_some());
    assert!(b.tool_calls.is_some());
}

#[test]
fn the_usage_only_chunk_has_no_choices_and_still_parses() {
    // 流末尾的一条事件只带 `usage`，属于常态
    let adapter = adapter_of(AdapterType::OpenAICompletion);
    let event = wind_ai::eventsource::Event {
        data: r#"{"id":"x","object":"chat.completion.chunk","created":7,"model":"m",
                  "choices":[],"usage":{"prompt_tokens":146,"completion_tokens":81,"total_tokens":227}}"#
            .to_string(),
        ..Default::default()
    };
    let chunk = adapter.parse_stream_chunk(&event).expect("parse");
    assert!(
        chunk.is_some(),
        "usage must be handed over; it is the payload of core/message.seal"
    );
    let chunk = chunk.expect("unwrap the chunk");
    assert_eq!(chunk.input_tokens, 146);
    assert_eq!(chunk.output_tokens, 81);
}

/// 构造一个走 responses 接口的模型描述
fn responses_model() -> ChatModel {
    ChatModel {
        name: "gpt-5.4".to_string(),
        adapter: AdapterType::OpenAIResponse,
        endpoint: None,
        config: None,
    }
}

/// 构造一个走 responses 接口、带请求配置的模型描述
fn responses_model_with_config() -> ChatModel {
    ChatModel {
        config: Some(serde_json::json!({
            "stream": true,
            "reasoning": "high",
            "model": "gpt-5.4-mini",
            "temperature": 0.3,
            "thinking": { "type": "enabled" }
        })),
        ..responses_model()
    }
}

#[test]
fn the_responses_request_carries_an_input_array() {
    let adapter = adapter_of(AdapterType::OpenAIResponse);
    let body = adapter
        .build_request(
            &responses_model(),
            &[Message {
                role: Role::User,
                content: vec![Content::new_text("hello".into())],
                ..Message::default()
            }],
            None,
        )
        .expect("construct");
    assert_eq!(body["model"], serde_json::json!("gpt-5.4"));
    assert!(
        body.get("input").is_some(),
        "the responses API uses the field input, not messages"
    );
}

#[test]
fn the_responses_config_is_merged_into_the_request() {
    let adapter = adapter_of(AdapterType::OpenAIResponse);
    let body = adapter
        .build_request(&responses_model_with_config(), &[], None)
        .expect("construct");
    assert_eq!(
        body["temperature"],
        serde_json::json!(0.3),
        "known fields are overridden by the config"
    );
    assert_eq!(
        body["model"],
        serde_json::json!("gpt-5.4-mini"),
        "explicitly set fields are overridden all the same"
    );
    assert_eq!(
        body["thinking"],
        serde_json::json!({ "type": "enabled" }),
        "unknown fields are merged in too: this is the opening for intermediary vendors' extensions"
    );
    assert_eq!(
        body["reasoning"],
        serde_json::json!({ "effort": "high" }),
        "reasoning is handled explicitly and not merged again: without the skip this would turn from an object into a bare string"
    );
    assert!(
        body.get("input").is_some(),
        "merging must not corrupt the request body itself"
    );
}

#[test]
fn a_whole_responses_body_becomes_one_message() {
    let adapter = adapter_of(AdapterType::OpenAIResponse);
    let message = adapter
        .parse_response(
            r#"{"id":"x","object":"response","created_at":7,"model":"m","status":"completed",
                 "output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed",
                            "content":[{"type":"output_text","text":"hello","annotations":[]}]}],
                 "parallel_tool_calls":false,"temperature":0.0,"top_p":0.0,
                 "usage":{"input_tokens":120,"output_tokens":45,"total_tokens":165}}"#
                .as_bytes(),
        )
        .expect("parse");
    assert_eq!(message.role, Role::Assistant);
    assert_eq!(
        serde_json::to_value(&message.content).expect("serialize"),
        serde_json::json!([{ "type": "text", "data": "hello" }]),
        "text in the response body must land on the unified content shape"
    );
    assert_eq!(message.input_tokens, 120);
    assert_eq!(message.output_tokens, 45);
}

#[test]
fn a_responses_output_item_added_chunk_carries_a_tool_call() {
    let adapter = adapter_of(AdapterType::OpenAIResponse);
    let event = wind_ai::eventsource::Event {
        event: "response.output_item.added".to_string(),
        data: r#"{"type":"response.output_item.added","sequence_number":1,"output_index":0,
                  "item":{"id":"fc_1","type":"function_call","status":"in_progress",
                          "arguments":"","call_id":"call_1","name":"get_weather"}}"#
            .to_string(),
        ..Default::default()
    };
    let chunk = adapter
        .parse_stream_chunk(&event)
        .expect("parse")
        .expect("unwrap the chunk");
    let calls = chunk.tool_calls.expect("carry the tool call");
    assert_eq!(calls[0].name, "get_weather");
    assert_eq!(calls[0].call_id, "call_1");
}

#[test]
fn a_responses_argument_delta_chunk_carries_the_fragment() {
    let adapter = adapter_of(AdapterType::OpenAIResponse);
    let event = wind_ai::eventsource::Event {
        event: "response.function_call_arguments.delta".to_string(),
        data: r#"{"type":"response.function_call_arguments.delta","sequence_number":1,
                  "delta":"{\"area\":\"Shanghai\"}","item_id":"fc_1","output_index":0}"#
            .to_string(),
        ..Default::default()
    };
    let chunk = adapter.parse_stream_chunk(&event).expect("parse");
    let chunk = chunk.expect("carry the argument fragment");
    let calls = chunk.tool_calls.expect("attach the fragment to the call");
    assert_eq!(
        calls[0].arguments, "{\"area\":\"Shanghai\"}",
        "the raw arguments must be handed over; concatenation is the block layer's job"
    );
}

#[test]
fn a_responses_argument_delta_keeps_the_item_id_opened_before_it() {
    let adapter = adapter_of(AdapterType::OpenAIResponse);
    let added = wind_ai::eventsource::Event {
        event: "response.output_item.added".to_string(),
        data: r#"{"type":"response.output_item.added","sequence_number":1,"output_index":0,
                  "item":{"id":"fc_1","type":"function_call","status":"in_progress",
                          "arguments":"","call_id":"call_1","name":"get_weather"}}"#
            .to_string(),
        ..Default::default()
    };
    let opened = adapter
        .parse_stream_chunk(&added)
        .expect("parse the added item")
        .expect("carry the opened call");
    let opened = opened.tool_calls.expect("carry the opened call");
    assert_eq!(
        opened[0].item_id.as_deref(),
        Some("fc_1"),
        "the added item must hand over the item id the deltas refer to"
    );

    let delta = wind_ai::eventsource::Event {
        event: "response.function_call_arguments.delta".to_string(),
        data: r#"{"type":"response.function_call_arguments.delta","sequence_number":2,
                  "delta":"{\"area\":\"Shanghai\"}","item_id":"fc_1","output_index":0}"#
            .to_string(),
        ..Default::default()
    };
    let chunk = adapter
        .parse_stream_chunk(&delta)
        .expect("parse the delta")
        .expect("carry the fragment");
    let calls = chunk.tool_calls.expect("attach the fragment to the call");
    assert_eq!(
        calls[0].item_id.as_deref(),
        Some("fc_1"),
        "the delta must keep the item id that identifies the call"
    );
    assert_eq!(
        calls[0].call_id, "",
        "the delta carries no call_id; the block layer pairs by item id"
    );
    assert_eq!(calls[0].arguments, "{\"area\":\"Shanghai\"}");
}

#[test]
fn a_completion_audio_delta_keeps_the_audio_item_id() {
    let adapter = adapter_of(AdapterType::OpenAICompletion);
    let event = wind_ai::eventsource::Event {
        event: "chat.completion.chunk".to_string(),
        data: r#"{"id":"c","object":"chat.completion.chunk","created":1,"model":"m",
                  "choices":[{"index":0,"delta":{"audio":{
                      "id":"audio_1","data":"QUJD","expires_at":99}}}]}"#
            .to_string(),
        ..Default::default()
    };
    let chunk = adapter
        .parse_stream_chunk(&event)
        .expect("parse the audio chunk")
        .expect("carry the audio chunk");
    let audio = chunk
        .content
        .iter()
        .find_map(|content| match content {
            Content::Audio { data } => Some(data),
            _ => None,
        })
        .expect("carry the audio");
    assert_eq!(
        audio.item_id.as_deref(),
        Some("audio_1"),
        "the audio id is the identity the block layer pairs by"
    );
    assert_eq!(audio.content, "QUJD");
}

#[test]
fn a_completion_audio_transcript_becomes_text_when_nothing_else_does() {
    let adapter = adapter_of(AdapterType::OpenAICompletion);
    let event = wind_ai::eventsource::Event {
        event: "chat.completion.chunk".to_string(),
        data: r#"{"id":"c","object":"chat.completion.chunk","created":1,"model":"m",
                  "choices":[{"index":0,"delta":{"audio":{"transcript":"hello"}}}]}"#
            .to_string(),
        ..Default::default()
    };
    let chunk = adapter
        .parse_stream_chunk(&event)
        .expect("parse the transcript chunk")
        .expect("carry the transcript chunk");
    assert!(
        !chunk
            .content
            .iter()
            .any(|content| matches!(content, Content::Audio { .. })),
        "a chunk without audio bytes must not open an audio block"
    );
    assert_eq!(
        chunk.content,
        vec![Content::new_text("hello".to_string())],
        "the transcript is the only text the chunk carries"
    );
}

#[test]
fn a_completion_argument_delta_maps_the_index_to_the_item_id() {
    let adapter = adapter_of(AdapterType::OpenAICompletion);
    let event = wind_ai::eventsource::Event {
        event: "chat.completion.chunk".to_string(),
        data: r#"{"id":"c","object":"chat.completion.chunk","created":1,"model":"m",
                  "choices":[{"index":0,"delta":{"tool_calls":[
                      {"index":0,"function":{"arguments":"{\"city\""}}]}}]}"#
            .to_string(),
        ..Default::default()
    };
    let chunk = adapter
        .parse_stream_chunk(&event)
        .expect("parse the delta")
        .expect("carry the fragment");
    let calls = chunk.tool_calls.expect("attach the fragment to the call");
    assert_eq!(
        calls[0].item_id.as_deref(),
        Some("0"),
        "the completion index is the identity the block layer pairs by"
    );
}

#[test]
fn a_completed_chunk_carries_the_usage() {
    let adapter = adapter_of(AdapterType::OpenAIResponse);
    let event = wind_ai::eventsource::Event {
        event: "response.completed".to_string(),
        data: r#"{"type":"response.completed","sequence_number":1,
                  "response":{"id":"r","object":"response",
                  "created_at":1,"model":"m","status":"completed","output":[],
                  "parallel_tool_calls":false,"temperature":0.0,"top_p":0.0,
                  "usage":{"input_tokens":146,"output_tokens":51,"total_tokens":197}}}"#
            .to_string(),
        ..Default::default()
    };
    let chunk = adapter.parse_stream_chunk(&event).expect("parse");
    let chunk = chunk.expect("carry usage on completion");
    assert_eq!(chunk.output_tokens, 51);
}

#[test]
fn an_event_type_the_adapter_does_not_know_yields_nothing() {
    // responses 接口的事件类型很多，不认识的事件必须跳过，不能让整条流失败
    let adapter = adapter_of(AdapterType::OpenAIResponse);
    let event = wind_ai::eventsource::Event {
        event: "response.some_future_thing".to_string(),
        data: r#"{"type":"response.some_future_thing","sequence_number":1}"#.to_string(),
        ..Default::default()
    };
    assert!(
        adapter
            .parse_stream_chunk(&event)
            .expect("parse without error")
            .is_none()
    );
}
