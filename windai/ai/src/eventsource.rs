//! A basic building block for building an Eventsource from a Stream of bytes array like objects. To
//! learn more about Server Sent Events (SSE) take a look at [the MDN
//! docs](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
//!
//! # Example
//!
//! ```ignore
//! let mut stream = reqwest::Client::new()
//!     .get("http://localhost:7020/notifications")
//!     .send()
//!     .await?
//!     .bytes_stream()
//!     .eventsource();
//!
//!
//! while let Some(event) = stream.next().await {
//!     match event {
//!         Ok(event) => println!(
//!             "received event[type={}]: {}",
//!             event.event,
//!             event.data
//!         ),
//!         Err(e) => eprintln!("error occured: {}", e),
//!     }
//! }
//! ```

mod event;
mod event_stream;
mod parser;
mod traits;
mod utf8_stream;

pub use event::Event;
// pub use event_stream::{EventStream, EventStreamError};
pub use traits::Eventsource;

#[cfg(test)]
mod tests {
    use crate::{
        eventsource::event_stream::EventStream, message::Message, model::AdapterType,
        provider::adapter,
    };
    use futures::prelude::*;

    #[tokio::test]
    async fn openai_chat_sse() {
        let res = EventStream::new(futures::stream::iter(vec![Ok::<_, ()>(
            b"data: {\"id\":\"a208d9cd-2681-407a-8e60-29aa45179b71\",\"object\":\"chat.completion.chunk\",\"created\":1775619894,\"model\":\"deepseek-chat\",\"system_fingerprint\":\"fp_eaab8d114b_prod0820_fp8_kvcache_new_kvcache\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"logprobs\":null,\"finish_reason\":null}]}\n\ndata: {\"id\":\"a208d9cd-2681-407a-8e60-29aa45179b71\",\"object\":\"chat.completion.chunk\",\"created\":1775619894,\"model\":\"deepseek-chat\",\"system_fingerprint\":\"fp_eaab8d114b_prod0820_fp8_kvcache_new_kvcache\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\xe6\x88\x91\xe6\x98\xaf\"},\"logprobs\":null,\"finish_reason\":null}]}\n\n",
        )]))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert_eq!(res.len(), 2);
        let chat_adapter = adapter::get_chat_adapter(AdapterType::OpenAICompletion);
        let mut msg = Message::default();

        for ev in &res {
            if let Some(chunk) = chat_adapter.parse_stream_chunk(ev).unwrap() {
                msg.append_chunk(chunk);
            }
        }

        assert_eq!(msg.content.len(), 1);
    }

    #[tokio::test]
    async fn function_call_sse() {
        let res = EventStream::new(futures::stream::iter(vec![Ok::<_, ()>(r#"data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}

data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_ZogyTlZEZW1UhS1KNyH7b1O2","type":"function","function":{"name":"get_local_weather","arguments":""}}]},"finish_reason":null}]}

data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"","arguments":"{\"area\":\"Shanghai\"}"}}]},"finish_reason":null}]}

data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"call_MEdPCSVxuSuiCiUW04tsaxuH","type":"function","function":{"name":"get_local_date","arguments":""}}]},"finish_reason":null}]}

data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"name":"","arguments":"{\"area\":\"Beijing\"}"}}]},"finish_reason":null}]}

data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[{"index":0,"delta":{"content":""},"finish_reason":"tool_calls"}]}

data: {"id":"resp_01457e2c8e82466e0169f8b485868c8191b1f44690071f996c","object":"chat.completion.chunk","created":1777906821,"model":"gpt-5.2","choices":[],"usage":{"prompt_tokens":146,"completion_tokens":81,"total_tokens":227}}

data: [DONE]"#
            )]))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        let chat_adapter = adapter::get_chat_adapter(AdapterType::OpenAICompletion);
        let mut msg = Message::default();

        for ev in &res {
            if let Some(chunk) = chat_adapter.parse_stream_chunk(ev).unwrap() {
                msg.append_chunk(chunk);
            }
        }

        assert!(msg.tool_calls.is_some());
        assert!(msg.tool_calls.unwrap().len() == 2);
    }

    #[tokio::test]
    async fn function_call_response_sse() {
        let res = EventStream::new(futures::stream::iter(vec![Ok::<_, ()>(r#"event: response.output_item.added
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
data: {"type":"response.completed","response":{"id":"resp_03c724975b1dc3ab0169f9f800e9ac819196212334c1889489","object":"response","created_at":1777989632,"status":"completed","background":false,"completed_at":1777989634,"error":null,"frequency_penalty":0.0,"incomplete_details":null,"instructions":"You are a helpful coding assistant.","max_output_tokens":null,"max_tool_calls":null,"model":"gpt-5.4","moderation":null,"output":[],"parallel_tool_calls":true,"presence_penalty":0.0,"previous_response_id":null,"prompt_cache_key":"5f05c2a2-92b7-4aaf-b5c6-e9e9bf5d721e","prompt_cache_retention":"24h","reasoning":{"effort":"none","summary":null},"safety_identifier":"user-xF6Vy8Q10Oaiori4gfaCK4fK","service_tier":"default","store":false,"temperature":1.0,"text":{"format":{"type":"text"},"verbosity":"medium"},"tool_choice":"auto","tool_usage":{"image_gen":{"input_tokens":0,"input_tokens_details":{"image_tokens":0,"text_tokens":0},"output_tokens":0,"output_tokens_details":{"image_tokens":0,"text_tokens":0},"total_tokens":0},"web_search":{"num_requests":0}},"tools":[{"type":"function","description":"根据输入的地区查询当地的天气情况","name":"get_local_weather","parameters":{"properties":{"area":{"description":"要查询的指定地区的拼音简写. 比如: 北京 -> Beijing","type":"string"}},"required":["area"],"type":"object","additionalProperties":false},"strict":true},{"type":"function","description":"根据输入的地区查询该地区当前的时间","name":"get_local_date","parameters":{"properties":{"area":{"description":"要查询的指定地区的拼音简写. 比如: 北京 -> Beijing","type":"string"}},"required":["area"],"type":"object","additionalProperties":false},"strict":true}],"top_logprobs":0,"top_p":0.98,"truncation":"disabled","usage":{"input_tokens":146,"input_tokens_details":{"cached_tokens":0},"output_tokens":51,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":197},"user":null,"metadata":{}},"sequence_number":10}"#
            )]))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let chat_adapter = adapter::get_chat_adapter(AdapterType::OpenAIResponse);
        let mut msg = Message::default();
        for ev in &res {
            if let Some(chunk) = chat_adapter.parse_stream_chunk(ev).unwrap() {
                msg.append_chunk(chunk);
            }
        }
        assert!(msg.tool_calls.is_some());
        assert!(msg.tool_calls.unwrap().len() == 2);
    }
}
