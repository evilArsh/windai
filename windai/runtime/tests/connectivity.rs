//! 真实对话连通性检查
//!
//! 只发一次模型请求，把配置、请求体、每个分片和最终消息打印出来。不建数据库、
//! 不装配运行时、不加载任何资产。配置取自仓库根的 `.env`：
//!
//! ```text
//! TEST_STREAM  TEST_BASE_URL  TEST_KEY  TEST_MODEL  TEST_ADAPTER  TEST_ENDPOINT
//! ```
//!
//! 运行：
//!
//! ```text
//! cargo test -p wind-runtime --test connectivity -- --ignored --nocapture
//! ```

#[path = "support/mod.rs"]
mod support;

use futures::StreamExt;
use support::Env;
use wind_ai::adapter::{ChatAdapter, adapter_of};
use wind_ai::chat::{Chunk, Request, once, stream};
use wind_ai::model::{ChatModel, Target};
use wind_wire::message::{Content, Message, Role};

/// 只用于连通性检查：发一次真实请求并打印全过程
#[tokio::test]
#[ignore = "need config .env"]
async fn one_real_chat_round_trip() {
    let env = Env::load();

    let adapter = adapter_of(env.adapter);
    let request = Request {
        model: ChatModel {
            name: env.model.clone(),
            adapter: env.adapter,
            endpoint: env.endpoint.clone(),
            config: Some(serde_json::json!({ "stream": env.stream })),
        },
        target: Target {
            base_url: env.base_url.clone(),
            api_key: Some(env.key.clone()),
        },
        contexts: vec![
            Message {
                role: Role::System,
                content: vec![Content::new_text(
                    "you are a helpful assistant, response in Chinese".into(),
                )],
                ..Message::default()
            },
            Message {
                role: Role::User,
                content: vec![Content::new_text("who are you".into())],
                ..Message::default()
            },
        ],
        tools: None,
    };

    print_setup(&env, adapter.as_ref(), &request);

    if env.stream {
        stream_once(adapter.as_ref(), &request).await;
    } else {
        once_and_print(adapter.as_ref(), &request).await;
    }
}

fn print_setup(env: &Env, adapter: &dyn ChatAdapter, request: &Request) {
    env.print();
    println!("[env] adapter path: {}", adapter.path());

    match adapter.build_request(&request.model, &request.contexts, request.tools.as_deref()) {
        Ok(body) => println!(
            "[request] {}",
            serde_json::to_string_pretty(&body).expect("serialize the request body")
        ),
        Err(error) => panic!("cannot build the request body: {error}"),
    }
}

/// 非流式的一次往返
async fn once_and_print(adapter: &dyn ChatAdapter, request: &Request) {
    let message = once(adapter, request)
        .await
        .unwrap_or_else(|error| panic!("the request failed: {error}"));
    println!("[response] {message:#?}");
    println!("[text] {}", text_of(&message));
    println!(
        "[usage] input={} output={}",
        message.input_tokens, message.output_tokens
    );
}

/// 流式的一次往返：逐片打印，最后拼出全文
async fn stream_once(adapter: &dyn ChatAdapter, request: &Request) {
    let mut chunks = Box::pin(
        stream(adapter, request)
            .await
            .unwrap_or_else(|error| panic!("the request failed: {error}")),
    );
    let mut text = String::new();
    let mut reasoning = String::new();
    let mut deltas = 0usize;
    let mut usage = (0, 0);

    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.unwrap_or_else(|error| panic!("the stream failed: {error}"));
        match chunk {
            Chunk::Delta(message) => {
                deltas += 1;
                println!("[chunk {deltas}] {message:?}");
                text.push_str(&text_of(&message));
                if let Some(fragment) = &message.reasoning_content {
                    reasoning.push_str(fragment);
                }
                if message.input_tokens != 0 || message.output_tokens != 0 {
                    usage = (message.input_tokens, message.output_tokens);
                }
            }
            Chunk::Done => {
                println!("[done] after {deltas} delta chunks");
                break;
            }
        }
    }

    println!("[text] {text}");
    if !reasoning.is_empty() {
        println!("[reasoning] {reasoning}");
    }
    println!("[usage] input={} output={}", usage.0, usage.1);
}

/// 取出消息里的全部文本片段
fn text_of(message: &Message) -> String {
    message
        .content
        .iter()
        .filter_map(|content| match content {
            Content::Text { data } => Some(data.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}
