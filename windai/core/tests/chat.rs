use futures::StreamExt;
use std::{env, str::FromStr};
use tokio::pin;
use windai_core::{
    message::{AdaptorType, Content, Message, Model, ReqConfig, Role},
    provider::{adaptor::get_chat_adaptor, handle_chat},
};

#[tokio::test]
async fn test_handle_chat() {
    // https://api.openai.com/v1"
    let _ = env_logger::builder().is_test(true).try_init();

    let stream = env::var("STREAM")
        .map(|s| s.parse::<bool>().unwrap())
        .unwrap_or(true);
    let api_url = env::var("API_BASE_URL").unwrap_or(String::from("https://api.deepseek.com"));
    let api_key = env::var("API_KEY").unwrap_or(String::new());
    let model = env::var("MODEL").unwrap_or(String::from("deepseek-v4-flash"));
    let adaptor = env::var("ADAPTOR")
        .map(|a| AdaptorType::from_str(&a).unwrap())
        .unwrap_or(AdaptorType::OpenAICompletion);

    let chat_config = ReqConfig {
        temperature: None,
        top_p: None,
        max_tokens: None,
        stream: Some(stream),
        presence_penalty: None,
        frequency_penalty: None,
        parallel_tool_calls: None,
        reasoning: Some(true),
    };
    let model = Model {
        name: model,
        adaptor,
        endpoint: None,
    };
    let contexts = vec![
        Message::new_simple(
            Role::System,
            vec![Content::new_text(String::from(
                "you are a helpful assistant, response in Chinese",
            ))],
            None,
        ),
        Message::new_simple(
            Role::User,
            vec![Content::new_text(String::from("who are you"))],
            None,
        ),
    ];
    let res = handle_chat(&contexts, &chat_config, &model, &api_url, &api_key, None);
    pin!(res);
    while let Some(value) = res.next().await {
        println!("[data]\n{}", value);
    }
}

#[test]
fn test_build_request() {
    let _ = env_logger::builder().is_test(true).try_init();

    let model = Model {
        name: String::from("deepseek-v4-flash"),
        adaptor: AdaptorType::OpenAICompletion,
        endpoint: None,
    };

    let chat_config = ReqConfig {
        temperature: None,
        top_p: None,
        max_tokens: None,
        stream: Some(false),
        presence_penalty: None,
        frequency_penalty: None,
        parallel_tool_calls: None,
        reasoning: Some(true),
    };

    let contexts = vec![
        Message::new_simple(
            Role::System,
            vec![Content::new_text(String::from(
                "you are a helpful assistant and MUST response in Chinese",
            ))],
            None,
        ),
        Message::new_simple(
            Role::User,
            vec![Content::new_text(String::from("who are you"))],
            None,
        ),
    ];

    let chat_adaptor = get_chat_adaptor(AdaptorType::OpenAICompletion);
    let stream = chat_config.stream;
    let req_body = chat_adaptor
        .build_request(&model.name, &chat_config, &contexts, None)
        .unwrap();

    let obj = req_body.as_object().unwrap();
    log::info!(
        "[body]\n{}",
        serde_json::to_string_pretty(&req_body).unwrap()
    );
    assert!(obj.contains_key("stream"));
    assert!(obj.contains_key("reasoning_effort"));

    assert!(obj.get("stream").unwrap() == stream.unwrap());
}
