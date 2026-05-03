use futures::StreamExt;
use std::env;
use tokio::pin;
use windai_chat::{
    AdaptorType, Content, ContentType, Context, Model, ReqConfig, Role, adaptor::get_chat_adaptor,
    handle_chat,
};

#[tokio::test]
async fn test_handle_chat() {
    let url = String::from("https://api.deepseek.com");
    let chat_config = ReqConfig {
        temperature: None,
        top_p: None,
        max_tokens: None,
        stream: Some(true),
        presence_penalty: None,
        frequency_penalty: None,
        parallel_tool_calls: None,
        reasoning: Some(true),
        tools: None,
    };
    let model = Model {
        name: String::from("deepseek-v4-flash"),
        adaptor: AdaptorType::OpenAICompletion,
        endpoint: None,
    };
    let user_input = vec![Content::new(ContentType::Text, String::from("who are you"))];

    let contexts = vec![Context::new_simple(
        Role::System,
        vec![Content {
            content: String::from("you are a helpful assistant, response in Chinese"),
            content_type: ContentType::Text,
        }],
        None,
    )];
    let api_key = env::var("API_KEY").unwrap_or(String::new());
    let res = handle_chat(Some(user_input), contexts, chat_config, model, url, api_key);
    pin!(res);
    while let Some(value) = res.next().await {
        println!("[data]\n{:?}", value);
    }
}

#[tokio::test]
async fn test_handle_chat_response() {
    // https://api.openai.com/v1"
    let _ = env_logger::builder().is_test(true).try_init();

    let api_url = env::var("API_BASE_URL").unwrap_or(String::new());
    let api_key = env::var("API_KEY").unwrap_or(String::new());
    let model = env::var("MODEL").unwrap_or(String::new());

    let chat_config = ReqConfig {
        temperature: None,
        top_p: None,
        max_tokens: None,
        stream: Some(true),
        presence_penalty: None,
        frequency_penalty: None,
        parallel_tool_calls: None,
        reasoning: Some(true),
        tools: None,
    };
    let model = Model {
        name: model,
        adaptor: AdaptorType::OpenAIResponse,
        endpoint: None,
    };
    let user_input = vec![Content::new(ContentType::Text, String::from("who are you"))];
    let contexts = vec![Context::new_simple(
        Role::System,
        vec![Content {
            content: String::from("you are a helpful assistant, response in Chinese"),
            content_type: ContentType::Text,
        }],
        None,
    )];
    let res = handle_chat(
        Some(user_input),
        contexts,
        chat_config,
        model,
        api_url,
        api_key,
    );
    pin!(res);
    while let Some(value) = res.next().await {
        println!("[data]\n{}", value);
    }
}

#[test]
fn test_build_request() {
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
        reasoning: Some(false),
        tools: None,
    };

    let contexts = vec![
        Context::new_simple(
            Role::System,
            vec![Content {
                content: String::from("you are a helpful assistant and MUST response in Chinese"),
                content_type: ContentType::Text,
            }],
            None,
        ),
        Context::new_simple(
            Role::User,
            vec![Content {
                content: String::from("who are you"),
                content_type: ContentType::Text,
            }],
            None,
        ),
    ];

    let chat_adaptor = get_chat_adaptor(AdaptorType::OpenAICompletion);
    let stream = chat_config.stream;
    let req_body = chat_adaptor
        .build_request(&model.name, chat_config, contexts)
        .unwrap();

    let obj = req_body.as_object().unwrap();
    assert!(obj.contains_key("stream"));
    assert!(obj.contains_key("reasoning_effort"));

    assert!(obj.get("stream").unwrap() == stream.unwrap());
}
