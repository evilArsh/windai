use futures::StreamExt;
use wind_ai::{
    chat,
    message::{Content, Message, ReqConfig, Role},
    model::{AdaptorType, Model},
    provider::adaptor::{self, get_chat_adaptor},
};
#[path = "./common/lib.rs"]
mod common;

#[tokio::test]
async fn test_handle_chat() {
    let env = common::load_env();

    let chat_config = ReqConfig {
        temperature: None,
        top_p: None,
        max_tokens: None,
        stream: Some(env.test_stream),
        presence_penalty: None,
        frequency_penalty: None,
        parallel_tool_calls: None,
        reasoning: Some(true),
    };
    let model = Model {
        name: env.test_model,
        adaptor: env.test_adaptor,
        endpoint: env.test_endpoint,
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
    let chat_adaptor = adaptor::get_chat_adaptor(model.adaptor);
    let req_body =
        chat::build_request(chat_adaptor.as_ref(), &model, &chat_config, &contexts, None).unwrap();

    let res = chat::handle_chat(
        chat_adaptor.as_ref(),
        &req_body,
        &env.test_base_url,
        &env.test_key,
        None,
    );
    let mut res = Box::pin(res);
    while let Some(value) = res.next().await {
        if value.error.is_some() {
            panic!("{}", value.error.unwrap());
        }
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
