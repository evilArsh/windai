use futures::StreamExt;
use wind_ai::{
    JsonObject, chat,
    message::{Content, Message, Role},
    model::Model,
    provider::adapter,
};
use wind_core::models::{ModelConfig, ReasonEffort};

#[path = "./common/lib.rs"]
mod common;

/// 由 `ModelConfig` 生成请求配置，与 `chat::runner::to_ai_model` 保持一致
fn model_config(stream: bool) -> JsonObject {
    ModelConfig {
        stream: Some(stream),
        reasoning: Some(ReasonEffort::Medium),
    }
    .to_json_obj()
    .expect("model config to json")
}

#[tokio::test]
#[ignore = "need to complete .env config file"]
async fn test_handle_chat() {
    let env = common::load_env();

    let model = Model {
        name: env.test_model,
        adapter: env.test_adapter,
        endpoint: env.test_endpoint,
        config: Some(model_config(env.test_stream)),
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
    let chat_adapter = adapter::get_chat_adapter(model.adapter);
    let req_body = chat::build_request(chat_adapter.as_ref(), &model, &contexts, None)
        .expect("build chat request");

    let res = chat::handle_chat(
        chat_adapter.as_ref(),
        &req_body,
        &env.test_base_url,
        &env.test_key,
        model.endpoint.as_deref(),
    );
    let mut res = Box::pin(res);
    while let Some(value) = res.next().await {
        log::info!("[res] {:?}", value);
        if let Some(err) = value.error {
            panic!("{}", err);
        }
    }
}
