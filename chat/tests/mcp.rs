use futures::StreamExt;
use serde_json::json;
use std::env;
use tokio::pin;
use windai_chat::{
    AdaptorType, Content, ContentType, Context, Model, ReqConfig, ResEventStatus, Role,
    ToolCallParam, handle_chat,
};

fn build_default() -> (ReqConfig, Model, Vec<Content>, Vec<Context>) {
    let _ = env_logger::builder().is_test(true).try_init();

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
    let user_input = vec![Content::new(
        ContentType::Text,
        String::from("what's the weather in Shanghai? and what's the date now in Beijing?"),
    )];

    let contexts = vec![Context::new_simple(
        Role::System,
        vec![Content::new(
            ContentType::Text,
            String::from("you are a helpful assistant"),
        )],
        None,
    )];

    (chat_config, model, user_input, contexts)
}

#[tokio::test]
async fn test_handle_chat_mcp() {
    unsafe {
        env::set_var("RUST_LOG", "debug");
    }
    let tools = vec![
        ToolCallParam {
            name: "get_local_weather".to_string(),
            description: Some("根据输入的地区查询当地的天气情况".to_string()),
            parameters: Some(json!({
                "type": "object",
                "properties": {
                    "area": {
                        "type": "string",
                        "description": "要查询的指定地区的拼音简写. 比如: 北京 -> Beijing"
                    }
                },
                "required": ["area"]
            })),
            strict: Some(true),
        },
        ToolCallParam {
            name: "get_local_date".to_string(),
            description: Some("根据输入的地区查询该地区当前的时间".to_string()),
            parameters: Some(json!({
                "type": "object",
                "properties": {
                    "area": {
                        "type": "string",
                        "description": "要查询的指定地区的拼音简写. 比如: 北京 -> Beijing"
                    }
                },
                "required": ["area"]
            })),
            strict: Some(true),
        },
    ];

    let url = String::from("https://api.deepseek.com");
    let (mut chat_config, model, user_input, contexts) = build_default();
    chat_config.tools = Some(tools);

    let api_key = env::var("API_KEY").unwrap_or(String::new());
    let res = handle_chat(Some(user_input), contexts, chat_config, model, url, api_key);
    pin!(res);
    while let Some(event) = res.next().await {
        println!("[data]\n{:?}", &event);
        if event.status == ResEventStatus::Error {
            println!("[error]\n{:?}", &event.error);
        }
        if event.status == ResEventStatus::Finish
            && let Some(data) = event.data
        {
            println!("[success]\n{:?}", &data);
            assert!(data.tool_calls.is_some());
            let calls = data.tool_calls.unwrap();
            assert_eq!(calls.len(), 2);
        }
    }
}
