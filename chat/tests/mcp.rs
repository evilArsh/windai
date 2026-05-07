use futures::StreamExt;
use serde_json::json;
use std::{env, str::FromStr};
use tokio::pin;
use windai_chat::{
    AdaptorType, Content, ContentType, Context, Message, Model, ReqConfig, ResEventStatus, Role,
    ToolCallSchema, handle_chat,
};

#[tokio::test]
async fn test_handle_chat_mcp() {
    unsafe {
        env::set_var("RUST_LOG", "debug");
    }

    let stream = env::var("STREAM")
        .map(|s| s.parse::<bool>().unwrap())
        .unwrap_or(true);
    let api_url = env::var("API_BASE_URL").unwrap_or(String::from("https://api.deepseek.com"));
    let api_key = env::var("API_KEY").unwrap_or(String::new());
    let adaptor = env::var("ADAPTOR")
        .map(|a| AdaptorType::from_str(&a).unwrap())
        .unwrap_or(AdaptorType::OpenAICompletion);
    let model = env::var("MODEL").unwrap_or(String::from("deepseek-v4-flash"));

    let _ = env_logger::builder().is_test(true).try_init();

    let model = Model {
        name: model,
        adaptor,
        endpoint: None,
    };

    let tools = vec![
        ToolCallSchema {
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
        ToolCallSchema {
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

    let mut chat_config = ReqConfig {
        temperature: None,
        top_p: None,
        max_tokens: None,
        stream: Some(stream),
        presence_penalty: None,
        frequency_penalty: None,
        parallel_tool_calls: None,
        reasoning: Some(false),
        tools: None,
    };

    let mut contexts = vec![
        Context::new_normal(
            Role::System,
            vec![Content::new(
                ContentType::Text,
                String::from("you are a helpful assistant"),
            )],
            None,
        ),
        Context::new_normal(
            Role::User,
            vec![Content::new(
                ContentType::Text,
                String::from("what's the weather in Shanghai? and what's the date now in Beijing?"),
            )],
            None,
        ),
    ];

    chat_config.tools = Some(tools);

    let res = handle_chat(
        contexts.clone(),
        chat_config.clone(),
        model.clone(),
        &api_url,
        &api_key,
    );
    let mut msg = Message::default_assistant();
    pin!(res);
    while let Some(event) = res.next().await {
        // println!("[data]\n{:?}", &event);
        if event.status == ResEventStatus::Error {
            log::error!("[error]\n{:?}", &event.error);
            return;
        }
        if event.status == ResEventStatus::Finish
            && let Some(data) = event.data
        {
            log::info!("[success]\n{:?}", &data);
            assert!(&data.tool_calls.is_some());
            assert_eq!(data.tool_calls.as_ref().unwrap().len(), 2);
            msg = data;
        }
    }

    let tools = msg.tool_calls.unwrap();
    contexts.push(Context::new_tool_request(
        tools.clone(),
        msg.reasoning_content,
    ));

    // mcp 调用
    tools.into_iter().for_each(|tc| {
        let res = match tc.name.as_ref() {
            "get_local_weather" => "{area: '上海',weather: '晴天 24℃'}".to_string(),
            "get_local_date" | _ => "{area: '北京',date: '2026/05/06 14:50'}".to_string(),
        };
        contexts.push(Context::new_tool_result(tc.call_id, res));
    });

    let res = handle_chat(contexts.clone(), chat_config, model, &api_url, &api_key);
    pin!(res);
    while let Some(event) = res.next().await {
        // println!("[data]\n{:?}", &event);
        if event.status == ResEventStatus::Error {
            log::error!("[mcp error]\n{:?}", &event.error);
            return;
        }
        if event.status == ResEventStatus::Finish
            && let Some(data) = event.data
        {
            log::info!("[mcp success]\n{:?}", &data);
        }
    }
}
