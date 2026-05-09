use futures::StreamExt;
use serde_json::json;
use std::{env, str::FromStr};
use tokio::pin;
use windai_core::{
    message::{
        AdaptorType, Content, Message, Model, ReqConfig, Role,
        tool::{FunctionCallOutput, FunctionTool, Tools},
    },
    provider::{ResEventStatus, handle_chat},
};

async fn handle_chat_mcp(
    stream: bool,
    api_url: String,
    api_key: String,
    model: String,
    adaptor: AdaptorType,
) {
    let _ = env_logger::builder().is_test(true).try_init();
    unsafe {
        env::set_var("RUST_LOG", "debug");
    }

    let model = Model {
        name: model,
        adaptor,
        endpoint: None,
    };

    let tools = vec![
        Tools::Function(FunctionTool {
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
        }),
        Tools::Function(FunctionTool {
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
        }),
    ];

    let chat_config = ReqConfig {
        temperature: None,
        top_p: None,
        max_tokens: None,
        stream: Some(stream),
        presence_penalty: None,
        frequency_penalty: None,
        parallel_tool_calls: None,
        reasoning: Some(false),
    };

    let mut contexts = vec![
        Message::new_simple(
            Role::System,
            vec![Content::new_text(String::from(
                "you are a helpful assistant",
            ))],
            None,
        ),
        Message::new_simple(
            Role::User,
            vec![Content::new_text(String::from(
                "what's the weather in Shanghai? and what's the date now in Beijing?",
            ))],
            None,
        ),
    ];

    let mut msg = Message::default();
    {
        let res = handle_chat(
            &contexts,
            &chat_config,
            &model,
            &api_url,
            &api_key,
            Some(&tools),
        );
        pin!(res);
        while let Some(event) = res.next().await {
            if event.status == ResEventStatus::Error {
                log::error!("[error]\n{:?}", &event.error);
                return;
            }
            if event.status == ResEventStatus::Finish
                && let Some(data) = event.data
            {
                log::info!("[success]\n{}", &data);
                assert!(&data.tool_calls.is_some());
                assert_eq!(data.tool_calls.as_ref().unwrap().len(), 2);
                msg = data;
            }
        }
    }
    let tool_calls = msg.tool_calls.unwrap();
    contexts.push(Message::new_tool_request(
        tool_calls.clone(),
        msg.reasoning_content,
    ));

    // 模拟 mcp 调用
    let tools_output = tool_calls
        .into_iter()
        .map(|tc| {
            let result = match tc.name.as_ref() {
                "get_local_weather" => "{area: '上海',weather: '晴天 24℃'}".to_string(),
                "get_local_date" | _ => "{area: '北京',date: '2026/05/06 14:50'}".to_string(),
            };

            FunctionCallOutput {
                id: tc.id,
                content: serde_json::Value::String(result),
            }
        })
        .collect();
    contexts.push(Message::new_tool_result(tools_output));

    log::debug!("[contexts]\n{:#?}", &contexts);

    let res = handle_chat(
        &contexts,
        &chat_config,
        &model,
        &api_url,
        &api_key,
        Some(&tools),
    );
    pin!(res);
    while let Some(event) = res.next().await {
        if event.status == ResEventStatus::Error {
            log::error!("[mcp error]\n{:?}", &event.error);
            return;
        }
        if event.status == ResEventStatus::Finish
            && let Some(data) = event.data
        {
            log::info!("[mcp success]\n{}", &data);
        }
    }
}

#[tokio::test]
async fn test_chat_mcp_completion() {
    let api_key = env::var("API_KEY").unwrap_or(String::new());
    if api_key.is_empty() {
        log::warn!("[warning] api key is empty");
        return;
    }
    handle_chat_mcp(
        false,
        String::from("https://api.deepseek.com"),
        api_key,
        String::from("deepseek-v4-flash"),
        AdaptorType::OpenAICompletion,
    )
    .await;
}

#[tokio::test]
async fn test_chat_mcp_completion_stream() {
    let api_key = env::var("API_KEY").unwrap_or(String::new());
    if api_key.is_empty() {
        log::warn!("[warning] api key is empty");
        return;
    }
    handle_chat_mcp(
        true,
        String::from("https://api.deepseek.com"),
        api_key,
        String::from("deepseek-v4-flash"),
        AdaptorType::OpenAICompletion,
    )
    .await;
}

#[tokio::test]
async fn test_chat_mcp_responses() {
    let api_key = env::var("API_KEY").unwrap_or(String::new());
    if api_key.is_empty() {
        log::warn!("[warning] api key is empty");
        return;
    }
    handle_chat_mcp(
        false,
        String::from("https://www.nekoapi.com/v1"),
        api_key,
        String::from("gpt-5.4"),
        AdaptorType::OpenAIResponse,
    )
    .await;
}

#[tokio::test]
async fn test_chat_mcp_responses_stream() {
    let api_key = env::var("API_KEY").unwrap_or(String::new());
    if api_key.is_empty() {
        log::warn!("[warning] api key is empty");
        return;
    }
    handle_chat_mcp(
        true,
        String::from("https://www.nekoapi.com/v1"),
        api_key,
        String::from("gpt-5.4"),
        AdaptorType::OpenAIResponse,
    )
    .await;
}

#[tokio::test]
async fn test_chat_mcp_env() {
    let stream = env::var("STREAM")
        .map(|s| s.parse::<bool>().unwrap())
        .unwrap_or(true);
    let api_url = env::var("API_BASE_URL").unwrap_or(String::from("https://api.deepseek.com"));
    let api_key = env::var("API_KEY").unwrap_or(String::new());
    let adaptor = env::var("ADAPTOR")
        .map(|a| AdaptorType::from_str(&a).unwrap())
        .unwrap_or(AdaptorType::OpenAICompletion);
    let model = env::var("MODEL").unwrap_or(String::from("deepseek-v4-flash"));

    if api_key.is_empty() {
        log::warn!("[warning] api key is empty");
        return;
    }
    handle_chat_mcp(stream, api_url, api_key, model, adaptor).await;
}
