use futures::StreamExt;
use serde_json::{Value, json};
use std::{env, str::FromStr};
use tokio::pin;
use windai_conversation::{
    message::{Content, Message, ReqConfig, Role},
    model::{AdaptorType, Model},
    provider::{ResEventStatus, handle_chat},
    tool::{FunctionCallOutput, FunctionTool, Tools},
};
use windai_mcp::client::{self, CallToolParam, Tool};

fn everything_params() -> client::ServerParams {
    client::ServerParams::Stdio(client::StdioParams {
        name: "test-everything".to_string(),
        description: None,
        command: "npx".to_string(),
        args: vec![
            "-y".to_string(),
            "@modelcontextprotocol/server-everything".to_string(),
        ],
        env: serde_json::from_str(
            r#"{
            "NPM_CONFIG_REGISTRY": "https://registry.npmmirror.com",
        }"#,
        )
        .ok(),
    })
}

fn fetch_params() -> client::ServerParams {
    client::ServerParams::Stdio(client::StdioParams {
        name: "mcp-server-fetch".to_string(),
        description: None,
        command: "uvx".to_string(),
        args: vec!["mcp-server-fetch".to_string()],
        env: serde_json::from_str(
            r#"{
            "UV_DEFAULT_INDEX": "https://pypi.tuna.tsinghua.edu.cn/simple/",
            "PIP_INDEX_URL": "https://pypi.tuna.tsinghua.edu.cn/simple/",
        }"#,
        )
        .ok(),
    })
}

async fn handle_chat_mcp_real(
    stream: bool,
    api_url: String,
    api_key: String,
    model: String,
    adaptor: AdaptorType,
) {
    unsafe {
        env::set_var("RUST_LOG", "debug");
    }
    let _ = env_logger::builder().is_test(true).try_init();
    let mcp = client::registry::Registry::new();
    let session_id = "test-session-id";

    let r1 = mcp.acquire(session_id, everything_params()).await.unwrap();
    log::info!("[acquire]\n{:#?}", &r1);
    let r2 = mcp.acquire(session_id, fetch_params()).await.unwrap();
    log::info!("[acquire]\n{:#?}", &r2);

    let tools = mcp.list_all_tools().await.unwrap();
    log::info!("[list_all_tools]\n{:#?}", &tools);

    let model = Model {
        name: model,
        adaptor,
        endpoint: None,
    };

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
                "you are a helpful assistant, respond in Chinese",
            ))],
            None,
        ),
        Message::new_simple(
            Role::User,
            vec![Content::new_text(String::from(
                "I want to know the content in https://vivcode.cn/; and then add sum of two numbers: 1000 and 2000",
            ))],
            None,
        ),
    ];

    let tools = tools
        .iter()
        .map(|tool| {
            Tools::Function(FunctionTool {
                name: tool.name.clone(),
                description: tool.description.clone(),
                parameters: Some(Value::Object((*tool.input_schema).clone())),
                strict: None,
            })
        })
        .collect::<Vec<Tools>>();

    let mut msg = Message::default();
    loop {
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
                    msg = data;
                }
            }
        }
        if let Some(tool_calls) = msg.tool_calls
            && tool_calls.len() > 0
        {
            contexts.push(Message::new_tool_request(
                tool_calls.clone(),
                msg.reasoning_content,
            ));

            let params = tool_calls
                .iter()
                .filter_map(|tool| {
                    let (server_name, tool_name) = Tool::parse_name(&tool.name);
                    match server_name {
                        Some(name) => Some(CallToolParam {
                            server_name: name,
                            tool_name,
                            arguments: Some(serde_json::from_str(&tool.arguments).unwrap()),
                        }),
                        None => {
                            log::error!(
                                "cannot parse mcp server name, invalid tool name: {}",
                                tool.name
                            );
                            None
                        }
                    }
                })
                .collect::<Vec<CallToolParam>>();

            let tools_len = params.len();

            let pending = params.into_iter().map(|param| {
                let mcp_clone = mcp.clone();
                async move {
                    let res = mcp_clone.call_tool(&param).await;
                    res
                }
            });
            let results = futures::future::try_join_all(pending).await.unwrap();
            assert!(results.len() == tools_len);

            contexts.push(Message::new_tool_result(
                results
                    .into_iter()
                    .enumerate()
                    .map(|(index, res)| FunctionCallOutput {
                        id: tool_calls[index].id.clone(),
                        content: res.content,
                    })
                    .collect(),
            ));
        } else {
            break;
        }
        msg = Message::default();
    }

    mcp.shutdown().await;
}

async fn handle_chat_mcp(
    stream: bool,
    api_url: String,
    api_key: String,
    model: String,
    adaptor: AdaptorType,
) {
    unsafe {
        env::set_var("RUST_LOG", "debug");
    }
    let _ = env_logger::builder().is_test(true).try_init();

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

// -------------

#[tokio::test]
async fn test_chat_mcp_completion_real() {
    let api_key = env::var("API_KEY").unwrap_or(String::new());
    if api_key.is_empty() {
        log::warn!("[warning] api key is empty");
        return;
    }
    handle_chat_mcp_real(
        false,
        String::from("https://api.deepseek.com"),
        api_key,
        String::from("deepseek-v4-flash"),
        AdaptorType::OpenAICompletion,
    )
    .await;
}

#[tokio::test]
async fn test_chat_mcp_completion_stream_real() {
    let api_key = env::var("API_KEY").unwrap_or(String::new());
    if api_key.is_empty() {
        log::warn!("[warning] api key is empty");
        return;
    }
    handle_chat_mcp_real(
        true,
        String::from("https://api.deepseek.com"),
        api_key,
        String::from("deepseek-v4-flash"),
        AdaptorType::OpenAICompletion,
    )
    .await;
}
