use crate::adaptor::{self, AdaptorError, ChatAdaptor, get_chat_adaptor};
use crate::api::request::{ChatConfig, ChatInput};
use crate::api::response::ChatMessage;
use crate::client::{self, ClientError};
use crate::storage;
use async_stream::stream;
use chrono::Utc;
use futures::stream::Stream;
use reqwest::Method;
use serde_json::Value;
use url::Url;
use windai_domain::chat::{Message, MessageBuilder, Role, Topic};
use windai_domain::model::Model;
use windai_domain::provider::Provider;

#[derive(Debug)]
pub enum ChatStreamEventStatus {
    Partial,
    Finish,
    Error,
}
#[derive(Debug)]
pub struct ChatStreamEvent {
    pub status: ChatStreamEventStatus,
    pub data: Option<ChatMessage>,
    pub error: Option<ClientError>,
}
impl From<ClientError> for ChatStreamEvent {
    fn from(value: ClientError) -> Self {
        ChatStreamEvent {
            status: ChatStreamEventStatus::Error,
            data: None,
            error: Some(value),
        }
    }
}
impl From<AdaptorError> for ChatStreamEvent {
    fn from(value: AdaptorError) -> Self {
        ChatStreamEvent {
            status: ChatStreamEventStatus::Error,
            data: None,
            error: Some(value.into()),
        }
    }
}

struct StreamContext {
    chat_msg: ChatMessage,
    user_msg: ChatMessage,
    chat_adaptor: Box<dyn ChatAdaptor>,
    is_stream: bool,
    req_body: Value,
    url: Url,
    api_key: String,
}

fn build_stream_context(
    user_input: Vec<ChatInput>,
    topic_id: i64,
    model_id: i64,
    config: &ChatConfig,
) -> Result<StreamContext, ClientError> {
    let db = storage::global();
    let is_stream = config.stream.unwrap_or(false);
    let model = db
        .get_model(model_id)?
        .ok_or_else(|| ClientError::Internal(format!("cannot find model: {}", model_id)))?;
    let chat_adaptor = get_chat_adaptor(model.adaptor);

    let topic = db.get_topic(topic_id)?.ok_or_else(|| {
        ClientError::Internal(format!("cannot find topic: {}", model.provider_id))
    })?;

    let provider = db.get_provider(model.provider_id)?.ok_or_else(|| {
        ClientError::Internal(format!("cannot find provider: {}", model.provider_id))
    })?;

    let mut messages = db.list_chat_messages_by_topic(topic_id)?;
    let mut user_msg = create_user_message(user_input, is_stream, &model, &topic, &provider);
    db.create_message(&mut user_msg.base)?;
    let mut chat_msg = ChatMessage {
        base: MessageBuilder::default()
            .stream(is_stream)
            .role(Role::Assistant)
            .from_id(user_msg.base.id)
            .model_id(model_id)
            .topic_id(topic_id)
            .is_boundary(false)
            .build()
            .map_err(|e| ClientError::Internal(e.to_string()))?,
        model_name: model.name.clone(),
        provider_name: provider.name,
        provider_id: provider.id,
        adaptor: model.adaptor,
    };
    db.create_message(&mut chat_msg.base)?;

    messages.push(user_msg.clone());
    let messages_contexts = filter_chat_contexts(messages, topic.max_context as usize);
    let req_body = chat_adaptor.build_request(
        &model.name,
        config,
        &messages_contexts
            .into_iter()
            .map(|m| m.to_context())
            .collect(),
    )?;
    let api_key = db
        .get_credentials_by_provider(model.provider_id)?
        .into_iter()
        .next()
        .map(|credent| credent.key)
        .ok_or_else(|| {
            ClientError::Internal(format!("no credentials found for model: {}", model.name))
        })?;

    let base_url = provider.base_url.ok_or_else(|| {
        ClientError::Internal(format!(
            "base_url is not configured for provider: {}",
            model.provider_id
        ))
    })?;
    let endpoint = model
        .endpoint
        .unwrap_or_else(|| adaptor::get_default_endpoint(model.adaptor));

    let url = Url::parse(&base_url)?.join(&endpoint)?;

    Ok(StreamContext {
        chat_msg,
        user_msg,
        chat_adaptor,
        is_stream,
        req_body,
        url,
        api_key,
    })
}

fn forward_chat_stream(
    chat_msg: &mut ChatMessage,
    chat_adaptor: Box<dyn ChatAdaptor>,
    is_stream: bool,
    url: Url,
    req_body: Value,
    api_key: String,
) -> impl Stream<Item = ChatStreamEvent> {
    stream! {
        if is_stream {
            let response = match client::request_sse(
                url.as_str(),
                Method::POST,
                |req| req.json(&req_body).bearer_auth(&api_key),
            ).await {
                Ok(r) => r,
                Err(e) => { yield e.into(); return; }
            };
            let stream = client::handle_stream(response);
            for await result in stream {
                match result {
                    Ok(bytes) => {
                        let msg_chunks = match chat_adaptor.parse_stream_chunk(bytes) {
                            Ok(c) => c,
                            Err(e) => { yield e.into(); return; }
                        };
                        for msg_chunk in msg_chunks {
                            msg_chunk.apply_to_message(&mut chat_msg.base);
                            yield ChatStreamEvent {
                                status: ChatStreamEventStatus::Partial,
                                data: Some(chat_msg.clone()),
                                error: None,
                            }
                        }
                    }
                    Err(err) => yield ChatStreamEvent {
                        status: ChatStreamEventStatus::Error,
                        data: None,
                        error: Some(err),
                    }
                };
            }
            yield ChatStreamEvent {
                status: ChatStreamEventStatus::Finish,
                data: Some(chat_msg.clone()),
                error: None,
            }
        } else {
            let response = match client::request(
                url.as_str(),
                Method::POST,
                |req| req.json(&req_body).bearer_auth(&api_key),
            ).await {
                Ok(r) => r,
                Err(e) => { yield e.into(); return; }
            };
            let res = match client::handle_response(response).await {
                Ok(r) => r,
                Err(e) => { yield e.into(); return; }
            };
            let response = match chat_adaptor.parse_response(res) {
                Ok(r) => r,
                Err(e) => { yield e.into(); return; }
            };
            response.apply_to_message(&mut chat_msg.base);
            yield ChatStreamEvent {
                status: ChatStreamEventStatus::Finish,
                data: Some(chat_msg.clone()),
                error: None,
            }
        }
    }
}

/// 发送一次对话请求
pub fn handle_chat(
    user_input: Vec<ChatInput>,
    topic_id: i64,
    model_id: i64,
    config: ChatConfig,
) -> impl Stream<Item = ChatMessage> {
    stream! {
        let mut ctx = match build_stream_context(user_input, topic_id, model_id, &config) {
            Ok(ctx) => ctx,
            Err(e) => {
                log::error!("{e}");
                return;
            }
        };
        yield ctx.user_msg;
        for await event in forward_chat_stream(
            &mut ctx.chat_msg,
            ctx.chat_adaptor,
            ctx.is_stream,
            ctx.url,
            ctx.req_body,
            ctx.api_key,
        ) {
            match event.status {
                ChatStreamEventStatus::Partial => {
                    let data = match event.data {
                        Some(d) => d,
                        None => return,
                    };
                    yield data;
                },
                ChatStreamEventStatus::Finish => {
                    let db = storage::global();
                    let data = match event.data {
                        Some(d) => d,
                        None => return,
                    };
                    db.update_message(&data.base).unwrap_or_else(|e| {
                        log::error!("update message failed: {}", e);
                    });
                    yield data;
                },
                ChatStreamEventStatus::Error => {
                    log::error!("{:?}", event.error);
                    return;
                }
            }
        }
    }
}

/// 过滤上下文消息，从最新消息开始向前收集成对的 User-Assistant 对话轮次，
/// 并按 max_context 限制截取后返回。
/// - 至少有一个 [Role::User] 上下文会被保留
/// - 不成对的消息会被忽略，第一条消息永远是 [Role::User]
fn filter_chat_contexts(messages: Vec<ChatMessage>, max_context: usize) -> Vec<ChatMessage> {
    let max_context = std::cmp::max(1, max_context);
    let mut currents: Vec<usize> = Vec::new();
    let len = messages.len();
    for (i, message) in messages.iter().rev().enumerate() {
        let last = match currents.last() {
            Some(current) => current,
            None => {
                if message.base.role == Role::User {
                    currents.push(i);
                }
                continue;
            }
        };
        if message.base.role == Role::User {
            match messages[len - *last - 1].base.role {
                Role::Assistant => {
                    if let Some(from_id) = messages[len - *last - 1].base.from_id
                        && message.base.id == from_id
                    {
                        currents.push(i);
                    } else {
                        currents.pop();
                    }
                }
                _ => {
                    continue;
                }
            }
        } else if message.base.role == Role::Assistant {
            match messages[len - *last - 1].base.role {
                Role::Assistant => {
                    currents.pop();
                    currents.push(i);
                }
                Role::User => {
                    currents.push(i);
                }
                _ => {
                    currents.pop();
                }
            }
        } else {
            continue;
        }
    }
    currents.drain(std::cmp::min(max_context, currents.len())..);
    currents.reverse();
    // 第一条数据必须是用户消息
    if let Some(offset) = currents
        .iter()
        .position(|i| messages[*i].base.role == Role::User)
    {
        currents.drain(..offset);
    }
    currents
        .iter()
        .map(|index| messages[len - *index - 1].clone())
        .collect::<Vec<ChatMessage>>()
}

/// 创建用户消息
/// - 将用户消息的 id 和 index 初始化为0
fn create_user_message(
    content: Vec<ChatInput>,
    is_stream: bool,
    model: &Model,
    topic: &Topic,
    provider: &Provider,
) -> ChatMessage {
    return ChatMessage {
        base: Message {
            id: 0,
            stream: is_stream,
            from_id: None,
            role: Role::User,
            raw_content: String::new(),
            content: content.into_iter().map(|c| c.to_content()).collect(),
            reasoning_content: None,
            transcript: None,
            created_at: Utc::now().timestamp(),
            model_id: model.id,
            topic_id: topic.id,
            index: 0,
            is_boundary: false,
            input_tokens: 0,
            output_tokens: 0,
        },
        model_name: model.name.clone(),
        provider_name: provider.name.clone(),
        provider_id: provider.id,
        adaptor: model.adaptor,
    };
}

#[cfg(test)]
mod test {
    use crate::adaptor::get_chat_adaptor;
    use crate::api::request::{ChatConfig, ChatMessageContext};
    use crate::api::response::ChatMessage;
    use futures::StreamExt;
    use tokio::pin;
    use url::Url;
    use windai_domain::adaptor::AdaptorType;
    use windai_domain::chat::{ContentType, MessageBuilder, MessageContent, Role};
    use windai_domain::provider::Credentials;

    #[tokio::test]
    async fn forward_chat_stream() {
        let model_name = String::from("deepseek-v4-flash");
        let chat_adaptor = get_chat_adaptor(AdaptorType::OpenAICompletion);
        let url = Url::parse("https://api.deepseek.com/chat/completions").unwrap();
        let chat_config = ChatConfig {
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
            ChatMessageContext {
                role: Role::System,
                content: vec![MessageContent {
                    content: String::from(
                        "you are a helpful assistant and MUST response in Chinese",
                    ),
                    content_type: ContentType::Text,
                }],
            },
            ChatMessageContext {
                role: Role::User,
                content: vec![MessageContent {
                    content: String::from("who are you"),
                    content_type: ContentType::Text,
                }],
            },
        ];
        let req = chat_adaptor
            .build_request(&model_name, &chat_config, &contexts)
            .unwrap();
        let mut chat_msg = ChatMessage {
            base: MessageBuilder::default()
                .stream(chat_config.stream.unwrap())
                .role(Role::Assistant)
                .from_id(1)
                .model_id(1)
                .topic_id(1)
                .is_boundary(false)
                .build()
                .unwrap(),
            model_name,
            provider_name: String::from("deepseek"),
            provider_id: 1,
            adaptor: AdaptorType::OpenAICompletion,
        };
        let cred = Credentials::from_env();
        if cred.key.is_empty() {
            return;
        }
        let res = super::forward_chat_stream(
            &mut chat_msg,
            chat_adaptor,
            chat_config.stream.unwrap(),
            url,
            req,
            cred.key,
        );
        pin!(res);
        while let Some(value) = res.next().await {
            println!("[data]\n{:?}", value);
        }
    }
    #[test]
    fn filter_chat_contexts() {
        fn build(id: i64, from_id: Option<i64>, role: Role) -> ChatMessage {
            ChatMessage {
                base: MessageBuilder::default()
                    .id(id)
                    .from_id(from_id.unwrap_or(-1))
                    .role(role)
                    .build()
                    .unwrap(),
                model_name: String::new(),
                provider_name: String::new(),
                provider_id: 0,
                adaptor: AdaptorType::OpenAICompletion,
            }
        }
        {
            // 不存在成对的 User-Assistant 中间插入节点的情况
            let messages = vec![
                build(1, None, Role::User),
                build(20, None, Role::User),          // <-
                build(21, Some(20), Role::Assistant), // <-
                build(40, None, Role::User),          // <-
                build(41, Some(40), Role::Assistant), // <-
                build(50, Some(49), Role::Assistant),
                build(60, None, Role::User),          // <-
                build(61, Some(60), Role::Assistant), // <-
                build(63, None, Role::User),
                build(64, None, Role::Tool),
                build(65, None, Role::User), // <-
                build(80, Some(79), Role::Assistant),
            ];
            let len = messages.len();
            let msgs = super::filter_chat_contexts(messages, len);
            assert!(msgs.len() <= len);
            assert!(msgs.len() == 7);
            assert!(msgs[0].base.id == 20);
            assert!(msgs[1].base.id == 21);
            assert!(msgs[2].base.id == 40);
            assert!(msgs[3].base.id == 41);
            assert!(msgs[4].base.id == 60);
            assert!(msgs[5].base.id == 61);
            assert!(msgs[6].base.id == 65);
        }
        {
            let messages = vec![build(1, None, Role::User)];
            let msgs = super::filter_chat_contexts(messages, 1);
            assert!(msgs.len() == 1);

            let messages = vec![build(1, None, Role::User)];
            let msgs = super::filter_chat_contexts(messages, 0);
            assert!(msgs.len() == 1);

            let messages = vec![build(1, None, Role::Assistant)];
            let msgs = super::filter_chat_contexts(messages, 1);
            assert!(msgs.len() == 0);

            let messages = vec![build(1, None, Role::Assistant)];
            let msgs = super::filter_chat_contexts(messages, 0);
            assert!(msgs.len() == 0);

            let messages = vec![build(1, None, Role::Tool)];
            let msgs = super::filter_chat_contexts(messages, 1);
            assert!(msgs.len() == 0);

            let messages = vec![];
            let msgs = super::filter_chat_contexts(messages, 1);
            assert!(msgs.len() == 0);
        }
    }
}
