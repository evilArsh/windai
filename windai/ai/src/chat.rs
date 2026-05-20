use async_stream::stream;
use futures::stream::Stream;
use log;
use reqwest::Method;
use serde_json::Value;
use std::fmt::Display;
use url::Url;

use super::{
    ProviderError,
    message::{Message, ReqConfig},
    model::Model,
    provider::adaptor::AdaptorError,
    tool::Tools,
};
use crate::provider::{
    adaptor::{self, ChatAdaptor},
    client,
};

/// 聊天统一响应事件
#[derive(Debug, PartialEq, Eq, strum::Display)]
pub enum ResEventStatus {
    /// 用于流式消息
    Partial,
    /// 流式/非流式 数据接收完毕
    Finish,
    /// 发生错误后，终止请求并且返回具体错误
    Error,
}
#[derive(Debug)]
pub struct ResEvent {
    pub status: ResEventStatus,
    pub data: Option<Message>,
    pub error: Option<ProviderError>,
}
impl ResEvent {
    #[inline]
    pub fn new_partial(data: Message) -> Self {
        Self {
            status: ResEventStatus::Partial,
            data: Some(data),
            error: None,
        }
    }
    #[inline]
    pub fn new_finish(data: Message) -> Self {
        Self {
            status: ResEventStatus::Finish,
            data: Some(data),
            error: None,
        }
    }
    #[inline]
    pub fn new_error(error: ProviderError) -> Self {
        Self {
            status: ResEventStatus::Error,
            data: None,
            error: Some(error),
        }
    }
}

impl Display for ResEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "ResEvent {{")?;
        writeln!(f, "  status: {}", self.status)?;
        if let Some(ref error) = self.error {
            writeln!(f, "  error: {}", error)?;
        }
        if let Some(ref data) = self.data {
            for line in format!("{}", data).lines() {
                writeln!(f, "  {}", line)?;
            }
        }
        write!(f, "}}")
    }
}
impl From<client::ClientError> for ResEvent {
    fn from(value: client::ClientError) -> Self {
        ResEvent {
            status: ResEventStatus::Error,
            data: None,
            error: Some(value.into()),
        }
    }
}
impl From<AdaptorError> for ResEvent {
    fn from(value: AdaptorError) -> Self {
        ResEvent {
            status: ResEventStatus::Error,
            data: None,
            error: Some(value.into()),
        }
    }
}
impl From<url::ParseError> for ResEvent {
    fn from(value: url::ParseError) -> Self {
        ResEvent {
            status: ResEventStatus::Error,
            data: None,
            error: Some(value.into()),
        }
    }
}

/// 生成请求体
pub fn build_request(
    chat_adaptor: &dyn ChatAdaptor,
    model: &Model,
    config: &ReqConfig,
    contexts: &Vec<Message>,
    tools: Option<&Vec<Tools>>,
) -> Result<Value, ProviderError> {
    let req_body = match chat_adaptor.build_request(&model.name, config, contexts, tools) {
        Ok(body) => body,
        Err(e) => return Err(e.into()),
    };
    Ok(req_body)
}

fn parse_url(
    chat_adaptor: &dyn ChatAdaptor,
    api_base_url: &str,
    api_endpoint: Option<&str>,
) -> Result<Url, url::ParseError> {
    let endpoint = api_endpoint.map_or(
        adaptor::get_default_endpoint(chat_adaptor.get_type()),
        |e| e.to_string(),
    );

    Url::parse(&format!(
        "{}/{}",
        api_base_url.trim_end_matches("/"),
        endpoint.trim_start_matches("/")
    ))
}
/// 一次对话请求
///
/// 根据 `req_body` 中的 `stream` 字段来决定启用流式或者非流式对话请求
pub fn handle_chat(
    chat_adaptor: &dyn ChatAdaptor,
    req_body: &Value,
    api_base_url: &str,
    api_key: &str,
    api_endpoint: Option<&str>,
) -> impl Stream<Item = ResEvent> {
    stream! {
        let api_url = match parse_url(chat_adaptor, api_base_url, api_endpoint) {
            Ok(api_url) => api_url,
            Err(err) => {
                yield err.into();
                return;
            }
        };
        let is_stream = req_body
            .get("stream")
            .and_then(|v| v.as_bool())
            .unwrap_or_else(|| false);
        match is_stream {
            true => {
                let api_url = match parse_url(chat_adaptor, api_base_url, api_endpoint) {
                    Ok(r) => r,
                    Err(e) => {
                        yield e.into();
                        return;
                    }
                };

                let response = match client::request_sse(api_url.as_str(), Method::POST, |req| {
                    req.json(req_body).bearer_auth(api_key)
                })
                .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        yield e.into();
                        return;
                    }
                };
                let stream = client::handle_stream(response);
                let mut msg = Message::default();
                for await result in stream {
                    match result {
                        Ok(bytes) => {
                            let chunks = match chat_adaptor.parse_stream_chunk(&bytes) {
                                Ok(c) => c,
                                Err(e) => {
                                    log::error!("[parse_stream_chunk error]\n{}", e.to_string());
                                    yield e.into();
                                    return;
                                }
                            };
                            for chunk in chunks {
                                // log::debug!("[chunk]\n{}", &chunk);
                                msg.append_chunk(chunk);
                                yield ResEvent::new_partial(msg.clone());
                            }
                        }
                        Err(err) => {
                            yield ResEvent::new_error(err.into());
                        }
                    };
                }
                yield ResEvent::new_finish(msg);
            }
            _ => {
                let response = match client::request(api_url.as_str(), Method::POST, |req| {
                    req.json(req_body).bearer_auth(api_key)
                })
                .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("[response error] status:{}, text: {}", e.code, e.msg);
                        yield e.into();
                        return;
                    }
                };
                let res = match client::handle_response(response).await {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("[handle_response error]\n{}", e);
                        yield e.into();
                        return;
                    }
                };
                let response = match chat_adaptor.parse_response(&res) {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("[parse response error]\n{}", e);
                        yield e.into();
                        return;
                    }
                };
                yield ResEvent::new_finish(response);
            }
        }
    }
}
