use async_stream::stream;
use futures::stream::Stream;
use log;
use reqwest::Method;
use std::fmt::Display;
use url::Url;

pub mod adaptor;
mod client;
pub mod error;

mod sse;

use super::conversation::{
    message::{Message, Model, ReqConfig},
    tool::Tools,
};
use adaptor::AdaptorError;
use client::ClientError;
use error::ProviderError;

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
impl From<ClientError> for ResEvent {
    fn from(value: ClientError) -> Self {
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

/// 发送一次对话请求
pub fn handle_chat(
    contexts: &Vec<Message>,
    config: &ReqConfig,
    model: &Model,
    api_base: &str,
    api_key: &str,
    tools: Option<&Vec<Tools>>,
) -> impl Stream<Item = ResEvent> {
    stream! {
        let chat_adaptor = adaptor::get_chat_adaptor(model.adaptor);
        let endpoint = model
            .endpoint
            .clone()
            .unwrap_or_else(|| adaptor::get_default_endpoint(model.adaptor));

        let url = match Url::parse(&format!(
            "{}/{}",
            api_base.trim_end_matches("/"),
            endpoint.trim_start_matches("/")
        )) {
            Ok(u) => u,
            Err(e) => {
                yield e.into();
                return;
            }
        };
        let is_stream = config.stream;
        let req_body = match chat_adaptor.build_request(&model.name, config, contexts, tools) {
            Ok(body) => body,
            Err(e) => {
                yield e.into();
                return;
            }
        };
        log::debug!(
            "[request body]\n{}",
            serde_json::to_string_pretty(&req_body).unwrap_or_default()
        );
        match is_stream {
            Some(true) => {
                let response = match client::request_sse(url.as_str(), Method::POST, |req| {
                    req.json(&req_body).bearer_auth(api_key)
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
                                log::debug!("[chunk]\n{}", &chunk);
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
                let response = match client::request(url.as_str(), Method::POST, |req| {
                    req.json(&req_body).bearer_auth(api_key)
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
