//! 一次非流式或流式请求的发送与响应读取

use super::adapter::ChatAdapter;
use super::client;
use super::error::AdapterError;
use super::eventsource::{Event, EventStreamError, Eventsource};
use super::model::{ChatModel, Target};
use futures::Stream;
use pin_project_lite::pin_project;
use reqwest::header;
use reqwest::{Method, Response};
use serde_json::Value;
use std::fmt::Display;
use std::pin::Pin;
use std::task::{Context, Poll};
use url::{ParseError, Url};
use wind_wire::message::Message;
use wind_wire::tool::Tools;

pub struct Request {
    pub model: ChatModel,
    pub target: Target,
    pub contexts: Vec<Message>,
    pub tools: Option<Vec<Tools>>,
}

#[derive(Debug, Clone)]
pub enum Chunk {
    Delta(Message),
    Done,
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    #[error("the provider answered {code}: {body}")]
    Status { code: u16, body: String },
    #[error("transfer failed: {0}")]
    Transport(String),
    #[error("the request url is invalid: {0}")]
    Url(#[from] ParseError),
    #[error(transparent)]
    Adapter(#[from] AdapterError),
}

fn url_of(
    adapter: &dyn ChatAdapter,
    model: &ChatModel,
    target: &Target,
) -> Result<String, ProviderError> {
    let base = target.base_url.trim_end_matches('/');
    let path = model.endpoint.as_deref().unwrap_or_else(|| adapter.path());
    let path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    Ok(Url::parse(&format!("{base}{path}"))?.to_string())
}

pub async fn once(adapter: &dyn ChatAdapter, request: &Request) -> Result<Message, ProviderError> {
    let url = url_of(adapter, &request.model, &request.target)?;
    let body =
        adapter.build_request(&request.model, &request.contexts, request.tools.as_deref())?;
    let response = send(&url, &request.target, body).await?;
    let bytes = response
        .bytes()
        .await
        .map_err(|error| ProviderError::Transport(error.to_string()))?;
    Ok(adapter.parse_response(&bytes)?)
}

pub async fn stream(
    adapter: &dyn ChatAdapter,
    request: &Request,
) -> Result<impl Stream<Item = Result<Chunk, ProviderError>>, ProviderError> {
    let body =
        adapter.build_request(&request.model, &request.contexts, request.tools.as_deref())?;
    stream_body(adapter, &request.model, &request.target, body).await
}

pub async fn stream_body(
    adapter: &dyn ChatAdapter,
    model: &ChatModel,
    target: &Target,
    body: Value,
) -> Result<impl Stream<Item = Result<Chunk, ProviderError>>, ProviderError> {
    let url = url_of(adapter, model, target)?;
    let response = send(&url, target, body).await?;
    let events = response.bytes_stream().eventsource();
    Ok(EventsToChunks {
        adapter,
        events,
        finished: false,
    })
}

async fn send(url: &str, target: &Target, body: Value) -> Result<Response, ProviderError> {
    let client = client::get().map_err(ProviderError::Transport)?;
    let mut builder = client
        .request(Method::POST, url)
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
        .header(header::ACCEPT, "text/event-stream");
    if let Some(key) = &target.api_key {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {key}"));
    }
    let response = builder
        .json(&body)
        .send()
        .await
        .map_err(|error| ProviderError::Transport(error.to_string()))?;
    if !response.status().is_success() {
        let code = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        return Err(ProviderError::Status { code, body });
    }
    Ok(response)
}

pin_project! {
struct EventsToChunks<'a, E> {
    adapter: &'a dyn ChatAdapter,
    #[pin]
    events: E,
    finished: bool,
}
}

impl<'a, E, F> Stream for EventsToChunks<'a, E>
where
    E: Stream<Item = Result<Event, EventStreamError<F>>>,
    F: Display,
{
    type Item = Result<Chunk, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.finished {
            return Poll::Ready(None);
        }
        loop {
            match this.events.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(event))) => match this.adapter.parse_stream_chunk(&event) {
                    Ok(Some(message)) => return Poll::Ready(Some(Ok(Chunk::Delta(message)))),
                    Ok(None) => continue,
                    Err(error) => {
                        *this.finished = true;
                        return Poll::Ready(Some(Err(error.into())));
                    }
                },
                Poll::Ready(Some(Err(error))) => {
                    *this.finished = true;
                    return Poll::Ready(Some(Err(ProviderError::Transport(error.to_string()))));
                }
                Poll::Ready(None) => {
                    // `[DONE]` 不会成为一个事件，事件流的自然结束就是整条流的结束
                    *this.finished = true;
                    return Poll::Ready(Some(Ok(Chunk::Done)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
