use async_stream::stream;
use bytes::Bytes;
use futures::stream::Stream;
use reqwest::{Client, ClientBuilder};
use reqwest::{Method, RequestBuilder, Response, header};
use serde::Serialize;
use std::error::Error;
use std::fmt::Display;
use std::{sync::OnceLock, time::Duration};

static HTTP_CLIENT: OnceLock<Client> = OnceLock::new();

#[derive(Debug, Serialize)]
pub struct ClientError {
    pub code: u16,
    pub msg: String,
}
impl Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "request failed. code: {}, msg: {}", self.code, self.msg)
    }
}
impl Error for ClientError {}

impl From<reqwest::Error> for ClientError {
    fn from(error: reqwest::Error) -> Self {
        let err = error
            .source()
            .map(|e| e.to_string())
            .unwrap_or(error.to_string());
        match error.status() {
            Some(status) => Self {
                code: status.as_u16(),
                msg: err,
            },
            None => Self {
                code: 500,
                msg: err,
            },
        }
    }
}

/// 创建一个新的HTTP客户端
/// # Panics
/// 如果客户端创建失败则 Panic
pub fn create_new() -> Client {
    ClientBuilder::new()
        .timeout(Duration::from_secs(600))
        .build()
        .expect("create request client failed")
}

/// 获取全局HTTP客户端
/// # Panics
/// 如果客户端创建失败则 Panic
pub fn get() -> Client {
    HTTP_CLIENT.get_or_init(create_new).clone()
}

/// 发送请求
pub async fn request<F>(url: &str, method: Method, builder_fn: F) -> Result<Response, ClientError>
where
    F: FnOnce(RequestBuilder) -> RequestBuilder,
{
    log::debug!("request: url: {}, method: {}", url, method);
    let builder = builder_fn(get().request(method, url))
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8");
    let response = builder.send().await?;
    if response.status().is_success() {
        Ok(response)
    } else {
        let status = response.status().as_u16();
        let text = response.text().await?;
        Err(ClientError {
            code: status,
            msg: text,
        })
    }
}

/// 发送请求，并返回流式数据
pub async fn request_sse<F>(
    url: &str,
    method: Method,
    builder_fn: F,
) -> Result<Response, ClientError>
where
    F: FnOnce(RequestBuilder) -> RequestBuilder,
{
    request(url, method, |req| {
        builder_fn(req).header(header::ACCEPT, "text/event-stream")
    })
    .await
}

/// 获取一次http响应body数据并返回bytes
pub async fn handle_response(response: Response) -> Result<Bytes, ClientError> {
    match response.bytes().await {
        Ok(json_bytes) => Ok(json_bytes),
        Err(err) => Err(err.into()),
    }
}

/// 处理流式数据
pub fn handle_stream(response: Response) -> impl Stream<Item = Result<Bytes, ClientError>> {
    stream! {
        let stream = response.bytes_stream();
        for await result in stream {
            yield match result {
                Ok(bytes) => Ok(bytes),
                Err(err) => Err(err.into()),
            };
        }
    }
}
