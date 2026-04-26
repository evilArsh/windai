use async_stream::stream;
use bytes::Bytes;
use futures::stream::Stream;
use reqwest::{Client, ClientBuilder};
use reqwest::{Method, RequestBuilder, Response, header};
use std::{sync::OnceLock, time::Duration};

static HTTP_CLIENT: OnceLock<Client> = OnceLock::new();

use crate::adaptor::AdaptorError;
use crate::storage::StorageError;
use std::error::Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("io error: ${0}")]
    Io(#[from] std::io::Error),

    #[error("json error: ${0}")]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Request(#[from] RequestError),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("url parse error: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error(transparent)]
    Adaptor(#[from] AdaptorError),

    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[derive(Error, Debug)]
pub enum RequestError {
    #[error("request error: code: {code}: {msg}")]
    Http {
        code: u16,
        msg: String,
        #[source]
        source: Option<reqwest::Error>,
    },

    #[error("request error: {msg}")]
    Other {
        msg: String,
        #[source]
        source: Option<reqwest::Error>,
    },
}
impl RequestError {
    pub fn from_reqwest(error: reqwest::Error) -> Self {
        match error.status() {
            Some(status) => Self::Http {
                code: status.as_u16(),
                msg: error.to_string(),
                source: Some(error),
            },
            None => match error.is_timeout() {
                true => Self::Other {
                    msg: "request timeout".to_string(),
                    source: Some(error),
                },
                false => Self::Other {
                    msg: error
                        .source()
                        .map(|e| e.to_string())
                        .unwrap_or(error.to_string()),
                    source: Some(error),
                },
            },
        }
    }
}
impl From<reqwest::Error> for ClientError {
    fn from(err: reqwest::Error) -> Self {
        ClientError::Request(RequestError::from_reqwest(err))
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
    let builder = builder_fn(get().request(method, url))
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8");
    let response = builder.send().await?;
    if response.status().is_success() {
        Ok(response)
    } else {
        Err(ClientError::Request(RequestError::Http {
            code: response.status().as_u16(),
            msg: response.text().await?,
            source: None,
        }))
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
