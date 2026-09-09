use bytes::Bytes;
use reqwest::{Client, ClientBuilder};
use reqwest::{Method, RequestBuilder, Response, header};
use std::error::Error;
use std::fmt::Display;
use std::{sync::OnceLock, time::Duration};

static HTTP_CLIENT: OnceLock<Client> = OnceLock::new();

#[derive(Debug)]
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

fn walk_source_chain(error: &reqwest::Error) -> String {
    let mut current_err = None;
    let mut current = error.source();
    while let Some(err) = current {
        current_err = Some(err);
        current = err.source();
    }
    match current_err {
        Some(err) => err.to_string(),
        None => "unknown error".to_string(),
    }
}

async fn build_http_error(response: Response) -> ClientError {
    let status = response.status();
    let code = status.as_u16();
    match response.text().await {
        Ok(body) if !body.is_empty() => ClientError { code, msg: body },
        _ => ClientError {
            code,
            msg: status
                .canonical_reason()
                .unwrap_or("Unknown Error")
                .to_string(),
        },
    }
}

impl From<reqwest::Error> for ClientError {
    fn from(error: reqwest::Error) -> Self {
        if let Some(status) = error.status() {
            return Self {
                code: status.as_u16(),
                msg: status
                    .canonical_reason()
                    .unwrap_or("Unknown Error")
                    .to_string(),
            };
        }
        Self {
            code: 500,
            msg: walk_source_chain(&error),
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
    if !response.status().is_success() {
        return Err(build_http_error(response).await);
    }
    Ok(response)
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
