use super::{
    client,
    error::{ProxyError, RequestError},
};
use async_stream::stream;
use bytes::Bytes;
use futures::stream::{Stream, StreamExt};
use reqwest::{Method, RequestBuilder, Response, header};

/// 发送请求
pub async fn request<F>(url: &str, method: Method, builder_fn: F) -> Result<Response, ProxyError>
where
    F: FnOnce(RequestBuilder) -> RequestBuilder,
{
    let builder = builder_fn(client::get().request(method, url))
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8");
    let response = builder.send().await?;
    if response.status().is_success() {
        return Ok(response);
    } else {
        return Err(ProxyError::Request(RequestError::Http {
            code: response.status().as_u16(),
            msg: response.text().await?,
            source: None,
        }));
    }
}

/// 发送请求，并返回流式数据
pub async fn request_sse<F>(
    url: &str,
    method: Method,
    builder_fn: F,
) -> Result<Response, ProxyError>
where
    F: FnOnce(RequestBuilder) -> RequestBuilder,
{
    return request(url, method, |req| {
        builder_fn(req).header(header::ACCEPT, "text/event-stream")
    })
    .await;
}

/// 获取一次http响应body数据并返回bytes
pub async fn handle_response(response: Response) -> Result<Bytes, ProxyError> {
    return match response.bytes().await {
        Ok(json_bytes) => Ok(json_bytes),
        Err(err) => Err(err.into()),
    };
}

/// 处理流式数据
pub async fn handle_stream<F>(response: Response) -> impl Stream<Item = Result<Bytes, ProxyError>> {
    stream! {
        let mut stream = response.bytes_stream();
        while let Some(item) = stream.next().await {
            yield match item {
                Ok(bytes) => Ok(bytes),
                Err(err) => Err(err.into()),
            };
        }
    }
}
