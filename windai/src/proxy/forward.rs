use super::{
    client,
    error::{ProxyError, RequestError},
};
use reqwest::{Method, RequestBuilder, Response, header};

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

pub async fn request_sse<F>(url: &str, method: Method, builder_fn: F) -> Result<Response, ProxyError>
where
    F: FnOnce(RequestBuilder) -> RequestBuilder,
{
    return request(url, method, |req| {
        builder_fn(req).header(header::ACCEPT, "text/event-stream")
    })
    .await;
}
