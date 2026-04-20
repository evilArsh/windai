use crate::adaptor::{self, get_chat_adaptor};
use crate::dto::chat::{MessageResponse, RequestConfig, filter_context};
use crate::storage;
use async_stream::stream;
use error::ProxyError;
use futures::stream::{Stream, StreamExt};
use reqwest::Method;
use tokio::pin;
use url::Url;

pub mod client;
pub mod error;
pub mod forward;
pub mod sse;

/// 发送一次对话请求
pub fn handle_chat(
    content: String,
    topic_id: i64,
    model_id: i64,
    config: RequestConfig,
) -> impl Stream<Item = Result<MessageResponse, ProxyError>> {
    stream! {
        // debug!("[handle_chat] {}, {:?}, {}", content, model_id, topic_id);
        let db = storage::global();
        let is_stream = config.stream.unwrap_or(false);
        let model = db
            .get_model(model_id)?
            .ok_or_else(|| ProxyError::Internal(format!("cannot find model: {}", model_id)))?;
        let adaptor = get_chat_adaptor(model.adaptor);
        let messages = db.list_chat_messages_by_topic(topic_id)?;
        // TODO: ContentType::Text和content放入消息上下文中
        let chat_messages = filter_context(messages);
        let provider = db.get_provider(model.provider_id)?.ok_or_else(|| {
            ProxyError::Internal(format!("cannot find provider: {}", model.provider_id))
        })?;
        let base_url = provider.base_url.ok_or_else(|| {
            ProxyError::Internal(format!(
                "base_url is not configured for provider: {}",
                model.provider_id
            ))
        })?;
        let api_key = db
            .get_credentials_by_provider(model.provider_id)?
            .first()
            .map(|credent| credent.key.clone())
            .ok_or_else(|| {
                ProxyError::Internal(format!("no credentials found for model: {}", model.name))
            })?;
        let endpoint = model
            .endpoint
            .unwrap_or_else(|| adaptor::get_default_endpoint(model.adaptor));
        let req_body = adaptor.build_request(&content, &config, &chat_messages)?;
        if is_stream {
            let response = forward::request_sse(
                Url::parse(&base_url)?.join(&endpoint)?.as_str(),
                Method::POST,
                |req| req.json(&req_body).bearer_auth(api_key),
            )
            .await?;
            let stream = forward::handle_stream(response);
            pin!(stream);
            while let Some(result) = stream.next().await {
                match result {
                    Ok(bytes) => {
                        let result = adaptor.parse_stream_response(bytes)?;
                        // yield Ok(result);
                    }
                    Err(err) => yield Err(err),
                };
            }
            // let response = adaptor.parse_response(res)?;
        } else {
            let response = forward::request(
                Url::parse(&base_url)?.join(&endpoint)?.as_str(),
                Method::POST,
                |req| req.json(&req_body).bearer_auth(api_key),
            )
            .await?;
            let res = forward::handle_response(response).await?;
            let response = adaptor.parse_response(res)?;
            yield Ok(response);
        }
    }
}
