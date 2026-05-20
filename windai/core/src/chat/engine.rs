use std::sync::Arc;

use futures::{StreamExt, try_join};
use sqlx::SqlitePool;
use wind_ai::chat::{self as wind_ai_chat, ResEventStatus, handle_chat};
use wind_ai::message::{Message as AiMessage, ReqConfig};
use wind_ai::model::Model as AiModel;
use wind_ai::provider::adaptor::get_chat_adaptor;
use wind_ai::tool::Tools;
use wind_js::JsEngine;
use wind_mcp::client::registry::RegistryHandle;

use super::context;
use super::events::ChatEvent;
use super::function_call::{build_tools_from_mcp, execute_function_calls};
use super::js_hook::{apply_js_hook, lookup_js_hook};
use crate::error::{CoreError, Result};
use crate::models::{Message as CoreMessage, Model, Provider, Topic};
use crate::storage::message::service::MessageService;
use crate::storage::model::service::ModelService;
use crate::storage::provider::service::ProviderService;
use crate::storage::topic::service::TopicService;

pub struct ChatEngine {
    db: SqlitePool,
    js_engine: Arc<JsEngine>,
    mcp_registry: RegistryHandle,
}

impl ChatEngine {
    pub fn new(db: SqlitePool, js_engine: Arc<JsEngine>, mcp_registry: RegistryHandle) -> Self {
        Self {
            db,
            js_engine,
            mcp_registry,
        }
    }

    /// 获取必要的聊天请求信息.
    async fn load_chat_context(
        &self,
        topic_svc: &TopicService,
        model_svc: &ModelService,
        provider_svc: &ProviderService,
        topic_id: i64,
        model_id: i64,
    ) -> Result<(Topic, Model, Provider, ReqConfig, String)> {
        let (topic, mut model) = try_join!(
            async {
                topic_svc.get_topic(topic_id).await?.ok_or_else(|| {
                    CoreError::NotFound(format!("Cannot find a topic. topic_id: {}", topic_id))
                })
            },
            async {
                model_svc.get(model_id).await?.ok_or_else(|| {
                    CoreError::NotFound(format!("Cannot find a model. model_id: {}", model_id))
                })
            }
        )?;
        model.endpoint = model.endpoint.filter(|e| !e.is_empty());
        let provider = provider_svc.get(model.provider_id).await?.ok_or_else(|| {
            CoreError::NotFound(format!(
                "Cannot find a provider. provider_id: {}",
                model.provider_id
            ))
        })?;

        let credentials = provider_svc.list_credentials(model.provider_id).await?;
        if credentials.is_empty() {
            return Err(CoreError::NotFound(format!(
                "no credentials for provider {}",
                model.provider_id
            )));
        }
        let req_config = topic_svc
            .get_chat_config(topic_id)
            .await?
            .and_then(|opt| Some(opt))
            .map(|c| c.data)
            .unwrap_or_else(|| {
                log::warn!(
                    "Cannot find a chat config, use a default value. topic_id: {}",
                    topic_id
                );
                ReqConfig::default()
            });

        // TODO: 支持用户自选账户
        let api_key = credentials[0].key.clone();

        Ok((topic, model, provider, req_config, api_key))
    }

    /// 获取某个 topic 下可用的 MCP 工具
    async fn get_topic_tools(
        &self,
        topic_svc: &TopicService,
        topic_id: i64,
    ) -> Result<Option<Vec<Tools>>> {
        let params = topic_svc.list_mcp_servers(topic_id).await?;
        if params.is_empty() {
            return Ok(None);
        }
        let names = params.into_iter().map(|p| p.name).collect::<Vec<String>>();
        let mcp_tools = self.mcp_registry.list_tools_by_names(&names).await?;
        if mcp_tools.is_empty() {
            return Ok(None);
        }

        Ok(Some(build_tools_from_mcp(&mcp_tools)))
    }

    /// 发起对话请求
    ///
    /// - `from_message_id` 为用户消息的 id
    /// - `message_id` 为此次对话消息 id
    pub fn send(
        &self,
        topic_id: i64,
        model_id: i64,
        from_message_id: i64,
        message_id: i64,
    ) -> impl futures::Stream<Item = ChatEvent> {
        async_stream::stream! {
            let result = self
                .send_stream_impl(topic_id, model_id, from_message_id, message_id)
                .await;
            match result {
                Ok(stream) => {
                    let mut stream = std::pin::pin!(stream);
                    while let Some(event) = stream.next().await {
                        yield event;
                    }
                }
                Err(e) => {
                    yield ChatEvent::finish(message_id, None, Some(e.into()));
                }
            }
        }
    }

    async fn send_stream_impl(
        &self,
        topic_id: i64,
        model_id: i64,
        from_message_id: i64,
        message_id: i64,
    ) -> Result<impl futures::Stream<Item = ChatEvent>> {
        let topic_svc = TopicService::new(self.db.clone());
        let model_svc = ModelService::new(self.db.clone());
        let provider_svc = ProviderService::new(self.db.clone());
        let msg_svc = MessageService::new(self.db.clone());

        let (topic, model, provider, req_config, api_key) = self
            .load_chat_context(&topic_svc, &model_svc, &provider_svc, topic_id, model_id)
            .await?;

        // 聊天上下文，包含用户输入的消息, 和此次对话的消息
        let messages = msg_svc
            .list_by_topic(topic_id)
            .await?
            .into_iter()
            .filter(|m| !m.is_excluded)
            .collect::<Vec<CoreMessage>>();
        let (mut core_msg, mut contexts) = context::build_chat_context(
            messages,
            topic_id,
            message_id,
            from_message_id,
            topic.max_context,
        )?;

        let tools = self.get_topic_tools(&topic_svc, topic_id).await?;
        let mcp_client = self.mcp_registry.clone();
        let base_url = provider.base_url.clone();
        let endpoint = model.endpoint.clone();
        let ai_model = AiModel {
            name: model.name.clone(),
            adaptor: model.adaptor,
            endpoint: endpoint.clone(),
        };
        let provider_name = provider.name;
        let provider_id = model.provider_id;
        let model_name = model.name.clone();
        let adaptor_type = model.adaptor;
        let js_hook = lookup_js_hook(&provider_svc, provider_id, adaptor_type).await?;
        let js_engine = self.js_engine.clone();

        let stream = async_stream::stream! {
            yield ChatEvent::created(message_id);
            let mut msg = AiMessage::default();
            let mut iter_index = 0;
            let chat_adaptor = get_chat_adaptor(adaptor_type);
            let mut has_error = false;
            let mut error_obj: Option<CoreError> = None;
            loop {
                let mut req_body = match wind_ai_chat::build_request(
                    chat_adaptor.as_ref(),
                    &ai_model,
                    &req_config,
                    &contexts,
                    tools.as_ref(),
                ) {
                    Ok(req_body) => req_body,
                    Err(e) => {
                        error_obj = Some(e.into());
                        break;
                    }
                };

                req_body = match apply_js_hook(
                    &*js_engine,
                    js_hook.as_ref(),
                    req_body,
                    &provider_name,
                    &model_name,
                    endpoint.as_deref(),
                )
                .await
                {
                    Ok(req_body) => req_body,
                    Err(e) => {
                        error_obj = Some(e.into());
                        break;
                    }
                };

                let stream = handle_chat(
                    chat_adaptor.as_ref(),
                    &req_body,
                    &base_url,
                    &api_key,
                    endpoint.as_deref(),
                );
                let mut stream = std::pin::pin!(stream);
                while let Some(res_event) = stream.next().await {
                    match res_event.status {
                        ResEventStatus::Partial => {
                            if let Some(msg) = res_event.data {
                                yield ChatEvent::partial(iter_index, core_msg.id, msg);
                            }
                        }
                        ResEventStatus::Finish => {
                            if let Some(msg_finish) = res_event.data {
                                msg = msg_finish;
                            }
                        }
                        ResEventStatus::Error => {
                            has_error = true;
                            error_obj = res_event.error.map(|e| Some(e.into())).unwrap_or_else(|| {
                                Some(CoreError::Internal(String::from("Unknown chat error")))
                            });
                            break;
                        }
                    }
                }
                if has_error {
                    break;
                }
                let tool_calls = match msg.tool_calls {
                    Some(tools) if !tools.is_empty() => tools,
                    _ => {
                        core_msg.append_content(&msg);
                        break;
                    }
                };
                let tool_request =
                    AiMessage::new_tool_request(tool_calls.clone(), msg.reasoning_content);
                core_msg.append_content(&tool_request);
                contexts.push(tool_request.clone());
                iter_index += 1;
                yield ChatEvent::partial(iter_index, message_id, tool_request);

                let tool_call_result = match execute_function_calls(&mcp_client, &tool_calls).await {
                    Ok(results) => results,
                    Err(e) => {
                        error_obj = Some(e.into());
                        break;
                    }
                };
                iter_index += 1;
                yield ChatEvent::partial(iter_index, message_id, tool_call_result.clone());
                core_msg.append_content(&tool_call_result);
                contexts.push(tool_call_result);

                iter_index += 1;
                msg = AiMessage::default();
            }
            if let Some(error) = &error_obj {
                log::error!("Error when handling chat. {}", error);
            };
            if !core_msg.content.is_empty() {
                if let Err(e) = msg_svc.update(core_msg.id, core_msg.clone().into()).await {
                    error_obj = Some(e.into());
                };
            }
            yield ChatEvent::finish(core_msg.id, Some(core_msg.content), error_obj);
        };

        Ok(stream)
    }
}
