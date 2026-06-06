use super::context;
use super::events::ChatEvent;
use super::function_call::{
    build_tools_from_mcp, execute_tool_calls, filter_tool_calls, merge_approved_tools,
    resume_tool_approval,
};
use super::rule::{apply_json_rule, build_rule};
use crate::error::{CoreError, Result};
use crate::models::{
    Credentials, JsonRule, McpServerParam, Message as CoreMessage, Model, Provider, Topic,
};
use crate::storage::Storage;
use async_stream::{stream, try_stream};
use futures::{Stream, StreamExt};
use std::pin::pin;
use wind_ai::chat::{ResEventStatus, build_request, handle_chat};
use wind_ai::message::{Content, Message as AiMessage, ReqConfig, Role};
use wind_ai::model::Model as AiModel;
use wind_ai::provider::adaptor::{ChatAdaptor, get_chat_adaptor};
use wind_ai::tool::Tools;
use wind_mcp::client::registry::RegistryHandle;
use wind_rule::RuleSet;

struct Context {
    topic: Topic,
    model: Model,
    provider: Provider,
    req_config: ReqConfig,
    credential: Credentials,
    rule_set: Option<JsonRule>,
    tools: Option<Vec<Tools>>,
    approved_tools: Option<Vec<String>>,
}

pub struct ChatEngine<'c> {
    mcp_registry: RegistryHandle,
    storage: &'c Storage,
}

impl<'c> ChatEngine<'c> {
    pub fn new(mcp_registry: RegistryHandle, storage: &'c Storage) -> Self {
        Self {
            mcp_registry,
            storage,
        }
    }
    /// 获取必要的对话请求信息.
    async fn load_info(&self, topic_id: i64, model_id: i64) -> Result<Context> {
        // TODO: 一次性联合查询出所有？
        let topic = self
            .storage
            .topic()
            .get_topic(topic_id)
            .await?
            .ok_or_else(|| {
                CoreError::NotFound(format!("Cannot find a topic. topic_id: {}", topic_id))
            })?;
        let mut model = self.storage.model().get(model_id).await?.ok_or_else(|| {
            CoreError::NotFound(format!("Cannot find a model. model_id: {}", model_id))
        })?;
        let rule_set = self
            .storage
            .provider()
            .get_json_rule(model.provider_id, model.adaptor)
            .await?;

        model.endpoint = model.endpoint.filter(|e| !e.is_empty());
        let provider = self
            .storage
            .provider()
            .get(model.provider_id)
            .await?
            .ok_or_else(|| {
                CoreError::NotFound(format!(
                    "Cannot find a provider. provider_id: {}",
                    model.provider_id
                ))
            })?;

        // TODO: 支持用户自选账户
        let credentials = self
            .storage
            .provider()
            .get_provider_credentials(model.provider_id)
            .await?;
        let credential = credentials.into_iter().next().ok_or_else(|| {
            CoreError::NotFound(format!("no credentials for provider {}", model.provider_id))
        })?;

        let tool_params = self.get_topic_tools(&topic).await?;
        let approved_tools = merge_approved_tools(&topic, tool_params.as_deref());
        let tools = match tool_params {
            Some(params) => Some(build_tools_from_mcp(
                self.mcp_registry
                    .list_tools_by_names(
                        &params.into_iter().map(|p| p.name).collect::<Vec<String>>(),
                    )
                    .await?
                    .as_slice(),
            )),
            _ => None,
        };

        let req_config = self
            .storage
            .topic()
            .get_chat_config(topic_id)
            .await?
            .map(|c| c.data)
            .unwrap_or_else(|| {
                log::warn!(
                    "Cannot find a chat config, use a default value. topic_id: {}",
                    topic_id
                );
                ReqConfig::default()
            });

        Ok(Context {
            topic,
            model,
            provider,
            req_config,
            rule_set,
            credential,
            tools,
            approved_tools,
        })
    }
    /// 获取 topic 下详细的MCP服务信息
    async fn get_topic_tools(&self, topic: &Topic) -> Result<Option<Vec<McpServerParam>>> {
        let ids = match topic.mcp_server_ids {
            Some(ref ids) if !ids.is_empty() => ids,
            _ => return Ok(None),
        };
        let params = self.storage.mcp().batch_get_by_ids(ids.as_slice()).await?;
        if params.is_empty() {
            return Ok(None);
        }
        Ok(Some(params))
    }
    async fn get_raw_messages(&self, topic_id: i64) -> Result<Vec<CoreMessage>> {
        Ok(self
            .storage
            .message()
            .list_by_topic(topic_id)
            .await?
            .into_iter()
            .filter(|m| !m.is_excluded)
            .collect::<Vec<CoreMessage>>())
    }
    async fn start_prepare(
        &self,
        topic_id: i64,
        user_message_id: i64,
        message_id: i64,
    ) -> Result<impl Stream<Item = ChatEvent>> {
        let mut assistant = match self.storage.message().get(message_id).await? {
            Some(m) => {
                if m.topic_id != topic_id {
                    return Err(CoreError::Chat(format!("Message is not in current topic")));
                }
                m
            }
            None => {
                return Err(CoreError::Chat(format!(
                    "Message not found. messageId: {}",
                    message_id
                )));
            }
        };
        let ctx = self.load_info(topic_id, assistant.model_id).await?;
        let mut contexts = context::build_chat_context(
            self.get_raw_messages(topic_id).await?,
            topic_id,
            user_message_id,
            ctx.topic.max_context,
        )?;
        match assistant.content.iter().last() {
            Some(msg) if msg.is_simple() => {
                return Err(CoreError::Chat("Chat completed".to_string()));
            }
            Some(msg) if msg.is_tool_request() || msg.is_tool_result() => {
                // FIXME: 用户审批了空的工具名或者审批了无关的工具名，会导致无限循环
                resume_tool_approval(&self.mcp_registry, &mut assistant, &mut contexts).await?;
                return self.start_chat(ctx, assistant, contexts).await;
            }
            // 未知的消息类型
            Some(msg) => {
                let err_str = format!("Unknown message type. messageId: {}", message_id);
                log::warn!("{}, message: {}", err_str, msg);
                return Err(CoreError::Chat(err_str));
            }
            // 开始新对话
            None => {
                return self.start_chat(ctx, assistant, contexts).await;
            }
        }
    }
    // 发起新的对话请求
    //
    // 如果对话存在工具调用，且即将调用的函数没有被允许自动执行，
    // 对话会立即停止且当前状态被保存。
    // 当用户手动同意此轮对话中工具调用请求后并且再次
    // 调用该方法时对话将会继续进行。
    //
    // - `from_message_id` 为用户消息的 id
    // - `message_id` 为此次对话消息 id
    async fn start_chat(
        &self,
        ctx: Context,
        mut assistant: CoreMessage,
        mut contexts: Vec<AiMessage>,
    ) -> Result<impl Stream<Item = ChatEvent>> {
        let assistant_id = assistant.id;
        let rule = build_rule(ctx.rule_set.as_ref())?;
        let ai_model = AiModel {
            name: ctx.model.name.clone(),
            adaptor: ctx.model.adaptor,
            endpoint: ctx.model.endpoint.clone(),
        };
        let chat_adaptor = get_chat_adaptor(ctx.model.adaptor);

        let mut iter_index = 0;
        let mut error_obj: Option<CoreError> = None;
        let mut msg: Option<AiMessage> = None;
        let mut break_event: Option<ChatEvent> = None;
        let stream = stream! {
            yield ChatEvent::created(assistant_id);
            loop {
                {
                    let forward = pin!(self.forward_stream(
                        chat_adaptor.as_ref(),
                        &ai_model,
                        &ctx,
                        contexts.as_slice(),
                        rule.as_ref(),
                    ));
                    for await value in forward {
                        match value {
                            Ok((is_finished, value)) => {
                                if is_finished {
                                    msg = Some(value);
                                    log::debug!("[start_chat] finished, msg:\n{:#?}", msg);
                                } else {
                                    yield ChatEvent::Partial {
                                        index: iter_index,
                                        message_id: assistant_id,
                                        delta: value,
                                    };
                                }
                            }
                            Err(err) => {
                                error_obj = Some(err);
                                log::debug!("[start_chat] error:\n{:#?}", &error_obj);
                            }
                        }
                    }
                }
                if let Some(msg) = msg {
                    let tools = match msg.tool_calls {
                        Some(tools) if !tools.is_empty() => tools,
                        _ => {
                            assistant.append_content(&msg);
                            log::debug!("[start_chat] chat completed:\n{:#?}", &msg);
                            break;
                        }
                    };

                    let tool_request =
                        AiMessage::new_tool_request(tools.clone(), msg.reasoning_content);
                    log::debug!("[start_chat] append tool_request:\n{:#?}", &tool_request);
                    assistant.append_content(&tool_request);
                    contexts.push(tool_request.clone());
                    iter_index += 1;
                    yield ChatEvent::partial(iter_index, assistant_id, tool_request);

                    let (auto_approved, left) =
                        filter_tool_calls(ctx.approved_tools.as_deref(), &tools);
                    if auto_approved.is_empty() {
                        log::debug!("[start_chat] all tools need to be approved:\n{:#?}", &left);
                        break_event = Some(ChatEvent::await_tool_calls(assistant_id, &tools));
                        break;
                    }
                    // 执行 MCP 工具调用
                    let tool_call_result =
                        match execute_tool_calls(&self.mcp_registry, &auto_approved).await {
                            Ok(results) => results,
                            Err(e) => {
                                log::debug!("[start_chat] error exec tool_calls:\n{:#?}", &e);
                                error_obj = Some(e.into());
                                break;
                            }
                        };
                    iter_index += 1;
                    yield ChatEvent::partial(iter_index, assistant_id, tool_call_result.clone());
                    assistant.append_content(&tool_call_result);
                    contexts.push(tool_call_result);

                    // 等待用户审批
                    if !left.is_empty() {
                        log::debug!("[start_chat] need approved by user:\n{:#?}", &left);
                        break_event = Some(ChatEvent::await_tool_calls(assistant_id, &left));
                        break;
                    }
                } else {
                    break;
                }

                iter_index += 1;
                msg = None;
            } // loop end

            if let Some(event) = break_event {
                yield event;
                if let Err(e) = self
                    .storage
                    .message()
                    .update(assistant_id, assistant.into())
                    .await
                {
                    log::error!("[start_chat] error when saving break_event assistant:\n{:#?}", &e);
                    yield ChatEvent::finish(assistant_id, None, Some(e.into()));
                };
                return;
            }
            if let Some(error) = &error_obj {
                log::error!("[start_chat] error when handling chat:\n{:#?}", error);
                assistant.append_content(&AiMessage::new_simple(
                    Role::Assistant,
                    vec![Content::new_text(error.to_string())],
                    None,
                ));
            };
            if let Err(e) = self
                .storage
                .message()
                .update(assistant_id, assistant.clone().into())
                .await
            {
                log::error!("[start_chat] error when saving assistant:\n{:#?}", &e);
                error_obj = Some(e.into());
            };
            log::debug!("[start_chat] finish, assistant:\n{:#?}", assistant);
            yield ChatEvent::finish(assistant_id, Some(assistant.content), error_obj);
        };

        Ok(stream)
    }
    /// 请求数据
    ///
    /// 返回 (is_finished, stream)
    fn forward_stream(
        &self,
        chat_adaptor: &dyn ChatAdaptor,
        model: &AiModel,
        ctx: &Context,
        contexts: &[AiMessage],
        rule: Option<&RuleSet>,
    ) -> impl Stream<Item = Result<(bool, AiMessage)>> {
        try_stream! {
            let mut req_body = build_request(
                chat_adaptor,
                model,
                &ctx.req_config,
                contexts,
                ctx.tools.as_deref(),
            )?;

            apply_json_rule(
                rule,
                &mut req_body,
                ctx.model.adaptor,
                &ctx.provider.name,
                &ctx.model.name,
                ctx.model.endpoint.as_deref(),
            );

            let stream = handle_chat(
                chat_adaptor,
                &req_body,
                &ctx.provider.base_url,
                &ctx.credential.key,
                ctx.model.endpoint.as_deref(),
            );
            let mut stream = std::pin::pin!(stream);
            while let Some(res_event) = stream.next().await {
                match res_event.status {
                    ResEventStatus::Partial => {
                        if let Some(msg) = res_event.data {
                            yield (false, msg);
                        }
                    }
                    ResEventStatus::Finish => {
                        if let Some(msg_finish) = res_event.data {
                            yield (true, msg_finish);
                        }
                        break;
                    }
                    ResEventStatus::Error => {
                        let err = res_event
                            .error
                            .map(|e| e.into())
                            .unwrap_or_else(|| CoreError::Internal("Unknown chat error".to_string()));
                        Err(err)?;
                        break;
                    }
                }
            }
        }
    }
    pub fn start(
        &self,
        topic_id: i64,
        user_message_id: i64,
        message_id: i64,
    ) -> impl Stream<Item = ChatEvent> {
        stream! {
            let result = self
                .start_prepare(topic_id, user_message_id, message_id)
                .await;
            match result {
                Ok(stream) => {
                    let mut stream = std::pin::pin!(stream);
                    while let Some(event) = stream.next().await {
                        yield event;
                    }
                }
                Err(e) => {
                    yield ChatEvent::from_core_error(message_id, e);
                }
            }
        }
    }
}
