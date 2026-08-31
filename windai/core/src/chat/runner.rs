use super::{
    events::ChatEvent,
    rule::{apply_json_rule, build_rule},
};
use crate::models::Provider;
use crate::models::{JsonRule, Message as CoreMessage, Model};
use crate::{
    error::{CoreError, Result},
    models::Credentials,
};
use async_stream::{stream, try_stream};
use futures::{Stream, StreamExt};
use std::collections::HashSet;
use std::pin::{Pin, pin};
use wind_ai::message::{Content, Message as AiMessage, ReqConfig, Role};
use wind_ai::provider::adapter::{ChatAdapter, get_chat_adapter};
use wind_ai::tool::FunctionCall;
use wind_ai::{
    chat::{ResEventStatus, build_request, handle_chat},
    tool::Tools,
};
use wind_rule::RuleSet;

#[derive(Clone, Debug)]
pub struct ChatContext {
    pub model: Model,
    pub provider: Provider,
    pub credential: Credentials,
    pub req_config: ReqConfig,
    pub rule_set: Option<JsonRule>,
    pub tools: Option<Vec<Tools>>,
}
pub struct ChatRunner {}

impl ChatRunner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn run<'a>(
        &self,
        ctx: &'a ChatContext,
        assistant: CoreMessage,
        contexts: Vec<AiMessage>,
    ) -> Pin<Box<dyn Stream<Item = ChatEvent> + Send + 'a>> {
        match assistant.content.iter().last() {
            Some(msg) if msg.is_simple() => {
                return Box::pin(stream! {
                    yield ChatEvent::finish(assistant, contexts, Some(CoreError::Chat("Chat completed".to_string())));
                });
            }
            Some(msg) if msg.is_tool_request() || msg.is_tool_result() => {
                match self.find_pending_calls(&assistant.content) {
                    Ok(pending) => {
                        if !pending.is_empty() {
                            return Box::pin(stream! {
                                yield ChatEvent::await_tool_calls(assistant, contexts, pending);
                            });
                        }
                    }
                    Err(err) => {
                        return Box::pin(stream! {
                            yield ChatEvent::finish(assistant, contexts, Some(err));
                        });
                    }
                }
            }
            Some(msg) => {
                let err_str = format!("Unknown message type. messageId: {}", assistant.id);
                log::warn!("{}, message: {:#?}", err_str, msg);
                return Box::pin(stream! {
                    yield ChatEvent::finish(assistant, contexts, Some(CoreError::Chat(err_str)));
                });
            }
            None => {}
        }

        let rule = match build_rule(ctx.rule_set.as_ref()) {
            Ok(rule) => rule,
            Err(err) => {
                return Box::pin(stream! {
                    yield ChatEvent::finish(assistant, contexts, Some(err));
                });
            }
        };

        self.start_chat(ctx, rule, assistant, contexts)
    }

    fn start_chat<'a>(
        &self,
        ctx: &'a ChatContext,
        rule: Option<RuleSet>,
        mut assistant: CoreMessage,
        mut contexts: Vec<AiMessage>,
    ) -> Pin<Box<dyn Stream<Item = ChatEvent> + Send + 'a>> {
        Box::pin(stream! {
            let assistant_id = assistant.id;
            let chat_adapter = get_chat_adapter(ctx.model.adapter);
            let mut error_obj: Option<CoreError> = None;
            let mut msg = AiMessage::default();
            {
                let forward = pin!(Self::forward_stream(
                    chat_adapter.as_ref(),
                    ctx,
                    contexts.as_slice(),
                    rule.as_ref(),
                ));
                for await value in forward {
                    match value {
                        Ok(Some(value)) => {
                            msg.append_chunk(value.clone());
                            yield ChatEvent::Partial {
                                message_id: assistant_id,
                                delta: value,
                            };
                        }
                        Ok(None) => {}
                        Err(err) => {
                            error_obj = Some(err);
                            log::debug!("[llm_loop] error:\n{:#?}", &error_obj);
                        }
                    }
                }
            }
            if let Some(error) = &error_obj {
                let msg = AiMessage::new_simple(
                    Role::Assistant,
                    vec![Content::new_text(error.to_string())],
                    None,
                );
                contexts.push(msg.clone());
                assistant.append_content(msg);
                yield ChatEvent::finish(assistant, contexts, error_obj);
            } else {
                match msg.tool_calls {
                    Some(tools) if !tools.is_empty() => {
                        let tool_request =
                            AiMessage::new_tool_request(tools.clone(), msg.reasoning_content);
                        contexts.push(tool_request.clone());
                        assistant.append_content(tool_request.clone());
                        yield ChatEvent::partial(assistant_id, tool_request);
                        yield ChatEvent::await_tool_calls(assistant, contexts, tools);
                    }
                    _ => {
                        contexts.push(msg.clone());
                        assistant.append_content(msg);
                        yield ChatEvent::finish(assistant, contexts, None);
                    }
                };
            }
        })
    }

    fn forward_stream(
        chat_adapter: &dyn ChatAdapter,
        ctx: &ChatContext,
        contexts: &[AiMessage],
        rule: Option<&RuleSet>,
    ) -> impl Stream<Item = Result<Option<AiMessage>>> {
        try_stream! {
            log::debug!(
                "[request body]\n[user_input]\n{},\n\n[config]\n{:#?}",
                contexts.last().and_then(|c|Some(Content::arr_to_string(&c.content))).unwrap_or(String::new()),
                ctx
            );
            let mut req_body = build_request(
                chat_adapter,
                &ctx.model.name,
                &ctx.req_config,
                contexts,
                ctx.tools.as_deref(),
            )?;

            apply_json_rule(
                rule,
                &mut req_body,
                ctx.model.adapter,
                &ctx.provider.name,
                &ctx.model.name,
                ctx.model.endpoint.as_deref(),
            );

            let stream = handle_chat(
                chat_adapter,
                &req_body,
                &ctx.provider.base_url,
                &ctx.credential.key,
                ctx.model.endpoint.as_deref(),
            );
            let mut stream = std::pin::pin!(stream);
            // let mut msg = AiMessage::default();
            while let Some(res_event) = stream.next().await {
                match res_event.status {
                    ResEventStatus::Partial => {
                        // if let Some(partial_msg) = res_event.data {
                        //     msg.append_chunk(partial_msg);
                        //     yield (false, Some(msg));
                        // }
                        // yield (false, res_event.data);
                        yield res_event.data;
                    }
                    ResEventStatus::Finish => {
                        // if let Some(msg_finish) = res_event.data {
                        //     yield (true, msg_finish);
                        // }
                        // yield (true, None);
                        yield None;
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
    /// 找出待处理的工具调用
    /// ```text
    /// [ 上一轮的tool_result ]
    /// [ 截断 ]
    /// [ tool_request 1 ] // 包含所有的请求数组
    /// [ tool_result  1-1 ]
    /// [ tool_result  1-2 ]
    /// ```
    /// [ tool_request ]
    fn find_pending_calls(&self, content: &[AiMessage]) -> Result<Vec<FunctionCall>> {
        let mut executed_ids: HashSet<&str> = HashSet::new();

        for msg in content.iter().rev() {
            if msg.is_tool_result() {
                for c in &msg.content {
                    if let Content::FunctionCall { data } = c {
                        executed_ids.insert(&data.id);
                    }
                }
            }

            if msg.is_tool_request() {
                let all_calls = msg
                    .tool_calls
                    .as_ref()
                    .ok_or_else(|| CoreError::Chat("tool_request without tool_calls".into()))?;

                return Ok(all_calls
                    .iter()
                    .filter(|c| !executed_ids.contains(c.id.as_str()))
                    .cloned()
                    .collect());
            }
        }

        Err(CoreError::Chat(
            "No tool_request found in assistant content".into(),
        ))
    }
}
