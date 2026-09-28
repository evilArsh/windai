use super::{
    events::ChatEvent,
    rule::{apply_json_rule, build_rule},
};
use crate::models::{JsonRule, Model};
use crate::models::{Provider, Topic};
use crate::{
    error::{CoreError, Result},
    models::Credentials,
};
use async_stream::{stream, try_stream};
use futures::{Stream, StreamExt};
use std::pin::{Pin, pin};
use wind_ai::message::{Content, Message, Role};
use wind_ai::model::Model as AiModel;
use wind_ai::provider::adapter::get_chat_adapter;
use wind_ai::tool::FunctionCall;
use wind_ai::{
    chat::{ResEventStatus, build_request, handle_chat},
    tool::Tools,
};
use wind_rule::RuleSet;

fn to_ai_model(model: &Model) -> Result<AiModel> {
    Ok(AiModel {
        name: model.name.clone(),
        adapter: model.adapter.clone(),
        endpoint: model.endpoint.clone(),
        config: model.config.as_ref().map(|c| c.to_json_obj()).transpose()?,
    })
}
#[derive(Clone, Debug)]
pub struct ChatContext {
    pub model: Model,
    pub provider: Provider,
    pub topic: Topic,
    pub credential: Credentials,
    pub rule_set: Option<JsonRule>,
    pub tools: Option<Vec<Tools>>,
}

pub fn run_chat<'a>(
    ctx: &'a ChatContext,
    contexts: Vec<Message>,
) -> Pin<Box<dyn Stream<Item = ChatEvent> + Send + 'a>> {
    if has_pending_calls(&contexts) {
        return Box::pin(stream! {
            yield ChatEvent::Finish{ contexts ,error:None};
        });
    }
    let rule = match build_rule(ctx.rule_set.as_ref()) {
        Ok(rule) => rule,
        Err(err) => {
            return Box::pin(stream! {
                yield ChatEvent::Finish {
                    contexts,
                    error: Some(err.to_string()),
                };
            });
        }
    };
    start_chat(ctx, contexts, rule)
}

fn start_chat<'a>(
    ctx: &'a ChatContext,
    contexts: Vec<Message>,
    rule: Option<RuleSet>,
) -> Pin<Box<dyn Stream<Item = ChatEvent> + Send + 'a>> {
    Box::pin(stream! {
        let mut error_obj: Option<CoreError> = None;
        {
            let forward = pin!(forward_stream(&ctx, &contexts, rule.as_ref(),));
            for await value in forward {
                match value {
                    Ok(Some(delta)) => {
                        // 非流式消息，返回一次 Partial 包含完整消息
                        yield ChatEvent::Partial { delta };
                    }
                    Ok(None) => {}
                    Err(err) => {
                        error_obj = Some(err);
                        log::debug!("[chat runner] error: {:#?}", &error_obj);
                    }
                }
            }
        }
        yield ChatEvent::Finish {
            contexts,
            error: error_obj.map(|error| error.to_string()),
        };
    })
}

fn forward_stream(
    ctx: &ChatContext,
    contexts: &[Message],
    rule: Option<&RuleSet>,
) -> impl Stream<Item = Result<Option<Message>>> {
    try_stream! {
        log::debug!(
            "[request body]\n[user_input]\n{},\n\n[config]\n{:#?}",
            contexts
                .last()
                .and_then(|c| Some(Content::arr_to_string(&c.content)))
                .unwrap_or(String::new()),
            ctx
        );
        let model = to_ai_model(&ctx.model)?;
        let chat_adapter = get_chat_adapter(model.adapter);
        let mut req_body = build_request(
            &*chat_adapter,
            &model,
            contexts,
            ctx.tools.as_deref(),
        )?;

        apply_json_rule(
            rule,
            &mut req_body,
            model.adapter,
            &ctx.provider.name,
            &model.name,
            model.endpoint.as_deref(),
        );

        let stream = handle_chat(
            &*chat_adapter,
            &req_body,
            &ctx.provider.base_url,
            &ctx.credential.key,
            model.endpoint.as_deref(),
        );
        let mut stream = std::pin::pin!(stream);
        while let Some(res_event) = stream.next().await {
            match res_event.status {
                ResEventStatus::Partial => {
                    yield res_event.data;
                }
                ResEventStatus::Finish => {
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

/// 本轮上下文中尚未执行完的工具调用
///
/// 只扫描最后一条用户简单消息之后的内容块：更早轮次里遗留的未执行调用属于历史，
/// 不能在新一轮里被重新执行。返回 `None` 表示本轮没有工具调用请求
///
/// 特殊情况是调用者不会一次性传递所有的请求调用结果：
///
/// ```text
/// [ 上一轮的 tool_result ]
/// [ tool_request 1:10 ] // 所有的调用请求, Message::tool_calls []
/// [ tool_result  1:4 ] // 所有或者部分的调用结果，Message::content []
/// [ tool_result  1:6 ] // 所有或者部分的调用结果
/// ```
pub(crate) fn pending_tool_calls(contexts: &[Message]) -> Option<Vec<&FunctionCall>> {
    let start = contexts
        .iter()
        .rposition(|m| m.is_simple() && m.role == Role::User)
        .map_or(0, |last_user| last_user + 1);

    let mut executed_ids: Vec<&str> = vec![];
    for msg in contexts[start..].iter().rev() {
        if msg.is_tool_result() {
            for content in &msg.content {
                if let Content::FunctionCall { data } = content {
                    executed_ids.push(&data.id);
                }
            }
        }
        if msg.is_tool_request()
            && let Some(all_calls) = msg.tool_calls.as_ref()
        {
            return Some(
                all_calls
                    .iter()
                    .filter(|call| !executed_ids.contains(&call.id.as_str()))
                    .collect(),
            );
        }
    }
    None
}

/// 判断上下文是否还有未执行完的工具调用
fn has_pending_calls(contexts: &[Message]) -> bool {
    pending_tool_calls(contexts).is_some_and(|calls| !calls.is_empty())
}
