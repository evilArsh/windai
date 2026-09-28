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

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use wind_ai::tool::FunctionCallOutput;

    fn call(id: &str) -> FunctionCall {
        FunctionCall {
            id: id.to_string(),
            name: "tool".to_string(),
            arguments: "{}".to_string(),
        }
    }

    fn output(id: &str) -> FunctionCallOutput {
        FunctionCallOutput {
            id: id.to_string(),
            content: json!({"ok": true}),
        }
    }

    /// 构造一条 tool_request 消息（role=Assistant + tool_calls）
    fn req(calls: &[&str]) -> Message {
        Message::new_tool_request(calls.iter().map(|id| call(id)).collect(), None)
    }

    /// 构造一条 tool_result 消息（role=Tool，全部为函数调用结果）
    fn result(ids: &[&str]) -> Message {
        Message::new_tool_result(ids.iter().map(|id| output(id)).collect())
    }

    /// 取待处理调用的 id 列表，便于断言
    fn pending_ids(content: &[Message]) -> Vec<String> {
        pending_tool_calls(content)
            .expect("tool request")
            .into_iter()
            .map(|call| call.id.clone())
            .collect()
    }

    #[test]
    fn pending_returns_all_calls_when_none_executed() {
        // 无任何 tool_result，全部调用待执行，顺序保持请求原始顺序
        let content = vec![req(&["id1", "id2", "id3"])];
        assert_eq!(pending_ids(&content), vec!["id1", "id2", "id3"]);
    }

    #[test]
    fn pending_filters_out_executed_calls() {
        // 请求了 3 个调用，只有 id2 返回了结果，其余继续待执行
        let content = vec![req(&["id1", "id2", "id3"]), result(&["id2"])];
        assert_eq!(pending_ids(&content), vec!["id1", "id3"]);
    }

    #[test]
    fn pending_is_empty_when_all_executed() {
        // 结果分两条 tool_result 消息返回，executed_ids 跨消息累积
        let content = vec![req(&["id1", "id2"]), result(&["id1"]), result(&["id2"])];
        assert!(
            pending_tool_calls(&content)
                .expect("tool request")
                .is_empty()
        );
    }

    #[test]
    fn pending_reentrant_multi_round() {
        // 多轮完整对话后判断当前待执行的调用：
        // [旧轮 result] [reqA] [result 1-1] [reqB] [result 1-2]
        let content = vec![
            result(&["id_x"]),    // 旧轮结果，早于最近 req，不应计入
            req(&["id1", "id2"]), // 第一轮请求
            result(&["id1"]),     // 第一轮只执行了 id1
            req(&["id1", "id2"]), // 第二轮（模型重新发起的）请求
            result(&["id2"]),     // 第二轮执行了 id2
        ];
        // 只考虑最近 reqB 之后的 result（id2），id1 仍待执行
        assert_eq!(pending_ids(&content), vec!["id1"]);
    }

    #[test]
    fn pending_uses_most_recent_tool_request() {
        // 存在多个互不重叠的 tool_request 时，只处理最近的一条
        let content = vec![req(&["id_a"]), req(&["id_b"])];
        assert_eq!(pending_ids(&content), vec!["id_b"]);
    }

    #[test]
    fn pending_ignores_non_function_result_content() {
        // tool_result 判定只看 role；content 中混入的文本不参与 id 收集
        let tool_msg = Message {
            role: Role::Tool,
            content: vec![
                Content::new_text("tool internal note".to_string()),
                Content::new_function_call("id1".to_string(), json!({"ok": true})),
            ],
            tool_calls: None,
            reasoning_content: None,
            created_at: 0,
            input_tokens: 0,
            output_tokens: 0,
        };
        let content = vec![req(&["id1", "id2"]), tool_msg];
        assert_eq!(pending_ids(&content), vec!["id2"]);
    }

    #[test]
    fn pending_is_none_without_tool_request() {
        // 空内容 / 纯 simple 消息 / 只有 tool_result / 空 tool_calls 的 assistant
        let cases: Vec<Vec<Message>> = vec![
            vec![],
            vec![Message::new_simple(
                Role::User,
                vec![Content::new_text("hi".to_string())],
                None,
            )],
            vec![result(&["id1"])],
            vec![Message::new_tool_request(vec![], None)],
        ];
        for content in cases {
            assert!(
                pending_tool_calls(&content).is_none(),
                "expected no tool request, content: {content:?}"
            );
        }
    }

    /// 只扫描最后一条用户简单消息之后的内容块
    #[test]
    fn pending_ignores_calls_before_the_last_user_message() {
        let content = vec![
            Message::new_simple(Role::User, vec![Content::new_text("old".into())], None),
            req(&["stale"]),
            Message::new_simple(Role::User, vec![Content::new_text("new".into())], None),
        ];
        assert!(pending_tool_calls(&content).is_none());
    }
}
