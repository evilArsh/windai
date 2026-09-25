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
use wind_ai::message::{Content, Message as AiMessage, Role};
use wind_ai::model::Model as AiModel;
use wind_ai::provider::adapter::get_chat_adapter;
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
    mut contexts: Vec<AiMessage>,
) -> Pin<Box<dyn Stream<Item = ChatEvent> + Send + 'a>> {
    if has_pending_calls(&contexts) {
        return Box::pin(stream! {
            yield ChatEvent::AwaitToolCall{ contexts };
        });
    }
    let rule = match build_rule(ctx.rule_set.as_ref()) {
        Ok(rule) => rule,
        Err(err) => {
            return Box::pin(stream! {
                contexts.push(AiMessage::new_simple(
                    Role::Assistant,
                    vec![Content::new_text(err.to_string())],
                    None,
                ));
                yield ChatEvent::Finish {
                    contexts,
                    error: true,
                };
            });
        }
    };

    start_chat(ctx, rule, contexts)
}

fn start_chat<'a>(
    ctx: &'a ChatContext,
    rule: Option<RuleSet>,
    mut contexts: Vec<AiMessage>,
) -> Pin<Box<dyn Stream<Item = ChatEvent> + Send + 'a>> {
    Box::pin(stream! {
        let mut error_obj: Option<CoreError> = None;
        let mut msg = AiMessage::default();
        {
            let forward = pin!(forward_stream(
                &ctx,
                contexts.as_slice(),
                rule.as_ref(),
            ));
            for await value in forward {
                match value {
                    Ok(Some(delta)) => {
                        // 非流式消息，返回一次 Partial 包含完整消息
                        msg.append_chunk(&delta);
                        yield ChatEvent::Partial { delta };
                    }
                    Ok(None) => {}
                    Err(err) => {
                        error_obj = Some(err);
                        log::debug!("[llm_loop] error: {:#?}", &error_obj);
                    }
                }
            }
        }
        if let Some(error) = &error_obj {
            contexts.push(AiMessage::new_simple(
                Role::Assistant,
                vec![Content::new_text(error.to_string())],
                None,
            ));
            yield ChatEvent::Finish {
                contexts,
                error: true,
            };
        } else {
            let has_tool_calls = msg.tool_calls.as_ref().is_some_and(|t| !t.is_empty());
            contexts.push(msg);
            if has_tool_calls {
                yield ChatEvent::AwaitToolCall { contexts };
            } else {
                yield ChatEvent::Finish {
                    contexts,
                    error: false,
                };
            }
        }
    })
}

fn forward_stream(
    ctx: &ChatContext,
    contexts: &[AiMessage],
    rule: Option<&RuleSet>,
) -> impl Stream<Item = Result<Option<AiMessage>>> {
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

fn has_pending_calls(contexts: &[AiMessage]) -> bool {
    let mut res = 0;
    let mut req = 0;
    for msg in contexts.iter().rev() {
        if msg.is_tool_result() {
            res += msg.content.len();
        }
        if msg.is_tool_request() {
            req = msg.tool_calls.as_ref().map_or(0, |t| t.len());
            return req > res;
        }
    }
    return req > res;
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
    fn req(calls: &[&str]) -> AiMessage {
        AiMessage::new_tool_request(calls.iter().map(|id| call(id)).collect(), None)
    }

    /// 构造一条 tool_result 消息（role=Tool，全部为函数调用结果）
    fn result(ids: &[&str]) -> AiMessage {
        AiMessage::new_tool_result(ids.iter().map(|id| output(id)).collect())
    }

    /// 取待处理调用的 id 列表，便于断言
    fn pending_ids(content: &[AiMessage]) -> Vec<String> {
        ChatRunner::new()
            .find_pending_calls(content)
            .unwrap()
            .iter()
            .map(|c| c.id.clone())
            .collect()
    }

    #[test]
    fn find_pending_returns_all_calls_when_none_executed() {
        // 无任何 tool_result，全部调用待执行，顺序保持请求原始顺序
        let content = vec![req(&["id1", "id2", "id3"])];
        assert_eq!(pending_ids(&content), vec!["id1", "id2", "id3"]);
    }

    #[test]
    fn find_pending_filters_out_executed_calls() {
        // 请求了 3 个调用，只有 id2 返回了结果，其余继续待执行
        let content = vec![req(&["id1", "id2", "id3"]), result(&["id2"])];
        assert_eq!(pending_ids(&content), vec!["id1", "id3"]);
    }

    #[test]
    fn find_pending_returns_empty_when_all_executed() {
        // 结果分两条 tool_result 消息返回，executed_ids 跨消息累积
        let content = vec![req(&["id1", "id2"]), result(&["id1"]), result(&["id2"])];
        assert!(
            ChatRunner::new()
                .find_pending_calls(&content)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn find_pending_reentrant_multi_round() {
        // 注释中的重入场景，多轮完整对话后判断当前待执行的调用：
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
    fn find_pending_uses_most_recent_tool_request() {
        // 存在多个互不重叠的 tool_request 时，只处理最近的一条
        let content = vec![req(&["id_a"]), req(&["id_b"])];
        assert_eq!(pending_ids(&content), vec!["id_b"]);
    }

    #[test]
    fn find_pending_ignores_non_function_result_content() {
        // tool_result 判定只看 role；content 中混入的文本不参与 id 收集
        let tool_msg = AiMessage {
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
    fn find_pending_errors_when_no_tool_request() {
        let runner = ChatRunner::new();
        // 空内容 / 纯 simple 消息 / 只有 tool_result / 空 tool_calls 的 assistant
        let cases: Vec<Vec<AiMessage>> = vec![
            vec![],
            vec![AiMessage::new_simple(
                Role::User,
                vec![Content::new_text("hi".to_string())],
                None,
            )],
            vec![result(&["id1"])],
            vec![AiMessage::new_tool_request(vec![], None)],
        ];
        for content in cases {
            let err = runner.find_pending_calls(&content).unwrap_err();
            assert!(
                err.to_string().contains("No tool_request"),
                "unexpected error: {err}"
            );
        }
    }
}
