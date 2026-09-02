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
                            yield ChatEvent::partial(assistant_id, value);
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
                        assistant.append_content(tool_request);
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
        assert!(ChatRunner::new()
            .find_pending_calls(&content)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn find_pending_reentrant_multi_round() {
        // 注释中的重入场景，多轮完整对话后判断当前待执行的调用：
        // [旧轮 result] [reqA] [result 1-1] [reqB] [result 1-2]
        let content = vec![
            result(&["id_x"]),     // 旧轮结果，早于最近 req，不应计入
            req(&["id1", "id2"]),  // 第一轮请求
            result(&["id1"]),      // 第一轮只执行了 id1
            req(&["id1", "id2"]),  // 第二轮（模型重新发起的）请求
            result(&["id2"]),      // 第二轮执行了 id2
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
