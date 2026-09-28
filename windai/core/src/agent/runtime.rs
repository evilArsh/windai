use super::function_call::partition_tool_calls_by_policy;
use super::host::AgentHost;
use super::task::{TaskNotification, TaskSpec};
use super::tool::{self, AGENT_TOOL_PREFIX, SpawnAgentResponse};
use crate::chat::runner::pending_tool_calls;
use crate::chat::{ChatEvent, run_chat};
use crate::error::{CoreError, Result};
use crate::models::ToolApprovalStatus;
use futures::stream::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use wind_ai::message::{Content, Message, Role};
use wind_ai::tool::{FunctionCall, FunctionCallOutput};

struct ToolPlan {
    exec_mcp: Vec<FunctionCall>,
    exec_agent: Vec<FunctionCall>,
    denied: Vec<FunctionCall>,
    waiting: Vec<FunctionCall>,
}

impl std::fmt::Display for ToolPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[ToolPlan] (exec_mcp = {}, exec_agent = {}, denied = {} waiting = {})",
            self.exec_mcp.len(),
            self.exec_agent.len(),
            self.denied.len(),
            self.waiting.len()
        )
    }
}

#[derive(strum::AsRefStr)]
enum Action {
    Continue,
    Resume { contexts: Vec<Message> },
    Stop,
}

/// 找出本轮待处理的工具调用
fn find_pending_calls(contexts: &[Message]) -> Result<Vec<&FunctionCall>> {
    pending_tool_calls(contexts)
        .ok_or_else(|| CoreError::Chat("No tool_request found in assistant content".into()))
}

struct BlockState {
    /// partial 缓存块
    data: Message,
    /// 本次运行内的块序号
    index: i64,
}

impl BlockState {
    fn new() -> Self {
        Self {
            data: Message::default(),
            index: 0,
        }
    }
}

pub struct AgentRuntime {
    host: Arc<dyn AgentHost>,
    ctx: CancellationToken,
    task: TaskSpec,
}

impl AgentRuntime {
    pub fn new(ctx: CancellationToken, host: Arc<dyn AgentHost>, task: TaskSpec) -> Self {
        Self { ctx, host, task }
    }

    /// 开始对话
    pub async fn run(mut self) {
        let chat_context = &self.task.chat_context;
        let mut contexts = std::mem::take(&mut self.task.contexts);
        let mut block = BlockState::new();
        let mut auto_resume_count = 0usize;
        const MAX_AUTO_RESUME: usize = 32;
        loop {
            let mut stream = run_chat(chat_context, contexts);
            self.send_event(TaskNotification::Started {
                instance_id: self.task.instance.id,
            })
            .await;
            loop {
                tokio::select! {
                    biased;
                    _ = self.ctx.cancelled() => {
                        return;
                    }
                    Some(event) = stream.next() => {
                        let action = self.handle_chat_event(&mut block, event).await;
                        match action {
                            Action::Continue => {
                                // 内容块尚未结束
                            }
                            Action::Stop => return,
                            Action::Resume {
                                contexts: next_contexts,
                            } => {
                                auto_resume_count += 1;
                                if auto_resume_count > MAX_AUTO_RESUME {
                                    self.finish_with_error("max auto resume limit exceeded".to_string())
                                        .await;
                                    return;
                                }
                                contexts = next_contexts;
                                break;
                            }
                        }
                    }
                    else => {
                        return;
                    }
                }
            }
            log::debug!("next block index: {}", block.index);
        }
    }

    async fn finish_with_error(&self, error: String) {
        self.send_event(TaskNotification::Failed {
            instance_id: self.task.instance.id,
            message_id: self.task.message_id,
            error,
        })
        .await;
    }

    async fn finish(&self, content: Vec<Content>) {
        self.send_event(TaskNotification::Finish {
            instance_id: self.task.instance.id,
            message_id: self.task.message_id,
            content,
        })
        .await;
    }

    /// 拼接内容块，并且向上发送消息
    async fn push_chunk(&self, block: &mut BlockState, delta: Message) {
        block.data.append_chunk(&delta);
        self.send_event(TaskNotification::Message {
            message_id: self.task.message_id,
            instance_id: self.task.instance.id,
            index: block.index,
            delta,
            partial: true,
        })
        .await;
    }

    /// 结束当前内容块，递增块索引
    async fn close_block(&self, block: &mut BlockState) -> Message {
        let data = std::mem::take(&mut block.data);
        self.persist_block(block, data.clone()).await;
        data
    }

    async fn persist_block(&self, block: &mut BlockState, data: Message) {
        if self.ctx.is_cancelled() {
            return;
        }
        self.send_event(TaskNotification::Message {
            instance_id: self.task.instance.id,
            message_id: self.task.message_id,
            index: block.index,
            delta: data,
            partial: false,
        })
        .await;
        block.index += 1;
    }

    /// 执行工具调用和提交工具调用审批，存在以下情况
    ///
    /// 1. 新一轮对话需要执行调用和请求审批
    ///
    /// 2. 上一轮对话中工具审批完毕，处理审批结果
    async fn handle_await_tool_call(
        &self,
        block: &mut BlockState,
        contexts: &mut Vec<Message>,
        tools: Vec<FunctionCall>,
    ) -> Result<()> {
        let plan = self.make_tool_plan(tools).await?;
        let mut call_results: Vec<FunctionCallOutput> = vec![];
        log::debug!("{}", plan);
        // MCP 工具执行
        if !plan.exec_mcp.is_empty() {
            let tool_result = self.host.execute_tool_calls(&plan.exec_mcp).await?;
            call_results.extend(tool_result);
        }
        // Allowed 工具执行
        if !plan.exec_agent.is_empty() {
            let action_plan = tool::parse_agent_action(&plan.exec_agent)?;
            // 合并后的 list_agents 只查询一次
            if let Some(call_ids) = action_plan.list_agents {
                let response = self.host.list_agents().await?;
                let result_json = serde_json::to_value(&response)?;
                for call_id in call_ids {
                    call_results.push(FunctionCallOutput {
                        id: call_id,
                        content: result_json.clone(),
                    });
                }
            }
            let futures = action_plan.spawn_agents.into_iter().map(|action| {
                let host = self.host.clone();
                async move {
                    let call_id = action.call_id;
                    let result = host.spawn_agent(call_id, action.data).await?;
                    Ok::<SpawnAgentResponse, CoreError>(result)
                }
            });
            let results = futures::future::try_join_all(futures).await?;
            for result in results {
                call_results.push(FunctionCallOutput {
                    id: result.call_id,
                    content: Value::String(Content::arr_to_string(&result.output)),
                });
            }
        }
        // Denied 工具执行
        if !plan.denied.is_empty() {
            let tool_result = plan
                .denied
                .into_iter()
                .map(|call| FunctionCallOutput {
                    id: call.id,
                    content: serde_json::json!({
                        "error": "tool call denied",
                        "tool": call.name,
                    }),
                })
                .collect::<Vec<FunctionCallOutput>>();
            call_results.extend(tool_result);
        };

        if !call_results.is_empty() {
            let tool_result = Message::new_tool_result(call_results);
            contexts.push(tool_result.clone());
            self.persist_block(block, tool_result).await;
        }

        // 通知审批
        if !plan.waiting.is_empty() {
            self.send_event(TaskNotification::ApprovalRequired {
                calls: plan.waiting,
                message_id: self.task.message_id,
                instance_id: self.task.instance.id,
            })
            .await;
        }
        Ok(())
    }

    async fn handle_chat_event(&self, block: &mut BlockState, event: ChatEvent) -> Action {
        log::debug!("{}", event);
        match event {
            ChatEvent::Partial { delta } => {
                self.push_chunk(block, delta).await;
                Action::Continue
            }
            ChatEvent::Finish {
                mut contexts,
                error,
            } => {
                if let Some(error) = error {
                    self.push_chunk(
                        block,
                        Message::new_simple(
                            Role::Assistant,
                            vec![Content::new_text(error.clone())],
                            None,
                        ),
                    )
                    .await;
                    self.close_block(block).await;
                    self.finish_with_error(error).await;
                    return Action::Stop;
                }
                // 本轮内容结束，工具调用结果从下一块开始
                let message = self.close_block(block).await;
                let has_tool_calls = message
                    .tool_calls
                    .as_ref()
                    .is_some_and(|calls| !calls.is_empty());
                if !has_tool_calls {
                    self.finish(message.content).await;
                    return Action::Stop;
                }
                contexts.push(message);
                let pendings = match find_pending_calls(&contexts) {
                    Ok(tools) => tools.into_iter().cloned().collect::<Vec<FunctionCall>>(),
                    Err(err) => {
                        self.push_chunk(
                            block,
                            Message::new_simple(
                                Role::Assistant,
                                vec![Content::new_text(err.to_string())],
                                None,
                            ),
                        )
                        .await;
                        self.close_block(block).await;
                        self.finish_with_error(err.to_string()).await;
                        return Action::Stop;
                    }
                };
                // 执行工具调用，Block 序列递增
                match self
                    .handle_await_tool_call(block, &mut contexts, pendings)
                    .await
                {
                    Ok(_) => Action::Resume { contexts },
                    Err(err) => {
                        self.finish_with_error(err.to_string()).await;
                        Action::Stop
                    }
                }
            }
        }
    }

    async fn send_event(&self, signal: TaskNotification) {
        self.host.emit(signal).await;
    }
    async fn make_tool_plan(&self, pending: Vec<FunctionCall>) -> Result<ToolPlan> {
        // 获取所有历史审批请求
        let approvals = self.host.list_approvals(self.task.message_id).await?;
        let by_call_id: HashMap<_, _> = approvals
            .iter()
            .map(|approval| (approval.tool_call_id.as_str(), approval))
            .collect();

        let mut approved = Vec::new();
        let mut denied = Vec::new();
        let mut waiting = Vec::new();
        let mut unhandled = Vec::new();

        // 根据审批状态处理剩余的工具调用
        for call in pending {
            match by_call_id
                .get(call.id.as_str())
                .map(|approval| &approval.status)
            {
                Some(ToolApprovalStatus::Approved) => approved.push(call),
                Some(ToolApprovalStatus::Denied) => denied.push(call),
                Some(ToolApprovalStatus::Pending) => waiting.push(call),
                None => unhandled.push(call),
            }
        }

        let (auto, manual) = partition_tool_calls_by_policy(
            unhandled,
            self.task.chat_context.topic.tool_approval_policy.as_ref(),
        );
        approved.extend(auto);
        waiting.extend(manual);

        let (agent_calls, mcp_calls): (Vec<_>, Vec<_>) = approved
            .iter()
            .cloned()
            .partition(|call| call.name.starts_with(AGENT_TOOL_PREFIX));

        Ok(ToolPlan {
            exec_mcp: mcp_calls,
            exec_agent: agent_calls,
            denied,
            waiting,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use wind_ai::message::Role;

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
        find_pending_calls(content)
            .expect("find pending calls")
            .into_iter()
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
        assert!(find_pending_calls(&content).expect("find").is_empty());
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
    fn find_pending_errors_when_no_tool_request() {
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
            let err = match find_pending_calls(&content) {
                Ok(_) => panic!("expected an error, content: {content:?}"),
                Err(err) => err,
            };
            assert!(
                err.to_string().contains("No tool_request"),
                "unexpected error: {err}"
            );
        }
    }
}
