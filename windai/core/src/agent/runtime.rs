use super::function_call::partition_tool_calls_by_policy;
use super::host::AgentHost;
use super::task::AgentOutput;
use super::tool::{self, AGENT_TOOL_PREFIX, SpawnAgentResponse};
use crate::chat::runner::ChatContext;
use crate::chat::{ChatEvent, run_chat};
use crate::error::{CoreError, Result};
use crate::models::{AgentInstance, ToolApprovalStatus};
use futures::stream::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use wind_ai::message::{Content, Message as AiMessage};
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

macro_rules! try_or_finish {
    ($msg_id:expr, $expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => return Output::Agent(Self::build_finish_error($msg_id, e)),
        }
    };
}

#[derive(strum::AsRefStr)]
enum Action {
    Continue,
    Resume { contexts: Vec<AiMessage> },
    Stop,
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name_ref = self.as_ref();
        let (name, args) = match self {
            Action::Continue => (name_ref, String::new()),
            Action::Resume { contexts } => {
                (name_ref, format!("(contexts_len = {})", contexts.len(),))
            }
            Action::Stop => (name_ref, String::new()),
        };
        write!(f, "[Action {name}] {}", args)
    }
}

enum Output {
    Agent(AgentOutput),
    Resume { contexts: Vec<AiMessage> },
}

pub struct AgentRuntime<'a> {
    host: Arc<dyn AgentHost>,
    instance: Option<AgentInstance>,
    chat_ctx: Option<&'a ChatContext>,
}

impl<'a> AgentRuntime<'a> {
    pub fn new(host: Arc<dyn AgentHost>) -> Self {
        Self {
            host,
            instance: None,
            chat_ctx: None,
        }
    }

    /// 开始对话
    pub async fn run(
        mut self,
        message_id: i64,
        ctx: CancellationToken,
        chat_ctx: ChatContext,
        instance: AgentInstance,
        mut contexts: Vec<AiMessage>,
    ) {
        self.instance = Some(instance);
        self.chat_ctx = Some(&chat_ctx);
        let mut auto_resume_count = 0usize;
        const MAX_AUTO_RESUME: usize = 32;
        let mut iter_index = -1;
        loop {
            let mut stream = run_chat(&chat_ctx, contexts);
            self.send_event(AgentOutput::Started).await;
            iter_index += 1;
            loop {
                tokio::select! {
                    biased;
                    _ = ctx.cancelled() => {
                        return;
                    }
                    Some(event) = stream.next() => {
                        let action = self.handle_chat_event(message_id, iter_index, event).await;
                        match action {
                            Action::Continue => {
                                // 206 partial
                            }
                            Action::Stop => return,
                            Action::Resume {
                                contexts: next_contexts,
                            } => {
                                auto_resume_count += 1;
                                if auto_resume_count > MAX_AUTO_RESUME {
                                    self.send_event(Self::build_finish_error(
                                        message_id,
                                        "max auto resume limit exceeded",
                                    ))
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
        }
    }

    fn build_finish_error(message_id: i64, error: impl ToString) -> AgentOutput {
        let err_str = error.to_string();
        AgentOutput::Finish {
            message_id,
            error: Some(err_str),
        }
    }

    /// 执行工具调用和提交工具调用审批，存在以下情况
    ///
    /// 1. 新一轮对话需要执行调用和请求审批
    ///
    /// 2. 上一轮对话中工具审批完毕，处理审批结果
    async fn handle_await_tool_call(
        &self,
        message_id: i64,
        index: i32,
        mut contexts: Vec<AiMessage>,
        tools: Vec<FunctionCall>,
    ) -> Output {
        let plan = try_or_finish!(message_id, self.make_tool_plan(message_id, tools).await);
        let mut call_results: Vec<FunctionCallOutput> = vec![];
        log::debug!("{}", plan);
        // MCP 工具执行
        if !plan.exec_mcp.is_empty() {
            let tool_result = try_or_finish!(
                message_id,
                self.host.execute_tool_calls(&plan.exec_mcp).await
            );
            call_results.extend(tool_result);
        }
        // Allowed 工具执行
        if !plan.exec_agent.is_empty() {
            let plan = try_or_finish!(message_id, tool::parse_agent_action(&plan.exec_agent));
            // 合并后的 list_agents 只查询一次
            if let Some(call_ids) = plan.list_agents {
                let response = try_or_finish!(message_id, self.host.list_agents().await);
                let result_json = try_or_finish!(message_id, serde_json::to_value(&response));
                for call_id in call_ids {
                    call_results.push(FunctionCallOutput {
                        id: call_id,
                        content: result_json.clone(),
                    });
                }
            }
            let futures = plan.spawn_agents.into_iter().map(|action| async move {
                let call_id = action.call_id;
                let result = self.host.spawn_agent(call_id, action.data).await?;
                Ok::<SpawnAgentResponse, CoreError>(result)
            });
            let results = try_or_finish!(message_id, futures::future::try_join_all(futures).await);
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
        let tool_result = AiMessage::new_tool_result(call_results);
        contexts.push(tool_result.clone());
        // 工具调用结果消息块
        self.send_event(AgentOutput::Message {
            message_id,
            index,
            delta: tool_result,
        })
        .await;

        // 通知审批
        if !plan.waiting.is_empty() {
            Output::Agent(AgentOutput::ApprovalRequired {
                contexts: contexts,
                calls: plan.waiting,
                message_id,
            })
        } else {
            Output::Resume { contexts: contexts }
        }
    }
    async fn handle_chat_event(&self, message_id: i64, index: i32, event: ChatEvent) -> Action {
        log::debug!("{}", event);
        match event {
            ChatEvent::Partial { delta } => {
                self.send_event(AgentOutput::Message {
                    message_id,
                    index,
                    delta,
                })
                .await;

                Action::Continue
            }
            ChatEvent::AwaitToolCall { contexts } => {
                let (partial, pendings) = match self.find_pending_calls(&contexts) {
                    Ok((partial, tools)) => (
                        partial,
                        tools.into_iter().cloned().collect::<Vec<FunctionCall>>(),
                    ),
                    Err(err) => {
                        self.send_event(AgentOutput::Finish {
                            message_id,
                            error: Some(err.to_string()),
                        })
                        .await;
                        return Action::Stop;
                    }
                };
                if !partial {
                    // 发送工具调用请求消息块，非 partial 状态下已经发送全量请求，partial 状态跳过发送
                    self.send_event(AgentOutput::Message {
                        message_id,
                        index,
                        delta: AiMessage::new_tool_request(
                            pendings.clone(),
                            contexts
                                .iter()
                                .last()
                                .map_or_default(|c| c.reasoning_content.clone()),
                        ),
                    })
                    .await;
                }
                let action = match self
                    .handle_await_tool_call(message_id, index, contexts, pendings)
                    .await
                {
                    Output::Resume { contexts } => Action::Resume { contexts },
                    Output::Agent(output) => {
                        self.send_event(output).await;
                        Action::Stop
                    }
                };
                log::debug!("iter_index: {}, action: {}", index, action);
                action
            }
            ChatEvent::Finish { contexts, error } => {
                self.send_event(AgentOutput::Finish {
                    message_id,
                    error: match error {
                        true => contexts
                            .iter()
                            .last()
                            .map_or_default(|c| Some(Content::arr_to_string(&c.content))),
                        false => None,
                    },
                })
                .await;
                let action = Action::Stop;
                log::debug!("iter_index: {}, action: {}", index, action);
                action
            }
        }
    }

    async fn send_event(&self, signal: AgentOutput) {
        self.host.emit(signal).await;
    }
    async fn make_tool_plan(
        self: &Self,
        message_id: i64,
        pending: Vec<FunctionCall>,
    ) -> Result<ToolPlan> {
        // 获取所有历史审批请求
        let approvals = self.host.list_approvals(message_id).await?;
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
            self.chat_ctx
                .as_ref()
                .and_then(|b| b.topic.tool_approval_policy.as_ref()),
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

    /// 找出待处理的工具调用
    ///
    /// 特殊情况下，调用者可能不会一次性传递所有的请求调用结果
    ///
    /// ```text
    /// [ 上一轮的 tool_result ]
    /// [ 截断 ]
    /// [ tool_request 1:10 ] // 所有的调用请求, Message::tool_calls []
    /// [ tool_result  1:4 ] // 所有或者部分的调用结果，Message::content    []
    /// [ tool_result  1:6 ] // 所有或者部分的调用结果
    /// ```
    fn find_pending_calls(
        &self,
        contexts: &'a [AiMessage],
    ) -> Result<(bool, impl Iterator<Item = &'a FunctionCall> + 'a)> {
        let mut executed_ids: Vec<&str> = vec![];
        // 区分完整的一次审核请求或者分批的请求
        for msg in contexts.iter().rev() {
            if msg.is_tool_result() {
                for c in &msg.content {
                    if let Content::FunctionCall { data } = c {
                        executed_ids.push(&data.id);
                    }
                }
            }
            if msg.is_tool_request() {
                let all_calls = msg
                    .tool_calls
                    .as_ref()
                    .ok_or_else(|| CoreError::Chat("tool_request without tool_calls".into()))?;

                return Ok((
                    all_calls.len() > executed_ids.len(),
                    all_calls
                        .iter()
                        .filter(move |c| !executed_ids.contains(&c.id.as_str())),
                ));
            }
        }
        Err(CoreError::Chat(
            "No tool_request found in assistant content".into(),
        ))
    }
}
