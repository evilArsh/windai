use super::function_call::partition_tool_calls_by_policy;
use super::host::AgentHost;
use super::tool::{self, AGENT_TOOL_PREFIX, SpawnAgentResponse};
use crate::agent::task::AgentOutput;
use crate::chat::runner::ChatContext;
use crate::chat::{ChatEvent, ChatRunner};
use crate::error::{CoreError, Result};
use crate::models::{AgentMode, Message, ToolApprovalPolicy, ToolApprovalStatus};
use futures::stream::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use wind_ai::message::{Content, Message as AiMessage, Role};
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
            "[ToolPlan]\n(exec_mcp = {}, exec_agent = {}, denied = {} waiting = {})",
            self.exec_mcp.len(),
            self.exec_agent.len(),
            self.denied.len(),
            self.waiting.len()
        )
    }
}

macro_rules! try_or_finish {
    ($expr:expr, $msg:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => return Output::Agent(Self::build_finish_error($msg, e)),
        }
    };
}

#[derive(Debug, Clone)]
pub struct AgentRunConfig {
    pub binding_id: i64,
    pub topic_id: i64,
    pub parent_topic_id: i64,
    pub tool_approval_policy: Option<ToolApprovalPolicy>,
    /// 本次运行的 Agent 模式
    pub mode: AgentMode,
}

#[derive(strum::AsRefStr)]
enum Action {
    Continue,
    Resume {
        assistant: Message,
        contexts: Vec<AiMessage>,
    },
    Stop,
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name_ref = self.as_ref();
        let (name, args) = match self {
            Action::Continue => (name_ref, String::new()),
            Action::Resume {
                assistant,
                contexts,
            } => (
                name_ref,
                format!(
                    "(message_id = {}, contexts_len = {})",
                    assistant.id,
                    contexts.len(),
                ),
            ),
            Action::Stop => (name_ref, String::new()),
        };
        write!(f, "[Action {name}]\n{}", args)
    }
}

enum Output {
    Agent(AgentOutput),
    Resume {
        data: Message,
        contexts: Vec<AiMessage>,
    },
}

pub struct AgentRuntime {
    chat: ChatRunner,
    host: Arc<dyn AgentHost>,
    config: AgentRunConfig,
}

impl AgentRuntime {
    pub fn new(host: Arc<dyn AgentHost>, config: AgentRunConfig) -> Self {
        Self {
            chat: ChatRunner::new(),
            host,
            config,
        }
    }

    /// 开始对话
    pub async fn run(
        mut self,
        ctx: CancellationToken,
        chat_ctx: ChatContext,
        mut assistant: Message,
        mut contexts: Vec<AiMessage>,
        config: Option<AgentRunConfig>,
    ) {
        if let Some(conf) = config {
            self.config = conf;
        }
        let mut auto_resume_count = 0usize;
        const MAX_AUTO_RESUME: usize = 32;
        let mut iter_index = -1;
        loop {
            let mut stream = self.chat.run(&chat_ctx, assistant, contexts);
            self.send_event(AgentOutput::Started).await;
            iter_index += 1;
            loop {
                tokio::select! {
                    biased;
                    _ = ctx.cancelled() => {
                        return;
                    }
                    Some(event) = stream.next() => {
                        let action = self.handle_chat_event(iter_index,event).await;
                        match action {
                            Action::Continue => {}
                            Action::Stop => {
                                return
                            },
                            Action::Resume { assistant: next_assistant, contexts: next_contexts } => {
                                auto_resume_count += 1;
                                if auto_resume_count > MAX_AUTO_RESUME {
                                    self.send_event(Self::build_finish_error(
                                        next_assistant,
                                        "max auto resume limit exceeded",
                                    )).await;
                                    return;
                                }
                                assistant = next_assistant;
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

    fn build_finish_error(mut message: Message, error: impl ToString) -> AgentOutput {
        let err_str = error.to_string();
        AgentOutput::Finish {
            data: {
                message.append_content(AiMessage::new_simple(
                    Role::Assistant,
                    vec![Content::new_text(err_str.clone())],
                    None,
                ));
                message
            },
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
        iter_index: i32,
        mut message: Message,
        mut contexts: Vec<AiMessage>,
        tools: Vec<FunctionCall>,
    ) -> Output {
        let message_id = message.id;
        let plan = try_or_finish!(self.make_tool_plan(message.id, tools).await, message);
        log::debug!("{}", plan);
        // MCP 工具执行
        if !plan.exec_mcp.is_empty() {
            let tool_result =
                try_or_finish!(self.host.execute_tool_calls(&plan.exec_mcp).await, message);
            self.send_event(AgentOutput::Message {
                message_id,
                index: iter_index,
                delta: tool_result.clone(),
            })
            .await;
            message.append_content(tool_result.clone());
            contexts.push(tool_result);
        }
        // Allowed 工具执行
        if !plan.exec_agent.is_empty() {
            let plan = try_or_finish!(tool::parse_agent_action(&plan.exec_agent), message);

            // 合并后的 list_agents 只查询一次
            if let Some(call_ids) = plan.list_agents {
                let response = try_or_finish!(self.host.list_agents().await, message);
                let result_json = try_or_finish!(serde_json::to_value(&response), message);
                for call_id in call_ids {
                    let tool_result = AiMessage::new_tool_result(vec![FunctionCallOutput {
                        id: call_id,
                        content: result_json.clone(),
                    }]);
                    self.send_event(AgentOutput::Message {
                        message_id,
                        index: iter_index,
                        delta: tool_result.clone(),
                    })
                    .await;
                    message.append_content(tool_result.clone());
                    contexts.push(tool_result);
                }
            }

            let futures = plan.spawn_agents.into_iter().map(|action| async move {
                let call_id = action.call_id;
                let result = self.host.spawn_agent(call_id, action.data).await?;
                Ok::<SpawnAgentResponse, CoreError>(result)
            });
            let results = try_or_finish!(futures::future::try_join_all(futures).await, message);
            for result in results {
                let tool_result = AiMessage::new_tool_result(vec![FunctionCallOutput {
                    id: result.call_id,
                    content: Value::String(Content::arr_to_string(&result.output)),
                }]);
                self.send_event(AgentOutput::Message {
                    message_id,
                    index: iter_index,
                    delta: tool_result.clone(),
                })
                .await;
                message.append_content(tool_result.clone());
                contexts.push(tool_result);
            }
        }
        // Denied 工具执行
        if !plan.denied.is_empty() {
            let tool_result = AiMessage::new_tool_result(
                plan.denied
                    .into_iter()
                    .map(|call| FunctionCallOutput {
                        id: call.id,
                        content: serde_json::json!({
                            "error": "tool call denied",
                            "tool": call.name,
                        }),
                    })
                    .collect(),
            );
            self.send_event(AgentOutput::Message {
                message_id,
                index: iter_index,
                delta: tool_result.clone(),
            })
            .await;
            message.append_content(tool_result.clone());
            contexts.push(tool_result);
        };

        // 通知用户审批
        if !plan.waiting.is_empty() {
            Output::Agent(AgentOutput::ApprovalRequired {
                data: message,
                contexts: contexts,
                calls: plan.waiting,
            })
        } else {
            Output::Resume {
                data: message,
                contexts: contexts,
            }
        }
    }
    async fn handle_chat_event(&self, iter_index: i32, event: ChatEvent) -> Action {
        log::debug!("{}", event);
        match event {
            ChatEvent::Partial { message_id, delta } => {
                self.send_event(AgentOutput::Message {
                    message_id,
                    index: iter_index,
                    delta,
                })
                .await;

                Action::Continue
            }
            ChatEvent::AwaitToolCall {
                message,
                contexts,
                tools,
            } => {
                let action = match self
                    .handle_await_tool_call(iter_index, message, contexts, tools)
                    .await
                {
                    Output::Resume { data, contexts } => Action::Resume {
                        assistant: data,
                        contexts,
                    },
                    Output::Agent(output) => {
                        self.send_event(output).await;
                        Action::Stop
                    }
                };
                log::debug!("iter_index: {}, action: {}", iter_index, action);
                action
            }
            ChatEvent::Finish {
                message,
                contexts: _,
                error,
            } => {
                self.send_event(AgentOutput::Finish {
                    data: message,
                    error,
                })
                .await;
                let action = Action::Stop;
                log::debug!("iter_index: {}, action: {}", iter_index, action);
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

        let (auto, manual) =
            partition_tool_calls_by_policy(unhandled, self.config.tool_approval_policy.as_ref());
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
