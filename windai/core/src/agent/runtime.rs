use super::function_call::partition_tool_calls_by_policy;
use super::host::AgentHost;
use super::tool::{self, AGENT_TOOL_PREFIX, SpawnAgentResponse};
use crate::agent::task::AgentOutput;
use crate::chat::loops::ChatContext;
use crate::chat::{ChatEvent, ChatLoops};
use crate::error::{CoreError, Result};
use crate::models::{Message, ToolApprovalPolicy, ToolApprovalStatus};
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

macro_rules! try_or_finish {
    ($expr:expr, $msg:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => return Outoput::Agent(Self::build_finish_error($msg, e)),
        }
    };
}

pub struct AgentRunConfig {
    pub binding_id: i64,
    pub topic_id: i64,
    pub parent_topic_id: i64,
    pub tool_approval_policy: Option<ToolApprovalPolicy>,
}

enum Action {
    Continue,
    Resume {
        assistant: Message,
        contexts: Vec<AiMessage>,
    },
    Stop,
}

enum Outoput {
    Agent(AgentOutput),
    Resume {
        data: Message,
        contexts: Vec<AiMessage>,
    },
}

pub struct AgentRuntime {
    chat: ChatLoops,
    host: Arc<dyn AgentHost>,
    config: AgentRunConfig,
}

impl AgentRuntime {
    pub fn new(host: Arc<dyn AgentHost>, config: AgentRunConfig) -> Self {
        Self {
            chat: ChatLoops::new(),
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

        loop {
            let mut stream = self.chat.run(&chat_ctx, assistant, contexts).await;
            self.send_event(AgentOutput::Started).await;
            loop {
                tokio::select! {
                    biased;

                    _ = ctx.cancelled() => {
                        return;
                    }

                    Some(event) = stream.next() => {
                        match self.handle_chat_event(event).await {
                            Action::Continue => {}
                            Action::Stop => return,
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
        mut message: Message,
        mut contexts: Vec<AiMessage>,
        tools: Vec<FunctionCall>,
    ) -> Outoput {
        let plan = try_or_finish!(self.make_tool_plan(message.id, tools).await, message);
        // MCP 工具执行
        if !plan.exec_mcp.is_empty() {
            let tool_result =
                try_or_finish!(self.host.execute_tool_calls(&plan.exec_mcp).await, message);
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
                    let output = AiMessage::new_tool_result(vec![FunctionCallOutput {
                        id: call_id,
                        content: result_json.clone(),
                    }]);
                    message.append_content(output.clone());
                    contexts.push(output);
                }
            }

            let futures = plan.spawn_agents.into_iter().map(|action| async move {
                let call_id = action.call_id;
                let result = self.host.spawn_agent(call_id, action.data).await?;
                Ok::<SpawnAgentResponse, CoreError>(result)
            });
            let results = try_or_finish!(futures::future::try_join_all(futures).await, message);
            for result in results {
                let val = AiMessage::new_tool_result(vec![FunctionCallOutput {
                    id: result.call_id,
                    content: Value::String(Content::arr_to_string(&result.output)),
                }]);
                message.append_content(val.clone());
                contexts.push(val);
            }
        }
        // Denied 工具执行
        if !plan.denied.is_empty() {
            let denied_result = AiMessage::new_tool_result(
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
            message.append_content(denied_result.clone());
            contexts.push(denied_result);
        };

        // 通知用户审批
        if !plan.waiting.is_empty() {
            Outoput::Agent(AgentOutput::ApprovalRequired {
                data: message,
                contexts: contexts,
                calls: plan.waiting,
            })
        } else {
            Outoput::Resume {
                data: message,
                contexts: contexts,
            }
        }
    }
    async fn handle_chat_event(&self, event: ChatEvent) -> Action {
        match event {
            ChatEvent::Partial {
                index,
                message_id,
                delta,
            } => {
                self.send_event(AgentOutput::MessageDelta {
                    message_id,
                    index,
                    delta,
                })
                .await;

                Action::Continue
            }

            ChatEvent::AwaitToolCall {
                message,
                contexts,
                tools,
            } => match self.handle_await_tool_call(message, contexts, tools).await {
                Outoput::Resume { data, contexts } => Action::Resume {
                    assistant: data,
                    contexts,
                },
                Outoput::Agent(output) => {
                    self.send_event(output).await;
                    Action::Stop
                }
            },

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

                Action::Stop
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
