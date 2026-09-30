use super::function_call::partition_tool_calls_by_policy;
use super::host::AgentHost;
use super::task::{TaskNotification, TaskSpec};
use super::tool::{self, AGENT_TOOL_PREFIX, SpawnAgentResponse};
use crate::chat::runner::pending_tool_calls;
use crate::chat::{ChatEvent, run_chat};
use crate::error::{CoreError, Result};
use crate::models::{AgentMode, ToolApprovalStatus};
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

/// 消息是否为空块：无文本、无工具调用、无推理内容
fn is_blank(message: &Message) -> bool {
    message
        .content
        .iter()
        .all(|content| matches!(content, Content::Text { data } if data.is_empty()))
        && message
            .tool_calls
            .as_ref()
            .is_none_or(|calls| calls.is_empty())
        && message
            .reasoning_content
            .as_ref()
            .is_none_or(String::is_empty)
}

fn fork_contexts(contexts: &[Message]) -> Vec<Message> {
    let end = contexts
        .iter()
        .rposition(|ctx| ctx.is_simple() && ctx.role == Role::Assistant)
        .map_or(0, |last| last + 1);
    contexts[..end].to_vec()
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

    /// 下发一个内容分片
    async fn broadcast_chunk(&self, block: &BlockState, delta: Message) {
        self.send_event(TaskNotification::Message {
            message_id: self.task.message_id,
            instance_id: self.task.instance.id,
            index: block.index,
            delta,
            partial: true,
        })
        .await;
    }

    /// 拼接内容块，并且向上发送消息
    async fn push_chunk(&self, block: &mut BlockState, delta: Message) {
        block.data.append_chunk(&delta);
        self.broadcast_chunk(block, delta).await;
    }

    /// 结束当前内容块，递增块索引；跳过空块
    async fn close_block(&self, block: &mut BlockState) -> Option<Message> {
        let data = std::mem::take(&mut block.data);
        if is_blank(&data) {
            return None;
        }
        self.persist_block(block, data.clone()).await;
        Some(data)
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
    ///
    /// 返回 `false` 表示仍有调用在等待审批，本轮到此为止
    async fn handle_await_tool_call(
        &self,
        block: &mut BlockState,
        contexts: &mut Vec<Message>,
        tools: Vec<FunctionCall>,
    ) -> Result<bool> {
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
                let contexts_clone = match action.data.mode {
                    AgentMode::Fork => Some(fork_contexts(contexts)),
                    _ => None,
                };
                async move {
                    let call_id = action.call_id;
                    let result = host
                        .spawn_agent(call_id, action.data, contexts_clone)
                        .await?;
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
            // 工具结果整块下发后落库
            self.broadcast_chunk(block, tool_result.clone()).await;
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
            return Ok(false);
        }
        Ok(true)
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
                    self.close_block(block).await;
                    self.finish_with_error(error).await;
                    return Action::Stop;
                }
                let message = self.close_block(block).await;
                let content = message
                    .as_ref()
                    .map(|message| message.content.clone())
                    .unwrap_or_default();
                if let Some(message) = message {
                    contexts.push(message);
                }
                // 处理上下文中待执行的工具调用
                let pendings = pending_tool_calls(&contexts)
                    .unwrap_or_default()
                    .into_iter()
                    .cloned()
                    .collect::<Vec<FunctionCall>>();
                if pendings.is_empty() {
                    self.finish(content).await;
                    return Action::Stop;
                }
                // 执行工具调用，Block 序列递增
                match self
                    .handle_await_tool_call(block, &mut contexts, pendings)
                    .await
                {
                    Ok(true) => Action::Resume { contexts },
                    Ok(false) => Action::Stop,
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
