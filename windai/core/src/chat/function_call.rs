use std::collections::HashSet;

use super::events::ChatEvent;
use crate::error::{CoreError, Result};
use crate::models::Message as CoreMessage;
use crate::models::ToolApprovalPolicy;
use serde_json::Value;
use wind_ai::message::{Content, Message as AiMessage};
use wind_ai::tool::{FunctionCall, FunctionCallOutput, FunctionTool, Tools};
use wind_mcp::client::registry::RegistryHandle;
use wind_mcp::client::{CallToolParam, Tool as McpTool};

fn mcp_tool_to_function_tool(mcp_tool: &McpTool) -> FunctionTool {
    FunctionTool {
        name: mcp_tool.name.clone(),
        description: mcp_tool.description.clone(),
        parameters: Some(Value::Object((*mcp_tool.input_schema).clone())),
        strict: None,
    }
}
/// [wind_mcp::client::Tool] 转换为 [wind_ai::tool::Tools]
pub fn build_tools_from_mcp(mcp_tools: &[McpTool]) -> Vec<Tools> {
    mcp_tools
        .iter()
        .map(|t| Tools::Function(mcp_tool_to_function_tool(t)))
        .collect()
}
/// 并发执行函数调用。
pub async fn execute_tool_calls(
    mcp_registry: &RegistryHandle,
    tool_calls: &[FunctionCall],
) -> Result<AiMessage> {
    let params = tool_calls
        .iter()
        .map(|tool| match McpTool::parse_name(&tool.name) {
            Ok((server_name, tool_name)) => Ok(CallToolParam {
                server_name,
                tool_name,
                arguments: Some(serde_json::from_str(&tool.arguments)?),
            }),
            Err(err) => Err(err.into()),
        })
        .collect::<Result<Vec<CallToolParam>>>()?;

    let tools_len = params.len();

    let pending = params
        .into_iter()
        .map(|param| async move { mcp_registry.call_tool(param).await });
    // TODO: 超时控制
    let results = futures::future::try_join_all(pending).await?;
    if tools_len != results.len() {
        return Err(CoreError::Internal(format!(
            "MCP returned a different number of results than expected. Expected {}, got {}",
            tools_len,
            results.len()
        )));
    }

    Ok(AiMessage::new_tool_result(
        results
            .into_iter()
            .enumerate()
            .map(|(index, res)| {
                log::debug!(
                    "[tool call result] id: {}, res: {:?}",
                    &tool_calls[index].id,
                    &res
                );
                return FunctionCallOutput {
                    id: tool_calls[index].id.clone(),
                    content: res.content,
                };
            })
            .collect(),
    ))
}

/// 根据 Topic 级审批策略拆分可自动执行和需要人工审批的工具调用。
///
/// (自动审批,手动审批)
pub fn partition_tool_calls_by_policy(
    topic_policy: &ToolApprovalPolicy,
    pending: &[FunctionCall],
) -> (Vec<FunctionCall>, Vec<FunctionCall>) {
    match topic_policy {
        ToolApprovalPolicy::AllowAll => (pending.to_vec(), vec![]),
        ToolApprovalPolicy::AllowList(approved_list) => pending
            .iter()
            .cloned()
            .partition(|call| approved_list.contains(&call.name)),
        ToolApprovalPolicy::Manual => (vec![], pending.to_vec()),
    }
}

/// 从 assistant.content 反向遍历，找出尚未执行的 tool_calls。
///
/// - 遇到 `ToolResult` → 收集已执行的 call id
/// - 遇到 `ToolRequest` → 返回其中去除已执行 id 后的差集
fn find_pending_calls(content: &[AiMessage]) -> Result<Vec<FunctionCall>> {
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

            let pending: Vec<FunctionCall> = all_calls
                .iter()
                .filter(|c| !executed_ids.contains(c.id.as_str()))
                .cloned()
                .collect();

            return Ok(pending);
        }
    }

    Err(CoreError::Chat(
        "No tool_request found in assistant content".into(),
    ))
}

/// 处理工具调用审批。
///
/// 用户未审批的工具将交由用户继续审批，直到所有工具都审批。
/// （一般情况下每一轮的tool call, 用户会一次性审批通过。但为了通用性考虑，假设用户可能分批次审批）
///
/// 恢复 tool request。
///
/// 阶段 1 已移除 Message 上的 tools_allowed/tools_denied 字段，
/// 人工审批恢复将在 ToolApprovalService 接入后实现。
/// 当前仅支持根据 Topic 策略可自动批准的 pending calls。
pub async fn resume_tool_approval(
    mcp_registry: &RegistryHandle,
    policy: &ToolApprovalPolicy,
    assistant: &mut CoreMessage,
    contexts: &mut Vec<AiMessage>,
) -> Result<Option<ChatEvent>> {
    // 剩下未自动审批的工具，需要用户手动审批
    let pending = find_pending_calls(&assistant.content)?;
    // 恢复上下文
    contexts.extend(assistant.content.iter().cloned());
    if pending.is_empty() {
        log::debug!(
            "no pending tool calls to approve. assistant_id: {}",
            assistant.id
        );
        return Ok(None);
    }
    let (auto_approved, unreviewed) = partition_tool_calls_by_policy(policy, &pending);
    log::debug!(
        "auto_approved: {}, unreviewed: {}",
        auto_approved.len(),
        unreviewed.len(),
    );
    if auto_approved.is_empty() {
        return Err(CoreError::Chat(
            "Tool approvals need ToolApprovalService before resuming".into(),
        ));
    }

    let tool_result = execute_tool_calls(mcp_registry, &auto_approved).await?;
    contexts.push(tool_result.clone());
    assistant.append_content(&tool_result);

    if unreviewed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(ChatEvent::await_tool_calls(assistant.id, &unreviewed)))
    }
}
