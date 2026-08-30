use crate::error::{CoreError, Result};
use crate::models::ToolApprovalPolicy;
use serde_json::Value;
use wind_ai::message::Message as AiMessage;
use wind_ai::tool::{FunctionCall, FunctionCallOutput, FunctionTool, Tools};
use wind_mcp::client::registry::RegistryHandle;
use wind_mcp::client::{CallToolParam, Tool as McpTool};

/// [wind_mcp::client::Tool] 转换为 [wind_ai::tool::Tools]
pub fn build_tools_from_mcp(mcp_tools: Vec<McpTool>) -> Vec<Tools> {
    mcp_tools
        .into_iter()
        .map(|tool| {
            Tools::Function(FunctionTool {
                name: tool.name,
                description: tool.description,
                parameters: Some(Value::Object((*tool.input_schema).clone())),
                strict: None,
            })
        })
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
/// 当不存在审批策略时，所有工具调用都自动执行。
///
/// (自动审批,手动审批)
pub fn partition_tool_calls_by_policy(
    pending: Vec<FunctionCall>,
    topic_policy: Option<&ToolApprovalPolicy>,
) -> (Vec<FunctionCall>, Vec<FunctionCall>) {
    match topic_policy {
        Some(policy) => match policy {
            ToolApprovalPolicy::AllowAll => (pending, vec![]),
            ToolApprovalPolicy::AllowList(approved_list) => pending
                .into_iter()
                .partition(|call| approved_list.contains(&call.name)),
            ToolApprovalPolicy::Manual => (vec![], pending),
        },
        None => (pending, vec![]),
    }
}
