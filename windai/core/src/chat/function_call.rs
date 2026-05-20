use crate::error::CoreError;
use serde_json::Value;
use wind_ai::message::Message as AiMessage;
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
pub async fn execute_function_calls(
    mcp_registry: &RegistryHandle,
    tool_calls: &[FunctionCall],
) -> Result<AiMessage, CoreError> {
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
        .collect::<Result<Vec<CallToolParam>, CoreError>>()?;

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
            .map(|(index, res)| FunctionCallOutput {
                id: tool_calls[index].id.clone(),
                content: res.content,
            })
            .collect(),
    ))
}
