use std::collections::{HashMap, HashSet};

use crate::error::{CoreError, Result};
use crate::models::Message as CoreMessage;
use crate::models::{McpServerParam, Topic};
use serde_json::{Value, json};
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
            .map(|(index, res)| FunctionCallOutput {
                id: tool_calls[index].id.clone(),
                content: res.content,
            })
            .collect(),
    ))
}

/// 合并并去重 topic 级别和 MCP 服务级别的 `auto_approves`。
///
/// - `topic.auto_approves`：话题维度配置的自动执行工具名
/// - `tools`：各 MCP 服务的 `auto_approves`
///
/// 返回 `None` 表示没有任何自动执行工具配置。
pub fn merge_approved_tools(
    topic: &Topic,
    servers: Option<&[McpServerParam]>,
) -> Option<Vec<String>> {
    let mut seen = HashSet::new();
    for tool_name in topic.auto_approves.iter().flatten() {
        seen.insert(tool_name.clone());
    }
    for server in servers.into_iter().flatten() {
        server.auto_approves.iter().flatten().for_each(|tool_name| {
            seen.insert(McpTool::build_name(&server.name, &tool_name));
        });
    }
    match seen.len() {
        0 => None,
        _ => Some(seen.into_iter().collect()),
    }
}

/// 筛选出 dst 中工具名称在 approved 中的函数调用
pub fn filter_tool_calls(
    approved: Option<&[String]>,
    dst: &[FunctionCall],
) -> (Vec<FunctionCall>, Vec<FunctionCall>) {
    match approved {
        Some(approved_list) => dst
            .iter()
            .cloned()
            .partition(|call| approved_list.contains(&call.name)),
        None => (vec![], dst.to_vec()),
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

/// 处理暂停的工具调用审批。
///
/// 根据 `tools_allowed` 决定哪些工具执行、哪些拒绝：
/// - 在 `tools_allowed` 中的 → 执行 MCP 调用
/// - 不在 `tools_allowed` 中的 → 构造 `{"error": "User denied this tool call"}` 占位结果
///
/// 调用后 `assistant.content` 和 `contexts` 中会追加新的 `ToolResult` 帧，
pub async fn resume_tool_approval(
    mcp_registry: &RegistryHandle,
    assistant: &mut CoreMessage,
    contexts: &mut Vec<AiMessage>,
) -> Result<()> {
    let pending = find_pending_calls(&assistant.content)?;
    // 恢复上下文
    contexts.extend(assistant.content.iter().cloned());
    let allowed_set: HashSet<&str> = match assistant.tools_allowed {
        Some(ref tools) => tools.iter().map(|t| t.as_str()).collect(),
        None => HashSet::new(),
    };
    let calls_to_exec: Vec<FunctionCall> = pending
        .iter()
        .filter(|c| allowed_set.contains(c.id.as_str()))
        .cloned()
        .collect();

    let mut result_map: HashMap<String, FunctionCallOutput> = HashMap::new();
    if !calls_to_exec.is_empty() {
        let exec_msg = execute_tool_calls(mcp_registry, &calls_to_exec).await?;
        for c in exec_msg.content {
            if let Content::FunctionCall { data } = c {
                result_map.insert(data.id.clone(), data);
            }
        }
    } else {
        log::warn!("No tools to execute");
    }

    // 按原始 tool_request 顺序组装结果，未执行的视为拒绝
    let results: Vec<FunctionCallOutput> = pending
        .iter()
        .map(|call| {
            result_map
                .remove(&call.id)
                .unwrap_or_else(|| FunctionCallOutput {
                    id: call.id.clone(),
                    content: json!({"error": "User denied this tool call"}),
                })
        })
        .collect();

    let tool_result = AiMessage::new_tool_result(results);
    contexts.push(tool_result.clone());
    assistant.append_content(&tool_result);

    Ok(())
}
