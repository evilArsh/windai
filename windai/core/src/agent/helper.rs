use super::function_call::build_tools_from_mcp;
use super::tool::{self, AgentCatalog, ListAgentsResponse};
use crate::chat::runner::ChatContext;
use crate::env::app_dirs;
use crate::error::{CoreError, Result};
use crate::models::{
    AgentDefinition, AgentInstance, AgentMcpBinding, AgentMode, AgentRole, AgentStatus,
    CreateInstance, CreateMessage, CreateToolApprovalCall, CreateToolApprovalRequests, Message,
    ToolApprovalRequest,
};
use crate::storage::Storage;
use futures::future::{try_join, try_join3};
use std::path::PathBuf;
use wind_ai::message::{Content, Message as AiMessage, Role};
use wind_ai::tool::{FunctionCall, Tools};
use wind_mcp::client::Tool;
use wind_mcp::client::registry::RegistryHandle;

/// 获取当前 Topic 能力列表中的全部 Agent
pub async fn list_agents(storage: &Storage, topic_id: i64) -> Result<ListAgentsResponse> {
    let agents = storage
        .agent()
        .list_definitions_by_topic(topic_id)
        .await?
        .into_iter()
        .map(|agent| AgentCatalog {
            key: agent.key,
            alias: Some(agent.name),
            description: agent.description,
        })
        .collect();
    Ok(ListAgentsResponse { agents })
}

/// 查询指定消息关联的工具审批请求列表
pub async fn list_approval_requests(
    storage: &Storage,
    message_id: i64,
) -> Result<Vec<ToolApprovalRequest>> {
    storage.approval().list_by_message(message_id).await
}

pub async fn get_message_contexts(storage: &Storage, instance_id: i64) -> Result<Vec<Message>> {
    storage.message().list_contexts(instance_id).await
}

pub async fn create_fork_contexts(
    cwd: &PathBuf,
    storage: &Storage,
    parent_instance_id: i64,
    instance_id: i64,
    user_input: &[Content],
    chat_ctx: &ChatContext,
    agent: Option<&AgentDefinition>,
) -> Result<(Message, Message, Vec<AiMessage>)> {
    let (mut main_raw, mut raw) = match try_join(
        get_message_contexts(storage, parent_instance_id),
        get_message_contexts(storage, instance_id),
    )
    .await
    {
        Ok(res) => res,
        Err(e) => {
            return Err(e);
        }
    };

    main_raw.append(&mut raw);
    create_context_inner(
        cwd,
        storage,
        instance_id,
        user_input,
        main_raw,
        chat_ctx,
        agent,
    )
    .await
}
/// 为一次 Agent 对话创建完整的上下文：User 消息、Assistant 消息和历史消息列表
pub async fn create_contexts(
    storage: &Storage,
    cwd: &PathBuf,
    chat_ctx: &ChatContext,
    instance_id: i64,
    user_input: &[Content],
    agent: Option<&AgentDefinition>,
) -> Result<(Message, Message, Vec<AiMessage>)> {
    let raw = get_message_contexts(storage, instance_id).await?;
    create_context_inner(cwd, storage, instance_id, user_input, raw, chat_ctx, agent).await
}

/// 获取主 Agent 实例，不存在时创建
pub async fn get_or_create_main_instance(
    storage: &Storage,
    topic_id: i64,
) -> Result<AgentInstance> {
    if let Some(instance) = storage.agent().get_main_instance(topic_id).await? {
        return Ok(instance);
    }

    storage
        .agent()
        .create_instance(CreateInstance::new_main(topic_id, None))
        .await
}

/// 为子任务创建新的 Agent 实例
pub async fn create_child_instance(
    storage: &Storage,
    topic_id: i64,
    parent_id: i64,
    agent_id: i64,
    mode: AgentMode,
) -> Result<AgentInstance> {
    storage
        .agent()
        .create_instance(CreateInstance {
            topic_id,
            parent_id: Some(parent_id),
            agent_id: Some(agent_id),
            mode: Some(mode),
            status: Some(AgentStatus::Idle),
            role: Some(AgentRole::Child),
        })
        .await
}

pub async fn get_instance_by_id(storage: &Storage, instance_id: i64) -> Result<AgentInstance> {
    storage
        .agent()
        .get_instance(instance_id)
        .await?
        .ok_or_else(|| CoreError::RowNotFound(format!("instance_id: {}", instance_id)))
}

/// 通过 Agent ID 获取 Agent 定义（会校验 active 状态）
pub async fn get_def_by_id(storage: &Storage, agent_id: i64) -> Result<AgentDefinition> {
    let agent = storage
        .agent()
        .get_definition(agent_id)
        .await?
        .ok_or_else(|| CoreError::RowNotFound(format!("agent definition by id: {}", agent_id)))?;
    if !agent.active {
        return Err(CoreError::Validation(format!(
            "agent {} is disabled",
            agent.key
        )));
    }
    Ok(agent)
}

/// 通过 Agent Key 获取 Agent 定义（会校验 active 状态）
pub async fn get_def_by_key(storage: &Storage, key: &str) -> Result<AgentDefinition> {
    let agent = storage
        .agent()
        .get_definition_by_key(key)
        .await?
        .ok_or_else(|| CoreError::RowNotFound(format!("agent definition by key: {}", key)))?;

    if !agent.active {
        return Err(CoreError::Validation(format!(
            "agent {} is disabled",
            agent.key
        )));
    }
    Ok(agent)
}

/// 加载 Agent 对话所需的全部基础信息：模型、提供商、凭证、JSON 规则和 MCP 工具
pub async fn get_base_info(storage: &Storage, topic_id: i64) -> Result<ChatContext> {
    let topic = match storage.topic().get_topic(topic_id).await? {
        Some(topic) => topic,
        None => return Err(CoreError::RowNotFound(format!("topic_id: {}", topic_id))),
    };
    let model_id = match topic.model_id {
        Some(id) => id,
        None => {
            return Err(CoreError::RowNotFound(format!(
                "No model in current topic, topic_id: {}",
                topic_id
            )));
        }
    };
    let model = storage.model().get(model_id).await?.ok_or_else(|| {
        CoreError::RowNotFound(format!("Cannot find a model. model_id: {model_id}"))
    })?;
    let provider_id = model.provider_id;

    let (rule_set, provider, credentials) = try_join3(
        storage
            .provider()
            .get_json_rule(model.provider_id, model.adapter),
        storage.provider().get(model.provider_id),
        storage
            .provider()
            .get_provider_credentials(model.provider_id),
    )
    .await?;

    Ok(ChatContext {
        topic,
        model,
        provider: provider.ok_or_else(|| {
            CoreError::RowNotFound(format!("Cannot find a provider. model_id: {}", model_id))
        })?,
        // TODO: 手动选择凭证
        credential: credentials.into_iter().next().ok_or_else(|| {
            CoreError::RowNotFound(format!("No credentials for provider {}", provider_id))
        })?,
        rule_set,
        tools: None,
    })
}

/// 保存 Assistant 消息和工具审批请求
pub async fn save_approval_state(
    storage: &Storage,
    topic_id: i64,
    instance_id: i64,
    assistant: Message,
    calls: Vec<FunctionCall>,
) -> Result<Vec<ToolApprovalRequest>> {
    let message_id = assistant.id;
    let ((), requests) = storage
        .with_tx(|storage| async move {
            storage
                .message()
                .update(message_id, assistant.into())
                .await?;
            let requests = storage
                .approval()
                .create_requests(CreateToolApprovalRequests {
                    topic_id,
                    message_id,
                    instance_id,
                    calls: calls
                        .into_iter()
                        .map(|call| CreateToolApprovalCall {
                            tool_call_id: call.id,
                            tool_name: call.name,
                            arguments: serde_json::Value::String(call.arguments),
                        })
                        .collect(),
                })
                .await?;
            Ok(((), requests))
        })
        .await?;

    return Ok(requests);
}

pub fn transfer_contexts(raw: Vec<Message>) -> Result<Vec<AiMessage>> {
    todo!()
    // raw.into_iter()
    //     .map(|m| {
    //         // 无法找到 is_simple 消息,
    //         // 该消息未正常结束（用户未授权 MCP 调用或者模型未正常返回结果）
    //         if let Some(c) = m.content.into_iter().rev().find(|c| c.is_simple()) {
    //             return Ok(c);
    //         } else {
    //             return Err(CoreError::Chat(format!(
    //                 "Incomplete message found. messageId: {}",
    //                 m.id
    //             )));
    //         }
    //     })
    //     .collect::<Result<Vec<AiMessage>>>()
}

// TODO 需要重构
async fn create_context_inner(
    cwd: &PathBuf,
    storage: &Storage,
    instance_id: i64,
    user_input: &[Content],
    raw_contexts: Vec<Message>,
    chat_ctx: &ChatContext,
    agent: Option<&AgentDefinition>,
) -> Result<(Message, Message, Vec<AiMessage>)> {
    let prompt = assemble_prompt(storage, agent).await?;
    let user_content = AiMessage::new_simple(Role::User, user_input.to_vec(), None);
    let content_cloned = user_content.clone();
    let (user_message, assistant_message) = storage
        .with_tx(|storage| async move {
            // TODO: 先创建Message，再插入索引为0的MessageContent
            let user = storage
                .message()
                .create(CreateMessage {
                    from_id: None,
                    content: vec![content_cloned],
                    model_id: chat_ctx.model.id,
                    is_boundary: false,
                    is_excluded: false,
                    input_tokens: 0,
                    output_tokens: 0,
                    instance_id,
                })
                .await?;

            let assistant = storage
                .message()
                .create(CreateMessage {
                    from_id: Some(user.id),
                    content: vec![],
                    model_id: chat_ctx.model.id,
                    is_boundary: false,
                    is_excluded: false,
                    input_tokens: 0,
                    output_tokens: 0,
                    instance_id,
                })
                .await?;

            Ok((user, assistant))
        })
        .await?;

    let mut contexts = build_context(raw_contexts, agent)?;
    contexts.push(user_content);

    let mut sys_p = vec![Content::new_text(format!(
        "<AppDataDirectory>\n{}\n</AppDataDirectory>\n
        <CurrentWorkingDirectory>\n{}\n</CurrentWorkingDirectory>\n
        <SkillsResourceDirectory>\n{}\n</SkillsResourceDirectory>\n",
        app_dirs().root_dir().to_string_lossy(),
        cwd.to_string_lossy().to_string(),
        app_dirs().skills_dir().to_string_lossy(),
    ))];
    if let Some(system_prompt) = prompt {
        sys_p.push(Content::new_text(system_prompt));
    }
    contexts.insert(0, AiMessage::new_simple(Role::System, sys_p, None));

    Ok((user_message, assistant_message, contexts))
}

/// 判断 MCP 工具是否被允许：allowed_tools 为空则默认允许，denied_tools 中存在则拒绝
fn is_tool_allowed(tool: &Tool, gates: &[(&[String], &[String])]) -> bool {
    gates.iter().any(|(allowed, denied)| {
        // 为空默认允许，denied_tools 列表中存在时视为拒绝
        let allowed = allowed.is_empty() || allowed.iter().any(|name| name == &tool.name);
        let denied = denied.iter().any(|name| name == &tool.name);
        allowed && !denied
    })
}
/// TODO: 
/// 构建历史消息上下文
///
/// 不校验消息上下文合理性，考虑以下情况：
/// - (User, Assistant) 消息对缺失。比如 User 消息被删除后应该标记 Assistant 消息为`is_excluded`
/// - 忽略 MCP 调用中间结果。历史消息上下文不会包含实时 MCP 调用产生的中间结果，只包含最终的结果，中间结果只在实时请求中包含
fn build_context(raw: Vec<Message>, agent: Option<&AgentDefinition>) -> Result<Vec<AiMessage>> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }

    let start_index = match agent.and_then(|a| a.data.context_policy.max_context) {
        Some(c) => raw.len().saturating_sub(c.max(1) as usize),
        None => 0,
    };

    let start = start_index
        + raw[start_index..]
            .iter()
            .position(|slice| {
                slice
                    .content
                    .iter()
                    .any(|c| c.is_simple() && c.role == Role::User)
            })
            .unwrap_or(0);

    let contexts = transfer_contexts(raw.into_iter().skip(start).collect())?;
    Ok(contexts)
}

/// 组装 Agent 可用的工具列表：核心内建工具 + 过滤后的 MCP 工具
pub async fn build_agent_tools(
    storage: &Storage,
    mcp_registry: &RegistryHandle,
    instance: &AgentInstance,
    agent: Option<&AgentDefinition>,
) -> Result<Option<Vec<Tools>>> {
    // 加载内建 MCP 工具，用于 Agent 调度
    // FIXME: 避免递归创建 Agent
    let mut tools = match instance.role {
        AgentRole::Main => tool::list_catalogs(),
        AgentRole::Child => vec![],
    };
    if let Some(agent) = agent {
        let enabled = agent
            .data
            .mcp_servers
            .iter()
            .filter(|mcp_binding| mcp_binding.enabled)
            .collect::<Vec<&AgentMcpBinding>>();
        if !enabled.is_empty() {
            let ids = enabled
                .iter()
                .map(|mcp_binding| mcp_binding.mcp_server_id)
                .collect::<Vec<_>>();
            let servers = storage.mcp().batch_get_by_ids(&ids).await?;
            let mut server_names = servers
                .into_iter()
                .map(|server| server.name)
                .collect::<Vec<_>>();
            let mut buildin_server_names = agent
                .data
                .builtin_mcp_servers
                .iter()
                .filter_map(|mcp_binding| {
                    if mcp_binding.enabled {
                        Some(mcp_binding.name.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<String>>();
            server_names.append(&mut buildin_server_names);

            if server_names.is_empty() {
                log::debug!("No MCP server found for agent: {}", agent.id);
            } else {
                // 拼接出的 MCP 函数名包含了 server name
                let mcp_tools = mcp_registry.list_tools_by_names(&server_names).await?;
                let gates = enabled
                    .iter()
                    .map(|b| (b.allowed_tools.as_slice(), b.denied_tools.as_slice()))
                    .collect::<Vec<_>>();
                let filtered = mcp_tools
                    .into_iter()
                    .filter(|tool| is_tool_allowed(tool, &gates))
                    .collect::<Vec<_>>();
                tools.extend(build_tools_from_mcp(filtered));
            }
        }
    }

    Ok(Some(tools))
}

/// 组装 Agent 的系统提示词：加载已启用的 prompt 模块并拼接
async fn assemble_prompt(
    storage: &Storage,
    agent: Option<&AgentDefinition>,
) -> Result<Option<String>> {
    if let Some(agent) = agent {
        let bindings = agent
            .data
            .prompt_modules
            .iter()
            .filter(|prompt_binding| prompt_binding.enabled)
            .collect::<Vec<_>>();

        let ids = bindings
            .into_iter()
            .map(|b| b.prompt_module_id)
            .collect::<Vec<_>>();

        let prompts = storage
            .prompt()
            .batch_get(&ids)
            .await?
            .into_iter()
            .filter_map(|p| p.active.then(|| p.content))
            .collect::<Vec<_>>();

        if prompts.is_empty() {
            Ok(None)
        } else {
            let joined = prompts
                .into_iter()
                .map(|content| format!("<Prompt>\n{content}\n</Prompt>"))
                .collect::<Vec<_>>()
                .join("\n\n");
            Ok(Some(joined))
        }
    } else {
        Ok(None)
    }
}
