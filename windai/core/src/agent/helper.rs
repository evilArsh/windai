use super::function_call::build_tools_from_mcp;
use super::tool::{self, AgentBindingView, ListAgentsResponse};
use crate::chat::runner::ChatContext;
use crate::error::{CoreError, Result};
use crate::models::{
    AgentBinding, AgentDefinition, AgentMcpBinding, AgentRole, CreateMessage,
    CreateToolApprovalCall, CreateToolApprovalRequests, CreateTopic, Message, ToolApprovalRequest,
    Topic, UpdateAgentBinding,
};
use crate::storage::Storage;
use futures::future::{try_join, try_join5};
use wind_ai::message::{Content, Message as AiMessage, ReqConfig, Role};
use wind_ai::tool::{FunctionCall, Tools};
use wind_mcp::client::Tool;
use wind_mcp::client::registry::RegistryHandle;

/// 获取当前Topic下可用的子Agent列表（过滤掉主Agent和已禁用的绑定）
pub async fn list_agents(storage: &Storage, topic_id: i64) -> Result<ListAgentsResponse> {
    let agents = storage
        .agent()
        .list_definitions_by_topic(topic_id)
        .await?
        .into_iter()
        .map(|agent| AgentBindingView {
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

/// 创建当前Topic的子Topic，用于Agent对话隔离
pub async fn create_sub_topic(
    storage: &Storage,
    parent_topic_id: i64,
    binding_id: i64,
    title: String,
) -> Result<Topic> {
    storage
        .topic()
        .create(CreateTopic {
            label: title,
            parent_id: Some(parent_topic_id),
            binding_id: Some(binding_id),
            icon: None,
        })
        .await
}

pub async fn get_message_contexts(storage: &Storage, topic_id: i64) -> Result<Vec<Message>> {
    storage.message().list_contexts(topic_id).await
}

pub async fn create_fork_contexts(
    storage: &Storage,
    main_agent_topic_id: i64,
    agent_topic_id: i64,
    user_input: Vec<Content>,
    agent: &AgentDefinition,
    chat_ctx: &ChatContext,
) -> Result<(Message, Message, Vec<AiMessage>)> {
    let (mut main_raw, mut raw) = match try_join(
        get_message_contexts(storage, main_agent_topic_id),
        get_message_contexts(storage, agent_topic_id),
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
        storage,
        agent_topic_id,
        user_input,
        main_raw,
        agent,
        chat_ctx,
    )
    .await
}
/// 为一次Agent对话创建完整的上下文：User消息、Assistant消息和历史消息列表
pub async fn create_contexts(
    storage: &Storage,
    agent_topic_id: i64,
    user_input: Vec<Content>,
    agent: &AgentDefinition,
    chat_ctx: &ChatContext,
) -> Result<(Message, Message, Vec<AiMessage>)> {
    let raw = get_message_contexts(storage, agent_topic_id).await?;
    create_context_inner(storage, agent_topic_id, user_input, raw, agent, chat_ctx).await
}

/// 获取当前Topic的主Agent绑定
pub async fn get_main_binding(storage: &Storage, topic_id: i64) -> Result<AgentBinding> {
    storage
        .agent()
        .get_main_binding(topic_id)
        .await?
        .ok_or_else(|| {
            CoreError::RowNotFound(format!("topic: {} has no main agent binding", topic_id))
        })
}

/// 通过Agent ID查找当前Topic下的绑定关系
pub async fn get_binding_by_agent_id(
    storage: &Storage,
    parent_topic_id: i64,
    agent_id: i64,
) -> Result<AgentBinding> {
    storage
        .agent()
        .get_binding_by_agent_id(parent_topic_id, agent_id)
        .await?
        .ok_or_else(|| {
            CoreError::RowNotFound(format!(
                "agent binding by agent_id: {}, topic: {}",
                agent_id, parent_topic_id
            ))
        })
}

pub async fn get_binding_by_id(storage: &Storage, binding_id: i64) -> Result<AgentBinding> {
    storage
        .agent()
        .get_binding(binding_id)
        .await?
        .ok_or_else(|| CoreError::RowNotFound(format!("agent binding by id: {}", binding_id)))
}

/// 查询子 Agent 绑定的 Topic
pub async fn get_topic_by_binding_id(
    storage: &Storage,
    parent_topic_id: i64,
    binding_id: i64,
) -> Result<Option<Topic>> {
    storage
        .topic()
        .get_topic_by_binding_id(parent_topic_id, binding_id)
        .await
}
/// 通过Agent ID获取Agent定义（会校验active状态）
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

/// 通过Agent Key获取Agent定义（会校验active状态）
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

/// 加载Agent对话所需的全部基础信息：模型、提供商、凭证、JSON规则和MCP工具
pub async fn get_base_info(
    storage: &Storage,
    mcp_registry: &RegistryHandle,
    binding: &AgentBinding,
    agent: &AgentDefinition,
) -> Result<ChatContext> {
    let model_id = binding.model_id.ok_or_else(|| {
        CoreError::Validation(format!("no model for current binding: {}", binding.id))
    })?;
    let model = storage
        .model()
        .get(binding.model_id.ok_or_else(|| {
            CoreError::Validation(format!("no model for current binding: {}", binding.id))
        })?)
        .await?
        .ok_or_else(|| {
            CoreError::RowNotFound(format!("Cannot find a model. model_id: {model_id}"))
        })?;

    let model_id = model.id;
    let provider_id = model.provider_id;

    let (rule_set, provider, credentials, req_config, tools) = try_join5(
        storage
            .provider()
            .get_json_rule(model.provider_id, model.adapter),
        storage.provider().get(model.provider_id),
        storage
            .provider()
            .get_provider_credentials(model.provider_id),
        async move {
            if let Some(id) = binding.chat_config_id {
                storage
                    .topic()
                    .get_chat_config(id)
                    .await
                    .map(|c| c.map_or_else(ReqConfig::default, |c| c.data))
            } else {
                Ok(ReqConfig::default())
            }
        },
        build_agent_tools(storage, mcp_registry, &binding, &agent),
    )
    .await?;

    Ok(ChatContext {
        model,
        provider: provider.ok_or_else(|| {
            CoreError::RowNotFound(format!("Cannot find a provider. model_id: {}", model_id))
        })?,
        // TODO: 手动选择凭证
        credential: credentials.into_iter().next().ok_or_else(|| {
            CoreError::RowNotFound(format!("No credentials for provider {}", provider_id))
        })?,
        req_config,
        rule_set,
        tools,
    })
}

/// 并发保存Assistant消息和工具审批请求
pub async fn save_approval_state(
    storage: &Storage,
    binding_id: i64,
    parent_topic_id: i64,
    assistant: Message,
    calls: Vec<FunctionCall>,
) -> Result<Vec<ToolApprovalRequest>> {
    let message_id = assistant.id;
    let agent_topic_id = assistant.topic_id;
    let ((), requests) = storage
        .with_tx(|storage| async move {
            storage
                .message()
                .update(message_id, assistant.into())
                .await?;
            let requests = storage
                .approval()
                .create_requests(CreateToolApprovalRequests {
                    parent_topic_id,
                    topic_id: agent_topic_id,
                    message_id,
                    binding_id,
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

/// 将Assistant消息持久化到数据库
pub async fn save_message(storage: &Storage, assistant: Message) -> Result<()> {
    storage
        .message()
        .update(assistant.id, assistant.into())
        .await
}
pub async fn update_binding(
    storage: &Storage,
    binding_id: i64,
    data: UpdateAgentBinding,
) -> Result<()> {
    storage.agent().update_binding(binding_id, data).await
}

pub fn transfer_contexts(raw: Vec<Message>) -> Result<Vec<AiMessage>> {
    raw.into_iter()
        .map(|m| {
            // 无法找到 is_simple消息,
            // 该消息未正常结束（用户未授权MCP调用或者模型未正常返回结果）
            if let Some(c) = m.content.into_iter().rev().find(|c| c.is_simple()) {
                return Ok(c);
            } else {
                return Err(CoreError::Chat(format!(
                    "Incomplete message found. messageId: {}",
                    m.id
                )));
            }
        })
        .collect::<Result<Vec<AiMessage>>>()
}

async fn create_context_inner(
    storage: &Storage,
    agent_topic_id: i64,
    user_input: Vec<Content>,
    raw_contexts: Vec<Message>,
    agent: &AgentDefinition,
    chat_ctx: &ChatContext,
) -> Result<(Message, Message, Vec<AiMessage>)> {
    let prompt = assemble_prompt(storage, &agent).await?;
    let stream = chat_ctx.req_config.stream.unwrap_or(false);

    let user_content = AiMessage::new_simple(Role::User, user_input, None);

    let content_cloned = user_content.clone();
    let (user_message, assistant_message) = storage
        .with_tx(|storage| async move {
            let user = storage
                .message()
                .create(CreateMessage {
                    from_id: None,
                    stream,
                    content: vec![content_cloned],
                    model_id: chat_ctx.model.id,
                    topic_id: agent_topic_id,
                    is_boundary: false,
                    is_exclude: false,
                    input_tokens: 0,
                    output_tokens: 0,
                })
                .await?;

            let assistant = storage
                .message()
                .create(CreateMessage {
                    from_id: Some(user.id),
                    stream,
                    content: vec![],
                    model_id: chat_ctx.model.id,
                    topic_id: agent_topic_id,
                    is_boundary: false,
                    is_exclude: false,
                    input_tokens: 0,
                    output_tokens: 0,
                })
                .await?;

            Ok((user, assistant))
        })
        .await?;

    let mut contexts = build_context(raw_contexts, agent).await?;

    contexts.push(user_content);

    if let Some(system_prompt) = prompt {
        contexts.insert(
            0,
            AiMessage::new_simple(Role::System, vec![Content::new_text(system_prompt)], None),
        );
    }

    Ok((user_message, assistant_message, contexts))
}

/// 判断MCP工具是否被允许：allowed_tools为空则默认允许，denied_tools中存在则拒绝
fn is_tool_allowed(tool: &Tool, bindings: &[&AgentMcpBinding]) -> bool {
    bindings.iter().any(|binding| {
        // 为空默认允许，denied_tools 列表中存在时视为拒绝
        let allowed = binding.allowed_tools.is_empty()
            || binding.allowed_tools.iter().any(|name| name == &tool.name);
        let denied = binding.denied_tools.iter().any(|name| name == &tool.name);
        allowed && !denied
    })
}
/// 构建历史消息上下文
///
/// 不校验消息上下文合理性，考虑以下情况：
/// - (User, Assistant) 消息对缺失。比如User消息被删除后应该标记 Assistant 消息为`is_excluded`
/// - 忽略MCP调用中间结果。历史消息上下文不会包含实时 MCP 调用产生的中间结果，只包含最终的结果，中间结果只在实时请求中包含
async fn build_context(raw: Vec<Message>, agent: &AgentDefinition) -> Result<Vec<AiMessage>> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }

    let max_context = agent
        .data
        .context_policy
        .max_context
        .map(|c| c.max(1) as usize)
        .unwrap_or(1);

    // 寻找系统消息和边界消息
    let start_index = raw.len().saturating_sub(max_context);
    // 确保第一条记录是 Role::User
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

/// 组装Agent可用的工具列表：核心内建工具 + 过滤后的MCP工具
async fn build_agent_tools(
    storage: &Storage,
    mcp_registry: &RegistryHandle,
    binding: &AgentBinding,
    agent: &AgentDefinition,
) -> Result<Option<Vec<Tools>>> {
    // 加载内建MCP工具，用于 Agent 调度
    // FIXME: 避免递归创建Agent，仅主任务使用内建工具
    let mut tools = match binding.role {
        AgentRole::Main => tool::list_catalogs(),
        AgentRole::Child => vec![],
    };

    let enabled = agent
        .data
        .mcp_servers
        .iter()
        .filter(|binding| binding.enabled)
        .collect::<Vec<_>>();
    if enabled.is_empty() {
        return Ok(Some(tools));
    }

    let ids = enabled
        .iter()
        .map(|binding| binding.mcp_server_id)
        .collect::<Vec<_>>();
    let servers = storage.mcp().batch_get_by_ids(&ids).await?;
    let server_names = servers
        .into_iter()
        .map(|server| server.name)
        .collect::<Vec<_>>();
    if server_names.is_empty() {
        log::debug!("No MCP server found for agent: {}", agent.id);
        return Ok(Some(tools));
    }

    log::debug!("MCP server found for agent: {}", server_names.join(","));
    // 拼接出的 MCP 函数名包含了 server name
    let mcp_tools = mcp_registry.list_tools_by_names(&server_names).await?;
    let filtered = mcp_tools
        .into_iter()
        .filter(|tool| is_tool_allowed(tool, &enabled))
        .collect::<Vec<_>>();
    tools.extend(build_tools_from_mcp(filtered));
    Ok(Some(tools))
}

/// 组装Agent的系统提示词：按order排序加载已启用的prompt模块并拼接
async fn assemble_prompt(storage: &Storage, agent: &AgentDefinition) -> Result<Option<String>> {
    let bindings = agent
        .data
        .prompt_modules
        .iter()
        .filter(|binding| binding.enabled)
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
        Ok(Some(prompts.join("\n\n")))
    }
}
