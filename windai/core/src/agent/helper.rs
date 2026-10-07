use super::domain::AgentSpec;
use super::tool::{
    self, AgentCatalog, ListAgentsResponse, SPAWN_AGENT_NAME, SpawnAgentAction, SpawnAgentRequest,
};
use crate::chat::runner::ChatContext;
use crate::env::app_dirs;
use crate::error::{CoreError, Result};
use crate::models::ToolApprovalPolicy;
use crate::models::{
    AgentDefinition, AgentInstance, AgentMcpBinding, AgentMode, AgentRole, AgentStatus,
    CreateInstance, CreateMessage, CreateMessageContent, CreateToolApprovalCall,
    CreateToolApprovalRequests, Message, ToolApprovalRequest, UpdateInstance,
};
use crate::storage::Storage;
use futures::future::try_join3;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use wind_ai::message::{Content, Message as AiMessage, Role};
use wind_ai::tool::{FunctionCall, FunctionCallOutput, FunctionTool, Tools};
use wind_mcp::client::Tool;
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
/// 并发执行函数调用
pub async fn execute_tool_calls(
    mcp_registry: &RegistryHandle,
    tool_calls: &[FunctionCall],
) -> Result<Vec<FunctionCallOutput>> {
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

    Ok(results
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
        .collect())
}

/// 根据审批策略拆分可自动执行和需要人工审批的工具调用
///
/// 当不存在审批策略时，所有工具调用都自动执行
///
/// (自动审批, 手动审批)
pub fn partition_tool_calls_by_policy(
    pending: Vec<FunctionCall>,
    policy: Option<&ToolApprovalPolicy>,
) -> (Vec<FunctionCall>, Vec<FunctionCall>) {
    match policy {
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
struct MessageGroup {
    contents: Vec<AiMessage>,
}

/// 加载实例的上下文消息组
async fn load_contexts(storage: &Storage, instance_id: i64) -> Result<Vec<MessageGroup>> {
    let messages = storage.message().list_contexts(instance_id).await?;
    if messages.is_empty() {
        return Ok(Vec::new());
    }
    let ids = messages.iter().map(|m| m.id).collect::<Vec<i64>>();
    let mut contents: HashMap<i64, Vec<AiMessage>> = HashMap::new();
    for content in storage.message().list_contents_by_messages(&ids).await? {
        contents
            .entry(content.message_id)
            .or_default()
            .push(content.data);
    }

    Ok(messages
        .into_iter()
        .filter_map(|message| {
            Some(MessageGroup {
                contents: contents.remove(&message.id)?,
            })
        })
        .collect())
}

fn flatten_contexts(
    mut groups: Vec<MessageGroup>,
    agent: Option<&AgentDefinition>,
) -> Vec<AiMessage> {
    if groups.is_empty() {
        return Vec::new();
    }

    let max_context = match agent.and_then(|a| a.data.context_policy.max_context) {
        Some(c) => c.max(1) as usize,
        None => groups.len(),
    };
    let sliced = groups.split_off(groups.len().saturating_sub(max_context));
    // 保证上下文从用户轮开始
    let start = sliced
        .iter()
        .position(|group| {
            group
                .contents
                .iter()
                .any(|m| m.is_simple() && m.role == Role::User)
        })
        .unwrap_or(0);
    sliced
        .into_iter()
        .skip(start)
        .flat_map(|group| group.contents)
        .collect()
}

/// 实例的完整对话上下文
pub async fn get_message_contexts(
    storage: &Storage,
    instance_id: i64,
    agent: Option<&AgentDefinition>,
) -> Result<Vec<AiMessage>> {
    Ok(flatten_contexts(
        load_contexts(storage, instance_id).await?,
        agent,
    ))
}

/// 给模型上下文补上系统消息
pub async fn system_message(
    storage: &Storage,
    cwd: &Path,
    agent: Option<&AgentDefinition>,
    contexts: &mut Vec<AiMessage>,
) -> Result<()> {
    let mut sys_p = vec![Content::new_text(format!(
        "<AppDataDirectory>\n{}\n</AppDataDirectory>\n
        <CurrentWorkingDirectory>\n{}\n</CurrentWorkingDirectory>\n
        <SkillsResourceDirectory>\n{}\n</SkillsResourceDirectory>\n",
        app_dirs().root_dir().to_string_lossy(),
        cwd.to_string_lossy(),
        app_dirs().skills_dir().to_string_lossy(),
    ))];
    if let Some(prompt) = assemble_prompt(storage, agent).await? {
        sys_p.push(Content::new_text(prompt));
    }
    contexts.insert(0, AiMessage::new_simple(Role::System, sys_p, None));
    Ok(())
}

/// 实例当前一轮的 spec
pub async fn current_spec(storage: &Storage, instance_id: i64) -> Result<AgentSpec> {
    let instance = get_instance_by_id(storage, instance_id).await?;
    let message_id = instance.assistant_id.ok_or_else(|| {
        CoreError::Validation(format!("no assistant message for instance {instance_id}"))
    })?;
    Ok(AgentSpec {
        instance_id,
        message_id: Some(message_id),
    })
}

/// 开一条新的 assistant 消息，并把它记为本轮的助手消息
pub async fn open_assistant(storage: &Storage, instance_id: i64) -> Result<Message> {
    let instance = get_instance_by_id(storage, instance_id).await?;
    let user_id = instance.user_id.ok_or_else(|| {
        CoreError::Validation(format!("no user message for instance {instance_id}"))
    })?;
    let user = storage
        .message()
        .get(user_id)
        .await?
        .ok_or_else(|| CoreError::RowNotFound(format!("message {user_id}")))?;
    storage
        .with_tx(|storage| async move {
            let assistant = storage
                .message()
                .create(CreateMessage {
                    from_id: Some(user_id),
                    model_id: user.model_id,
                    is_boundary: false,
                    is_excluded: false,
                    instance_id,
                })
                .await?;
            storage
                .agent()
                .update_instance(
                    instance_id,
                    UpdateInstance {
                        assistant_id: Some(assistant.id),
                        ..Default::default()
                    },
                )
                .await?;
            Ok(assistant)
        })
        .await
}

/// 建一轮新的用户对话，返回本轮的 spec
pub async fn start_round(
    storage: &Storage,
    cwd: &Path,
    topic_id: i64,
    instance: AgentInstance,
    user_input: &[Content],
) -> Result<AgentSpec> {
    let model_id = topic_model_id(storage, topic_id).await?;
    create_user_message(storage, instance.id, model_id, user_input).await?;
    std::fs::create_dir_all(cwd)?;
    Ok(AgentSpec {
        instance_id: instance.id,
        message_id: None,
    })
}

/// 丢弃实例里尚未被消费的输入
pub async fn discard_unconsumed_input(storage: &Storage, instance_id: i64) -> Result<()> {
    let instance = get_instance_by_id(storage, instance_id).await?;
    let from = instance
        .user_id
        .unwrap_or(0)
        .max(instance.assistant_id.unwrap_or(0));
    let messages = storage
        .message()
        .list_by_instance_from(instance_id, from)
        .await?;
    let ids: Vec<i64> = messages
        .iter()
        .filter(|message| message.from_id.is_none() && !message.is_excluded)
        .map(|message| message.id)
        .collect();
    storage
        .with_tx(|storage| async move {
            storage.message().batch_exclude(&ids).await?;
            storage.agent().rewind_instance_messages(instance_id).await
        })
        .await
}

/// topic 绑定的模型 id
async fn topic_model_id(storage: &Storage, topic_id: i64) -> Result<i64> {
    let topic = storage
        .topic()
        .get_topic(topic_id)
        .await?
        .ok_or_else(|| CoreError::RowNotFound(format!("topic_id: {}", topic_id)))?;
    topic.model_id.ok_or_else(|| {
        CoreError::RowNotFound(format!("No model in current topic, topic_id: {topic_id}"))
    })
}

/// 把父实例**已完结的**对话复制成子实例的初始上下文
///
/// 截到「最后一条简单 assistant 消息」为止 —— 正在派生中的那一轮助手消息带着
/// 未回填的 tool_call，属于未完结的一轮，不该进子实例。父实例若还没有过任何
/// 一轮纯文本回复（首次工具轮就派生），子实例就只从自己的任务消息开始
pub async fn fork_contexts(
    storage: &Storage,
    parent_instance_id: i64,
    child_instance_id: i64,
) -> Result<()> {
    let messages = storage
        .message()
        .list_by_instance(parent_instance_id)
        .await?;
    let ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
    let mut contents: HashMap<i64, Vec<AiMessage>> = HashMap::new();
    for content in storage.message().list_contents_by_messages(&ids).await? {
        contents
            .entry(content.message_id)
            .or_default()
            .push(content.data);
    }
    let end = messages
        .iter()
        .rposition(|m| {
            contents.get(&m.id).is_some_and(|blocks| {
                blocks
                    .iter()
                    .any(|b| b.is_simple() && b.role == Role::Assistant)
            })
        })
        .map_or(0, |last| last + 1);

    // 克隆时保留原消息的配对：assistant 行的 from_id 改写成对应克隆 user 行的 id，
    // 否则子实例里全是「用户消息」，宣告锚点与删除配对都会认错
    storage
        .with_tx(|storage| async move {
            let mut cloned_ids: HashMap<i64, i64> = HashMap::new();
            // 串行 await：克隆按原顺序落库，assistant 行的 from_id 依赖前面 user 行已回填的克隆 id
            for message in messages.iter().take(end) {
                let cloned = storage
                    .message()
                    .create(CreateMessage {
                        from_id: message.from_id.and_then(|id| cloned_ids.get(&id).copied()),
                        model_id: message.model_id,
                        is_boundary: message.is_boundary,
                        is_excluded: message.is_excluded,
                        instance_id: child_instance_id,
                    })
                    .await?;
                cloned_ids.insert(message.id, cloned.id);
                for block in contents.get(&message.id).into_iter().flatten() {
                    storage
                        .message()
                        .create_content(CreateMessageContent {
                            message_id: cloned.id,
                            data: block.clone(),
                        })
                        .await?;
                }
            }
            Ok(())
        })
        .await
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

/// 实例在 topic 内的深度，主实例为 0
pub async fn instance_depth(storage: &Storage, topic_id: i64, instance_id: i64) -> Result<usize> {
    let instances = storage.agent().list_instances_by_topic(topic_id).await?;
    let parent_of: HashMap<i64, Option<i64>> =
        instances.iter().map(|i| (i.id, i.parent_id)).collect();
    let mut depth = 0usize;
    let mut cursor = parent_of.get(&instance_id).copied().flatten();
    while let Some(id) = cursor {
        depth += 1;
        cursor = parent_of.get(&id).copied().flatten();
        if depth > instances.len() {
            return Err(CoreError::Internal(format!(
                "instance {instance_id} has a cyclic parent chain"
            )));
        }
    }
    Ok(depth)
}

/// 为一次 `agent_spawn_agent` 调用建好子实例、上下文与消息
///
/// 返回子节点的 spec；`fork` 模式会先把父实例的消息历史复制进子实例
pub async fn spawn_child_round(
    storage: &Storage,
    cwd: &Path,
    topic_id: i64,
    parent: &AgentInstance,
    action: &SpawnAgentAction,
) -> Result<AgentSpec> {
    let agent = get_def_by_key(storage, &action.data.agent_key).await?;
    let depth = instance_depth(storage, topic_id, parent.id).await?;
    // 上限取自**派生的那一方**：这个节点允许自己展开多深
    // 懒建的主实例没有绑定定义，那一支不设上限（它必然在深度 0）
    if let Some(agent_id) = parent.agent_id {
        let ceiling = get_def_by_id(storage, agent_id)
            .await?
            .data
            .permission_policy
            .max_spawn_depth as usize;
        if depth + 1 > ceiling {
            return Err(CoreError::Validation(format!(
                "spawn depth {} exceeds the ceiling {ceiling}",
                depth + 1
            )));
        }
    }

    let instance =
        create_child_instance(storage, topic_id, parent.id, agent.id, action.data.mode).await?;

    if action.data.mode == AgentMode::Fork {
        fork_contexts(storage, parent.id, instance.id).await?;
    }

    let model_id = topic_model_id(storage, topic_id).await?;
    let user_input = vec![Content::new_text(action.data.task.clone())];
    create_user_message(storage, instance.id, model_id, &user_input).await?;
    std::fs::create_dir_all(cwd)?;

    Ok(AgentSpec {
        instance_id: instance.id,
        message_id: None,
    })
}

/// 父节点每轮开始前补上还没回报的派生调用
///
/// 关联不依赖任何内存态：父实例**全部历史**里、还没有配对
/// `Content::FunctionCall` 的 `agent_spawn_agent` 调用，
/// 与它的子实例按 (agent_key, task) 对齐。
/// 只要配到子实例行就回报（终态给结果，否则给「已启动」），
/// 只有一条子实例行都配不上时才留作未回报
///
/// 返回 None 表示没有可补报的调用
pub async fn report_spawns(
    storage: &Storage,
    instance: &AgentInstance,
) -> Result<Option<AiMessage>> {
    let messages = storage.message().list_by_instance(instance.id).await?;
    let ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
    let contents = storage.message().list_contents_by_messages(&ids).await?;

    let answered: HashSet<String> = contents
        .iter()
        .flat_map(|block| block.data.content.iter())
        .filter_map(|content| match content {
            Content::FunctionCall { data } => Some(data.id.clone()),
            _ => None,
        })
        .collect();
    let pending: Vec<&FunctionCall> = contents
        .iter()
        .filter_map(|block| block.data.tool_calls.as_ref())
        .flatten()
        .filter(|call| call.name == SPAWN_AGENT_NAME)
        .filter(|call| !answered.contains(&call.id))
        .collect();
    if pending.is_empty() {
        return Ok(None);
    }

    let children: Vec<AgentInstance> = storage
        .agent()
        .list_instances_by_topic(instance.topic_id)
        .await?
        .into_iter()
        .filter(|child| child.parent_id == Some(instance.id))
        .collect();
    // 配对要用的子实例数据先一次性取出，回报循环里只做纯计算
    let states = if children.is_empty() {
        HashMap::new()
    } else {
        child_states(storage, &children).await?
    };

    let mut used: Vec<i64> = Vec::new();
    let mut outputs: Vec<FunctionCallOutput> = Vec::new();
    for call in pending {
        // 参数来自模型写的文本，不可信：畸形只回报这一条，本轮继续
        let request: SpawnAgentRequest = match serde_json::from_str(&call.arguments) {
            Ok(request) => request,
            Err(err) => {
                log::warn!(
                    "[agent] spawn call {} has malformed arguments: {err}",
                    call.id
                );
                outputs.push(FunctionCallOutput {
                    id: call.id.clone(),
                    content: serde_json::json!({ "error": err.to_string() }),
                });
                continue;
            }
        };
        let Some(child) = pick_child(&children, &states, &used, &request) else {
            // 一条子实例行都没有：上次没派生成，留给下一轮重派
            log::warn!(
                "[agent] no child instance matches the spawn call {}",
                call.id
            );
            continue;
        };
        used.push(child.id);
        // 只要找到子实例行就回报：状态列可能过期，留着不报会让下一轮把同一个 spawn 再发一次
        match child.status {
            AgentStatus::Finished => {
                let text = states
                    .get(&child.id)
                    .map(|s| s.reply.clone())
                    .unwrap_or_default();
                outputs.push(FunctionCallOutput {
                    id: call.id.clone(),
                    content: Value::String(text),
                });
            }
            AgentStatus::Failed | AgentStatus::Cancelled => {
                outputs.push(FunctionCallOutput {
                    id: call.id.clone(),
                    content: serde_json::json!({ "error": "child task did not complete" }),
                });
            }
            _ => {
                outputs.push(FunctionCallOutput {
                    id: call.id.clone(),
                    content: serde_json::json!({
                        "status": "background task started",
                        "instance_id": child.id,
                    }),
                });
            }
        }
    }
    if outputs.is_empty() {
        return Ok(None);
    }
    Ok(Some(AiMessage::new_tool_result(outputs)))
}

/// 一批子实例的配对数据
struct ChildState {
    /// 子实例绑定的定义 key，没有绑定或定义已删时为 None
    agent_key: Option<String>,
    /// 子实例本轮任务
    task: String,
    /// 子实例本轮产出
    reply: String,
}

/// 一次性取出子实例的配对数据，按实例 id 索引
async fn child_states(
    storage: &Storage,
    children: &[AgentInstance],
) -> Result<HashMap<i64, ChildState>> {
    let keys: HashMap<i64, String> = storage
        .agent()
        .list_definitions()
        .await?
        .into_iter()
        .map(|definition| (definition.id, definition.key))
        .collect();
    let ids: Vec<i64> = children
        .iter()
        .flat_map(|child| [child.user_id, child.assistant_id])
        .flatten()
        .collect();
    let mut texts: HashMap<i64, String> = HashMap::new();
    for content in storage.message().list_contents_by_messages(&ids).await? {
        push_block_text(texts.entry(content.message_id).or_default(), &content.data);
    }

    let text_of = |message_id: Option<i64>| {
        message_id
            .and_then(|id| texts.get(&id).cloned())
            .unwrap_or_default()
    };
    Ok(children
        .iter()
        .map(|child| {
            (
                child.id,
                ChildState {
                    agent_key: child.agent_id.and_then(|id| keys.get(&id).cloned()),
                    task: text_of(child.user_id),
                    reply: text_of(child.assistant_id),
                },
            )
        })
        .collect())
}

/// 按 (agent_key, task) 找第一个还没被认领的子实例，退而求其次只按 agent_key
fn pick_child<'a>(
    children: &'a [AgentInstance],
    states: &HashMap<i64, ChildState>,
    used: &[i64],
    request: &SpawnAgentRequest,
) -> Option<&'a AgentInstance> {
    let mut by_key: Option<&AgentInstance> = None;
    for child in children.iter().filter(|c| !used.contains(&c.id)) {
        let Some(state) = states.get(&child.id) else {
            continue;
        };
        if state.agent_key.as_deref() != Some(request.agent_key.as_str()) {
            continue;
        }
        if state.task == request.task {
            return Some(child);
        }
        by_key.get_or_insert(child);
    }
    by_key
}

/// 实例本轮助手消息的文本
pub async fn last_assistant_text(storage: &Storage, instance_id: i64) -> Result<String> {
    let instance = get_instance_by_id(storage, instance_id).await?;
    let Some(assistant_id) = instance.assistant_id else {
        return Ok(String::new());
    };
    let contents = storage.message().list_contents(assistant_id).await?;
    Ok(content_text(contents.iter().map(|content| &content.data)))
}

/// 拼接一组内容块里的文本
fn content_text<'a>(blocks: impl Iterator<Item = &'a AiMessage>) -> String {
    let mut text = String::new();
    for block in blocks {
        push_block_text(&mut text, block);
    }
    text
}

/// 把一块内容里的文本追加到 `out`
fn push_block_text(out: &mut String, block: &AiMessage) {
    if out.len() != 0 {
        out.push_str("\n\n");
    }
    out.push_str(&Content::arr_to_string(&block.content));
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

/// 保存一次工具审批请求
pub async fn save_approval_state(
    storage: &Storage,
    topic_id: i64,
    instance_id: i64,
    message_id: i64,
    calls: Vec<FunctionCall>,
) -> Result<Vec<ToolApprovalRequest>> {
    storage
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
        .await
}

async fn create_user_message(
    storage: &Storage,
    instance_id: i64,
    model_id: i64,
    user_input: &[Content],
) -> Result<Message> {
    let data = AiMessage::new_simple(Role::User, user_input.to_vec(), None);
    storage
        .with_tx(|storage| async move {
            let user = storage
                .message()
                .create(CreateMessage {
                    from_id: None,
                    model_id,
                    is_boundary: false,
                    is_excluded: false,
                    instance_id,
                })
                .await?;
            storage
                .message()
                .create_content(CreateMessageContent {
                    message_id: user.id,
                    data,
                })
                .await?;
            storage
                .agent()
                .bind_unconsumed_input(instance_id, user.id)
                .await?;
            Ok(user)
        })
        .await
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
