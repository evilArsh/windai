use super::behavior::{Behavior, Ctx, Input, Outcome, SpawnRequest};
use super::domain::{AgentDomain, AgentFailure, AgentOutcome, AgentProgress, PendingApproval};
use super::helper;
use super::tool::{AGENT_TOOL_PREFIX, parse_agent_action};
use crate::chat::runner::{ChatContext, pending_tool_calls};
use crate::chat::{ChatEvent, run_chat};
use crate::env::app_dirs;
use crate::error::{CoreError, Result};
use crate::models::{
    AgentDefinition, AgentInstance, AgentMode, CreateMessageContent, Message, ToolApprovalStatus,
};
use crate::storage::Storage;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::path::PathBuf;
use wind_ai::message::{Content, Message as AiMessage, Role};
use wind_ai::tool::{FunctionCall, FunctionCallOutput};
use wind_mcp::client::registry::RegistryHandle;

/// 一批待办工具调用按去向切开
struct ToolPlan {
    /// 直接执行的 MCP 工具
    exec_mcp: Vec<FunctionCall>,
    /// 交给引擎派生的 Agent 工具
    exec_agent: Vec<FunctionCall>,
    /// 用户已拒绝的调用
    denied: Vec<FunctionCall>,
    /// 等待用户审批的调用
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

struct BlockState {
    data: AiMessage,
    index: i64,
}

impl BlockState {
    fn new() -> Self {
        Self {
            data: AiMessage::default(),
            index: 0,
        }
    }
}

/// 消息是否为空块：无文本、无工具调用、无推理内容
fn is_blank(message: &AiMessage) -> bool {
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

/// 把 LLM 循环写成 Behavior：一次 run 跑一轮 chat + 工具处理
pub struct AgentBehavior {
    storage: Storage,
    mcp: RegistryHandle,
    topic_id: i64,
    cwd: PathBuf,
}

impl AgentBehavior {
    pub fn new(storage: Storage, mcp: RegistryHandle, topic_id: i64) -> Self {
        Self {
            storage,
            mcp,
            topic_id,
            cwd: app_dirs().topic_dir().join(topic_id.to_string()),
        }
    }

    /// 宣告实例、本轮消费的输入与本轮的 assistant 消息
    ///
    /// 待宣告的输入从实例的 `user_id` 起查，再按本轮有没有续写收窄：
    /// 续写（`since` 是本轮那条 assistant）时只补上它之后新到的，
    /// 开新一轮时只取新 assistant 之前的 —— 它之后到达的属于下一轮。
    /// 每一轮都宣告，模型看到的输入与客户端看到的一致，本轮的 assistant 最后单独宣告一次
    async fn announce(
        &self,
        ctx: &Ctx<'_, AgentDomain>,
        instance_id: i64,
        message_id: i64,
        since: Option<i64>,
    ) -> Result<()> {
        let instance = helper::get_instance_by_id(&self.storage, instance_id).await?;
        let user_id = instance.user_id.ok_or_else(|| {
            CoreError::Validation(format!("no user message for instance {instance_id}"))
        })?;
        ctx.emit(AgentProgress::InstanceCreated(instance));
        let messages: Vec<Message> = self
            .storage
            .message()
            .list_by_instance_from(instance_id, user_id)
            .await?;
        let pending: Vec<&Message> = messages
            .iter()
            .filter(|m| m.from_id.is_none() && !m.is_excluded)
            .filter(|m| match since {
                Some(since) => m.id > since,
                None => m.id < message_id,
            })
            .collect();
        let ids: Vec<i64> = pending.iter().map(|m| m.id).collect();
        let mut blocks: HashMap<i64, Vec<AiMessage>> = HashMap::new();
        for content in self
            .storage
            .message()
            .list_contents_by_messages(&ids)
            .await?
        {
            blocks
                .entry(content.message_id)
                .or_default()
                .push(content.data);
        }
        for message in pending {
            ctx.emit(AgentProgress::MessageCreated(message.clone()));
            for (index, data) in blocks.remove(&message.id).into_iter().flatten().enumerate() {
                ctx.emit(AgentProgress::Block {
                    message_id: message.id,
                    index: index as i64,
                    data,
                });
            }
        }
        let assistant = self
            .storage
            .message()
            .get(message_id)
            .await?
            .ok_or_else(|| CoreError::RowNotFound(format!("message {message_id}")))?;
        ctx.emit(AgentProgress::MessageCreated(assistant));
        Ok(())
    }

    /// 加载本轮所需的实例、定义、请求上下文与消息上下文
    async fn load(
        &self,
        instance_id: i64,
    ) -> Result<(
        AgentInstance,
        Option<AgentDefinition>,
        ChatContext,
        Vec<AiMessage>,
    )> {
        let instance = helper::get_instance_by_id(&self.storage, instance_id).await?;
        let agent = match instance.agent_id {
            Some(agent_id) => Some(helper::get_def_by_id(&self.storage, agent_id).await?),
            None => None,
        };
        let mut chat_ctx = helper::get_base_info(&self.storage, instance.topic_id).await?;
        chat_ctx.tools =
            helper::build_agent_tools(&self.storage, &self.mcp, &instance, agent.as_ref()).await?;
        let mut contexts =
            helper::get_message_contexts(&self.storage, instance.id, agent.as_ref()).await?;
        helper::system_message(&self.storage, &self.cwd, agent.as_ref(), &mut contexts).await?;
        Ok((instance, agent, chat_ctx, contexts))
    }

    /// 结束当前块：空块丢弃，否则落库并把块序号推进
    async fn close_block(
        &self,
        ctx: &Ctx<'_, AgentDomain>,
        message_id: i64,
        block: &mut BlockState,
    ) -> Result<Option<AiMessage>> {
        let data = std::mem::take(&mut block.data);
        if is_blank(&data) {
            return Ok(None);
        }
        self.commit_block(ctx, message_id, block, data.clone())
            .await?;
        Ok(Some(data))
    }

    /// 整块落库并推进块序号
    async fn commit_block(
        &self,
        ctx: &Ctx<'_, AgentDomain>,
        message_id: i64,
        block: &mut BlockState,
        data: AiMessage,
    ) -> Result<()> {
        if ctx.cancel.is_cancelled() {
            return Ok(());
        }
        self.storage
            .message()
            .create_content(CreateMessageContent { message_id, data })
            .await?;
        block.index += 1;
        Ok(())
    }

    /// 下发一个流式分片
    fn push_chunk(
        &self,
        ctx: &Ctx<'_, AgentDomain>,
        message_id: i64,
        block: &mut BlockState,
        delta: AiMessage,
    ) {
        block.data.append_chunk(&delta);
        ctx.emit(AgentProgress::Block {
            message_id,
            index: block.index,
            data: delta,
        });
    }

    /// 整块下发一段非流式内容，再落库
    ///
    /// 流式块不能走这里：它的分片已经在 `push_chunk` 里发过了，整块重发就是重复
    async fn push_whole_block(
        &self,
        ctx: &Ctx<'_, AgentDomain>,
        message_id: i64,
        block: &mut BlockState,
        data: AiMessage,
    ) -> Result<()> {
        ctx.emit(AgentProgress::Block {
            message_id,
            index: block.index,
            data: data.clone(),
        });
        self.commit_block(ctx, message_id, block, data).await
    }

    async fn fail(
        &self,
        ctx: &Ctx<'_, AgentDomain>,
        message_id: i64,
        err: CoreError,
    ) -> Outcome<AgentDomain> {
        if !ctx.cancel.is_cancelled() {
            let message = AiMessage::new_simple(
                Role::Assistant,
                vec![Content::new_text(err.to_string())],
                None,
            );
            if let Err(write_err) = self
                .storage
                .message()
                .create_content(CreateMessageContent {
                    message_id,
                    data: message,
                })
                .await
            {
                log::error!("[agent] persist failure block failed: {write_err}");
            }
        }
        failure(Some(message_id), err)
    }
}

#[async_trait::async_trait]
impl Behavior<AgentDomain> for AgentBehavior {
    async fn run(
        &self,
        ctx: Ctx<'_, AgentDomain>,
        input: Input<AgentDomain>,
    ) -> Outcome<AgentDomain> {
        let spec = match input {
            Input::Fresh(spec) => spec,
            Input::Children(_) | Input::External(_) => {
                match helper::current_spec(&self.storage, ctx.node).await {
                    Ok(spec) => spec,
                    Err(err) => return failure(None, err),
                }
            }
        };
        let instance_id = spec.instance_id;
        // 续写本轮时轮首已经把停在库里的输入消费掉，指针随之回到本轮锚定的那条输入，
        // 它才能继续表示「有没有还没被消费的输入」
        if spec.message_id.is_some()
            && let Err(err) = self
                .storage
                .agent()
                .rewind_instance_messages(instance_id)
                .await
        {
            return failure(None, err);
        }
        let message_id = match spec.message_id {
            Some(message_id) => message_id,
            None => match helper::open_assistant(&self.storage, instance_id).await {
                Ok(message) => message.id,
                Err(err) => return failure(None, err),
            },
        };
        if let Err(err) = self
            .announce(&ctx, instance_id, message_id, spec.message_id)
            .await
        {
            return self.fail(&ctx, message_id, err).await;
        }
        match self.round(&ctx, instance_id, message_id).await {
            Ok(outcome) => outcome,
            Err(err) => self.fail(&ctx, message_id, err).await,
        }
    }
}

fn failure(message_id: Option<i64>, err: CoreError) -> Outcome<AgentDomain> {
    Outcome::Failed(AgentFailure {
        message_id,
        error: err.to_string(),
    })
}

/// 一轮还未走到任何判定的中间态
enum PartialRound {
    /// 工具已执行完，带着新上下文继续下一轮 chat
    Continue(Vec<AiMessage>),
    /// 本节点已有判定
    Done(Outcome<AgentDomain>),
}

impl AgentBehavior {
    /// 跑一轮：可含多次 chat 往返（每次工具执行后回到模型）
    async fn round(
        &self,
        ctx: &Ctx<'_, AgentDomain>,
        instance_id: i64,
        message_id: i64,
    ) -> Result<Outcome<AgentDomain>> {
        let (instance, _agent, chat_ctx, contexts) = self.load(instance_id).await?;
        let mut contexts = contexts;
        let mut block = BlockState::new();
        // 本轮开始前先补上上一轮派生、已经结束但还没回报的调用
        if let Some(report) = helper::report_spawns(&self.storage, &instance).await? {
            contexts.push(report.clone());
            self.push_whole_block(ctx, message_id, &mut block, report)
                .await?;
        }
        let mut rounds = 0usize;
        const MAX_ROUNDS: usize = 32;
        loop {
            rounds += 1;
            if rounds > MAX_ROUNDS {
                return Ok(self
                    .fail(
                        ctx,
                        message_id,
                        CoreError::Internal("max tool rounds exceeded".into()),
                    )
                    .await);
            }
            match self
                .chat_once(
                    ctx,
                    instance_id,
                    message_id,
                    &chat_ctx,
                    &mut block,
                    &mut contexts,
                )
                .await?
            {
                PartialRound::Continue(next) => contexts = next,
                PartialRound::Done(outcome) => return Ok(outcome),
            }
        }
    }

    /// 一次 chat 请求 + 一次工具处理
    ///
    /// 模型这一轮没有待办工具调用就收尾为 `Completed`，否则交给 `handle_tools`
    async fn chat_once(
        &self,
        ctx: &Ctx<'_, AgentDomain>,
        instance_id: i64,
        message_id: i64,
        chat_ctx: &ChatContext,
        block: &mut BlockState,
        contexts: &mut Vec<AiMessage>,
    ) -> Result<PartialRound> {
        let mut stream = run_chat(chat_ctx, contexts.clone());
        loop {
            tokio::select! {
                biased;
                _ = ctx.cancel.cancelled() => {
                    // worker 被取消时这个判定会被 job 层换成 Stopped，见 jobs.rs
                    let outcome = self
                        .fail(ctx, message_id, CoreError::Internal("cancelled".into()))
                        .await;
                    return Ok(PartialRound::Done(outcome));
                }
                event = stream.next() => match event {
                    None => {
                        let outcome = self
                            .fail(
                                ctx,
                                message_id,
                                CoreError::Internal("chat stream ended without a verdict".into()),
                            )
                            .await;
                        return Ok(PartialRound::Done(outcome));
                    }
                    Some(ChatEvent::Partial { delta }) => self.push_chunk(ctx, message_id, block, delta),
                    Some(ChatEvent::Finish { contexts: next, error }) => {
                        if let Some(error) = error {
                            let _ = self.close_block(ctx, message_id, block).await?;
                            let outcome = self.fail(ctx, message_id, CoreError::Chat(error)).await;
                            return Ok(PartialRound::Done(outcome));
                        }
                        let message = self.close_block(ctx, message_id, block).await?;
                        let mut merged = next;
                        if let Some(message) = message {
                            merged.push(message);
                        }
                        *contexts = merged;
                        // 处理上下文中待执行的工具调用
                        let pendings = pending_tool_calls(contexts)
                            .unwrap_or_default()
                            .into_iter()
                            .cloned()
                            .collect::<Vec<FunctionCall>>();
                        if pendings.is_empty() {
                            return Ok(PartialRound::Done(Outcome::Completed(
                                AgentOutcome { message_id },
                            )));
                        }
                        return self
                            .handle_tools(ctx, instance_id, message_id, block, contexts, pendings)
                            .await;
                    }
                },
            }
        }
    }

    /// 处理一批待办工具调用
    ///
    /// 已执行的调用当场落成工具结果块；需要审批的挂起本节点；
    /// 需要派生子任务的把请求交还引擎（见 Task 6）
    async fn handle_tools(
        &self,
        ctx: &Ctx<'_, AgentDomain>,
        instance_id: i64,
        message_id: i64,
        block: &mut BlockState,
        contexts: &mut Vec<AiMessage>,
        pending: Vec<FunctionCall>,
    ) -> Result<PartialRound> {
        let plan = self.make_tool_plan(message_id, pending).await?;
        log::debug!("{}", plan);
        let mut call_results: Vec<FunctionCallOutput> = Vec::new();

        if !plan.exec_mcp.is_empty() {
            call_results.extend(helper::execute_tool_calls(&self.mcp, &plan.exec_mcp).await?);
        }
        if !plan.denied.is_empty() {
            call_results.extend(plan.denied.into_iter().map(|call| FunctionCallOutput {
                id: call.id,
                content: serde_json::json!({
                    "error": "tool call denied",
                    "tool": call.name,
                }),
            }));
        }
        if !call_results.is_empty() {
            let tool_result = AiMessage::new_tool_result(call_results);
            contexts.push(tool_result.clone());
            self.push_whole_block(ctx, message_id, block, tool_result)
                .await?;
        }

        // 派生请求最后交还引擎：exec_mcp 的结果块、list_agents 的结果块与派生失败的错误块
        // 都已经先落库，派生成功的调用等子任务结束后由父节点续跑时补报
        let mut spawn: Vec<SpawnRequest<AgentDomain>> = Vec::new();
        if !plan.exec_agent.is_empty() {
            match parse_agent_action(&plan.exec_agent) {
                Ok(actions) => {
                    if let Some(call_ids) = actions.list_agents {
                        let response = helper::list_agents(&self.storage, self.topic_id).await?;
                        let result_json = serde_json::to_value(&response)?;
                        let list_results: Vec<FunctionCallOutput> = call_ids
                            .into_iter()
                            .map(|id| FunctionCallOutput {
                                id,
                                content: result_json.clone(),
                            })
                            .collect();
                        let message = AiMessage::new_tool_result(list_results);
                        contexts.push(message.clone());
                        self.push_whole_block(ctx, message_id, block, message)
                            .await?;
                    }
                    if !actions.spawn_agents.is_empty() {
                        let instance =
                            helper::get_instance_by_id(&self.storage, instance_id).await?;
                        // 串行 await：子实例与它的消息按调用顺序创建，批次内的顺序即上下文顺序
                        for action in actions.spawn_agents {
                            match helper::spawn_child_round(
                                &self.storage,
                                &self.cwd,
                                self.topic_id,
                                &instance,
                                &action,
                            )
                            .await
                            {
                                Ok(child_spec) => spawn.push(SpawnRequest {
                                    id: child_spec.instance_id,
                                    detached: action.data.mode == AgentMode::Background,
                                    spec: child_spec,
                                }),
                                // 派生失败不失败整个任务：把原因作为该调用的工具结果交回模型
                                Err(err) => {
                                    let message = AiMessage::new_tool_result(vec![
                                        FunctionCallOutput {
                                            id: action.call_id,
                                            content: serde_json::json!({ "error": err.to_string() }),
                                        },
                                    ]);
                                    contexts.push(message.clone());
                                    self.push_whole_block(ctx, message_id, block, message)
                                        .await?;
                                }
                            }
                        }
                    }
                }
                // 参数来自模型写的文本，不可信：这批调用各回报一条错误结果，本轮继续
                Err(err) => {
                    let message = AiMessage::new_tool_result(
                        plan.exec_agent
                            .iter()
                            .map(|call| FunctionCallOutput {
                                id: call.id.clone(),
                                content: serde_json::json!({ "error": err.to_string() }),
                            })
                            .collect(),
                    );
                    contexts.push(message.clone());
                    self.push_whole_block(ctx, message_id, block, message)
                        .await?;
                }
            }
        }
        // 审批行必须在交还派生之前落库：
        // 同一批里派生与待审批的调用混在一起时，否则用户的审批提示会被整棵子树挡住
        let waiting = !plan.waiting.is_empty();
        if waiting {
            helper::save_approval_state(
                &self.storage,
                self.topic_id,
                instance_id,
                message_id,
                plan.waiting,
            )
            .await?;
        }
        if !spawn.is_empty() {
            return Ok(PartialRound::Done(Outcome::Spawn { spawn }));
        }
        if waiting {
            return Ok(PartialRound::Done(Outcome::Suspend(PendingApproval {
                instance_id,
                message_id,
            })));
        }
        Ok(PartialRound::Continue(std::mem::take(contexts)))
    }

    /// 按审批策略切开待办调用
    async fn make_tool_plan(
        &self,
        message_id: i64,
        pending: Vec<FunctionCall>,
    ) -> Result<ToolPlan> {
        let approvals = helper::list_approval_requests(&self.storage, message_id).await?;
        let by_call_id: HashMap<_, _> = approvals
            .iter()
            .map(|approval| (approval.tool_call_id.as_str(), approval))
            .collect();

        let mut approved = Vec::new();
        let mut denied = Vec::new();
        let mut waiting = Vec::new();
        let mut unhandled = Vec::new();
        for call in pending {
            match by_call_id.get(call.id.as_str()).map(|a| &a.status) {
                Some(ToolApprovalStatus::Approved) => approved.push(call),
                Some(ToolApprovalStatus::Denied) => denied.push(call),
                Some(ToolApprovalStatus::Pending) => waiting.push(call),
                None => unhandled.push(call),
            }
        }
        let topic = self.storage.topic().get_topic(self.topic_id).await?;
        let policy = topic.as_ref().and_then(|t| t.tool_approval_policy.as_ref());
        let (auto, manual) = helper::partition_tool_calls_by_policy(unhandled, policy);
        approved.extend(auto);
        waiting.extend(manual);

        let (agent_calls, mcp_calls): (Vec<_>, Vec<_>) = approved
            .into_iter()
            .partition(|call| call.name.starts_with(AGENT_TOOL_PREFIX));

        Ok(ToolPlan {
            exec_mcp: mcp_calls,
            exec_agent: agent_calls,
            denied,
            waiting,
        })
    }
}
