pub mod adapt;

use super::domain::{AgentDomain, ApprovalAnswer};
use super::event::TopicEvent;
use super::helper;
use super::hub::{self, HubHandle, HubOptions};
use super::runtime::AgentBehavior;
use super::store::AgentStore;
use super::tree::TreeEvent;
use super::tree::event::Event;
use super::tree::phase::{CancelOrigin, Phase, Terminal};
use crate::env::app_dirs;
use crate::error::{CoreError, Result};
use crate::models::{AgentInstance, AgentMode, ApprovalRecord, ToolApprovalStatus};
use crate::storage::Storage;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;
use wind_ai::message::Content;
use wind_mcp::client::registry::RegistryHandle;

/// 不响应取消的 worker 的回收宽限期
const CANCEL_GRACE: Duration = Duration::from_secs(10);

/// 一个 topic 运行时的门面
#[derive(Clone)]
pub struct TopicRuntimeHandle {
    inner: Arc<Inner>,
}

struct Inner {
    hub: HubHandle<AgentDomain>,
    topic_id: i64,
    storage: Storage,
    /// 跟随 core 停机：投递 watcher 用它退出
    ctx: CancellationToken,
    /// 恢复闸：树从库重建一次，之后不再重放
    recovered: Mutex<bool>,
    /// 串行化建节点与投放
    start: Mutex<()>,
}

impl TopicRuntimeHandle {
    pub fn is_stopped(&self) -> bool {
        self.inner.hub.is_stopped()
    }

    fn ensure_alive(&self) -> Result<()> {
        if self.is_stopped() {
            Err(CoreError::Internal("topic runtime has stopped".into()))
        } else {
            Ok(())
        }
    }

    /// 订阅当前对话的事件流
    ///
    /// 整棵树安静（`Sealed`）或 runtime 停止时 channel 被关闭，每次新对话需重新订阅。
    /// 对一棵**已经安静**的树订阅拿不到 `Sealed`，这条连接会一直挂着，直到 runtime 停止
    pub async fn subscribe(&self) -> Result<broadcast::Receiver<TopicEvent>> {
        self.ensure_alive()?;
        self.ensure_recovered().await?;
        let mut bus = self.inner.hub.subscribe();
        let (tx, rx) = broadcast::channel(1024);
        let storage = self.inner.storage.clone();
        tokio::spawn(async move {
            loop {
                match bus.recv().await {
                    Ok(TreeEvent::Sealed) => break,
                    Ok(event) => match adapt::to_topic_events(&storage, &event).await {
                        Ok(events) => {
                            for event in events {
                                let _ = tx.send(event);
                            }
                        }
                        Err(err) => log::error!("[topic] adapt failed: {err}"),
                    },
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        log::warn!("[topic] event stream lagged, skipped {skipped}");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Ok(rx)
    }

    /// 创建新的任务
    ///
    /// 节点已在跑时输入入队，不拒绝：节点落终态时开下一轮，
    /// 停在等待审批时由审批恢复的那一轮一并消费，停在等待子节点时由重入的那一轮一并消费
    pub async fn create_task(&self, user_input: Vec<Content>) -> Result<Result<()>> {
        self.ensure_alive()?;
        self.ensure_recovered().await?;
        // 串行化建节点与投放
        let _guard = self.inner.start.lock().await;

        let instance =
            helper::get_or_create_main_instance(&self.inner.storage, self.inner.topic_id).await?;
        let snapshot = self.inner.hub.snapshot().await?;
        let exists = snapshot.iter().any(|node| node.id == instance.id);

        // 输入先落库，再入队
        let cwd = app_dirs().topic_dir().join(self.inner.topic_id.to_string());
        let spec = match helper::start_round(
            &self.inner.storage,
            &cwd,
            self.inner.topic_id,
            instance.clone(),
            &user_input,
        )
        .await
        {
            Ok(spec) => spec,
            Err(err) => return Ok(Err(err)),
        };

        let event = if exists {
            Event::Enqueue {
                node: instance.id,
                spec,
            }
        } else {
            Event::Spawn {
                id: instance.id,
                parent: None,
                spec,
                detached: false,
            }
        };
        self.inner.hub.send(event).await?;
        Ok(Ok(()))
    }

    /// 取消任务
    ///
    /// 传一个当前树里不存在的实例 id 会报错；传一个已终态的 id 是静默的。
    /// 取消丢弃该实例未消费的输入：库里那批用户消息被标为已排除；
    /// 丢弃写库失败只记日志，取消已经投递，不因此对这个调用报错
    pub async fn cancel_task(&self, instance_id: i64) -> Result<()> {
        self.ensure_alive()?;
        self.ensure_recovered().await?;
        let snapshot = self.inner.hub.snapshot().await?;
        if !snapshot.iter().any(|node| node.id == instance_id) {
            return Err(CoreError::Internal(format!(
                "Task not found, instance_id: {instance_id}"
            )));
        }
        self.inner
            .hub
            .send(Event::CancelRequest {
                root: instance_id,
                origin: CancelOrigin::External,
            })
            .await?;
        // 引擎只清内存里的 pending，库里那批输入要在这里一并丢弃。
        // 丢弃会把消息指针回退到末次配对，而正在开轮的那个 worker 可能刚读完指针还没落 assistant ——
        // 它随后锚到的是上一条输入。这一轮注定被取消、也不写内容块，留下的空 assistant 行只影响锚点
        if let Err(err) = helper::discard_unconsumed_input(&self.inner.storage, instance_id).await {
            log::error!("[topic] discarding the input of node {instance_id} failed: {err}");
        }
        Ok(())
    }

    /// 审批任务
    ///
    /// `(instance_id, allow_ids, deny_ids)` 的顺序与冻结面一致
    pub async fn approve(
        &self,
        instance_id: i64,
        allow_ids: Vec<i64>,
        deny_ids: Vec<i64>,
    ) -> Result<()> {
        self.ensure_alive()?;
        self.ensure_recovered().await?;
        let snapshot = self.inner.hub.snapshot().await?;
        let node = snapshot
            .iter()
            .find(|node| node.id == instance_id)
            .ok_or_else(|| {
                CoreError::Internal(format!("Task not found, instance_id: {instance_id}"))
            })?;
        let Phase::Suspended(payload) = node.phase.clone() else {
            return Err(CoreError::Internal(format!(
                "Instance {instance_id} is not waiting for approval"
            )));
        };

        self.persist_approval(allow_ids.clone(), deny_ids.clone())
            .await?;
        self.inner
            .hub
            .send(Event::Resume {
                node: instance_id,
                expect: payload,
                input: ApprovalAnswer {
                    allow_ids,
                    deny_ids,
                },
            })
            .await
    }

    /// 关闭当前运行时
    pub async fn shutdown(&self) -> Result<()> {
        if self.is_stopped() {
            return Ok(());
        }
        self.inner.hub.shutdown().await
    }

    /// 从库重建树并启动投递 watcher，只做一次（两者共用这把锁）
    ///
    /// watcher 必须落在 `recover` 之后订阅：恢复收敛出的终态不背投递义务
    /// （设计 §6.5），提前订阅会把它们也投递出去，还会与 `recover` 抢总线
    async fn ensure_recovered(&self) -> Result<()> {
        let mut done = self.inner.recovered.lock().await;
        if *done {
            return Ok(());
        }
        let store = AgentStore::new(self.inner.storage.clone());
        let nodes = store.load_snapshots(self.inner.topic_id).await?;
        self.inner.hub.recover(nodes).await?;
        spawn_delivery_watcher(
            self.inner.ctx.clone(),
            self.inner.hub.clone(),
            self.inner.storage.clone(),
            self.inner.topic_id,
        );
        *done = true;
        Ok(())
    }

    /// 把一批审批结果落库
    async fn persist_approval(&self, allow_ids: Vec<i64>, deny_ids: Vec<i64>) -> Result<()> {
        let mut records = Vec::with_capacity(allow_ids.len() + deny_ids.len());
        for id in deny_ids {
            records.push(ApprovalRecord {
                id,
                status: ToolApprovalStatus::Denied,
            });
        }
        for id in allow_ids {
            records.push(ApprovalRecord {
                id,
                status: ToolApprovalStatus::Approved,
            });
        }
        self.inner
            .storage
            .approval()
            .batch_set_status(records)
            .await
    }
}

/// 生成一个 topic 运行时
pub fn spawn_runtime(
    ctx: CancellationToken,
    topic_id: i64,
    mcp: RegistryHandle,
    storage: Storage,
) -> TopicRuntimeHandle {
    let behavior = Arc::new(AgentBehavior::new(storage.clone(), mcp, topic_id));
    let persist = Arc::new(AgentStore::new(storage.clone()));
    let hub = hub::spawn(HubOptions {
        behavior,
        persist,
        grace: CANCEL_GRACE,
        bus_capacity: hub::BUS_CAPACITY,
    });
    // 跟随 core 停机：hub 自己不知道进程级的取消
    {
        let hub = hub.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            ctx.cancelled().await;
            let _ = hub.shutdown().await;
        });
    }
    TopicRuntimeHandle {
        inner: Arc::new(Inner {
            hub,
            topic_id,
            storage,
            ctx,
            recovered: Mutex::new(false),
            start: Mutex::new(()),
        }),
    }
}

/// 订阅树事件，把后台子任务的产出投递回父节点的下一轮
///
/// 每条 `Terminated` 之后都发一条 `Event::Deliver`，投递与否都发；
/// 接收器跳帧时把漏掉的终态重扫一遍
fn spawn_delivery_watcher(
    ctx: CancellationToken,
    hub: HubHandle<AgentDomain>,
    storage: Storage,
    topic_id: i64,
) {
    let mut bus = hub.subscribe();
    tokio::spawn(async move {
        loop {
            let event = tokio::select! {
                biased;
                _ = ctx.cancelled() => return,
                event = bus.recv() => event,
            };
            match event {
                Ok(TreeEvent::Terminated { node, terminal }) => {
                    if let Err(err) =
                        deliver_background_result(&hub, &storage, topic_id, node, &terminal).await
                    {
                        log::error!("[topic] delivering the result of node {node} failed: {err}");
                    }
                    if let Err(err) = hub.send(Event::Deliver { node }).await {
                        log::error!("[topic] releasing the delivery of node {node} failed: {err}");
                    }
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    log::warn!("[topic] delivery watcher lagged, skipped {skipped}");
                    if let Err(err) = deliver_undelivered(&hub, &storage, topic_id).await {
                        log::error!("[topic] redelivering after a lag failed: {err}");
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    });
}

/// 把还没释放投递义务的节点重投一遍，投过与否都发一条 `Event::Deliver`
///
/// 跳帧遮住了「谁已经投过」，重扫可能对已投递的节点再投一次：父节点多一条
/// 后台结果消息，比义务永不释放、该话题的流永久挂着好
async fn deliver_undelivered(
    hub: &HubHandle<AgentDomain>,
    storage: &Storage,
    topic_id: i64,
) -> Result<()> {
    let undelivered = hub.undelivered_nodes().await?;
    if undelivered.is_empty() {
        return Ok(());
    }
    let snapshot = hub.snapshot().await?;
    for node in undelivered {
        // 串行 await：同一父节点的多条投递必须按终态顺序入队
        // 义务只登记在终态节点上；相位取不到（不可能）时只释放义务，不投递
        let terminal = snapshot
            .iter()
            .find(|entry| entry.id == node)
            .and_then(|entry| entry.phase.terminal());
        if let Some(terminal) = terminal
            && let Err(err) =
                deliver_background_result(hub, storage, topic_id, node, &terminal).await
        {
            log::error!("[topic] delivering the result of node {node} failed: {err}");
        }
        if let Err(err) = hub.send(Event::Deliver { node }).await {
            log::error!("[topic] releasing the delivery of node {node} failed: {err}");
        }
    }
    Ok(())
}

/// 把一个后台子任务的终结投递成父节点的一条用户消息并入队
///
/// 只处理 `Background` 子任务，且三种情况丢弃投递：
/// 父节点处于 `Cancelled` / `Cancelling` / `Aborted`；
/// 这次取消正是从该父节点发起（它没进取消终态，只可能是它本来就答完了）
async fn deliver_background_result(
    hub: &HubHandle<AgentDomain>,
    storage: &Storage,
    topic_id: i64,
    node: i64,
    terminal: &Terminal<AgentDomain>,
) -> Result<()> {
    let instance = helper::get_instance_by_id(storage, node).await?;
    // 同步子任务的产出由父节点重入时补报，这里只管后台子任务
    if instance.mode != Some(AgentMode::Background) {
        return Ok(());
    }
    let Some(parent_id) = instance.parent_id else {
        return Ok(());
    };
    if let Terminal::Cancelled(CancelOrigin::Ancestor(root)) = terminal
        && *root == parent_id
    {
        return Ok(());
    }
    if !parent_accepts_delivery(hub, parent_id).await? {
        return Ok(());
    }

    let parent = helper::get_instance_by_id(storage, parent_id).await?;
    let content = delivery_content(storage, &instance, node, terminal).await?;
    // 写消息之前再读一次父节点相位：上面那次读之后还有两个数据库往返（取父实例、
    // 取产出），取消落进这段窗口时消息会写在取消清扫之后，成为一条躲过丢弃的
    // 幽灵输入；残留窗口是 `start_round` 自己的读写（写消息的 actor 与取消的
    // actor 不同，无法串行化，彻底闭合要靠「已宣告前沿」那类机制）
    if !parent_accepts_delivery(hub, parent_id).await? {
        return Ok(());
    }
    let cwd = app_dirs().topic_dir().join(topic_id.to_string());
    let spec = helper::start_round(storage, &cwd, topic_id, parent, &[content]).await?;
    hub.send(Event::Enqueue {
        node: parent_id,
        spec,
    })
    .await
}

/// 父节点是否还能接收投递：`Cancelled` / `Cancelling` / `Aborted` 一律丢弃
///
/// 只有一格必须靠它拦：子节点已经落终态、取消在其后到达 —— 那个终态里没有
/// `Cancelled(Ancestor(父))`，取消来源判据拦不住；而 `reduce_enqueue` 只拒绝
/// `Cancelling` 相、终态父节点照单全收，少了这一判据，一条后台投递就能把已经
/// `Cancelled` 的父节点重新跑起来。运行期可达的其余组合都被那两条判据兜住，
/// 走到这一格要求 watcher 还没消费掉那条 `Terminated`（跳帧重扫，或被前一条
/// 投递挡住），删掉它只在这条路径上退化
async fn parent_accepts_delivery(hub: &HubHandle<AgentDomain>, parent_id: i64) -> Result<bool> {
    Ok(hub.snapshot().await?.iter().any(|parent| {
        parent.id == parent_id
            && !matches!(
                parent.phase,
                Phase::Cancelled(_) | Phase::Cancelling(_) | Phase::Aborted
            )
    }))
}

/// 投递给父节点的一条用户消息：[后台任务 … 已完成] 加子任务的产出
///
/// 产出按子任务的终态取：成功取它末条 assistant 消息的文本，失败取错误
async fn delivery_content(
    storage: &Storage,
    instance: &AgentInstance,
    node: i64,
    terminal: &Terminal<AgentDomain>,
) -> Result<Content> {
    let outcome = match terminal {
        Terminal::Done(_) => helper::last_assistant_text(storage, node).await?,
        Terminal::Failed(failure) => failure.error.clone(),
        Terminal::Cancelled(_) => "任务已取消".to_string(),
        Terminal::Aborted => "任务意外中断".to_string(),
    };
    Ok(Content::new_text(format!(
        "[后台任务 {} 已完成]\n{outcome}",
        agent_key(storage, instance).await?
    )))
}

/// 子实例绑定定义的 key，没有绑定或定义已删时退到实例 id
async fn agent_key(storage: &Storage, instance: &AgentInstance) -> Result<String> {
    let Some(agent_id) = instance.agent_id else {
        return Ok(instance.id.to_string());
    };
    Ok(storage
        .agent()
        .get_definition(agent_id)
        .await?
        .map_or_else(|| instance.id.to_string(), |definition| definition.key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::behavior::{Behavior, Ctx, Input, Outcome};
    use crate::agent::domain::AgentSpec;
    use crate::agent::helper;
    use crate::models::{
        AgentDefinitionData, AgentMode, AgentStatus, CreateAgentDefinition, CreateCredentials,
        CreateMessage, CreateMessageContent, CreateModel, CreateProvider, CreateTopic, ModelConfig,
        ModelType, UpdateInstance,
    };
    use wind_ai::message::{Message as AiMessage, Role};

    /// 给一个实例补一对 user/assistant 消息并记进指针，恢复才能重建它的相位载荷
    async fn seed_messages(storage: &Storage, instance_id: i64) {
        let user = storage
            .message()
            .create(CreateMessage {
                from_id: None,
                model_id: 1,
                is_boundary: false,
                is_excluded: false,
                instance_id,
            })
            .await
            .expect("user message");
        let assistant = storage
            .message()
            .create(CreateMessage {
                from_id: Some(user.id),
                model_id: 1,
                is_boundary: false,
                is_excluded: false,
                instance_id,
            })
            .await
            .expect("assistant message");
        bind_messages(storage, instance_id, user.id, assistant.id).await;
    }

    /// 把一对消息记成实例的当前一轮
    async fn bind_messages(storage: &Storage, instance_id: i64, user_id: i64, assistant_id: i64) {
        storage
            .agent()
            .update_instance(
                instance_id,
                UpdateInstance {
                    user_id: Some(user_id),
                    assistant_id: Some(assistant_id),
                    ..Default::default()
                },
            )
            .await
            .expect("bind messages");
    }

    /// 建一个 Agent 定义
    async fn seed_definition(
        core: &crate::WindCore,
        label: &str,
    ) -> crate::models::AgentDefinition {
        core.storage()
            .agent()
            .create_definition(CreateAgentDefinition {
                name: label.into(),
                description: format!("{label} description"),
                owner_topic_id: None,
                cloned_from_id: None,
                active: Some(true),
                data: AgentDefinitionData::default(),
            })
            .await
            .expect("definition")
    }

    /// 把一个实例本轮的 assistant 消息填上一块文本，它才有产出
    async fn seed_reply(storage: &Storage, instance_id: i64, text: &str) {
        let assistant = storage
            .agent()
            .get_instance(instance_id)
            .await
            .expect("instance")
            .expect("instance row")
            .assistant_id
            .expect("assistant message");
        storage
            .message()
            .create_content(CreateMessageContent {
                message_id: assistant,
                data: AiMessage::new_simple(
                    Role::Assistant,
                    vec![Content::new_text(text.to_string())],
                    None,
                ),
            })
            .await
            .expect("reply block");
    }

    /// 一个实例全部消息块拼起来的文本
    async fn instance_text(storage: &Storage, instance_id: i64) -> String {
        let messages = storage
            .message()
            .list_by_instance(instance_id)
            .await
            .expect("messages");
        let ids: Vec<i64> = messages.iter().map(|message| message.id).collect();
        storage
            .message()
            .list_contents_by_messages(&ids)
            .await
            .expect("contents")
            .into_iter()
            .map(|content| {
                content
                    .data
                    .content
                    .iter()
                    .map(|block| match block {
                        Content::Text { data } => data.clone(),
                        other => other.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join("")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// 立刻完成的 worker：产出指向该实例本轮的 assistant 消息，没有则退到节点 id
    struct DoneBehavior {
        storage: Storage,
    }

    #[async_trait::async_trait]
    impl Behavior<AgentDomain> for DoneBehavior {
        async fn run(
            &self,
            ctx: Ctx<'_, AgentDomain>,
            _input: Input<AgentDomain>,
        ) -> Outcome<AgentDomain> {
            let message_id = self
                .storage
                .agent()
                .get_instance(ctx.node)
                .await
                .ok()
                .flatten()
                .and_then(|instance| instance.assistant_id)
                .unwrap_or(ctx.node);
            Outcome::Completed(crate::agent::domain::AgentOutcome { message_id })
        }
    }

    /// 节点数量与相位都到达 `ready` 描述的状态
    async fn wait_until<F>(hub: &HubHandle<AgentDomain>, mut ready: F)
    where
        F: FnMut(&[crate::agent::hub::NodeSnapshot<AgentDomain>]) -> bool,
    {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let snapshot = hub.snapshot().await.expect("snapshot");
                if ready(&snapshot) {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("节点必须在超时前到达预期状态");
    }

    async fn set_status(storage: &Storage, id: i64, status: AgentStatus) {
        storage
            .agent()
            .update_instance(
                id,
                UpdateInstance {
                    status: Some(status),
                    ..Default::default()
                },
            )
            .await
            .expect("update instance");
    }

    /// 建一个带模型的 topic，返回它的 id
    async fn seed_topic(core: &crate::WindCore, label: &str) -> i64 {
        let provider = core
            .storage()
            .provider()
            .create(CreateProvider {
                name: format!("p-{label}"),
                description: None,
                base_url: "http://127.0.0.1:1".into(),
                doc: None,
                alias: None,
            })
            .await
            .expect("provider");
        core.storage()
            .provider()
            .create_credentials(CreateCredentials {
                provider_id: provider.id,
                key: "k".into(),
            })
            .await
            .expect("credentials");
        let model = core
            .storage()
            .model()
            .create(CreateModel {
                name: format!("m-{label}"),
                provider_id: provider.id,
                alias: None,
                adapter: wind_ai::model::AdapterType::OpenAICompletion,
                modalities: Some(vec![ModelType::Chat]),
                active: Some(true),
                icon: None,
                endpoint: None,
                config: Some(ModelConfig {
                    stream: Some(true),
                    reasoning: None,
                }),
            })
            .await
            .expect("model");
        core.storage()
            .topic()
            .create(CreateTopic {
                parent_id: None,
                label: label.into(),
                icon: None,
                model_id: Some(model.id),
                agent_id: None,
                tool_approval_policy: None,
            })
            .await
            .expect("topic")
            .id
    }

    #[tokio::test]
    async fn recovery_turns_cancelling_into_cancelled() {
        let core = crate::WindCore::init_memory().await.expect("core");
        let topic_id = seed_topic(&core, "recover-cancelling").await;
        let main = helper::get_or_create_main_instance(core.storage(), topic_id)
            .await
            .expect("main");
        seed_messages(core.storage(), main.id).await;
        set_status(core.storage(), main.id, AgentStatus::Cancelling).await;

        let handle = spawn_runtime(
            tokio_util::sync::CancellationToken::new(),
            topic_id,
            core.registry().clone(),
            core.storage().clone(),
        );
        handle.ensure_recovered().await.expect("recover");

        let snapshot = handle.inner.hub.snapshot().await.expect("snapshot");
        let node = snapshot.iter().find(|n| n.id == main.id).expect("node");
        assert!(
            matches!(node.phase, Phase::Cancelled(CancelOrigin::External)),
            "Cancelling 必须收敛为 Cancelled: {:?}",
            node.phase
        );
        let stored = core
            .storage()
            .agent()
            .get_instance(main.id)
            .await
            .expect("get")
            .expect("instance");
        assert_eq!(
            stored.status,
            AgentStatus::Cancelled,
            "收敛必须落库，否则库里还停在 cancelling"
        );
    }

    /// 总线跳帧漏掉的终态，watcher 重扫一遍仍要把义务释放掉
    ///
    /// 容量 1 的总线：第一个后台子节点的投递还没跑完，其余子节点的终态就把它顶掉，
    /// watcher 只能靠重扫把这批义务捞回来
    #[tokio::test]
    async fn a_lagged_watcher_still_releases_every_delivery_obligation() {
        const CHILDREN: usize = 8;
        let core = crate::WindCore::init_memory().await.expect("core");
        let topic_id = seed_topic(&core, "lagged-delivery").await;
        let definition = seed_definition(&core, "lagged-child").await;
        let main = helper::get_or_create_main_instance(core.storage(), topic_id)
            .await
            .expect("main");
        seed_messages(core.storage(), main.id).await;

        let mut children = Vec::new();
        for index in 0..CHILDREN {
            let child = helper::create_child_instance(
                core.storage(),
                topic_id,
                main.id,
                definition.id,
                AgentMode::Background,
            )
            .await
            .expect("child");
            seed_messages(core.storage(), child.id).await;
            seed_reply(core.storage(), child.id, &format!("child done {index}")).await;
            children.push(child);
        }

        let hub = hub::spawn(HubOptions {
            behavior: Arc::new(DoneBehavior {
                storage: core.storage().clone(),
            }),
            persist: Arc::new(AgentStore::new(core.storage().clone())),
            grace: Duration::from_millis(50),
            bus_capacity: 1,
        });
        spawn_delivery_watcher(
            CancellationToken::new(),
            hub.clone(),
            core.storage().clone(),
            topic_id,
        );

        let spec = |instance_id: i64| AgentSpec {
            instance_id,
            message_id: None,
        };
        hub.send(Event::Spawn {
            id: main.id,
            parent: None,
            spec: spec(main.id),
            detached: false,
        })
        .await
        .expect("spawn main");
        let root = main.id;
        wait_until(&hub, move |snapshot| {
            snapshot
                .iter()
                .any(|node| node.id == root && node.phase.is_terminal())
        })
        .await;

        // 一口气投放：第一个子节点开始投递时，其余子节点的终态都已经广播完了
        for child in &children {
            hub.send(Event::Spawn {
                id: child.id,
                parent: Some(main.id),
                spec: spec(child.id),
                detached: true,
            })
            .await
            .expect("spawn child");
        }

        // 义务全部释放 + 树收敛；两者缺一，该话题的流就永久挂着
        let settled = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let snapshot = hub.snapshot().await.expect("snapshot");
                let quiet = snapshot.len() == CHILDREN + 1
                    && snapshot
                        .iter()
                        .all(|node| node.phase.is_terminal() && node.pending.is_none())
                    && hub
                        .undelivered_nodes()
                        .await
                        .expect("undelivered")
                        .is_empty();
                if quiet {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            settled.is_ok(),
            "跳帧之后义务仍必须被释放，否则该话题的流永久挂着"
        );

        let text = instance_text(core.storage(), main.id).await;
        for index in 0..CHILDREN {
            assert!(
                text.contains(&format!("child done {index}")),
                "第 {index} 个后台产出必须回到父节点: {text}"
            );
        }
    }

    #[tokio::test]
    async fn recovery_wakes_a_parent_whose_children_are_all_terminal() {
        let core = crate::WindCore::init_memory().await.expect("core");
        let topic_id = seed_topic(&core, "recover-reenter").await;
        let main = helper::get_or_create_main_instance(core.storage(), topic_id)
            .await
            .expect("main");
        let child =
            helper::create_child_instance(core.storage(), topic_id, main.id, 1, AgentMode::Sync)
                .await
                .expect("child");
        seed_messages(core.storage(), main.id).await;
        seed_messages(core.storage(), child.id).await;
        set_status(core.storage(), main.id, AgentStatus::WaitingChild).await;
        set_status(core.storage(), child.id, AgentStatus::Finished).await;

        let handle = spawn_runtime(
            tokio_util::sync::CancellationToken::new(),
            topic_id,
            core.registry().clone(),
            core.storage().clone(),
        );
        handle.ensure_recovered().await.expect("recover");

        // 重入会真的发一个 chat 请求，这里只断言它离开了停泊相，不驱动 LLM
        let snapshot = handle.inner.hub.snapshot().await.expect("snapshot");
        let node = snapshot.iter().find(|n| n.id == main.id).expect("node");
        assert!(
            !matches!(node.phase, Phase::WaitingChildren),
            "子节点全终态的父节点必须被唤醒: {:?}",
            node.phase
        );
        handle.shutdown().await.expect("shutdown");
    }
}
