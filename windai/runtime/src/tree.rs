use super::error::RuntimeError;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::broadcast;
use wind_core::agent::domain::{AgentDomain, AgentSpec};
use wind_core::agent::hub::{
    BUS_CAPACITY, HubHandle, HubOptions, NodeSnapshot, SubmitOutcome, spawn,
};
use wind_core::agent::store::AgentStore;
use wind_core::agent::translate::translate;
use wind_core::agent::tree::{NodeId, Phase, Terminal, TreeEvent};
use wind_core::llm::turn::AgentRunner;
use wind_core::models::{AgentMode, PendingCreate, PendingKind};
use wind_core::storage::Storage;
use wind_event::dispatch::Dispatcher;

/// worker 不响应取消时的回收宽限期
const JOB_GRACE: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct TreeHandle {
    pub store: Arc<AgentStore>,
    pub hub: HubHandle<AgentDomain>,
    pub(crate) submit_lock: Arc<AsyncMutex<()>>,
}

pub struct Runtime {
    storage: Storage,
    bus: Arc<Dispatcher>,
    runner: Arc<AgentRunner>,
    stopped: AtomicBool,
    trees: Mutex<HashMap<i64, TreeHandle>>,
}

impl Runtime {
    pub fn new(storage: Storage, bus: Arc<Dispatcher>, runner: Arc<AgentRunner>) -> Self {
        Self {
            storage,
            bus,
            runner,
            stopped: AtomicBool::new(false),
            trees: Mutex::new(HashMap::new()),
        }
    }

    pub async fn tree_of(&self, topic_id: i64) -> Result<TreeHandle, RuntimeError> {
        if self.is_stopped() {
            return Err(RuntimeError::Stopped);
        }
        if let Some(tree) = self.registered(topic_id) {
            return Ok(tree);
        }
        let built = self.build(topic_id).await?;
        match self.register(topic_id, &built) {
            None => {
                let _ = built.hub.shutdown().await;
                Err(RuntimeError::Stopped)
            }
            Some((winner, loser)) => {
                if let Some(loser) = loser {
                    let _ = loser.hub.shutdown().await;
                }
                Ok(winner)
            }
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    pub fn begin_shutdown(&self) -> Option<Vec<TreeHandle>> {
        if self.stopped.swap(true, Ordering::Relaxed) {
            return None;
        }
        let mut trees = self
            .trees
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Some(trees.drain().map(|(_, tree)| tree).collect())
    }

    fn registered(&self, topic_id: i64) -> Option<TreeHandle> {
        let trees = self
            .trees
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        trees.get(&topic_id).cloned()
    }

    fn register(
        &self,
        topic_id: i64,
        built: &TreeHandle,
    ) -> Option<(TreeHandle, Option<TreeHandle>)> {
        let mut trees = self
            .trees
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.is_stopped() {
            return None;
        }
        if let Some(existing) = trees.get(&topic_id) {
            return Some((existing.clone(), Some(built.clone())));
        }
        trees.insert(topic_id, built.clone());
        Some((built.clone(), None))
    }

    /// 初始化构建 topic 实例树
    async fn build(&self, topic_id: i64) -> Result<TreeHandle, RuntimeError> {
        let store = Arc::new(AgentStore::new(
            self.storage.clone(),
            topic_id,
            self.bus.clone(),
        ));
        let hub = spawn(HubOptions {
            behavior: self.runner.clone(),
            persist: store.clone(),
            grace: JOB_GRACE,
            bus_capacity: BUS_CAPACITY,
            dispatcher: self.bus.clone(),
            topic_id,
            translator: translate,
        });
        store.attach(hub.clone());
        // 监听处理 background 任务结果
        watch_tree(store.clone(), hub.clone(), topic_id);

        // 树状态恢复
        if let Err(err) = restore(&store).await {
            let _ = hub.shutdown().await;
            return Err(err);
        }
        // 将还未投递的结果继续投递
        if let Err(err) = deliver_startup_results(&store, &hub, topic_id).await {
            log::error!("[runtime] the startup scan of topic {topic_id} failed: {err}");
        }
        Ok(TreeHandle {
            store,
            hub,
            submit_lock: Arc::new(AsyncMutex::new(())),
        })
    }
}

fn watch_tree(store: Arc<AgentStore>, hub: HubHandle<AgentDomain>, topic_id: i64) {
    let mut events = hub.subscribe();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = hub.wait_stopped() => break,
                received = events.recv() => match received {
                    Ok(TreeEvent::Terminated { node, terminal }) => {
                        settle(&store, &hub, node, &terminal).await;
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        log::warn!(
                            "[runtime] the tree event stream of topic {topic_id} lagged by {skipped}"
                        );
                        // 接收端处理速度落后于消息产生速度
                        rescan(&store, &hub).await;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            }
        }
    });
}

/// background 任务处理
async fn settle(
    store: &AgentStore,
    hub: &HubHandle<AgentDomain>,
    node: NodeId,
    terminal: &Terminal<AgentDomain>,
) {
    let snapshot = match hub.snapshot().await {
        Ok(snapshot) => snapshot,
        Err(err) => {
            log::error!("[runtime] failed to read the tree before delivering {node}: {err}");
            release(hub, node).await;
            return;
        }
    };
    if let Err(err) = hand_over(store, hub, &snapshot, node, terminal).await {
        log::error!("[runtime] failed to deliver the result of node {node}: {err}");
    }
    // 将一个树节点设置为`任务已投递`的最终状态
    release(hub, node).await;
}

/// 处理 node 节点的实例执行结果
///
/// node 节点是一个执行 background 任务的 detached 节点。
///
/// background 子任务由父任务通过 spawn(background) 开启
async fn hand_over(
    store: &AgentStore,
    hub: &HubHandle<AgentDomain>,
    snapshot: &[NodeSnapshot<AgentDomain>],
    node: NodeId,
    terminal: &Terminal<AgentDomain>,
) -> Result<(), RuntimeError> {
    let Some(mapping) = store.storage().spawn_call_get_by_child(node).await? else {
        return Ok(());
    };
    if mapping.result_delivered_at.is_some() {
        return Ok(());
    }
    let Some(instance) = store.storage().agent_get(node).await? else {
        mark_delivered(store, node).await;
        return Ok(());
    };
    if instance.mode != Some(AgentMode::Background) {
        mark_delivered(store, node).await;
        return Ok(());
    }
    let Some(parent) = snapshot
        .iter()
        .find(|current| current.id == mapping.parent_id)
    else {
        mark_delivered(store, node).await;
        return Ok(());
    };
    // 父节点异常退出
    if on_cancel_side(&parent.phase) {
        mark_delivered(store, node).await;
        return Ok(());
    }
    // node 任务结果
    let payload = store.child_result_content(node, terminal).await?;
    let parent_id = mapping.parent_id;
    let written = store
        .storage()
        .with_tx(move |tx| async move {
            // 为 instance_id 插入一条待处理任务
            let row = tx
                .pending_insert_if_absent(PendingCreate {
                    instance_id: parent_id,
                    kind: PendingKind::BackgroundResult,
                    source_id: Some(node),
                    payload: payload.to_vec(),
                })
                .await?;
            tx.spawn_call_mark_delivered(node).await?;
            Ok(row)
        })
        .await?;
    if let Some(pending_id) = written {
        let cancelled = match parent_phase_now(hub, parent_id).await {
            Ok(Some(phase)) => on_cancel_side(&phase),
            Ok(None) => true,
            Err(err) => {
                log::error!(
                    "[runtime] failed to re-read the parent {parent_id} before submitting: {err}"
                );
                true
            }
        };
        if cancelled {
            // 父任务已取消，删除待处理任务
            store.storage().pending_delete(&[pending_id]).await?;
            return Ok(());
        }
        // 新的一轮任务入队父节点
        let outcome = hub
            .submit(
                parent_id,
                AgentSpec {
                    instance_id: parent_id,
                    message_id: None,
                    pending_id: Some(pending_id),
                    turn_id: None,
                },
            )
            .await?;
        if outcome == SubmitOutcome::Dropped {
            store.storage().pending_delete(&[pending_id]).await?;
        }
    }
    Ok(())
}

async fn mark_delivered(store: &AgentStore, node: NodeId) {
    if let Err(err) = store.storage().spawn_call_mark_delivered(node).await {
        log::error!("[runtime] failed to mark the result of node {node} as delivered: {err}");
    }
}

fn on_cancel_side(phase: &Phase<AgentDomain>) -> bool {
    matches!(
        phase,
        Phase::Cancelled(_) | Phase::Cancelling(_) | Phase::Aborted
    )
}

/// parent fsm 节点的状态快照
async fn parent_phase_now(
    hub: &HubHandle<AgentDomain>,
    parent: NodeId,
) -> Result<Option<Phase<AgentDomain>>, RuntimeError> {
    Ok(hub
        .snapshot()
        .await?
        .into_iter()
        .find(|node| node.id == parent)
        .map(|node| node.phase))
}

async fn release(hub: &HubHandle<AgentDomain>, node: NodeId) {
    if let Err(err) = hub.deliver(node).await {
        log::error!("[runtime] failed to release the delivery of node {node}: {err}");
    }
}

async fn rescan(store: &AgentStore, hub: &HubHandle<AgentDomain>) {
    let nodes = match hub.undelivered_nodes().await {
        Ok(nodes) => nodes,
        Err(err) => {
            log::error!("[runtime] failed to list the undelivered nodes: {err}");
            return;
        }
    };
    if nodes.is_empty() {
        return;
    }
    let snapshot = match hub.snapshot().await {
        Ok(snapshot) => snapshot,
        Err(err) => {
            log::error!("[runtime] failed to read the tree for the rescan: {err}");
            return;
        }
    };
    let terminals: HashMap<NodeId, Terminal<AgentDomain>> = snapshot
        .iter()
        .filter_map(|node| node.phase.terminal().map(|terminal| (node.id, terminal)))
        .collect();
    for node in nodes {
        if let Some(terminal) = terminals.get(&node)
            && let Err(err) = hand_over(store, hub, &snapshot, node, terminal).await
        {
            log::error!("[runtime] failed to redeliver the result of node {node}: {err}");
        }
        release(hub, node).await;
    }
}

async fn deliver_startup_results(
    store: &AgentStore,
    hub: &HubHandle<AgentDomain>,
    topic_id: i64,
) -> Result<(), RuntimeError> {
    let rows = store
        .storage()
        .spawn_call_list_undelivered_background(topic_id)
        .await?;
    if rows.is_empty() {
        return Ok(());
    }
    let snapshot = hub.snapshot().await?;
    let terminals: HashMap<NodeId, Terminal<AgentDomain>> = snapshot
        .iter()
        .filter_map(|node| node.phase.terminal().map(|terminal| (node.id, terminal)))
        .collect();
    for row in rows {
        if let Some(terminal) = terminals.get(&row.child_id)
            && let Err(err) = hand_over(store, hub, &snapshot, row.child_id, terminal).await
        {
            log::error!(
                "[runtime] failed to deliver the restored result of node {}: {err}",
                row.child_id
            );
        }
        release(hub, row.child_id).await;
    }
    Ok(())
}

async fn restore(store: &Arc<AgentStore>) -> Result<(), RuntimeError> {
    store.recover().await?;
    Ok(())
}
