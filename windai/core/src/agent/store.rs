use super::domain::{
    AgentDomain, AgentSpec, CallDecision, SuspendedWait, WaitAnswer, pending_call_ids,
};
use super::events::{
    ApprovalRequest, ApprovalResolved, CORE_APPROVAL_REQUEST, CORE_APPROVAL_RESOLVED, PendingCall,
};
use super::hub::{HubHandle, NodeSnapshot, Persist};
use super::phase_codec::{StoredPhase, decode, encode};
use super::tree::{Event, NodeId, Phase, Terminal};
use crate::error::{CoreError, Result};
use crate::llm::tools::ToolOrigin;
use crate::models::{AgentInstance, AgentMode, BlockRow, ExternalWait, WaitOpen};
use crate::storage::Storage;
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use wind_event::context::DispatchCx;
use wind_event::def::EventDef;
use wind_event::dispatch::Dispatcher;
use wind_event::envelope::{Origin, Scope, Source};
use wind_stream::block::{BlockKind, MEDIA_IMAGE};
use wind_wire::message::Content;
use wind_wire::tool::FunctionCall;

/// 发起等待时派发观测的一次顶层操作预算
const OBSERVE_BUDGET: Duration = Duration::from_millis(50);

/// topic 树的状态保存与外部等待统一入口
pub struct AgentStore {
    storage: Storage,
    topic_id: i64,
    dispatcher: Arc<Dispatcher>,
    hub: OnceLock<HubHandle<AgentDomain>>,
}

impl AgentStore {
    pub fn new(storage: Storage, topic_id: i64, dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            storage,
            topic_id,
            dispatcher,
            hub: OnceLock::new(),
        }
    }

    pub fn attach(&self, hub: HubHandle<AgentDomain>) {
        let _ = self.hub.set(hub);
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub async fn suspend(
        &self,
        instance: i64,
        kind: &str,
        message_id: i64,
        handle: &str,
    ) -> Result<()> {
        let mut wait = SuspendedWait {
            instance_id: instance,
            kind: kind.to_string(),
            message_id,
            handle: handle.to_string(),
            payload: None,
        };
        let request = self.wait_request(&wait).await?;
        wait.payload = Some(serde_json::json!({
            "pending_call_ids": request.calls.iter().map(|call| call.call_id.clone()).collect::<Vec<_>>(),
        }));
        if self.open_wait(&wait).await? == WaitWrite::Opened {
            self.notify(&self.observe_cx(), &CORE_APPROVAL_REQUEST, request)
                .await;
        }
        Ok(())
    }

    /// 工具审批结果
    pub async fn answer(
        &self,
        cx: &DispatchCx,
        handle: &str,
        decisions: Vec<CallDecision>,
        by: &str,
    ) -> Result<()> {
        let decision =
            serde_json::to_value(&decisions).map_err(|err| CoreError::Bus(err.to_string()))?;
        // 先读后写：读取失败时尚未落库
        let wait = self
            .storage
            .wait_get(handle)
            .await?
            .ok_or_else(|| CoreError::Denied("unknown wait handle".into()))?;
        let recorded = match self.storage.wait_answer(handle, decision).await {
            Ok(()) => true,
            Err(CoreError::NoRowsAffected) => false,
            Err(err) => return Err(err),
        };
        let decisions = if recorded {
            decisions
        } else {
            let wait = self
                .storage
                .wait_get(handle)
                .await?
                .ok_or_else(|| CoreError::Denied("unknown wait handle".into()))?;
            decisions_of(&wait)?
        };
        if let Some(expect) = self.current_wait(wait.instance_id).await?
            && expect.handle == handle
        {
            self.hub()?
                .send(Event::Resume {
                    node: wait.instance_id,
                    expect,
                    input: WaitAnswer {
                        handle: handle.to_string(),
                        decisions: decisions.clone(),
                    },
                })
                .await?;
        }
        if recorded {
            self.notify(
                cx,
                &CORE_APPROVAL_RESOLVED,
                ApprovalResolved {
                    instance_id: wait.instance_id,
                    handle: handle.to_string(),
                    decisions,
                    by: by.to_string(),
                },
            )
            .await;
        }
        Ok(())
    }

    /// 将 background 模式的子实例任务的结果归一化
    pub async fn child_result_content(
        &self,
        child_id: NodeId,
        terminal: &Terminal<AgentDomain>,
    ) -> Result<Vec<Content>> {
        match terminal {
            Terminal::Done(outcome) => {
                let rows = self
                    .storage
                    .block_list_by_message(outcome.message_id)
                    .await?;
                Ok(rows.iter().filter_map(result_block).collect())
            }
            Terminal::Failed(failure) => Ok(vec![Content::new_text(format!(
                "background task {child_id} failed: {}",
                failure.error
            ))]),
            Terminal::Cancelled(_) => Ok(vec![Content::new_text(format!(
                "background task {child_id} was cancelled"
            ))]),
            Terminal::Aborted => Ok(vec![Content::new_text(format!(
                "background task {child_id} was aborted"
            ))]),
        }
    }

    pub async fn load_snapshots(&self) -> Result<Vec<NodeSnapshot<AgentDomain>>> {
        let instances = self.storage.agent_list_by_topic(self.topic_id).await?;
        let instance_ids: Vec<i64> = instances.iter().map(|instance| instance.id).collect();
        let pending = self
            .storage
            .pending_list_by_instances(&instance_ids)
            .await?;
        let mut queues: HashMap<i64, Vec<AgentSpec>> = HashMap::new();
        for row in pending {
            queues.entry(row.instance_id).or_default().push(AgentSpec {
                instance_id: row.instance_id,
                message_id: None,
                pending_id: Some(row.id),
                turn_id: None,
            });
        }
        let mut snaps = Vec::with_capacity(instances.len());
        for instance in instances {
            let phase = decode_instance_phase(&instance)?;
            let detached = instance.mode == Some(AgentMode::Background);
            let pending = queues.remove(&instance.id).unwrap_or_default();
            snaps.push(NodeSnapshot {
                id: instance.id,
                parent: instance.parent_id,
                children: Vec::new(),
                phase,
                detached,
                pending,
            });
        }
        attach_children(&mut snaps);
        Ok(snaps)
    }

    /// 程序重启后，把数据库里记着的任务树重新装回内存，
    /// 并把“用户已经答复、但答复还没来得及生效”的挂起补上
    pub async fn recover(&self) -> Result<()> {
        // 组装节点树快照
        let snapshots = self.load_snapshots().await?;
        // 过滤出正在"等外部答复"的任务，(tool_call approval ...)
        let expects: HashMap<String, SuspendedWait> = snapshots
            .iter()
            .filter_map(|snap| snap.phase.awaiting_external_input())
            .map(|wait| (wait.handle.clone(), wait.clone()))
            .collect();
        let handles: Vec<String> = expects.keys().cloned().collect();
        let answered = self.storage.wait_list_by_handles(&handles).await?;
        // 恢复节点树快照
        //
        // 把每个任务按快照写进内存里的任务树；
        // 对每个任务重放一次 Event::Recovered，让状态收敛到重启后合理的样子：
        // Phase::Cancelling(_) → Phase::Cancelled(origin)；
        // Phase::Running 且 job == None → Phase::Aborted；
        // Phase::Suspended、Phase::WaitingChildren、Phase::Done/Failed/Cancelled/Aborted → 原样保留；
        // Phase::WaitingChildren 且非 detached 子节点全部终态 → Phase::Running（Input::Children）；
        // 终态且 pending 非空 → drain_pending，Phase::Running（Input::Idle）；
        // 恢复出来的节点 delivered = true；
        // 重放 Event::Recovered 前 is_quiescent() 已为 true → 补发 TreeEvent::Sealed
        self.hub()?.recover(snapshots).await?;
        for wait in answered.iter().filter(|wait| wait.is_answered()) {
            let Some(expect) = expects.get(&wait.handle) else {
                continue;
            };
            let decisions = match decisions_of(wait) {
                Ok(decisions) => decisions,
                Err(err) => {
                    log::warn!("[store] dropping the answer of {}: {err}", wait.handle);
                    continue;
                }
            };
            let input = WaitAnswer {
                handle: expect.handle.clone(),
                decisions,
            };
            self.hub()?
                .send(Event::Resume {
                    node: expect.instance_id,
                    expect: expect.clone(),
                    input,
                })
                .await?;
        }
        Ok(())
    }

    fn hub(&self) -> Result<&HubHandle<AgentDomain>> {
        self.hub
            .get()
            .ok_or_else(|| CoreError::Internal("the store is not attached to a hub yet".into()))
    }

    async fn current_wait(&self, instance: NodeId) -> Result<Option<SuspendedWait>> {
        let snapshot = self.hub()?.snapshot().await?;
        Ok(snapshot
            .iter()
            .find(|node| node.id == instance)
            .and_then(|node| node.phase.awaiting_external_input().cloned()))
    }

    async fn wait_request(&self, wait: &SuspendedWait) -> Result<ApprovalRequest> {
        Ok(ApprovalRequest {
            instance_id: wait.instance_id,
            handle: wait.handle.clone(),
            calls: self.pending_calls(wait).await?,
        })
    }

    async fn pending_calls(&self, wait: &SuspendedWait) -> Result<Vec<PendingCall>> {
        if let Some(rewritten) = wait
            .payload
            .as_ref()
            .and_then(|payload| payload.get("pending_calls"))
            .cloned()
            .and_then(|value| serde_json::from_value::<Vec<FunctionCall>>(value).ok())
        {
            return Ok(rewritten.into_iter().map(pending_call_of).collect());
        }
        let wanted = wait
            .payload
            .as_ref()
            .filter(|payload| payload.get("pending_call_ids").is_some())
            .map(pending_call_ids);
        let rows = self.storage.block_list_by_message(wait.message_id).await?;
        rows.iter()
            .filter(|row| row.kind == BlockKind::ToolCall)
            .filter(|row| match &wanted {
                Some(ids) => row
                    .call_id
                    .as_deref()
                    .is_some_and(|call_id| ids.iter().any(|id| id == call_id)),
                None => true,
            })
            .map(pending_call)
            .collect()
    }

    async fn open_wait(&self, wait: &SuspendedWait) -> Result<WaitWrite> {
        let wait = wait.clone();
        self.storage
            .with_tx(move |tx| async move { write_pair(tx, wait.instance_id, &wait).await })
            .await
    }

    fn observe_cx(&self) -> DispatchCx {
        DispatchCx::root(
            Origin::Internal,
            Scope::Topic {
                topic_id: self.topic_id,
            },
            Source::core("agent"),
            OBSERVE_BUDGET,
        )
    }

    async fn notify<P>(&self, cx: &DispatchCx, def: &EventDef<P>, payload: P)
    where
        P: Serialize + Send + Sync,
    {
        self.dispatcher.notify(def, cx, payload).await;
    }
}

/// 树状态持久化
#[async_trait::async_trait]
impl Persist<AgentDomain> for AgentStore {
    async fn save(&self, node: NodeId, phase: &Phase<AgentDomain>) -> Result<()> {
        match phase.awaiting_external_input().cloned() {
            Some(wait) => {
                let request = self.wait_request(&wait).await?;
                let write = {
                    let wait = wait.clone();
                    self.storage
                        .with_tx(move |tx| async move { write_pair(tx, node, &wait).await })
                };
                if write.await? == WaitWrite::Replayed {
                    return Ok(());
                }
                self.notify(&self.observe_cx(), &CORE_APPROVAL_REQUEST, request)
                    .await;
                Ok(())
            }
            None => {
                let stored =
                    encode(phase).map_err(|error| CoreError::Internal(error.to_string()))?;
                let payload = stored.payload.as_ref().map(|value| value.to_string());
                self.storage
                    .agent_save_phase(node, stored.name.parse()?, payload.as_deref())
                    .await
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum WaitWrite {
    Opened,
    Replayed,
}

async fn write_pair(tx: Storage, node: NodeId, wait: &SuspendedWait) -> Result<WaitWrite> {
    let open = tx.wait_list_open_by_instance(node).await?;
    if let Some(other) = open.iter().find(|open| open.handle != wait.handle) {
        return Err(CoreError::Denied(format!(
            "instance {node} already waits on handle {}",
            other.handle
        )));
    }
    let written = match tx.wait_get(&wait.handle).await? {
        None => {
            let payload = wait.payload.clone().unwrap_or(serde_json::Value::Null);
            tx.wait_open(WaitOpen {
                handle: wait.handle.to_string(),
                instance_id: node,
                kind: wait.kind.to_string(),
                message_id: wait.message_id,
                payload: payload.clone(),
            })
            .await?;
            WaitWrite::Opened
        }
        Some(existing) if existing.is_answered() => {
            return Err(CoreError::Denied(format!(
                "wait handle {} was already answered; a suspension needs a fresh handle",
                wait.handle
            )));
        }
        Some(existing) if existing.instance_id != node => {
            return Err(CoreError::Denied(format!(
                "wait handle {} belongs to instance {}, not {node}",
                wait.handle, existing.instance_id
            )));
        }
        Some(_) => WaitWrite::Replayed,
    };
    let stored = encode(&Phase::Suspended(wait.clone()))
        .map_err(|error| CoreError::Internal(error.to_string()))?;
    let payload = stored.payload.as_ref().map(|value| value.to_string());
    tx.agent_save_phase(node, stored.name.parse()?, payload.as_deref())
        .await?;
    Ok(written)
}

fn pending_call(row: &BlockRow) -> Result<PendingCall> {
    let call_id = row
        .call_id
        .clone()
        .ok_or_else(|| CoreError::MalformedColumn {
            column: "block.call_id".into(),
            detail: "tool call block has no pairing key".into(),
        })?;
    let data = row.data.as_ref();
    let tool = data
        .and_then(|data| data.get("name"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| CoreError::MalformedColumn {
            column: "block.data".into(),
            detail: "tool call block has no tool name".into(),
        })?;
    let arguments = data
        .and_then(|data| data.get("arguments"))
        .cloned()
        .unwrap_or_else(|| {
            if row.content.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(row.content.clone())
            }
        });
    let server_name = match ToolOrigin::parse(tool) {
        Some(ToolOrigin::Mcp { server, .. }) => server,
        _ => String::new(),
    };
    Ok(PendingCall {
        call_id,
        server_name,
        tool: tool.to_string(),
        arguments,
    })
}

fn pending_call_of(call: FunctionCall) -> PendingCall {
    let server_name = match ToolOrigin::parse(&call.name) {
        Some(ToolOrigin::Mcp { server, .. }) => server,
        _ => String::new(),
    };
    PendingCall {
        call_id: call.id,
        server_name,
        tool: call.name,
        arguments: serde_json::from_str(&call.arguments)
            .unwrap_or_else(|_| serde_json::Value::String(call.arguments)),
    }
}

fn result_block(row: &BlockRow) -> Option<Content> {
    match row.kind {
        BlockKind::Text => Some(Content::new_text(row.content.clone())),
        BlockKind::Media
            if row
                .data
                .as_ref()
                .and_then(|data| data.get("mime"))
                .and_then(serde_json::Value::as_str)
                == Some(MEDIA_IMAGE) =>
        {
            Some(Content::new_image(row.content.clone()))
        }
        _ => None,
    }
}

fn decode_instance_phase(instance: &AgentInstance) -> Result<Phase<AgentDomain>> {
    let payload = instance
        .phase_payload
        .as_deref()
        .map(serde_json::from_str::<serde_json::Value>)
        .transpose()
        .map_err(|err| CoreError::MalformedColumn {
            column: "agent_instances.phase_payload".into(),
            detail: err.to_string(),
        })?;
    let stored = StoredPhase {
        name: instance.phase.to_string(),
        payload,
    };
    decode(&stored).map_err(|err| CoreError::MalformedColumn {
        column: "agent_instances.phase".into(),
        detail: err.to_string(),
    })
}

fn decisions_of(wait: &ExternalWait) -> Result<Vec<CallDecision>> {
    let value = wait
        .decision
        .clone()
        .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
    serde_json::from_value(value).map_err(|err| CoreError::MalformedColumn {
        column: "external_waits.decision_json".into(),
        detail: err.to_string(),
    })
}

/// 组装节点树
fn attach_children(snaps: &mut Vec<NodeSnapshot<AgentDomain>>) {
    loop {
        let ids: BTreeSet<NodeId> = snaps.iter().map(|snap| snap.id).collect();
        let mut dropped = Vec::new();
        snaps.retain(|snap| {
            let keep = snap.parent.is_none_or(|parent| ids.contains(&parent));
            if !keep {
                dropped.push(snap.id);
            }
            keep
        });
        if dropped.is_empty() {
            break;
        }
        for id in dropped {
            log::warn!("[store] instance {id} has no parent in the snapshot, dropped");
        }
    }

    for index in 0..snaps.len() {
        let id = snaps[index].id;
        let children: Vec<NodeId> = snaps
            .iter()
            .filter(|snap| snap.parent == Some(id))
            .map(|snap| snap.id)
            .collect();
        snaps[index].children = children;
    }
}
