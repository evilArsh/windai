use crate::agent::domain::{AgentDomain, AgentFailure, AgentOutcome, AgentSpec, PendingApproval};
use crate::agent::hub::{NodeSnapshot, Persist};
use crate::agent::tree::NodeId;
use crate::agent::tree::phase::{CancelOrigin, Phase};
use crate::error::{CoreError, Result};
use crate::models::{AgentMode, AgentStatus, Message, UpdateInstance};
use crate::storage::Storage;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::time::Duration;

/// 一次落库的重试次数
const SAVE_ATTEMPTS: usize = 3;
/// 第 n 次失败之后的退避；长度比 `SAVE_ATTEMPTS` 少一是故意的
const SAVE_BACKOFF: [Duration; 2] = [Duration::from_millis(20), Duration::from_millis(100)];

/// 有界重试一个写操作，`attempts` 次都失败后返回最后一个错误
///
/// 调用方必须保证 `op` 幂等；重试不看错误码；
/// 最后一次尝试失败后不再退避
pub(crate) async fn retry_save<F, Fut>(
    attempts: usize,
    backoff: &[Duration],
    mut op: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut last = None;
    for attempt in 0..attempts {
        match op().await {
            Ok(()) => return Ok(()),
            Err(err) => {
                last = Some(err);
                if attempt + 1 < attempts {
                    if let Some(delay) = backoff.get(attempt) {
                        tokio::time::sleep(*delay).await;
                    }
                }
            }
        }
    }
    Err(last.unwrap_or_else(|| CoreError::Internal("retry_save called with 0 attempts".into())))
}

/// 树相位到 `agent_instances.status` 的投影
///
/// `Aborted` 投影成 `failed`
pub(crate) fn status_of(phase: &Phase<AgentDomain>) -> AgentStatus {
    match phase {
        Phase::Running => AgentStatus::Running,
        Phase::Suspended(_) => AgentStatus::WaitingApproval,
        Phase::WaitingChildren => AgentStatus::WaitingChild,
        Phase::Cancelling(_) => AgentStatus::Cancelling,
        Phase::Done(_) => AgentStatus::Finished,
        Phase::Failed(_) => AgentStatus::Failed,
        Phase::Cancelled(_) => AgentStatus::Cancelled,
        Phase::Aborted => AgentStatus::Failed,
    }
}

/// 节点状态的持久化适配与恢复读取
pub struct AgentStore {
    storage: Storage,
}

impl AgentStore {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }

    /// 从库重建一个 topic 的节点快照
    ///
    /// `idle` 的实例尚未进入任务树，跳过它们；
    /// 相位载荷（`Output` / `Error` / `Suspended`）库里没有对应列，按约定从
    /// 「该实例最后一条 assistant 消息」重建；
    /// 载荷带 `message_id` 的相位（`WaitingApproval` / `Finished`）缺消息时整条实例跳过，
    /// 其余相位不要求实例有消息；
    /// 父不在结果集里的节点连同它的后代一并丢弃
    pub async fn load_snapshots(&self, topic_id: i64) -> Result<Vec<NodeSnapshot<AgentDomain>>> {
        let instances = self
            .storage
            .agent()
            .list_instances_by_topic(topic_id)
            .await?;
        let ids: Vec<i64> = instances.iter().map(|instance| instance.id).collect();
        let mut messages: HashMap<i64, Vec<Message>> = HashMap::new();
        for message in self.storage.message().list_by_instances(&ids).await? {
            messages
                .entry(message.instance_id)
                .or_default()
                .push(message);
        }
        let mut snaps = Vec::new();
        for instance in instances {
            if instance.status == AgentStatus::Idle {
                continue;
            }
            let own = messages.remove(&instance.id).unwrap_or_default();
            let message_id = last_message_id(&own);
            let phase = match (instance.status, message_id) {
                (AgentStatus::Idle, _) => unreachable!("已在上面跳过"),
                (AgentStatus::Running, _) => Phase::Running,
                (AgentStatus::WaitingChild, _) => Phase::WaitingChildren,
                (AgentStatus::Cancelling, _) => Phase::Cancelling(CancelOrigin::External),
                (AgentStatus::Cancelled, _) => Phase::Cancelled(CancelOrigin::External),
                (AgentStatus::Failed, message_id) => Phase::Failed(AgentFailure {
                    message_id,
                    error: "task failed before restart".into(),
                }),
                (AgentStatus::WaitingApproval, Some(message_id)) => {
                    Phase::Suspended(PendingApproval {
                        instance_id: instance.id,
                        message_id,
                    })
                }
                (AgentStatus::Finished, Some(message_id)) => {
                    Phase::Done(AgentOutcome { message_id })
                }
                (AgentStatus::WaitingApproval | AgentStatus::Finished, None) => {
                    log::warn!("[agent] instance {} has no message, skipped", instance.id);
                    continue;
                }
            };
            let pending = pending_of(instance.id, &phase, own.last());
            snaps.push(NodeSnapshot {
                id: instance.id,
                parent: instance.parent_id,
                children: Vec::new(),
                phase,
                detached: instance.mode == Some(AgentMode::Background),
                pending,
            });
        }
        // 父不在集合里的节点连同它的后代一起丢掉：留着会让它的父停在
        // `WaitingChildren` 而屏障为空，或让它变成没有根的孤儿
        loop {
            let ids: HashSet<NodeId> = snaps.iter().map(|snap| snap.id).collect();
            let mut dropped: Vec<NodeId> = Vec::new();
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
                log::warn!("[agent] instance {id} has no parent in the snapshot, dropped");
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
        Ok(snaps)
    }
}

/// 实例最后一条 assistant 消息的 id，没有则退到它最后一条消息
fn last_message_id(messages: &[Message]) -> Option<i64> {
    messages
        .iter()
        .rev()
        .find(|m| m.from_id.is_some())
        .or_else(|| messages.last())
        .map(|m| m.id)
}

/// 恢复出的待消费输入：完结的实例末条消息停在一条没被排除的 user 消息上
///
/// 取消的实例不派生 —— 取消语义是丢弃输入，恢复时不该把它捡回来重跑
fn pending_of(
    instance_id: i64,
    phase: &Phase<AgentDomain>,
    last: Option<&Message>,
) -> Option<AgentSpec> {
    match phase {
        Phase::Done(_) | Phase::Failed(_) => {}
        _ => return None,
    }
    let last = last?;
    if last.from_id.is_some() || last.is_excluded {
        return None;
    }
    Some(AgentSpec {
        instance_id,
        message_id: None,
    })
}

#[async_trait::async_trait]
impl Persist<AgentDomain> for AgentStore {
    async fn save(&self, node: NodeId, phase: &Phase<AgentDomain>) -> Result<()> {
        let status = status_of(phase);
        let agent = self.storage.agent();
        retry_save(SAVE_ATTEMPTS, &SAVE_BACKOFF, || async {
            agent
                .update_instance(
                    node,
                    UpdateInstance {
                        status: Some(status),
                        ..Default::default()
                    },
                )
                .await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::domain::{AgentFailure, AgentOutcome, PendingApproval};
    use crate::agent::tree::phase::{CancelOrigin, Phase};
    use crate::models::AgentStatus;

    #[test]
    fn every_phase_projects_to_an_agent_status() {
        let cases: Vec<(Phase<AgentDomain>, AgentStatus)> = vec![
            (Phase::Running, AgentStatus::Running),
            (
                Phase::Suspended(PendingApproval {
                    instance_id: 1,
                    message_id: 2,
                }),
                AgentStatus::WaitingApproval,
            ),
            (Phase::WaitingChildren, AgentStatus::WaitingChild),
            (
                Phase::Cancelling(CancelOrigin::External),
                AgentStatus::Cancelling,
            ),
            (
                Phase::Done(AgentOutcome { message_id: 2 }),
                AgentStatus::Finished,
            ),
            (
                Phase::Failed(AgentFailure {
                    message_id: Some(2),
                    error: "boom".into(),
                }),
                AgentStatus::Failed,
            ),
            (
                Phase::Cancelled(CancelOrigin::External),
                AgentStatus::Cancelled,
            ),
            (Phase::Aborted, AgentStatus::Failed),
        ];
        for (phase, expected) in cases {
            assert_eq!(status_of(&phase), expected, "相位 {phase:?} 的投影");
        }
    }

    #[test]
    fn every_agent_status_keeps_its_column_literal() {
        let cases = [
            (AgentStatus::Idle, "idle"),
            (AgentStatus::Running, "running"),
            (AgentStatus::WaitingApproval, "waiting_approval"),
            (AgentStatus::WaitingChild, "waiting_child"),
            (AgentStatus::Cancelling, "cancelling"),
            (AgentStatus::Finished, "finished"),
            (AgentStatus::Failed, "failed"),
            (AgentStatus::Cancelled, "cancelled"),
        ];
        for (status, literal) in cases {
            assert_eq!(
                status.to_string(),
                literal,
                "写进 agent_instances.status 的字面值"
            );
            let back: AgentStatus = literal.parse().expect("列值必须能解析回枚举");
            assert_eq!(back, status, "{literal} 必须原样往返");
        }
    }

    use std::cell::Cell;
    use std::time::Duration;

    /// 重试用尽后必须把最后一个错误原样交出去
    #[tokio::test]
    async fn retry_save_gives_up_after_the_configured_attempts() {
        let calls = Cell::new(0usize);
        let err = retry_save(3, &[Duration::from_millis(1)], || {
            calls.set(calls.get() + 1);
            async { Err(crate::error::CoreError::Internal("store down".into())) }
        })
        .await
        .expect_err("三次都失败必须把错误交出去");
        assert_eq!(calls.get(), 3, "必须恰好尝试 SAVE_ATTEMPTS 次");
        assert!(err.to_string().contains("store down"), "原始错误不能被吃掉");
    }

    #[tokio::test]
    async fn retry_save_succeeds_once_the_store_recovers() {
        let calls = Cell::new(0usize);
        retry_save(
            3,
            &[Duration::from_millis(1), Duration::from_millis(1)],
            || {
                let attempt = calls.get();
                calls.set(attempt + 1);
                async move {
                    if attempt < 2 {
                        Err(crate::error::CoreError::Internal("busy".into()))
                    } else {
                        Ok(())
                    }
                }
            },
        )
        .await
        .expect("第三次必须成功");
        assert_eq!(calls.get(), 3);
    }

    /// 时钟被暂停，退避时长是精确值而不是墙钟估计
    #[tokio::test(start_paused = true)]
    async fn retry_save_does_not_sleep_after_the_final_failure() {
        let started = tokio::time::Instant::now();
        let _ = retry_save(
            2,
            &[Duration::from_millis(30), Duration::from_millis(30)],
            || async { Err::<(), _>(crate::error::CoreError::Internal("busy".into())) },
        )
        .await;
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(30),
            "2 次尝试只该退避 1 次，最后一次失败不得再睡"
        );
    }
}
