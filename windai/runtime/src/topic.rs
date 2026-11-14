use super::RuntimeError;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wind_core::agent::Phase;
use wind_core::agent::handler::{Agent, AgentHandle, AgentOptions, NodeSnapshot};
use wind_core::llm::LlmRunner;
use wind_core::llm::domain::{LlmDomain, LlmSpec};
use wind_event::Dispatcher;
use wind_storage::{AgentInstance, AgentPending, Storage};
use wind_wire::{AgentMode, AgentRole};

pub(super) struct TopicAgents {
    pub storage: Storage,
    dispatcher: Arc<Dispatcher>,
    runner: Arc<LlmRunner>,
    slots: Mutex<HashMap<i64, Arc<SessionSlot>>>,
    stopped: AtomicBool,
}

struct SessionSlot {
    session: AsyncMutex<Option<Arc<Session>>>,
}

pub(super) struct Session {
    pub handle: AgentHandle<LlmDomain>,
    pub main: Mutex<Option<i64>>,
    pub gate: AsyncMutex<()>,
    task: AsyncMutex<Option<JoinHandle<()>>>,
}

impl TopicAgents {
    pub fn new(storage: Storage, dispatcher: Arc<Dispatcher>, runner: Arc<LlmRunner>) -> Arc<Self> {
        Arc::new(Self {
            storage,
            dispatcher,
            runner,
            slots: Mutex::new(HashMap::new()),
            stopped: AtomicBool::new(false),
        })
    }

    pub async fn session(&self, topic_id: i64) -> Result<Arc<Session>, RuntimeError> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(RuntimeError::Stopped);
        }
        let slot = self
            .slots
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(topic_id)
            .or_insert_with(|| {
                Arc::new(SessionSlot {
                    session: AsyncMutex::new(None),
                })
            })
            .clone();
        let mut guard = slot.session.lock().await;
        if let Some(session) = guard.as_ref() {
            return Ok(session.clone());
        }
        if self.storage.get_topic_by_id(topic_id).await?.is_none() {
            return Err(RuntimeError::Denied(format!(
                "topic {topic_id} does not exist"
            )));
        }
        let (instances, pending) = self
            .storage
            .with_read_tx(async move |tx| {
                Ok((
                    tx.list_instances_by_topic(topic_id).await?,
                    tx.list_pending_by_topic(topic_id).await?,
                ))
            })
            .await?;
        let main = instances
            .iter()
            .find(|instance| instance.role == AgentRole::Main)
            .map(|instance| instance.id);
        let restored = snapshots(instances, pending);
        let (agent, handle) = Agent::<LlmDomain>::new(AgentOptions {
            topic_id,
            storage: self.storage.clone(),
            dispatcher: self.dispatcher.clone(),
            runner: self.runner.clone(),
            delivery: Some(self.runner.clone()),
            restored,
            cancel: CancellationToken::new(),
        });
        let session = Arc::new(Session {
            handle,
            main: Mutex::new(main),
            gate: AsyncMutex::new(()),
            task: AsyncMutex::new(Some(tokio::spawn(agent.run()))),
        });
        *guard = Some(session.clone());
        Ok(session)
    }

    pub async fn shutdown(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        let slots: Vec<_> = self
            .slots
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect();
        for slot in slots {
            if let Some(session) = slot.session.lock().await.as_ref() {
                session.handle.shutdown();
                if let Some(task) = session.task.lock().await.take()
                    && let Err(error) = task.await
                {
                    log::error!("[runtime] agent task failed: {error}");
                }
            }
        }
    }
}

fn snapshots(
    instances: Vec<AgentInstance>,
    pending: Vec<AgentPending>,
) -> Vec<NodeSnapshot<LlmDomain>> {
    let mut children: HashMap<i64, Vec<i64>> = HashMap::new();
    let mut inputs: HashMap<i64, Vec<LlmSpec>> = HashMap::new();
    for instance in &instances {
        if let Some(parent) = instance.parent_id {
            children.entry(parent).or_default().push(instance.id);
        }
    }
    for row in pending {
        inputs
            .entry(row.instance_id)
            .or_default()
            .push(LlmSpec::from_pending(row.instance_id, row.id));
    }
    instances
        .into_iter()
        .map(|instance| {
            let phase = Phase::from_columns(&instance.phase, instance.phase_payload.as_deref())
                .unwrap_or_else(|error| {
                    log::warn!("[runtime] cannot decode phase for {}: {error}", instance.id);
                    Phase::Aborted
                });
            let pending = if matches!(
                phase,
                Phase::Running | Phase::WaitingChildren | Phase::Suspended(_) | Phase::Done(_)
            ) {
                inputs.remove(&instance.id).unwrap_or_default()
            } else {
                Vec::new()
            };
            NodeSnapshot {
                id: instance.id,
                parent: instance.parent_id,
                children: children.remove(&instance.id).unwrap_or_default(),
                phase,
                detached: instance.mode == Some(AgentMode::Background),
                pending,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Runtime, RuntimeOptions};
    use std::future::poll_fn;
    use std::task::Poll;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::time::timeout;
    use wind_core::agent::CancelOrigin;
    use wind_core::llm::domain::Suspended;
    use wind_wire::{AppDirs, Content, PendingKind};

    async fn unstarted(
        phase: Phase<LlmDomain>,
    ) -> (
        TempDir,
        Runtime,
        Agent<LlmDomain>,
        AgentHandle<LlmDomain>,
        i64,
        i64,
    ) {
        let dir = TempDir::new().unwrap();
        let runtime = Runtime::open(RuntimeOptions::new(AppDirs::under(dir.path())))
            .await
            .unwrap();
        let storage = runtime.storage();
        let topic = storage
            .create_topic(None, "receipt", None, None, None)
            .await
            .unwrap();
        let (name, payload) = phase.columns().unwrap();
        let root = storage
            .create_agent_instance(
                topic,
                None,
                AgentRole::Main,
                Some(AgentMode::Sync),
                &name,
                payload,
            )
            .await
            .unwrap();
        let (agent, handle) = Agent::new(AgentOptions {
            topic_id: topic,
            storage: storage.clone(),
            dispatcher: runtime.inner.dispatcher.clone(),
            runner: runtime.inner.agents.runner.clone(),
            delivery: None,
            cancel: CancellationToken::new(),
            restored: vec![NodeSnapshot {
                id: root,
                parent: None,
                children: vec![],
                phase,
                detached: false,
                pending: vec![],
            }],
        });
        (dir, runtime, agent, handle, topic, root)
    }

    #[tokio::test]
    async fn cancellation_acknowledges_persisted_phase_before_background_insertion() {
        let (_dir, runtime, agent, handle, topic, root) = unstarted(Phase::Suspended(Suspended {
            external_wait_id: 1,
        }))
        .await;
        let mut cancel = Box::pin(handle.cancel(root, CancelOrigin::External));
        let waiting = poll_fn(|cx| Poll::Ready(cancel.as_mut().poll(cx).is_pending())).await;
        assert!(
            waiting,
            "wait for the agent to apply cancellation before acknowledging it"
        );
        let task = tokio::spawn(agent.run());
        cancel.await.unwrap();
        assert_eq!(
            runtime
                .storage()
                .get_instance_by_id(root)
                .await
                .unwrap()
                .unwrap()
                .phase,
            "cancelled"
        );
        let child = runtime
            .storage()
            .create_agent_instance(
                topic,
                Some(root),
                AgentRole::Child,
                Some(AgentMode::Background),
                "aborted",
                None,
            )
            .await
            .unwrap();
        assert!(
            runtime
                .storage()
                .insert_background_result(root, vec![], child)
                .await
                .unwrap()
                .is_none()
        );
        handle.shutdown();
        task.await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn enqueue_acknowledges_reopening_before_immediate_shutdown_and_recovery() {
        let (dir, runtime, agent, handle, topic, root) =
            unstarted(Phase::Cancelled(CancelOrigin::External)).await;
        let pending = runtime
            .storage()
            .create_pending(
                root,
                PendingKind::User,
                vec![Content::new_text("accepted before shutdown".into())],
                None,
            )
            .await
            .unwrap();
        let mut enqueue = Box::pin(handle.enqueue(root, LlmSpec::from_pending(root, pending)));
        let waiting = poll_fn(|cx| Poll::Ready(enqueue.as_mut().poll(cx).is_pending())).await;
        assert!(waiting, "persist reopening before acknowledging the input");
        let task = tokio::spawn(agent.run());
        enqueue.await.unwrap();
        handle.shutdown();
        task.await.unwrap();
        runtime.shutdown().await.unwrap();
        let runtime = Runtime::open(RuntimeOptions::new(AppDirs::under(dir.path())))
            .await
            .unwrap();
        let trigger = runtime
            .storage()
            .create_agent_instance(
                topic,
                Some(root),
                AgentRole::Child,
                Some(AgentMode::Sync),
                "running",
                None,
            )
            .await
            .unwrap();
        runtime.cancel(trigger).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let context = runtime.storage().list_instance_context(root).await.unwrap();
                if !context.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            runtime
                .storage()
                .list_pending_by_topic(topic)
                .await
                .unwrap()
                .is_empty()
        );
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn live_child_without_spawn_mapping_still_releases_the_tree() {
        let tmp = TempDir::new().unwrap();
        let runtime = Runtime::open(RuntimeOptions::new(AppDirs::under(tmp.path())))
            .await
            .unwrap();
        let storage = runtime.storage();
        let topic = storage
            .create_topic(None, "live", None, None, None)
            .await
            .unwrap();
        let root = storage
            .create_agent_instance(
                topic,
                None,
                AgentRole::Main,
                Some(AgentMode::Sync),
                "done",
                Some(r#"{"message_id":0}"#.into()),
            )
            .await
            .unwrap();
        let session = runtime.inner.agents.session(topic).await.unwrap();
        let child = storage
            .create_agent_instance(
                topic,
                Some(root),
                AgentRole::Child,
                Some(AgentMode::Background),
                "running",
                None,
            )
            .await
            .unwrap();
        let pending = storage
            .create_pending(
                child,
                PendingKind::User,
                vec![Content::new_text("child".into())],
                None,
            )
            .await
            .unwrap();
        let mut sub = runtime.subscribe().await.unwrap().subscription;
        session
            .handle
            .spawn(
                child,
                Some(root),
                LlmSpec::from_pending(child, pending),
                true,
            )
            .await
            .unwrap();
        timeout(Duration::from_secs(5), async {
            loop {
                let event = sub.recv().await.unwrap().unwrap();
                if event.envelope.name == "core/agent.sealed" {
                    break;
                }
            }
        })
        .await
        .expect("release the live child's delivery mark");
        assert_eq!(
            storage
                .get_instance_by_id(child)
                .await
                .unwrap()
                .unwrap()
                .phase,
            "failed"
        );
        runtime.shutdown().await.unwrap();
    }
}
