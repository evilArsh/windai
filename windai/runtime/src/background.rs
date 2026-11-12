use super::RuntimeError;
use super::topic::{Session, TopicAgents, spec};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use wind_core::agent::events::{AgentStatus, CORE_AGENT_STATUS};
use wind_event::Observer;
use wind_storage::Storage;
use wind_wire::event::FeedEvent;
use wind_wire::{AgentMode, Content};

pub(super) struct DeliveryQueue {
    tx: Mutex<Option<mpsc::UnboundedSender<i64>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl DeliveryQueue {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<i64>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                tx: Mutex::new(Some(tx)),
                worker: Mutex::new(None),
            },
            rx,
        )
    }

    pub fn start(&self, agents: &Arc<TopicAgents>, mut rx: mpsc::UnboundedReceiver<i64>) {
        let agents = Arc::downgrade(agents);
        let task = tokio::spawn(async move {
            while let Some(child_id) = rx.recv().await {
                let Some(agents) = agents.upgrade() else {
                    break;
                };
                if let Err(error) = deliver(&agents, child_id).await {
                    log::error!("[runtime] background delivery for {child_id} failed: {error}");
                }
            }
        });
        *self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(task);
    }

    pub fn push(&self, child_id: i64) {
        if let Some(tx) = self
            .tx
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            let _ = tx.send(child_id);
        }
    }

    pub async fn shutdown(&self) {
        self.tx
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let task = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(task) = task
            && let Err(error) = task.await
        {
            log::error!("[runtime] delivery task failed: {error}");
        }
    }
}

impl Observer for TopicAgents {
    fn on_event(&self, event: &FeedEvent) {
        if event.envelope.name != CORE_AGENT_STATUS.name {
            return;
        }
        let Some(payload) = event.envelope.payload.as_ref() else {
            return;
        };
        let Ok(status) = serde_json::from_value::<AgentStatus>(payload.clone()) else {
            log::warn!("[runtime] a status payload could not be read");
            return;
        };
        if matches!(
            status.phase.as_str(),
            "done" | "failed" | "cancelled" | "aborted"
        ) {
            self.deliveries.push(status.instance_id);
        }
    }
}

async fn deliver(agents: &TopicAgents, child_id: i64) -> Result<(), RuntimeError> {
    if agents.is_stopped() {
        return Ok(());
    }
    let Some(child) = agents.storage.get_instance_by_id(child_id).await? else {
        return Ok(());
    };
    if child.mode != Some(AgentMode::Background) {
        return Ok(());
    }
    let session = agents.session(child.topic_id).await?;
    let _gate = session.gate.lock().await;
    inject(&agents.storage, &session, child_id).await?;
    if let Err(error) = session.handle.deliver(child_id).await {
        log::warn!("[runtime] the delivery mark of {child_id} was not cleared: {error}");
    }
    Ok(())
}

async fn inject(storage: &Storage, session: &Session, child_id: i64) -> Result<(), RuntimeError> {
    let Some(call) = storage.spawn_call_of_child(child_id).await? else {
        return Ok(());
    };
    if call.result_delivered_at.is_some() {
        return Ok(());
    }
    let rows = storage
        .list_child_states(call.parent_id, &[&call.call_id])
        .await?;
    let phase = rows
        .first()
        .map(|row| row.phase.as_str())
        .unwrap_or_default();
    let reply: String = rows
        .iter()
        .filter_map(|row| row.reply_part.as_deref())
        .collect();
    let content = content_of(child_id, phase, &reply);
    let parent_id = call.parent_id;
    let pending = storage
        .with_tx(async move |tx| {
            if tx.claim_result_delivery(child_id).await?.is_none() {
                return Ok(None);
            }
            tx.insert_background_result(parent_id, content, child_id)
                .await
        })
        .await?;
    if let Some(pending_id) = pending {
        session
            .handle
            .enqueue(parent_id, spec(parent_id, pending_id))
            .await?;
    }
    Ok(())
}

fn content_of(child_id: i64, phase: &str, reply: &str) -> Vec<Content> {
    let text = if phase == "done" && !reply.trim().is_empty() {
        reply.to_owned()
    } else {
        format!("background child {child_id} ended with phase {phase}")
    };
    vec![Content::new_text(text)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Runtime, RuntimeOptions};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::time::timeout;
    use wind_wire::{AgentRole, AppDirs, PendingKind};

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
            .spawn(child, Some(root), spec(child, pending), true)
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

    #[tokio::test]
    async fn duplicate_delivery_keeps_one_pending_result() {
        let tmp = TempDir::new().unwrap();
        let runtime = Runtime::open(RuntimeOptions::new(AppDirs::under(tmp.path())))
            .await
            .unwrap();
        let storage = runtime.storage();
        let topic = storage
            .create_topic(None, "duplicate", None, None, None)
            .await
            .unwrap();
        let root = storage
            .create_agent_instance(
                topic,
                None,
                AgentRole::Main,
                Some(AgentMode::Sync),
                "suspended",
                Some(r#"{"external_wait_id":1}"#.into()),
            )
            .await
            .unwrap();
        runtime.inner.agents.session(topic).await.unwrap();
        let child = storage
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
        storage
            .create_spawn_call_map(root, "duplicate-call", child)
            .await
            .unwrap();
        deliver(&runtime.inner.agents, child).await.unwrap();
        deliver(&runtime.inner.agents, child).await.unwrap();
        let pending = storage.list_pending_by_topic(topic).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].source_id, Some(child));
        assert_eq!(pending[0].instance_id, root);
        assert!(
            storage
                .spawn_call_of_child(child)
                .await
                .unwrap()
                .unwrap()
                .result_delivered_at
                .is_some()
        );
        runtime.shutdown().await.unwrap();
    }
}
