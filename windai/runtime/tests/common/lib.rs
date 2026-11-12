#![allow(dead_code)]
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;
use wind_core::agent::Phase;
use wind_core::feed::Subscription;
use wind_core::llm::domain::LlmDomain;
use wind_runtime::{Runtime, RuntimeOptions};
use wind_storage::Storage;
use wind_wire::event::FeedEvent;
use wind_wire::{AgentMode, AgentRole, AppDirs, Content, PendingKind};

pub struct Fixture {
    pub dir: TempDir,
    pub dirs: AppDirs,
    pub storage: Storage,
    pub topic: i64,
}
impl Fixture {
    pub async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let dirs = AppDirs::under(dir.path());
        let storage = Storage::open(&dirs.db_file()).await.unwrap();
        let topic = storage
            .create_topic(None, "runtime", None, None, None)
            .await
            .unwrap();
        Self {
            dir,
            dirs,
            storage,
            topic,
        }
    }
    pub async fn open(&self) -> Runtime {
        Runtime::open(RuntimeOptions::new(self.dirs.clone()))
            .await
            .unwrap()
    }
    pub async fn instance(
        &self,
        parent: Option<i64>,
        mode: AgentMode,
        phase: Phase<LlmDomain>,
    ) -> i64 {
        let (phase, payload) = phase.columns().unwrap();
        self.storage
            .create_agent_instance(
                self.topic,
                parent,
                if parent.is_some() {
                    AgentRole::Child
                } else {
                    AgentRole::Main
                },
                Some(mode),
                &phase,
                payload,
            )
            .await
            .unwrap()
    }
    pub async fn pending(&self, instance: i64, text: &str) -> i64 {
        self.storage
            .create_pending(
                instance,
                PendingKind::User,
                vec![Content::new_text(text.into())],
                None,
            )
            .await
            .unwrap()
    }
    pub async fn phase(&self, instance: i64) -> String {
        self.storage
            .get_instance_by_id(instance)
            .await
            .unwrap()
            .unwrap()
            .phase
    }
}

pub async fn event_until(
    subscription: &mut Subscription,
    predicate: impl Fn(&FeedEvent) -> bool,
) -> Arc<FeedEvent> {
    timeout(Duration::from_secs(5), async {
        loop {
            let event = subscription
                .recv()
                .await
                .unwrap()
                .expect("receive an event");
            if predicate(&event) {
                return event;
            }
        }
    })
    .await
    .expect("receive the expected event")
}

pub async fn eventually<F, Fut>(mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    timeout(Duration::from_secs(5), async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("observe the expected state");
}
