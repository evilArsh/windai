use super::{Runtime, RuntimeError, SubmitAck};
use std::path::Path;
use std::time::Duration;
use wind_core::agent::{CancelOrigin, Phase};
use wind_core::llm::domain::{LlmDomain, LlmSpec, WaitAnswer};
use wind_core::llm::events::{CORE_INPUT_ACCEPTED, InputAccepted};
use wind_core::resources::ApplyReport;
use wind_event::{DispatchCx, Origin, Scope, Source};
use wind_wire::{AgentMode, Content, PendingKind};

impl Runtime {
    pub async fn submit(
        &self,
        topic_id: i64,
        content: Vec<Content>,
    ) -> Result<SubmitAck, RuntimeError> {
        let _operation = self.enter().await?;
        let session = self.inner.agents.session(topic_id).await?;
        let _gate = session.gate.lock().await;
        let main = *session
            .main
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let instance_id = match main {
            Some(id) => id,
            None => {
                self.storage()
                    .ensure_main(topic_id, AgentMode::Sync)
                    .await?
            }
        };
        let pending_id = self
            .storage()
            .create_pending(instance_id, PendingKind::User, content, None)
            .await?;
        let input = LlmSpec::from_pending(instance_id, pending_id);
        let dispatched = if main.is_none() {
            session.handle.spawn(instance_id, None, input, false).await
        } else {
            session.handle.enqueue(instance_id, input).await
        };
        if let Err(error) = dispatched {
            self.storage().delete_pending_by_ids(&[pending_id]).await?;
            return Err(error.into());
        }
        *session
            .main
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(instance_id);
        self.inner.dispatcher.notify(
            &CORE_INPUT_ACCEPTED,
            &DispatchCx::root(
                Origin::Internal,
                Scope::Topic {
                    topic_id,
                    instance_id,
                },
                Source::core("runtime"),
                Duration::from_millis(50),
            ),
            InputAccepted {
                topic_id,
                instance_id,
                pending_id,
            },
        );
        Ok(SubmitAck {
            topic_id,
            instance_id,
            pending_id,
        })
    }

    pub async fn cancel(&self, instance_id: i64) -> Result<(), RuntimeError> {
        let _operation = self.enter().await?;
        let topic = self
            .storage()
            .get_topic_by_instance(instance_id)
            .await?
            .ok_or_else(|| {
                RuntimeError::Denied(format!("instance {instance_id} does not exist"))
            })?;
        let session = self.inner.agents.session(topic.id).await?;
        let _gate = session.gate.lock().await;
        session
            .handle
            .cancel(instance_id, CancelOrigin::External)
            .await?;
        self.storage().close_pending(instance_id).await?;
        Ok(())
    }

    pub async fn resume(&self, answer: WaitAnswer) -> Result<(), RuntimeError> {
        let _operation = self.enter().await?;
        let wait = self
            .storage()
            .get_external_wait(answer.external_wait_id)
            .await?
            .filter(|wait| wait.answered_at.is_none())
            .ok_or_else(|| {
                RuntimeError::Denied(format!(
                    "external wait {} is unavailable",
                    answer.external_wait_id
                ))
            })?;
        let instance = self
            .storage()
            .get_instance_by_id(wait.instance_id)
            .await?
            .ok_or_else(|| {
                RuntimeError::Denied(format!("instance {} does not exist", wait.instance_id))
            })?;
        let phase =
            Phase::<LlmDomain>::from_columns(&instance.phase, instance.phase_payload.as_deref());
        let expect = match phase {
            Ok(Phase::Suspended(expect)) if expect.external_wait_id == answer.external_wait_id => {
                expect
            }
            _ => {
                return Err(RuntimeError::Denied(format!(
                    "instance {} is not waiting for {}",
                    instance.id, answer.external_wait_id
                )));
            }
        };
        let session = self.inner.agents.session(instance.topic_id).await?;
        let _gate = session.gate.lock().await;
        session.handle.resume(instance.id, expect, answer).await?;
        Ok(())
    }
}

impl Runtime {
    pub async fn install(&self, package: &Path) -> Result<ApplyReport, RuntimeError> {
        let _operation = self.enter().await?;
        Ok(self.inner.services.install(&resource_cx(), package).await?)
    }

    pub async fn update(&self, package: &Path) -> Result<ApplyReport, RuntimeError> {
        let _operation = self.enter().await?;
        Ok(self.inner.services.update(&resource_cx(), package).await?)
    }

    pub async fn delete(&self, name: &str) -> Result<ApplyReport, RuntimeError> {
        let _operation = self.enter().await?;
        Ok(self.inner.services.delete(&resource_cx(), name).await?)
    }

    pub async fn enable(&self, topic_id: i64, name: &str) -> Result<ApplyReport, RuntimeError> {
        let _operation = self.enter().await?;
        Ok(self
            .inner
            .services
            .enable(&resource_cx(), topic_id, name)
            .await?)
    }

    pub async fn disable(&self, topic_id: i64, name: &str) -> Result<ApplyReport, RuntimeError> {
        let _operation = self.enter().await?;
        Ok(self
            .inner
            .services
            .disable(&resource_cx(), topic_id, name)
            .await?)
    }
}

fn resource_cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Internal,
        Scope::Global,
        Source::core("runtime"),
        Duration::from_millis(100),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeOptions;
    use tempfile::TempDir;
    use wind_wire::AppDirs;

    #[tokio::test]
    async fn failed_dispatch_removes_pending_without_caching_main() {
        let tmp = TempDir::new().unwrap();
        let runtime = Runtime::open(RuntimeOptions::new(AppDirs::under(tmp.path())))
            .await
            .unwrap();
        let topic = runtime
            .storage()
            .create_topic(None, "closed", None, None, None)
            .await
            .unwrap();
        let session = runtime.inner.agents.session(topic).await.unwrap();
        session.handle.shutdown();
        assert!(
            runtime
                .submit(topic, vec![Content::new_text("rejected".into())])
                .await
                .is_err()
        );
        assert!(
            runtime
                .storage()
                .list_pending_by_topic(topic)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(session.main.lock().unwrap().is_none());
        runtime.shutdown().await.unwrap();
    }
}
