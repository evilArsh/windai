use super::def::EventMeta;

#[async_trait::async_trait]
pub trait BuiltinObserver: Send + Sync {
    async fn on_event(&self, meta: &EventMeta, event: &serde_json::Value);
}
