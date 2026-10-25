//! 系统提示词的组装

use super::events::{CORE_PROMPT_COLLECT, PromptCollect};
use std::sync::Arc;
use wind_event::context::DispatchCx;
use wind_event::dispatch::Dispatcher;

pub struct PromptBuilder {
    bus: Arc<Dispatcher>,
}

impl PromptBuilder {
    pub fn new(bus: Arc<Dispatcher>) -> Self {
        Self { bus }
    }

    pub async fn collect(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
        instance_id: i64,
        fragments: Vec<String>,
    ) -> String {
        let payload = PromptCollect {
            topic_id,
            instance_id,
            fragments,
        };
        let value = self.bus.decide(&CORE_PROMPT_COLLECT, cx, payload).await;
        join(&value.fragments)
    }
}

pub(crate) fn join(fragments: &[String]) -> String {
    fragments.join("\n\n")
}
