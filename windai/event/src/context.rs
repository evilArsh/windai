use super::envelope::{EventSeq, Origin, Scope, Source};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct DispatchGuard {
    deadline: Instant,
    pub cancel: CancellationToken,
    emitted: AtomicU32,
}

impl DispatchGuard {
    fn new(total_budget: Duration) -> Self {
        Self {
            deadline: Instant::now() + total_budget,
            cancel: CancellationToken::new(),
            emitted: AtomicU32::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DispatchCx {
    pub origin: Origin,
    pub scope: Scope,
    pub source: Source,
    pub guard: Arc<DispatchGuard>,
    /// 嵌套层数，顶层为 0
    pub depth: u32,
    /// 起因事件的序号，顶层操作为 None
    pub cause: Option<EventSeq>,
}

impl DispatchCx {
    pub fn root(origin: Origin, scope: Scope, source: Source, total_budget: Duration) -> Self {
        Self {
            origin,
            scope,
            source,
            guard: Arc::new(DispatchGuard::new(total_budget)),
            depth: 0,
            cause: None,
        }
    }

    pub fn nested(&self, cause: EventSeq) -> Self {
        Self {
            depth: self.depth + 1,
            cause: Some(cause),
            ..self.clone()
        }
    }

    pub fn with_scope(&self, scope: Scope) -> Self {
        Self {
            scope,
            ..self.clone()
        }
    }

    pub fn with_source(&self, source: Source) -> Self {
        Self {
            source,
            ..self.clone()
        }
    }

    pub fn remaining_budget(&self) -> Duration {
        self.guard
            .deadline
            .saturating_duration_since(Instant::now())
    }

    pub fn deadline(&self) -> Instant {
        self.guard.deadline
    }

    pub fn depth_ok(&self, max_depth: u32) -> bool {
        self.depth <= max_depth
    }

    pub fn cancel(&self) -> &CancellationToken {
        &self.guard.cancel
    }

    pub fn count_emitted(&self, max: u32) -> bool {
        self.guard.emitted.fetch_add(1, Ordering::Relaxed) < max
    }

    pub fn emitted_count(&self) -> u32 {
        self.guard.emitted.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> DispatchCx {
        DispatchCx::root(
            Origin::Agent,
            Scope::Global,
            Source::core("agent"),
            Duration::from_millis(50),
        )
    }

    #[test]
    fn the_root_context_starts_at_depth_zero_without_a_cause() {
        let cx = root();
        assert_eq!(cx.depth, 0);
        assert!(cx.cause.is_none());
    }

    #[test]
    fn nesting_increments_depth_and_records_the_cause() {
        let root = root();
        let child = root.nested(EventSeq(1));
        let grandchild = child.nested(EventSeq(2));
        assert_eq!((root.depth, child.depth, grandchild.depth), (0, 1, 2));
        assert_eq!(child.cause, Some(EventSeq(1)));
        assert_eq!(grandchild.cause, Some(EventSeq(2)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_nested_context_draws_from_the_same_budget() {
        let root = root();
        let child = root.nested(EventSeq(1));
        tokio::time::advance(Duration::from_millis(30)).await;
        assert!(child.remaining_budget() <= Duration::from_millis(20));
        assert!(root.remaining_budget() <= Duration::from_millis(20));
        assert_eq!(child.deadline(), root.deadline());
    }

    #[tokio::test(start_paused = true)]
    async fn the_budget_never_goes_negative() {
        let cx = root();
        tokio::time::advance(Duration::from_millis(500)).await;
        assert_eq!(cx.remaining_budget(), Duration::ZERO);
    }

    #[test]
    fn emitted_events_are_counted_across_nesting() {
        let root = root();
        let child = root.nested(EventSeq(1));
        assert!(root.count_emitted(3));
        assert!(child.count_emitted(3));
        assert!(root.count_emitted(3));
        assert!(!child.count_emitted(3), "the fourth exceeds the limit of 3");
        assert_eq!(root.emitted_count(), 4);
    }

    #[test]
    fn depth_ok_is_inclusive_of_the_limit() {
        let root = root();
        assert!(root.depth_ok(0));
        let child = root.nested(EventSeq(1));
        assert!(!child.depth_ok(0));
        assert!(child.depth_ok(1));
    }

    #[test]
    fn with_scope_and_with_source_keep_the_shared_guard() {
        let root = root();
        let scoped = root.with_scope(Scope::Topic { topic_id: 7 });
        let sourced = root.with_source(Source::core("mcp"));
        assert_eq!(scoped.scope, Scope::Topic { topic_id: 7 });
        assert_eq!(scoped.depth, root.depth);
        assert_eq!(sourced.source, Source::core("mcp"));
        assert!(Arc::ptr_eq(&scoped.guard, &root.guard));
        assert!(Arc::ptr_eq(&sourced.guard, &root.guard));
    }

    #[test]
    fn the_cancel_token_is_shared_across_nesting() {
        let root = root();
        let child = root.nested(EventSeq(1));
        assert!(!child.cancel().is_cancelled());
        root.cancel().cancel();
        assert!(child.cancel().is_cancelled());
    }
}
