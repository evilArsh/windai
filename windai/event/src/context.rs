use super::envelope::{Origin, Scope, Source};
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

    pub fn bump_emitted(&self) -> u32 {
        self.emitted.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn emitted(&self) -> u32 {
        self.emitted.load(Ordering::Relaxed)
    }
}

/// 一次派发上下文
#[derive(Debug, Clone)]
pub struct DispatchCx {
    pub origin: Origin,
    pub scope: Scope,
    pub source: Source,
    pub guard: Arc<DispatchGuard>,
    /// 嵌套层数，顶层为 0
    pub depth: u32,
}

impl DispatchCx {
    pub fn root(origin: Origin, scope: Scope, source: Source, total_budget: Duration) -> Self {
        Self {
            origin,
            scope,
            source,
            guard: Arc::new(DispatchGuard::new(total_budget)),
            depth: 0,
        }
    }

    pub fn nested(&self) -> Self {
        Self {
            depth: self.depth + 1,
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

    pub fn cancel(&self) -> &CancellationToken {
        &self.guard.cancel
    }
}
