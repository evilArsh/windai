use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Default)]
pub struct StatementCount {
    count: Arc<AtomicU64>,
}

impl StatementCount {
    pub fn record(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_clone_records_into_the_same_counter() {
        let counter = StatementCount::default();
        let shared = counter.clone();
        counter.record();
        shared.record();
        assert_eq!(counter.get(), 2);
        assert_eq!(shared.get(), 2);
    }
}
