use std::collections::HashMap;
use std::sync::Mutex;

/// 重试熔断器
pub struct Breaker {
    threshold: u32,
    streaks: Mutex<HashMap<String, u32>>,
}

impl Breaker {
    pub fn new(threshold: u32) -> Self {
        Self {
            threshold: threshold.max(1),
            streaks: Mutex::new(HashMap::new()),
        }
    }

    pub fn refuse(&self, name: &str) -> bool {
        self.streaks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(name)
            .is_some_and(|count| *count >= self.threshold)
    }

    pub fn record_timeout(&self, name: &str) {
        let mut streaks = self
            .streaks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let count = streaks.entry(name.to_string()).or_insert(0);
        *count += 1;
        if *count == self.threshold {
            log::error!(
                "[script] {name} reached {} consecutive timeouts and is suspended",
                self.threshold
            );
        }
    }

    pub fn record_success(&self, name: &str) {
        self.streaks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(name);
    }
}
