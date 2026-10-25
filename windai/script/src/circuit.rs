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

    pub fn reset(&self, name: &str) {
        self.record_success(name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mod_is_refused_after_the_configured_number_of_consecutive_timeouts() {
        let breaker = Breaker::new(3);
        assert!(!breaker.refuse("demo"));
        breaker.record_timeout("demo");
        breaker.record_timeout("demo");
        assert!(!breaker.refuse("demo"), "has not reached the threshold");
        breaker.record_timeout("demo");
        assert!(
            breaker.refuse("demo"),
            "reaches the threshold and is refused"
        );
    }

    #[test]
    fn one_success_clears_the_streak() {
        let breaker = Breaker::new(3);
        breaker.record_timeout("demo");
        breaker.record_timeout("demo");
        breaker.record_success("demo");
        breaker.record_timeout("demo");
        breaker.record_timeout("demo");
        assert!(!breaker.refuse("demo"), "counts restart after a success");
    }

    #[test]
    fn each_mod_is_counted_on_its_own() {
        let breaker = Breaker::new(2);
        breaker.record_timeout("noisy");
        breaker.record_timeout("noisy");
        assert!(breaker.refuse("noisy"));
        assert!(!breaker.refuse("quiet"), "other mods are unaffected");
    }

    #[test]
    fn resetting_lets_a_mod_run_again() {
        let breaker = Breaker::new(1);
        breaker.record_timeout("demo");
        assert!(breaker.refuse("demo"));
        breaker.reset("demo");
        assert!(!breaker.refuse("demo"), "can retry after a reset");
    }
}
