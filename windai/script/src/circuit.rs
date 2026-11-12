use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub(crate) const SUSPEND_COOLDOWN: Duration = Duration::from_secs(30);

#[derive(Default)]
struct Streak {
    count: u32,
    until: Option<Instant>,
    probing: bool,
}

/// 重试熔断器
pub struct Breaker {
    threshold: u32,
    cooldown: Duration,
    streaks: Mutex<HashMap<String, Streak>>,
}

impl Breaker {
    pub fn new(threshold: u32, cooldown: Duration) -> Self {
        Self {
            threshold: threshold.max(1),
            cooldown,
            streaks: Mutex::new(HashMap::new()),
        }
    }

    pub fn enter<'a>(&'a self, name: &'a str) -> Option<Permit<'a>> {
        let mut streaks = self.streaks.lock().unwrap_or_else(|e| e.into_inner());
        let mut probe = false;
        if let Some(streak) = streaks.get_mut(name) {
            if streak.probing {
                return None;
            }
            if let Some(until) = streak.until {
                if Instant::now() < until {
                    return None;
                }
                streak.count = self.threshold - 1;
                streak.probing = true;
                probe = true;
            }
        }
        Some(Permit {
            breaker: self,
            name,
            probe,
        })
    }

    pub fn record_timeout(&self, name: &str) {
        let mut streaks = self.streaks.lock().unwrap_or_else(|e| e.into_inner());
        let streak = streaks.entry(name.to_string()).or_default();
        streak.probing = false;
        streak.count = streak.count.saturating_add(1);
        if streak.count >= self.threshold {
            streak.until = Some(Instant::now() + self.cooldown);
            log::error!(
                "[script] {name} reached {} consecutive timeouts and is suspended",
                self.threshold
            );
        }
    }

    pub fn record_success(&self, name: &str) {
        self.streaks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(name);
    }
}

pub(crate) struct Permit<'a> {
    breaker: &'a Breaker,
    name: &'a str,
    probe: bool,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        if !self.probe {
            return;
        }
        let mut streaks = self
            .breaker
            .streaks
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(streak) = streaks.get_mut(self.name)
            && streak.probing
        {
            streak.probing = false;
            streak.until = Some(Instant::now() + self.breaker.cooldown);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expire(breaker: &Breaker) {
        breaker
            .streaks
            .lock()
            .expect("streaks")
            .get_mut("demo")
            .expect("demo")
            .until = Some(Instant::now());
    }

    #[test]
    fn a_suspended_mod_recovers_after_the_cooldown() {
        let breaker = Breaker::new(3, Duration::from_secs(30));
        for _ in 0..3 {
            breaker.record_timeout("demo");
        }
        assert!(breaker.enter("demo").is_none());
        expire(&breaker);
        let _probe = breaker.enter("demo").expect("probe");
        assert!(breaker.enter("demo").is_none());
        breaker.record_timeout("demo");
        assert!(breaker.enter("demo").is_none());
    }

    #[test]
    fn an_outstanding_probe_remains_exclusive_after_another_cooldown() {
        let breaker = Breaker::new(1, Duration::ZERO);
        breaker.record_timeout("demo");
        let _probe = breaker.enter("demo").expect("probe");
        assert!(breaker.enter("demo").is_none(), "a probe is still running");
        breaker.record_success("demo");
        assert!(breaker.enter("demo").is_some());
    }

    #[test]
    fn a_successful_probe_resets_the_streak() {
        let breaker = Breaker::new(2, Duration::ZERO);
        breaker.record_timeout("demo");
        breaker.record_timeout("demo");
        let _probe = breaker.enter("demo").expect("probe");
        breaker.record_success("demo");
        breaker.record_timeout("demo");
        assert!(breaker.enter("demo").is_some());
        assert!(breaker.enter("other").is_some());
    }

    #[test]
    fn dropping_an_unfinished_probe_restarts_the_cooldown() {
        let breaker = Breaker::new(1, Duration::from_secs(30));
        breaker.record_timeout("demo");
        expire(&breaker);
        let probe = breaker.enter("demo").expect("probe");
        drop(probe);
        assert!(breaker.enter("demo").is_none());
        expire(&breaker);
        assert!(breaker.enter("demo").is_some());
    }
}
