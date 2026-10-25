use super::dispatch::Limits;
use std::collections::HashMap;
use tokio::time::Instant;

pub struct RateLimiter {
    limits: Limits,
    buckets: HashMap<String, (Instant, u32)>,
}

impl RateLimiter {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            buckets: HashMap::new(),
        }
    }

    pub fn depth_ok(&self, depth: u32) -> bool {
        depth <= self.limits.max_depth
    }

    pub fn allow_emit(&mut self, mod_name: &str) -> bool {
        let now = Instant::now();
        let entry = self.buckets.entry(mod_name.to_string()).or_insert((now, 0));
        if now.duration_since(entry.0) >= self.limits.window {
            *entry = (now, 0);
        }
        if entry.1 >= self.limits.per_window {
            return false;
        }
        entry.1 += 1;
        true
    }

    pub fn evict_stale(&mut self) -> usize {
        let window = self.limits.window;
        let before = self.buckets.len();
        self.buckets
            .retain(|_, (start, _)| start.elapsed() < window);
        before - self.buckets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn limiter() -> RateLimiter {
        RateLimiter::new(Limits::default())
    }

    #[test]
    fn depth_beyond_the_limit_is_rejected() {
        let limiter = limiter();
        assert!(limiter.depth_ok(8));
        assert!(!limiter.depth_ok(9));
    }

    #[test]
    fn emit_rate_is_bucketed_per_mod() {
        let mut limiter = RateLimiter::new(Limits {
            per_window: 3,
            ..Limits::default()
        });
        for _ in 0..3 {
            assert!(limiter.allow_emit("research-kit"));
        }
        assert!(!limiter.allow_emit("research-kit"));
        assert!(
            limiter.allow_emit("other-mod"),
            "buckets are per mod, not global"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_window_restores_the_budget() {
        let mut limiter = RateLimiter::new(Limits {
            window: Duration::from_millis(10),
            per_window: 1,
            ..Limits::default()
        });
        assert!(limiter.allow_emit("m"));
        assert!(!limiter.allow_emit("m"));
        tokio::time::advance(Duration::from_millis(15)).await;
        assert!(limiter.allow_emit("m"));
    }

    #[tokio::test(start_paused = true)]
    async fn evict_stale_drops_expired_buckets() {
        let mut limiter = RateLimiter::new(Limits {
            window: Duration::from_millis(10),
            ..Limits::default()
        });
        limiter.allow_emit("a");
        limiter.allow_emit("b");
        assert_eq!(
            limiter.evict_stale(),
            0,
            "the window has not elapsed, so nothing is evicted"
        );
        tokio::time::advance(Duration::from_millis(15)).await;
        assert_eq!(limiter.evict_stale(), 2);
    }
}
