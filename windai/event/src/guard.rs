use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// 起因链深度上限
    pub max_depth: u32,
    /// 单次触发产生的事件数上限
    pub max_events_per_trigger: u32,
    /// 逐 mod 速率限制的时间窗
    pub window: Duration,
    /// 每个时间窗内允许产生的事件数
    pub per_window: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_depth: 8,
            max_events_per_trigger: 1000,
            window: Duration::from_secs(3),
            per_window: 100,
        }
    }
}

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
