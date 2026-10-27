use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// 嵌套派发的层数限制
    ///
    /// mod 在处理函数里再产生事件：
    /// ```text
    /// 宿主发 core/llm.request
    ///   → mod A 的拦截函数顺手发一条 core/rule.matched
    ///      → mod B 订阅了它，又发一条 core/llm.request
    ///          → 回到第一步
    ///  一个 mod 只要“订阅自己发出的事件”，就是无限循环；
    /// ```
    pub max_depth: u32,
    /// 一次触发总共能产生多少条事件
    pub max_events_per_trigger: u32,
    /// 速率限制的时间窗
    pub window: Duration,
    /// 单个 mod 在 `window` 事件内最多产生 `per_window` 条事件
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

pub struct Guards {
    limits: Limits,
    buckets: HashMap<String, (Instant, u32)>, // mod 名 -> (窗口起点, 已用次数)
}

impl Guards {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            buckets: HashMap::new(),
        }
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    pub fn allow_emit(&mut self, mod_name: &str) -> bool {
        let now = Instant::now();
        let window = self.limits.window;
        let per_window = self.limits.per_window;

        if let Some(entry) = self.buckets.get_mut(mod_name) {
            if now.duration_since(entry.0) >= window {
                *entry = (now, 0);
            }
            if entry.1 >= per_window {
                return false;
            }
            entry.1 += 1;
            return true;
        }
        self.buckets.insert(mod_name.to_string(), (now, 1));
        per_window >= 1
    }

    pub fn evict_stale(&mut self) -> usize {
        let window = self.limits.window;
        let before = self.buckets.len();
        self.buckets
            .retain(|_, (start, _)| start.elapsed() < window);
        before - self.buckets.len()
    }
}
