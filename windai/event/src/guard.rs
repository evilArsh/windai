use super::context::DispatchCx;
use std::collections::HashMap;
use std::sync::Mutex;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverLimit {
    TooDeep { max: u32, depth: u32 },
    TooManyEvents { max: u32 },
    TooFast { per_window: u32, window: Duration },
}

impl std::fmt::Display for OverLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OverLimit::TooDeep { max, depth } => {
                write!(f, "event chain reached depth {depth}, limit {max}")
            }
            OverLimit::TooManyEvents { max } => {
                write!(f, "single trigger exceeded {max} events")
            }
            OverLimit::TooFast { per_window, window } => {
                write!(f, "mod exceeded {per_window} events in {window:?}")
            }
        }
    }
}

struct Window {
    start: Instant,
    used: u32,
}

pub struct Guards {
    limits: Limits,
    buckets: Mutex<HashMap<String, Window>>, // mod 名 -> 窗口
}

impl Guards {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    pub fn admit(&self, cx: &DispatchCx) -> Result<(), OverLimit> {
        if cx.depth > self.limits.max_depth {
            return Err(OverLimit::TooDeep {
                max: self.limits.max_depth,
                depth: cx.depth,
            });
        }
        if cx.guard.bump_emitted() > self.limits.max_events_per_trigger {
            return Err(OverLimit::TooManyEvents {
                max: self.limits.max_events_per_trigger,
            });
        }
        Ok(())
    }

    pub fn admit_mod_emit(&self, mod_name: &str) -> Result<(), OverLimit> {
        let now = Instant::now();
        let window = self.limits.window;
        let per_window = self.limits.per_window;
        let mut buckets = self
            .buckets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(entry) = buckets.get_mut(mod_name) else {
            if per_window == 0 {
                return Err(OverLimit::TooFast { per_window, window });
            }
            buckets.insert(
                mod_name.to_string(),
                Window {
                    start: now,
                    used: 1,
                },
            );
            return Ok(());
        };
        if now.duration_since(entry.start) >= window {
            *entry = Window {
                start: now,
                used: 0,
            };
        }
        if entry.used >= per_window {
            return Err(OverLimit::TooFast { per_window, window });
        }
        entry.used += 1;
        Ok(())
    }
}
