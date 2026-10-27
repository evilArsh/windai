use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// mod 订阅指定事件的信息
#[derive(Debug, Clone)]
pub struct Subscription {
    pub mod_name: String,
    pub mod_version: u32,
    pub event: &'static str,
    /// js 事件处理函数
    pub handler: String,
    /// 单次调用的时间预算，与剩余总预算冲突时总预算优先
    pub timeout_ms: u64,
    /// 同阶段内按此值排序
    pub rank: u64,
}

impl Subscription {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}

/// mod 清单
#[derive(Debug, Clone)]
pub struct EnabledSet {
    names: HashSet<String>,
}

impl EnabledSet {
    pub fn none() -> Self {
        Self {
            names: HashSet::new(),
        }
    }

    pub fn from_names(names: impl IntoIterator<Item = String>) -> Self {
        Self {
            names: names.into_iter().collect(),
        }
    }

    pub fn contains(&self, name: &str) -> bool {
        self.names.contains(name)
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

impl Default for EnabledSet {
    fn default() -> Self {
        Self::none()
    }
}

/// 事件名到订阅者的倒排索引
///
/// ```
/// event_x → [订阅者 A, 订阅者 C]
/// ```
#[derive(Debug, Default)]
pub struct SubscriptionIndex {
    by_event: HashMap<&'static str, Vec<Subscription>>,
}

impl SubscriptionIndex {
    pub fn new() -> Self {
        Self {
            by_event: HashMap::new(),
        }
    }

    pub fn rebuild(&mut self, subs: Vec<Subscription>) {
        self.by_event.clear();
        for sub in subs {
            self.by_event.entry(sub.event).or_default().push(sub);
        }
        for list in self.by_event.values_mut() {
            // 从小到大排序
            list.sort_by_key(|s| s.rank);
        }
    }

    pub fn ordered(&self, event: &str) -> Vec<&Subscription> {
        self.by_event
            .get(event)
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }

    pub fn ordered_in(&self, event: &str, enabled: &EnabledSet) -> Vec<&Subscription> {
        self.ordered(event)
            .into_iter()
            .filter(|s| enabled.contains(&s.mod_name))
            .collect()
    }

    pub fn has_any(&self, event: &str) -> bool {
        self.by_event.get(event).is_some_and(|v| !v.is_empty())
    }

    pub fn is_empty(&self) -> bool {
        self.by_event.values().all(|v| v.is_empty())
    }
}
