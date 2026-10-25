use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::AsRefStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Phase {
    Guard,
    Policy,
}

/// mod 订阅指定事件的信息
#[derive(Debug, Clone)]
pub struct Subscription {
    pub mod_name: String,
    pub mod_version: u32,
    pub event: &'static str,
    /// 只有 intercept 订阅有阶段
    pub phase: Option<Phase>,
    /// js 事件处理函数
    pub handler: String,
    /// 单次调用的时间预算，与剩余总预算冲突时总预算优先
    pub timeout_ms: u64,
    /// 同阶段内按此值排序
    pub registration_seq: u64,
}

impl Subscription {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}

/// 某个作用域下启用的 mod 集合
#[derive(Debug, Clone)]
pub struct EnabledSet {
    all: bool,
    names: HashSet<String>,
}

impl EnabledSet {
    pub fn all() -> Self {
        Self {
            all: true,
            names: HashSet::new(),
        }
    }

    pub fn none() -> Self {
        Self {
            all: false,
            names: HashSet::new(),
        }
    }

    pub fn from_names(names: impl IntoIterator<Item = String>) -> Self {
        Self {
            all: false,
            names: names.into_iter().collect(),
        }
    }

    pub fn contains(&self, name: &str) -> bool {
        self.all || self.names.contains(name)
    }

    pub fn is_empty(&self) -> bool {
        !self.all && self.names.is_empty()
    }
}

impl Default for EnabledSet {
    fn default() -> Self {
        Self::none()
    }
}

/// 事件名到订阅者的倒排索引
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
            list.sort_by_key(|s| (s.phase, s.registration_seq));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sub(event: &'static str, phase: Option<Phase>, seq: u64, name: &str) -> Subscription {
        Subscription {
            mod_name: name.into(),
            mod_version: 1,
            event,
            phase,
            handler: "onEvent".into(),
            timeout_ms: 50,
            registration_seq: seq,
        }
    }

    fn names(subs: Vec<&Subscription>) -> Vec<String> {
        subs.iter().map(|s| s.mod_name.clone()).collect()
    }

    #[test]
    fn intercept_subscribers_run_by_phase_then_registration() {
        let mut index = SubscriptionIndex::new();
        index.rebuild(vec![
            sub("core/mcp.tool.call", Some(Phase::Policy), 3, "policy-first"),
            sub("core/mcp.tool.call", Some(Phase::Guard), 7, "guard-second"),
            sub("core/mcp.tool.call", Some(Phase::Guard), 9, "guard-later"),
        ]);
        assert_eq!(
            names(index.ordered("core/mcp.tool.call")),
            vec!["guard-second", "guard-later", "policy-first"]
        );
    }

    #[test]
    fn observe_subscribers_keep_registration_order() {
        let mut index = SubscriptionIndex::new();
        index.rebuild(vec![
            sub("core/block.committed", None, 5, "second"),
            sub("core/block.committed", None, 2, "first"),
        ]);
        assert_eq!(
            names(index.ordered("core/block.committed")),
            vec!["first", "second"]
        );
    }

    #[test]
    fn an_event_without_subscribers_reports_empty() {
        let index = SubscriptionIndex::new();
        assert!(index.ordered("core/nobody.listens").is_empty());
        assert!(!index.has_any("core/nobody.listens"));
        assert!(index.is_empty());
    }

    #[test]
    fn rebuild_drops_the_previous_generation() {
        let mut index = SubscriptionIndex::new();
        index.rebuild(vec![sub("core/a.b", Some(Phase::Guard), 1, "old")]);
        index.rebuild(vec![sub("core/a.b", Some(Phase::Guard), 1, "new")]);
        assert_eq!(names(index.ordered("core/a.b")), vec!["new"]);
    }

    #[test]
    fn scope_filtering_keeps_only_enabled_mods() {
        let mut index = SubscriptionIndex::new();
        index.rebuild(vec![
            sub("core/a.b", Some(Phase::Guard), 1, "enabled"),
            sub("core/a.b", Some(Phase::Guard), 2, "disabled"),
        ]);
        let only = EnabledSet::from_names(["enabled".to_string()]);
        assert_eq!(names(index.ordered_in("core/a.b", &only)), vec!["enabled"]);
        assert_eq!(
            names(index.ordered_in("core/a.b", &EnabledSet::all())).len(),
            2
        );
        assert!(index.ordered_in("core/a.b", &EnabledSet::none()).is_empty());
    }

    #[test]
    fn enabled_set_distinguishes_all_from_a_named_subset() {
        assert!(EnabledSet::all().contains("anything"));
        assert!(!EnabledSet::none().contains("anything"));
        assert!(EnabledSet::from_names(["a".to_string()]).contains("a"));
        assert!(!EnabledSet::from_names(["a".to_string()]).contains("b"));
        assert!(EnabledSet::none().is_empty());
        assert!(!EnabledSet::all().is_empty());
    }

    #[test]
    fn timeout_comes_from_the_milliseconds() {
        let sub = sub("core/a.b", Some(Phase::Guard), 1, "m");
        assert_eq!(sub.timeout().as_millis(), 50);
    }
}
