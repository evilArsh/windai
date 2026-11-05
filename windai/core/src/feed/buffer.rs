use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;
use wind_event::{GroupKey, Retention};
use wind_wire::event::{EventKey, FeedEvent};

pub(crate) struct BufferedEvent {
    pub(crate) event: Arc<FeedEvent>,
    pub(crate) retention: Retention,
    pub(crate) bytes: usize,
    pub(crate) buffered_at: Instant,
}

#[derive(Default)]
pub(crate) struct Buffer {
    entries: BTreeMap<EventKey, BufferedEvent>,
    groups: HashMap<GroupKey, BTreeSet<EventKey>>,
}

impl Buffer {
    /// 只接收 Journal 与 Group 策略的事件
    pub(crate) fn insert(&mut self, event: Arc<FeedEvent>, retention: Retention, bytes: usize) {
        if let Retention::Group(group) = &retention {
            self.groups
                .entry(group.clone())
                .or_default()
                .insert(event.key);
        }
        self.entries.insert(
            event.key,
            BufferedEvent {
                event,
                retention,
                bytes,
                buffered_at: Instant::now(),
            },
        );
    }

    pub(crate) fn snapshot(&self) -> Vec<Arc<FeedEvent>> {
        self.entries
            .values()
            .map(|entry| Arc::clone(&entry.event))
            .collect()
    }

    pub(crate) fn release_group(&mut self, group: &GroupKey) {
        let Some(keys) = self.groups.remove(group) else {
            return;
        };
        for key in keys {
            self.entries.remove(&key);
        }
    }

    /// 释放指定身份；返回被释放的事件
    pub(crate) fn release_keys(&mut self, keys: &[EventKey]) -> Vec<Arc<FeedEvent>> {
        let mut released = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(entry) = self.entries.remove(key) else {
                continue;
            };
            if let Retention::Group(group) = &entry.retention {
                let mut empty = None;
                if let Some(set) = self.groups.get_mut(group) {
                    set.remove(key);
                    if set.is_empty() {
                        empty = Some(group.clone());
                    }
                }
                if let Some(group) = empty {
                    self.groups.remove(&group);
                }
            }
            released.push(entry.event);
        }
        released
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn groups_len(&self) -> usize {
        self.groups.len()
    }

    pub(crate) fn bytes(&self) -> usize {
        self.entries.values().map(|entry| entry.bytes).sum()
    }

    pub(crate) fn pending_write(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| matches!(entry.retention, Retention::Journal))
            .count()
    }

    pub(crate) fn oldest_pending_ms(&self) -> u64 {
        self.entries
            .values()
            .filter(|entry| matches!(entry.retention, Retention::Journal))
            .map(|entry| entry.buffered_at)
            .min()
            .map(|started| started.elapsed().as_millis() as u64)
            .unwrap_or(0)
    }
}
