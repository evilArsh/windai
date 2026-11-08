use crate::error::Result;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use tokio::sync::Mutex;
use wind_event::ModRoster;
use wind_event::envelope::Scope;
use wind_event::subscription::EnabledSet;
use wind_storage::Storage;

/// 每个话题作用域下启用的 mod 名字集合
#[derive(Debug)]
pub struct Roster {
    by_topic: RwLock<HashMap<i64, HashSet<String>>>,
    lock: Mutex<()>,
}

impl Roster {
    pub async fn load(storage: &Storage) -> Result<Self> {
        let view = Self {
            by_topic: RwLock::new(HashMap::new()),
            lock: Mutex::new(()),
        };
        view.refresh(storage).await?;
        Ok(view)
    }

    pub async fn refresh(&self, storage: &Storage) -> Result<()> {
        let _gate = self.lock.lock().await;
        let rows = storage.all_enabled_mods().await?;
        self.publish(rows);
        Ok(())
    }

    /// 用已读出的启用关系替换内存状态
    pub fn publish(&self, rows: Vec<(i64, String)>) {
        let mut grouped: HashMap<i64, HashSet<String>> = HashMap::new();
        for (topic_id, name) in rows {
            grouped.entry(topic_id).or_default().insert(name);
        }
        let mut guard = self
            .by_topic
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = grouped;
    }
}

impl ModRoster for Roster {
    fn enabled(&self, scope: &Scope) -> EnabledSet {
        let topic_id = match scope {
            Scope::Global => return EnabledSet::none(),
            Scope::Topic { topic_id, .. } => *topic_id,
        };
        let guard = self
            .by_topic
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.get(&topic_id) {
            Some(names) => EnabledSet::from_names(names.iter().cloned()),
            None => EnabledSet::none(),
        }
    }
}
