use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// 键为启动互斥键，值串行该键的启动
pub struct StartupLocks(pub(crate) Mutex<HashMap<String, Arc<AsyncMutex<()>>>>);

impl StartupLocks {
    pub fn new() -> Self {
        Self(Mutex::new(HashMap::new()))
    }

    pub async fn lock(&self, key: &str) -> OwnedMutexGuard<()> {
        let mutex = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(key.to_owned())
            .or_default()
            .clone();
        mutex.lock_owned().await
    }
}

impl Default for StartupLocks {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::join;

    use super::StartupLocks;

    async fn spawn_with(
        locks: &StartupLocks,
        key: &str,
        order: Arc<Mutex<Vec<&'static str>>>,
        label: &'static str,
        ms: u64,
    ) {
        let _guard = locks.lock(key).await;
        tokio::time::sleep(Duration::from_millis(ms)).await;
        order.lock().unwrap().push(label);
    }

    #[tokio::test]
    async fn same_key_serializes_and_different_keys_do_not() {
        let locks = StartupLocks::new();
        let order = Arc::new(Mutex::new(Vec::new()));
        let a = spawn_with(&locks, "pkg", order.clone(), "a", 50);
        let b = spawn_with(&locks, "pkg", order.clone(), "b", 0);
        join!(a, b);
        assert_eq!(*order.lock().unwrap(), vec!["a", "b"]);
    }

    #[tokio::test]
    async fn different_keys_do_not_block() {
        let locks = StartupLocks::new();
        let guard = locks.lock("pkg").await;
        let acquired = tokio::time::timeout(Duration::from_millis(100), locks.lock("other")).await;
        assert!(acquired.is_ok(), "second key should not wait");
        drop(guard);
    }
}
