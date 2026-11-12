use super::{Inner, Runtime, RuntimeError};
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl Runtime {
    pub async fn shutdown(&self) -> Result<(), RuntimeError> {
        let mut task = self.inner.shutdown_task.lock().await;
        if task.is_none() {
            if self.inner.stopped.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            let inner = self.inner.clone();
            *task = Some(tokio::spawn(async move { shutdown(inner).await }));
        }
        let result = match task.as_mut() {
            Some(task) => task
                .await
                .map_err(|error| RuntimeError::Internal(error.to_string()))?,
            None => Ok(()),
        };
        task.take();
        result
    }
}

async fn shutdown(inner: Arc<Inner>) -> Result<(), RuntimeError> {
    let _operations = inner.operations.write().await;
    let resources = inner.services.shutdown().await;
    inner.agents.shutdown().await;
    let feed = inner.feed.shutdown().await;
    let joined = if let Some(task) = inner.feed_task.lock().await.take() {
        task.await
            .map_err(|error| RuntimeError::Internal(error.to_string()))?
            .map(|_| ())
    } else {
        Ok(())
    };
    resources?;
    feed?;
    joined?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeOptions;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::time::timeout;
    use wind_wire::AppDirs;

    #[tokio::test]
    async fn cancelling_shutdown_caller_does_not_cancel_cleanup() {
        let tmp = TempDir::new().unwrap();
        let runtime = Runtime::open(RuntimeOptions::new(AppDirs::under(tmp.path())))
            .await
            .unwrap();
        let mut subscription = runtime.subscribe().await.unwrap().subscription;
        let active = runtime.enter().await.unwrap();
        let rt = runtime.clone();
        let caller = tokio::spawn(async move { rt.shutdown().await });
        timeout(Duration::from_secs(5), async {
            while !runtime.inner.stopped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!caller.is_finished());
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        drop(active);
        timeout(Duration::from_secs(5), runtime.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(subscription.recv().await.unwrap().is_none());
    }
}
