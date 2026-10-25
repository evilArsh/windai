use std::sync::OnceLock;
use std::sync::mpsc;
use std::time::Duration;
use tokio::runtime::{Builder, Handle, Runtime};

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("the call exceeded its budget")]
    Timeout,
    #[error("the call was cancelled")]
    Cancelled,
    #[error("the I/O side panicked before it could answer")]
    IoPanicked,
    #[error("the call failed: {0}")]
    Failed(String),
}

/// 进程级的 I/O 池
static IO: OnceLock<Result<Runtime, String>> = OnceLock::new();

pub fn io_pool() -> Result<&'static Runtime, BridgeError> {
    IO.get_or_init(|| {
        Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("wind-script-io")
            .enable_all()
            .build()
            .map_err(|error| error.to_string())
    })
    .as_ref()
    .map_err(|error| BridgeError::Failed(format!("cannot build the I/O pool: {error}")))
}

pub fn block_on<F, T>(pool: &Runtime, limit: Duration, future: F) -> Result<T, BridgeError>
where
    F: std::future::Future<Output = Result<T, BridgeError>> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = mpsc::channel::<Result<T, BridgeError>>();
    pool.spawn(async move {
        let outcome = tokio::time::timeout(limit, future).await;
        let _ = tx.send(match outcome {
            Ok(result) => result,
            Err(_) => Err(BridgeError::Timeout),
        });
    });
    match rx.recv() {
        Ok(outcome) => outcome,
        Err(_) => {
            log::error!("[bridge] the I/O side panicked before it could answer");
            Err(BridgeError::IoPanicked)
        }
    }
}

pub fn block_on_current<T>(
    limit: Duration,
    future: impl std::future::Future<Output = Result<T, BridgeError>> + Send + 'static,
) -> Result<T, BridgeError>
where
    T: Send + 'static,
{
    block_on(io_pool()?, limit, future)
}

pub fn handle() -> Result<Handle, BridgeError> {
    Ok(io_pool()?.handle().clone())
}
