use std::sync::{
    OnceLock,
    atomic::{AtomicUsize, Ordering},
};
use tokio::runtime::Runtime;

pub mod adaptor;
pub mod api;
pub mod domain;
pub(crate) mod env;
pub mod proxy;
pub(crate) mod storage;

static ASYNC_RUNTIME: OnceLock<Runtime> = OnceLock::new();
/// global async runtime, currently use tokio as default
pub fn rt() -> &'static Runtime {
    ASYNC_RUNTIME.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("[async-worker-{}]", id)
            })
            .enable_all()
            .build()
            .unwrap();
        rt
    })
}
