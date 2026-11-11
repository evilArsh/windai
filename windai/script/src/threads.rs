use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Instant;

#[derive(Debug, thiserror::Error)]
pub enum ThreadPoolError {
    #[error("the interpreter pool is busy")]
    Busy,
    #[error("the interpreter did not finish in time")]
    Timeout,
    #[error("the task panicked before it could hand back a value")]
    HandlerPanicked,
}

/// 一次要在解释器线程上运行的任务
pub type Task = Box<dyn FnOnce() + Send + 'static>;

/// 一个名额凭据，销毁时归还
struct Slot {
    busy: Arc<AtomicUsize>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.busy.fetch_sub(1, Ordering::AcqRel);
    }
}

/// 解释器线程的固定池
pub struct ThreadPool {
    idle: Arc<Mutex<mpsc::Receiver<Task>>>,
    submit: mpsc::Sender<Task>,
    busy: Arc<AtomicUsize>,
    limit: usize,
}

impl Clone for ThreadPool {
    fn clone(&self) -> Self {
        Self {
            idle: Arc::clone(&self.idle),
            submit: self.submit.clone(),
            busy: Arc::clone(&self.busy),
            limit: self.limit,
        }
    }
}

impl ThreadPool {
    pub fn new(limit: usize) -> Self {
        let limit = limit.max(1);
        let (submit, rx) = mpsc::channel::<Task>();
        let idle = Arc::new(Mutex::new(rx));
        let busy = Arc::new(AtomicUsize::new(0));
        for index in 0..limit {
            let take = Arc::clone(&idle);
            thread::Builder::new()
                .name(format!("wind-script-{index}"))
                .spawn(move || {
                    loop {
                        let task = match take.lock() {
                            Ok(guard) => guard.recv(),
                            Err(_) => return,
                        };
                        let task = match task {
                            Ok(task) => task,
                            Err(_) => return,
                        };
                        if panic::catch_unwind(AssertUnwindSafe(task)).is_err() {
                            log::error!("[script] an interpreter task panicked");
                        }
                    }
                })
                .expect("spawn the interpreter threads");
        }
        Self {
            idle,
            submit,
            busy,
            limit,
        }
    }

    fn acquire(&self) -> Result<(), ThreadPoolError> {
        let mut current = self.busy.load(Ordering::Acquire);
        loop {
            if current >= self.limit {
                return Err(ThreadPoolError::Busy);
            }
            match self.busy.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    pub async fn run_within<T, F>(&self, limit: Duration, task: F) -> Result<T, ThreadPoolError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.acquire()?;
        let slot = Slot {
            busy: Arc::clone(&self.busy),
        };
        let (done, wait) = oneshot::channel::<T>();
        let submitted = self.submit.send(Box::new(move || {
            let value = task();
            drop(slot);
            let _ = done.send(value);
        }));
        if submitted.is_err() {
            return Err(ThreadPoolError::Busy);
        }
        match tokio::time::timeout_at(Instant::now() + limit, wait).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_)) => Err(ThreadPoolError::HandlerPanicked),
            Err(_) => Err(ThreadPoolError::Timeout),
        }
    }
}
