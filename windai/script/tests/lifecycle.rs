//! 解释器线程的池、放弃与超时

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::runtime::RuntimeFlavor;
use wind_script::threads::{ThreadPool, ThreadPoolError};

/// 等一个条件成立，最多等 `limit`
///
/// 用真实时钟：暂停的 tokio 时钟会因为空转瞬间走完
async fn wait_until(mut done: impl FnMut() -> bool, limit: Duration) -> bool {
    let deadline = std::time::Instant::now() + limit;
    while std::time::Instant::now() < deadline {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    done()
}

#[tokio::test]
async fn a_task_runs_off_the_tokio_runtime() {
    let pool = ThreadPool::new(2);
    let marker = pool
        .run(move || match tokio::runtime::Handle::try_current() {
            // 解释器线程不得处在任何 tokio 运行时的上下文里
            Ok(_) => 1usize,
            Err(_) => 0usize,
        })
        .await
        .expect("run to completion");
    assert_eq!(
        marker, 0,
        "the interpreter must run on a thread without a tokio runtime"
    );
}

#[tokio::test]
async fn a_task_hands_its_value_back() {
    let pool = ThreadPool::new(1);
    let value = pool.run(|| 7usize).await.expect("run to completion");
    assert_eq!(value, 7, "the result of a call is computed by that thread");
}

#[tokio::test(start_paused = true)]
async fn a_task_that_never_returns_is_abandoned_at_its_deadline() {
    let pool = ThreadPool::new(2);
    let started = Arc::new(AtomicUsize::new(0));
    let flag = started.clone();
    let outcome = pool
        .run_within(Duration::from_millis(50), move || {
            flag.store(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_secs(3600));
            0usize
        })
        .await;
    assert!(
        matches!(outcome, Err(ThreadPoolError::Timeout)),
        "a timeout must be returned as an error instead of waiting for the thread, got {outcome:?}"
    );
    // 期限在虚拟时钟下几乎立刻到来，工作线程可能还没被调度上，因此按真实时间等它起跑
    assert!(
        wait_until(
            || started.load(Ordering::Relaxed) == 1,
            Duration::from_secs(5)
        )
        .await,
        "the thread has indeed started running"
    );
}

#[tokio::test]
async fn a_pool_at_its_limit_refuses_instead_of_queueing() {
    let pool = ThreadPool::new(1);
    let hold = Arc::new(AtomicUsize::new(0));
    let flag = hold.clone();
    // 先占住唯一的线程
    let first = {
        let pool = pool.clone();
        tokio::spawn(async move {
            pool.run(move || {
                while flag.load(Ordering::Relaxed) == 0 {
                    std::thread::sleep(Duration::from_millis(1));
                }
                0usize
            })
            .await
        })
    };
    assert!(
        wait_until(|| pool.busy() == 1, Duration::from_secs(5)).await,
        "the first thread must be occupied"
    );
    let refused = pool.run(|| 0usize).await;
    assert!(
        matches!(refused, Err(ThreadPoolError::Busy)),
        "a full pool must refuse, got {refused:?}"
    );
    hold.store(1, Ordering::Relaxed);
    first
        .await
        .expect("join the first task")
        .expect("run to completion");
}

#[tokio::test]
async fn an_idle_pool_accepts_after_a_task_finishes() {
    let pool = ThreadPool::new(1);
    pool.run(|| 0usize).await.expect("run the first task");
    assert!(
        wait_until(|| pool.busy() == 0, Duration::from_secs(5)).await,
        "the slot must be returned after the run finishes"
    );
    pool.run(|| 0usize).await.expect("run the second task");
}

#[tokio::test]
async fn a_finished_task_frees_its_slot_before_the_waiter_wakes() {
    let pool = ThreadPool::new(1);
    // 单次窗口只有几条指令，多跑几轮才能撞上偶尔慢一步的调度
    for round in 0..4096 {
        pool.run(|| 0usize).await.expect("run the task");
        assert_eq!(
            pool.busy(),
            0,
            "round {round}: the slot should be returned before the value is handed over"
        );
    }
}

#[tokio::test]
async fn abandoned_threads_still_count_against_the_limit() {
    let pool = ThreadPool::new(1);
    let _ = pool
        .run_within(Duration::from_millis(20), || {
            std::thread::sleep(Duration::from_secs(3600));
            0usize
        })
        .await;
    // 被放弃的线程仍在跑，名额不能提前归还
    assert_eq!(
        pool.busy(),
        1,
        "abandoning the wait does not mean the thread has ended"
    );
    assert!(matches!(
        pool.run(|| 0usize).await,
        Err(ThreadPoolError::Busy)
    ));
}

#[tokio::test]
async fn a_panicking_task_hands_its_slot_and_its_thread_back() {
    let pool = ThreadPool::new(1);
    let outcome = pool
        .run(|| -> usize {
            panic!("panic inside the interpreter task");
        })
        .await;
    assert!(
        outcome.is_err(),
        "a panic must be returned as a failure, got {outcome:?}"
    );
    assert!(
        wait_until(|| pool.busy() == 0, Duration::from_secs(5)).await,
        "the slot must be returned after a panic"
    );
    let value = pool.run(|| 7usize).await.expect("run after the panic");
    assert_eq!(value, 7, "the surviving thread must still be alive");
}

#[tokio::test]
async fn a_panicking_task_reports_a_panic_rather_than_a_full_pool() {
    let pool = ThreadPool::new(1);
    let outcome = pool
        .run(|| -> usize {
            panic!("panic inside the interpreter task");
        })
        .await;
    assert!(
        matches!(outcome, Err(ThreadPoolError::HandlerPanicked)),
        "a panic must be reported as a panic, not as a full pool, got {outcome:?}"
    );
}

use std::sync::mpsc as std_mpsc;
use wind_script::bridge::{BridgeError, block_on, io_pool};

#[tokio::test]
async fn a_bridge_call_returns_the_value_the_io_side_produced() {
    let pool = io_pool().expect("io pool");
    let out = block_on(pool, Duration::from_secs(5), async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok::<String, BridgeError>("body".to_string())
    })
    .expect("receive the value");
    assert_eq!(out, "body");
}

#[tokio::test(start_paused = true)]
async fn a_bridge_call_that_outlives_its_budget_times_out() {
    let pool = io_pool().expect("io pool");
    let outcome = block_on(pool, Duration::from_millis(30), async {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok::<String, BridgeError>("never".to_string())
    });
    assert!(matches!(outcome, Err(BridgeError::Timeout)), "{outcome:?}");
}

#[test]
fn a_panicking_io_task_reports_a_panic_rather_than_a_cancellation() {
    async fn boom() -> Result<String, BridgeError> {
        panic!("panic inside the I/O task")
    }
    let pool = io_pool().expect("io pool");
    let outcome = block_on(pool, Duration::from_secs(5), boom());
    assert!(
        matches!(outcome, Err(BridgeError::IoPanicked)),
        "an I/O-side panic must be reported as a panic, not as a cancellation, got {outcome:?}"
    );
}

#[test]
fn the_io_pool_is_a_tokio_pool_not_the_interpreter_pool() {
    let _ = std_mpsc::channel::<u8>();
    // io_pool 建的是 tokio 运行时，因此它可以在运行时之外被创建
    let pool = io_pool().expect("io pool");
    assert!(matches!(
        pool.handle().runtime_flavor(),
        RuntimeFlavor::MultiThread
    ));
}
