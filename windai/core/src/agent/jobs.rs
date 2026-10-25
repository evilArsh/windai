use super::behavior::{Behavior, Ctx, Input, Outcome};
use super::tree::event::Event;
use super::tree::{Domain, JobId, NodeId};
use futures::FutureExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub const REPORT_CAPACITY: usize = 256;

pub type JobSink<D> = mpsc::Sender<Event<D>>;

pub struct JobRequest<D: Domain> {
    pub node: NodeId,
    pub job: JobId,
    pub input: Input<D>,
}

enum Ended<D: Domain> {
    Verdict(Outcome<D>),
    Stopped,
    Aborted,
}

pub struct Jobs<D: Domain> {
    behavior: Arc<dyn Behavior<D>>,
    sink: JobSink<D>,
    grace: Duration,
    running: Arc<Mutex<HashMap<(NodeId, JobId), CancellationToken>>>,
}

impl<D: Domain> Jobs<D> {
    pub fn new(behavior: Arc<dyn Behavior<D>>, sink: JobSink<D>, grace: Duration) -> Self {
        Self {
            behavior,
            sink,
            grace,
            running: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn launch(&self, request: JobRequest<D>) {
        let JobRequest { node, job, input } = request;
        let cancel = CancellationToken::new();
        if let Ok(mut running) = self.running.lock() {
            running.insert((node, job), cancel.clone());
        }

        let behavior = self.behavior.clone();
        let sink = self.sink.clone();
        let grace = self.grace;
        let running = self.running.clone();
        let ctx_cancel = cancel.clone();

        tokio::spawn(async move {
            let ctx = Ctx::new(node, job, ctx_cancel);
            let mut runner = std::pin::pin!(
                std::panic::AssertUnwindSafe(behavior.run(ctx, input)).catch_unwind()
            );

            let ended = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    let _ = tokio::time::timeout(grace, runner.as_mut()).await;
                    Ended::Stopped
                }
                joined = runner.as_mut() => match joined {
                    Ok(outcome) => Ended::Verdict(outcome),
                    Err(_) => Ended::Aborted,
                },
            };

            let verdict = match ended {
                Ended::Verdict(Outcome::Completed(output)) => {
                    Event::Completed { node, job, output }
                }
                Ended::Verdict(Outcome::Failed(error)) => Event::Failed { node, job, error },
                Ended::Verdict(Outcome::Suspend(payload)) => {
                    Event::Suspended { node, job, payload }
                }
                Ended::Verdict(Outcome::Spawn { spawn }) => Event::Spawned { node, job, spawn },
                Ended::Aborted => Event::Aborted { node, job },
                Ended::Stopped => Event::Stopped { node, job },
            };
            let removed = running.lock().ok().map(|mut map| map.remove(&(node, job)));
            let report = match removed {
                Some(Some(_))
                    if cancel.is_cancelled() && !matches!(verdict, Event::Aborted { .. }) =>
                {
                    Event::Stopped { node, job }
                }
                _ => verdict,
            };
            let _ = sink.send(report).await;
        });
    }

    pub fn stop(&self, node: NodeId, job: JobId) {
        let Ok(running) = self.running.lock() else {
            return;
        };
        match running.get(&(node, job)) {
            Some(cancel) => cancel.cancel(),
            None => {
                drop(running);
                self.report(Event::Stopped { node, job });
            }
        }
    }

    fn report(&self, event: Event<D>) {
        if let Err(mpsc::error::TrySendError::Full(event)) = self.sink.try_send(event) {
            let sink = self.sink.clone();
            tokio::spawn(async move {
                let _ = sink.send(event).await;
            });
        }
    }

    pub fn shutdown(&self) {
        if let Ok(running) = self.running.lock() {
            for cancel in running.values() {
                cancel.cancel();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::behavior::{Behavior, Ctx, Input, Outcome};
    use crate::agent::tree::domain::test_support::{TestDomain, TestOutput, TestSpec};
    use crate::agent::tree::event::Event;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// 永不返回的 worker，用于验证宽限期回收
    struct UnresponsiveBehavior {
        /// 取消信号是否真的到过这里
        observed: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for UnresponsiveBehavior {
        async fn run(&self, ctx: Ctx, _input: Input<TestDomain>) -> Outcome<TestDomain> {
            // 等到取消信号再睡过宽限期，模拟不响应取消
            ctx.cancel.cancelled().await;
            self.observed.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(30)).await;
            Outcome::Completed(TestOutput::Ok)
        }
    }

    struct PanickingBehavior;

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for PanickingBehavior {
        async fn run(&self, _ctx: Ctx, _input: Input<TestDomain>) -> Outcome<TestDomain> {
            panic!("boom")
        }
    }

    struct ImmediateBehavior;

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for ImmediateBehavior {
        async fn run(&self, _ctx: Ctx, _input: Input<TestDomain>) -> Outcome<TestDomain> {
            Outcome::Completed(TestOutput::Ok)
        }
    }

    #[tokio::test]
    async fn a_normal_run_reports_completed() {
        let (tx, mut rx) = mpsc::channel(REPORT_CAPACITY);
        let jobs = Jobs::new(Arc::new(ImmediateBehavior), tx, Duration::from_millis(50));
        jobs.launch(JobRequest {
            node: 1,
            job: 1,
            input: Input::Fresh(TestSpec::Noop),
        });

        let report = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout")
            .expect("report");
        assert!(matches!(
            report,
            Event::Completed {
                node: 1,
                job: 1,
                ..
            }
        ));
    }

    struct CancelAwareBehavior;

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for CancelAwareBehavior {
        async fn run(&self, ctx: Ctx, _input: Input<TestDomain>) -> Outcome<TestDomain> {
            ctx.cancel.cancelled().await;
            Outcome::Completed(TestOutput::Ok)
        }
    }

    #[tokio::test]
    async fn a_job_that_ignores_cancellation_is_reclaimed_by_the_grace_period() {
        let (tx, mut rx) = mpsc::channel(REPORT_CAPACITY);
        let observed = Arc::new(AtomicBool::new(false));
        let jobs = Jobs::new(
            Arc::new(UnresponsiveBehavior {
                observed: observed.clone(),
            }),
            tx,
            Duration::from_millis(50),
        );
        // node 与 job 取不同的值，注册表键里的两个 id 可区分
        jobs.launch(JobRequest {
            node: 3,
            job: 4,
            input: Input::Fresh(TestSpec::Noop),
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        jobs.stop(3, 4);

        let report = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("reclaim an unresponsive worker")
            .expect("report");
        assert!(matches!(report, Event::Stopped { node: 3, job: 4 }));
        assert!(
            observed.load(Ordering::SeqCst),
            "the worker must actually have received the cancellation signal, otherwise a synthesised Stopped would mask a wrong registry key"
        );
    }

    #[tokio::test]
    async fn a_worker_that_returns_on_cancel_reports_stopped() {
        let (tx, mut rx) = mpsc::channel(REPORT_CAPACITY);
        let jobs = Jobs::new(Arc::new(CancelAwareBehavior), tx, Duration::from_secs(5));
        jobs.launch(JobRequest {
            node: 5,
            job: 6,
            input: Input::Fresh(TestSpec::Noop),
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        jobs.stop(5, 6);

        let report = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("report stopped during cancellation")
            .expect("report");
        assert!(matches!(report, Event::Stopped { node: 5, job: 6 }));
    }

    #[tokio::test]
    async fn a_stop_for_a_finished_job_synthesises_stopped() {
        let (tx, mut rx) = mpsc::channel(REPORT_CAPACITY);
        let jobs = Jobs::new(Arc::new(ImmediateBehavior), tx, Duration::from_millis(50));
        jobs.launch(JobRequest {
            node: 7,
            job: 8,
            input: Input::Fresh(TestSpec::Noop),
        });

        let first = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout")
            .expect("report");
        assert!(matches!(
            first,
            Event::Completed {
                node: 7,
                job: 8,
                ..
            }
        ));

        jobs.stop(7, 8);

        let second = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("confirm a finished job")
            .expect("report");
        assert!(matches!(second, Event::Stopped { node: 7, job: 8 }));
    }

    #[tokio::test]
    async fn a_panicking_job_reports_aborted() {
        let (tx, mut rx) = mpsc::channel(REPORT_CAPACITY);
        let jobs = Jobs::new(Arc::new(PanickingBehavior), tx, Duration::from_millis(50));
        jobs.launch(JobRequest {
            node: 2,
            job: 7,
            input: Input::Fresh(TestSpec::Noop),
        });

        let report = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("survive a worker panic")
            .expect("report");
        assert!(
            matches!(report, Event::Aborted { node: 2, job: 7 }),
            "a panic means it died without delivering a verdict, so it must not be recorded as stopped"
        );
    }

    #[tokio::test]
    async fn a_stop_for_an_unknown_job_is_confirmed() {
        let (tx, mut rx) = mpsc::channel(REPORT_CAPACITY);
        let jobs = Jobs::new(Arc::new(ImmediateBehavior), tx, Duration::from_millis(50));
        jobs.stop(99, 99);

        assert!(
            matches!(rx.try_recv(), Ok(Event::Stopped { node: 99, job: 99 })),
            "an unknown key must also get a confirmation; the reducer relies on it to drive cancelling nodes to a terminal phase"
        );
    }

    /// 回报通道满时判定只能被推迟，不得被丢掉
    #[tokio::test]
    async fn a_verdict_is_never_dropped_when_the_report_channel_is_full() {
        // 通道容量为 1。先用一条补发的确认占满通道，且不排空。
        let (tx, mut rx) = mpsc::channel(1);
        let jobs = Jobs::new(Arc::new(ImmediateBehavior), tx, Duration::from_millis(50));
        jobs.stop(99, 99);
        jobs.launch(JobRequest {
            node: 1,
            job: 1,
            input: Input::Fresh(TestSpec::Noop),
        });

        // 排空之前先让 worker 跑到发送判定的位置，通道满时 worker 只能等待空位
        tokio::time::sleep(Duration::from_millis(50)).await;

        let filler = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("receive the filler confirmation")
            .expect("report");
        assert!(matches!(filler, Event::Stopped { node: 99, job: 99 }));

        let verdict = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("keep the verdict when full")
            .expect("report");
        assert!(matches!(
            verdict,
            Event::Completed {
                node: 1,
                job: 1,
                ..
            }
        ));
    }
}
