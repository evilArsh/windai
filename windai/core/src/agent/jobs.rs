use super::behavior::{Behavior, Ctx, Input, Outcome};
use super::tree::event::Event;
use super::tree::{Domain, JobId, NodeId};
use futures::FutureExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// worker 回报通道的容量
pub const REPORT_CAPACITY: usize = 256;

/// job 向 hub 回报的通道
pub type JobSink<D> = mpsc::Sender<Event<D>>;

/// 启动一个 worker 所需的全部信息
pub struct JobRequest<D: Domain> {
    pub node: NodeId,
    pub job: JobId,
    pub input: Input<D>,
}

/// worker 任务结束的方式
enum Ended<D: Domain> {
    /// 正常交还判定
    Verdict(Outcome<D>),
    /// 被取消，或在宽限期内被回收
    Stopped,
    /// `run` 自己 panic
    Aborted,
}

/// worker 的生命周期管理
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

    /// 启动一个 worker
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
        let emit_sink = sink.clone();

        tokio::spawn(async move {
            let emit = move |chunk| {
                let _ = emit_sink.try_send(Event::Progress { node, job, chunk });
            };
            let ctx = Ctx::new(node, job, ctx_cancel, &emit);
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
                // 取消期间交还的判定一律让位给停止确认；panic 不是判定，不在此列
                Some(Some(_))
                    if cancel.is_cancelled() && !matches!(verdict, Event::Aborted { .. }) =>
                {
                    Event::Stopped { node, job }
                }
                _ => verdict,
            };
            // 判定是事实，通道满时阻塞等待，不能丢
            let _ = sink.send(report).await;
        });
    }

    /// 请求停止一个 worker
    ///
    /// 幂等：重复调用同一个 `(node, job)` 只补一条确认，不会产生第二次副作用。
    /// 任务已经结束并报告过的，直接补一条 `Stopped` —— 归约器靠它把取消中的节点落终态
    pub fn stop(&self, node: NodeId, job: JobId) {
        // 持锁取消：放锁后再 cancel 会让尾部有机会先判定并回报真实判定，
        // 而此处已看到条目、不会再补报，节点就等不到确认
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

    /// 补一条确认
    ///
    /// hub 是本通道唯一的接收方，在 hub 循环里阻塞等待会自锁：满则交给独立任务等待空位
    fn report(&self, event: Event<D>) {
        if let Err(mpsc::error::TrySendError::Full(event)) = self.sink.try_send(event) {
            let sink = self.sink.clone();
            tokio::spawn(async move {
                let _ = sink.send(event).await;
            });
        }
    }

    /// 停止全部 worker
    ///
    /// `shutdown` 之后才启动的 worker 不受它影响
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
    use crate::agent::tree::domain::test_support::{
        TestDomain, TestOutput, TestProgress, TestSpec,
    };
    use crate::agent::tree::event::Event;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// 永不返回的 worker，用于验证宽限期回收
    ///
    /// `observed` 记录取消信号是否真的到过这里
    struct UnresponsiveBehavior {
        observed: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for UnresponsiveBehavior {
        async fn run(
            &self,
            ctx: Ctx<'_, TestDomain>,
            _input: Input<TestDomain>,
        ) -> Outcome<TestDomain> {
            // 记录取消信号真的到过这里
            ctx.cancel.cancelled().await;
            self.observed.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(30)).await;
            Outcome::Completed(TestOutput::Ok)
        }
    }

    struct PanickingBehavior;

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for PanickingBehavior {
        async fn run(
            &self,
            _ctx: Ctx<'_, TestDomain>,
            _input: Input<TestDomain>,
        ) -> Outcome<TestDomain> {
            panic!("boom")
        }
    }

    struct ImmediateBehavior;

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for ImmediateBehavior {
        async fn run(
            &self,
            _ctx: Ctx<'_, TestDomain>,
            _input: Input<TestDomain>,
        ) -> Outcome<TestDomain> {
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
        async fn run(
            &self,
            ctx: Ctx<'_, TestDomain>,
            _input: Input<TestDomain>,
        ) -> Outcome<TestDomain> {
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
        // node 与 job 必须取不同的值：相同则注册表三元组里两个 id 互换也测不出来
        jobs.launch(JobRequest {
            node: 3,
            job: 4,
            input: Input::Fresh(TestSpec::Noop),
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        jobs.stop(3, 4);

        let report = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("宽限期必须回收不响应取消的 worker")
            .expect("report");
        assert!(matches!(report, Event::Stopped { node: 3, job: 4 }));
        assert!(
            observed.load(Ordering::SeqCst),
            "worker 必须真的收到过取消信号，否则补报的 Stopped 会掩盖写错的注册表键"
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
            .expect("取消期间 worker 交还的判定不得顶替 Stopped")
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
            .expect("已结束的 job 也必须补一条确认")
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
            .expect("panic 不得让树卡死")
            .expect("report");
        assert!(
            matches!(report, Event::Aborted { node: 2, job: 7 }),
            "panic 是「没有交出判定就死了」，不得记成被停"
        );
    }

    #[tokio::test]
    async fn a_stop_for_an_unknown_job_is_confirmed() {
        let (tx, mut rx) = mpsc::channel(REPORT_CAPACITY);
        let jobs = Jobs::new(Arc::new(ImmediateBehavior), tx, Duration::from_millis(50));
        jobs.stop(99, 99);

        assert!(
            matches!(rx.try_recv(), Ok(Event::Stopped { node: 99, job: 99 })),
            "未知键也要补一条确认，归约器靠它把取消中的节点落终态"
        );
    }

    /// 一次 run 里连发大量进度，再来一个判定
    struct FloodBehavior;

    #[async_trait::async_trait]
    impl Behavior<TestDomain> for FloodBehavior {
        async fn run(
            &self,
            ctx: Ctx<'_, TestDomain>,
            _input: Input<TestDomain>,
        ) -> Outcome<TestDomain> {
            for _ in 0..64 {
                ctx.emit(TestProgress::Tick);
            }
            Outcome::Completed(TestOutput::Ok)
        }
    }

    #[tokio::test]
    async fn a_full_report_channel_drops_progress_but_never_the_verdict() {
        // 容量 1：进度必然溢出，判定必须阻塞等到空位
        let (tx, mut rx) = mpsc::channel(1);
        let jobs = Jobs::new(Arc::new(FloodBehavior), tx, Duration::from_millis(50));
        jobs.launch(JobRequest {
            node: 4,
            job: 9,
            input: Input::Fresh(TestSpec::Noop),
        });

        // 先不排空，让通道停在满的状态上
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut drained = Vec::new();
        while let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(500), rx.recv()).await
        {
            let is_verdict = matches!(event, Event::Completed { .. });
            drained.push(event);
            if is_verdict {
                break;
            }
        }
        assert!(
            matches!(
                drained.last(),
                Some(Event::Completed {
                    node: 4,
                    job: 9,
                    ..
                })
            ),
            "判定不能因为通道满而丢"
        );
    }
}
