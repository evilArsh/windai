use super::tree::{Domain, JobId, NodeId, Terminal};
use tokio_util::sync::CancellationToken;

/// 一次子任务结束后交给父节点的结果
#[derive(Debug, Clone)]
pub struct ChildResult<D: Domain> {
    pub node: NodeId,
    pub terminal: Terminal<D>,
}

/// worker 的输入
#[derive(Debug)]
pub enum Input<D: Domain> {
    /// 首次启动
    Fresh(D::Spec),
    /// 子任务全部结束后重入
    Children(Vec<ChildResult<D>>),
    /// 外部输入到达后重入
    External(D::Resume),
}

/// 一次派生请求
///
/// id 由领域提供
#[derive(Debug)]
pub struct SpawnRequest<D: Domain> {
    pub id: NodeId,
    pub spec: D::Spec,
    /// true = 不进入父节点的等待屏障
    pub detached: bool,
}

/// worker 跑完一段后的判定
#[derive(Debug)]
pub enum Outcome<D: Domain> {
    /// 本节点完成
    Completed(D::Output),
    /// 本节点失败
    Failed(D::Error),
    /// 等外部输入，worker 退出
    Suspend(D::Suspend),
    /// 派生一批子节点，父节点是否等待由每个请求的 detached 决定
    Spawn { spawn: Vec<SpawnRequest<D>> },
}

/// worker 的执行上下文
pub struct Ctx<'a, D: Domain> {
    pub node: NodeId,
    pub job: JobId,
    /// 本 job 的取消信号
    pub cancel: CancellationToken,
    emit: &'a (dyn Fn(D::Progress) + Send + Sync),
}

impl<'a, D: Domain> Ctx<'a, D> {
    pub fn new(
        node: NodeId,
        job: JobId,
        cancel: CancellationToken,
        emit: &'a (dyn Fn(D::Progress) + Send + Sync),
    ) -> Self {
        Self {
            node,
            job,
            cancel,
            emit,
        }
    }

    /// 下发一个进度增量
    ///
    /// 取消后静默拒绝；回报通道满时丢弃
    /// 
    /// 进度是尽力而为的观测，
    pub fn emit(&self, chunk: D::Progress) {
        if self.cancel.is_cancelled() {
            return;
        }
        (self.emit)(chunk);
    }
}

/// 节点行为的实现者
#[async_trait::async_trait]
pub trait Behavior<D: Domain>: Send + Sync + 'static {
    async fn run(&self, ctx: Ctx<'_, D>, input: Input<D>) -> Outcome<D>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tree::domain::test_support::{
        TestDomain, TestError, TestOutput, TestProgress, TestResume, TestSpec, TestSuspend,
    };
    use std::sync::{Arc, Mutex};

    #[test]
    fn outcome_carries_domain_payloads() {
        let outcome: Outcome<TestDomain> = Outcome::Completed(TestOutput::Ok);
        assert!(matches!(outcome, Outcome::Completed(TestOutput::Ok)));

        let child = ChildResult::<TestDomain> {
            node: 3,
            terminal: Terminal::Failed(TestError::Boom),
        };
        let input: Input<TestDomain> = Input::Children(vec![child]);
        match input {
            Input::Children(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].node, 3);
                assert!(matches!(
                    results[0].terminal,
                    Terminal::Failed(TestError::Boom)
                ));
            }
            _ => panic!("expected Children"),
        }

        assert!(matches!(
            Outcome::<TestDomain>::Suspend(TestSuspend::Ask),
            Outcome::Suspend(TestSuspend::Ask)
        ));

        let spawned: Outcome<TestDomain> = Outcome::Spawn {
            spawn: vec![
                SpawnRequest {
                    id: 11,
                    spec: TestSpec::Noop,
                    detached: false,
                },
                SpawnRequest {
                    id: 12,
                    spec: TestSpec::Noop,
                    detached: true,
                },
            ],
        };
        match spawned {
            Outcome::Spawn { spawn } => {
                assert_eq!(spawn.len(), 2);
                assert!(!spawn[0].detached);
                assert!(spawn[1].detached, "后台任务不进入屏障");
            }
            _ => panic!("expected Spawn"),
        }
        assert!(matches!(
            Input::<TestDomain>::External(TestResume::Answer),
            Input::External(TestResume::Answer)
        ));
        assert!(matches!(
            Input::<TestDomain>::Fresh(TestSpec::Noop),
            Input::Fresh(TestSpec::Noop)
        ));
    }

    #[test]
    fn ctx_refuses_to_emit_after_cancellation() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let emit = move |chunk| {
            if let Ok(mut seen) = sink.lock() {
                seen.push(chunk);
            }
        };
        let cancel = CancellationToken::new();
        let ctx = Ctx::<TestDomain>::new(1, 1, cancel.clone(), &emit);

        ctx.emit(TestProgress::Tick);
        cancel.cancel();
        ctx.emit(TestProgress::Tock);

        let seen = seen.lock().expect("lock");
        assert_eq!(
            seen.as_slice(),
            [TestProgress::Tick],
            "取消前的分片必须落地，取消后的必须被拒绝 —— 门控写反同样得 1 片"
        );
    }
}
