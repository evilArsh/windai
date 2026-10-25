use super::tree::{Domain, JobId, NodeId, Terminal};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct ChildResult<D: Domain> {
    pub node: NodeId,
    pub terminal: Terminal<D>,
}

/// 输入的任务
#[derive(Debug)]
pub enum Input<D: Domain> {
    Fresh(D::Spec),
    Children(Vec<ChildResult<D>>),
    External(D::Resume),
    /// 节点空闲时一次取走的全部待消费输入
    Idle(Vec<D::Spec>),
}

#[derive(Debug)]
pub struct SpawnRequest<D: Domain> {
    pub id: NodeId,
    pub spec: D::Spec,
    pub detached: bool,
}

#[derive(Debug)]
pub enum Outcome<D: Domain> {
    Completed(D::Output),
    Failed(D::Error),
    Suspend(D::Suspend),
    Spawn { spawn: Vec<SpawnRequest<D>> },
}

pub struct Ctx {
    pub node: NodeId,
    pub job: JobId,
    pub cancel: CancellationToken,
}

impl Ctx {
    pub fn new(node: NodeId, job: JobId, cancel: CancellationToken) -> Self {
        Self { node, job, cancel }
    }
}

#[async_trait::async_trait]
pub trait Behavior<D: Domain>: Send + Sync + 'static {
    async fn run(&self, ctx: Ctx, input: Input<D>) -> Outcome<D>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tree::domain::test_support::{
        TestDomain, TestError, TestOutput, TestResume, TestSpec, TestSuspend,
    };

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
                assert!(
                    spawn[1].detached,
                    "background tasks do not enter the barrier"
                );
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
}
